//! LLM→TTS 流水线
//!
//! 流程：`llm.chat()` 流式 → 按句末标点切句 → 逐句 `tts.synthesize()` 流式
//! → 句间 [`SentenceCrossfader`] 混合 → 统一 seq 编号 → 结束标记（空 audio + is_last:true）
//!
//! 与 session::pipeline::run_pipeline 同源（WS pipeline 也用同一个函数）：
//! 两边走同一条 LLM→TTS 链路，只是 wire 侧不同（HTTP SSE / WS msgpack）。
//!
//! 错误流：阶段内出错时 yield 一条 [`LlmTtsItem::Failed`] 后立即 return（HTTP 200 已发，
//! 不能改 status，靠 error code 1001~1005 区分）。
//!
//! 阅读主流程时可以把它看成一个“边生成、边播放”的转换器：
//! 1. LLM 持续产出文本 delta；
//! 2. delta 先放入 `sentence_buf`，切出完整句后清洗并按需合并，再送 TTS；
//! 3. TTS 的 PCM chunk 经过句间 crossfade 后统一编号并向下游发送；
//! 4. LLM 结束后再冲刷残余文本，最后发送一个空音频结束标记。

use async_stream::stream;
use futures_util::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::client::{ArcLlm, ArcTts, ClientError, TtsInputSession};
use crate::events::{LlmEvent, TtsEvent};

use super::crossfade::SentenceCrossfader;
use super::sentence::next_sentence_end;
use super::text::{to_tts_text, IncrementalTtsCleaner, TtsSentenceBuffer};

/// `llm_tts_items()` 的输出事件，由各消费方映射成自己的 wire 格式
/// （SSE data event / WS VoicePayload）。`Failed` 是终端事件（产出后流即结束）。
///
/// `pub`：session 的 WS pipeline 也要消费。
#[derive(Debug)]
pub enum LlmTtsItem {
    /// LLM 文本 delta（/admin/llm_tts 不透出，/admin/asr_llm_tts 与 WS 侧透出）
    Llm { delta: String, is_final: bool },
    /// TTS 音频 chunk（audio 为 base64）；最后一条 audio 为空、is_last=true，是结束标记
    Tts {
        seq: u32,
        audio: String,
        is_last: bool,
    },
    /// 管线失败
    Failed { error: String, code: u16 },
}

// Large enough to absorb a normal complete response while one sentence is being
// synthesized, while still putting a hard bound on per-request memory use.
const LLM_EVENT_CHANNEL_CAPACITY: usize = 1024;

enum LlmReaderItem {
    Event(LlmEvent),
    CallFailed(ClientError),
    StreamFailed(ClientError),
}

/// Cancels the producer when the outer HTTP/WS response stream is dropped.
struct LlmReaderGuard(CancellationToken);

impl Drop for LlmReaderGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Drain the LLM HTTP stream independently from TTS work. Without this task,
/// a slow sentence synthesis pauses `bytes_stream()` and can make a healthy
/// SSE request exceed a client timeout.
fn spawn_llm_reader(
    llm: ArcLlm,
    sid: String,
    prompt: String,
    emotion_hint: Option<String>,
) -> (mpsc::Receiver<LlmReaderItem>, LlmReaderGuard) {
    let (tx, rx) = mpsc::channel(LLM_EVENT_CHANNEL_CAPACITY);
    let cancel = CancellationToken::new();
    let reader_cancel = cancel.clone();

    tokio::spawn(async move {
        let stream = tokio::select! {
            result = llm.chat(&sid, &prompt, emotion_hint.as_deref()) => result,
            () = reader_cancel.cancelled() => return,
        };
        let mut stream = match stream {
            Ok(stream) => stream,
            Err(error) => {
                let _ = tokio::select! {
                    () = reader_cancel.cancelled() => return,
                    result = tx.send(LlmReaderItem::CallFailed(error)) => result,
                };
                return;
            }
        };

        loop {
            let item = tokio::select! {
                () = reader_cancel.cancelled() => return,
                item = stream.next() => item,
            };
            let Some(item) = item else { return };

            let is_final = item.as_ref().map(|event| event.is_final).unwrap_or(true);
            let item = match item {
                Ok(event) => LlmReaderItem::Event(event),
                Err(error) => LlmReaderItem::StreamFailed(error),
            };
            let sent = tokio::select! {
                () = reader_cancel.cancelled() => return,
                result = tx.send(item) => result,
            };
            if sent.is_err() || is_final {
                return;
            }
        }
    });

    (rx, LlmReaderGuard(cancel))
}

async fn flush_incremental_sentence(
    session: &mut dyn TtsInputSession,
) -> Result<Vec<TtsEvent>, ClientError> {
    session.flush().await?;
    let mut events = Vec::new();
    loop {
        let event = session
            .next_event()
            .await
            .ok_or_else(|| ClientError::Ws("TTS stream closed".into()))??;
        let terminal = event.is_last;
        events.push(event);
        if terminal {
            return Ok(events);
        }
    }
}

/// 把 llm + sentence-split + tts 三个阶段串成一条事件流，
/// 供 /admin/llm_tts、/admin/asr_llm_tts 与 session.rs 的 WS pipeline 共用
/// （后两者额外透出 Llm 文本事件）。
/// 接受 Arc 而非 &Arc 是因为 actix-web 的 streaming() 要求 `Stream + 'static`，Arc 便宜 clone。
///
/// 返回的是惰性 Stream：调用本函数只会构造流水线，真正的网络请求和音频处理发生在
/// 消费方不断调用 `next().await` 时。事件顺序固定为 LLM 文本事件、若干 TTS 音频事件，
/// 最后是一个 `is_last=true` 的空音频事件；任一阶段失败则改为发送 `Failed` 并结束。
///
/// `emotion_hint`：从 ASR 文本里解析出的情绪与事件参考（参见
/// `crate::utils::postprocess_utils::parse_asr_text`），作为 system message 传给 LLM。
/// `None` 表示没有 ASR 参考信号（如 `/admin/llm_tts` 直接调用的场景）。
///
/// `sample_rate_override`：端侧 SessionStart 上报的 TTS 输出采样率。
///   - `Some(n)` —— 覆盖 `TtsConfig.sample_rate`
///   - `None` —— 走配置兜底
///   - /admin/llm_tts 与 /admin/asr_llm_tts 没有 SessionStart，传 `None`。
///
/// `voice_override`：端侧（前端的 voice 下拉）选中的音色短名（如 `"alex"`）。
///   - `Some("alex")` —— 覆盖 `TtsConfig.voice`
///   - `None` —— 走配置兜底
///   - /admin/llm_tts 与 /admin/asr_llm_tts 没有下拉时传 `None`。
pub fn llm_tts_items(
    prompt: String,
    emotion_hint: Option<String>,
    sid: String,
    llm: ArcLlm,
    tts: ArcTts,
    sample_rate_override: Option<u32>,
    voice_override: Option<String>,
) -> impl Stream<Item = LlmTtsItem> + 'static {
    stream! {
        // 阶段 1：后台持续读取 LLM SSE。TTS 可以慢，但不能因此暂停 HTTP body 的
        // 消费，否则一个健康的长回复会被误判为超时。
        let (mut llm_rx, _llm_reader_guard) = spawn_llm_reader(llm, sid.clone(), prompt, emotion_hint);

        // `sentence_buf` 保存尚未形成完整句子的尾巴。例如 LLM 先后返回“你好”和“，
        // 今天好吗？”，第一次 delta 不能合成，第二次到达后才能切出完整句子。
        let mut sentence_buf = String::new();
        // seq 必须跨句递增，不能每句从 1 开始，否则前端无法按顺序拼接音频。
        let mut global_seq: u32 = 0;
        // crossfader 暂存每句首尾少量 PCM，让相邻句子衔接自然；它只处理音频，不改变
        // 事件协议，因此外层消费者无需知道淡化细节。
        let mut fader = SentenceCrossfader::default();
        let mut short_sentence_buf = TtsSentenceBuffer::default();
        let mut ws_session = match tts
            .open_input_session(&sid, sample_rate_override, voice_override.clone())
            .await
        {
            Ok(session) => session,
            Err(e) => {
                warn!(target: "voice_server.pipeline", stage = "tts_connect", session_id = %sid, "TTS 增量会话建立失败: {}", e);
                yield LlmTtsItem::Failed { error: format!("tts error: {}", e), code: 1004 };
                return;
            }
        };

        // 把一段 PCM 转成统一的 Tts 事件。TTS 可能返回空 chunk（例如只携带状态的
        // chunk），这类数据没有播放意义，所以不占用 seq，也不向下游发送。
        macro_rules! emit_pcm {
            ($pcm:expr) => {{
                let pcm: Vec<u8> = $pcm;
                if !pcm.is_empty() {
                    global_seq += 1;
                    yield LlmTtsItem::Tts {
                        seq: global_seq,
                        audio: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &pcm),
                        is_last: false,
                    };
                }
            }};
        }

        // 阶段 2：逐个消费 LLM delta。每个 delta 先原样透出给需要显示文字的消费者，
        // 同时追加到缓冲区；一个 delta 里也可能包含多个句子，因此用 while 一次性切完。
        while let Some(item) = llm_rx.recv().await {
            let evt = match item {
                LlmReaderItem::Event(event) => event,
                LlmReaderItem::CallFailed(e) => {
                    if let Some(session) = ws_session.as_mut() {
                        let _ = session.close().await;
                    }
                    warn!(target: "voice_server.pipeline", stage = "llm_call", session_id = %sid, "LLM 调用失败: {}", e);
                    yield LlmTtsItem::Failed { error: format!("llm error: {}", e), code: 1002 };
                    return;
                }
                LlmReaderItem::StreamFailed(e) => {
                    if let Some(session) = ws_session.as_mut() {
                        let _ = session.close().await;
                    }
                    warn!(target: "voice_server.pipeline", stage = "llm_stream", session_id = %sid, "LLM 流错误: {}", e);
                    yield LlmTtsItem::Failed { error: format!("llm stream: {}", e), code: 1003 };
                    return;
                }
            };
            yield LlmTtsItem::Llm { delta: evt.delta.clone(), is_final: evt.is_final };

            if let Some(session) = ws_session.as_mut() {
                let cleaned = IncrementalTtsCleaner::clean(&evt.delta);
                debug!(target: "voice_server.pipeline", session_id = %sid, original_chars = evt.delta.chars().count(), filtered_chars = cleaned.chars().count(), text_preview = %cleaned.chars().take(200).collect::<String>().replace('\n', "\\n"), "增量文本清洗");
                if !cleaned.is_empty()
                    && (!cleaned.trim().is_empty() || !sentence_buf.trim().is_empty())
                {
                    let mut remaining = cleaned;
                    while !remaining.is_empty() {
                        let combined = format!("{sentence_buf}{remaining}");
                        if let Some(end) = next_sentence_end(&combined) {
                            let already_sent = sentence_buf.len();
                            let segment = combined[already_sent..end].to_string();
                            if !segment.is_empty() {
                                if let Err(e) = session.send_text(&segment).await {
                                    let _ = session.close().await;
                                    warn!(target: "voice_server.pipeline", stage = "tts_send_text", session_id = %sid, "TTS 增量文本发送失败: {}", e);
                                    yield LlmTtsItem::Failed { error: format!("tts error: {}", e), code: 1004 };
                                    return;
                                }
                            }
                            remaining = combined[end..].to_string();
                            sentence_buf.clear();

                            let events = match flush_incremental_sentence(session.as_mut()).await {
                                Ok(events) => events,
                                Err(e) => {
                                    let _ = session.close().await;
                                    warn!(target: "voice_server.pipeline", stage = "tts_flush", session_id = %sid, "TTS 增量 flush 失败: {}", e);
                                    yield LlmTtsItem::Failed { error: format!("tts error: {}", e), code: 1004 };
                                    return;
                                }
                            };
                            fader.begin_sentence();
                            for t in events {
                                emit_pcm!(fader.feed(&t.data));
                            }
                            emit_pcm!(fader.end_sentence());
                        } else {
                            if let Err(e) = session.send_text(&remaining).await {
                                let _ = session.close().await;
                                warn!(target: "voice_server.pipeline", stage = "tts_send_text", session_id = %sid, "TTS 增量文本发送失败: {}", e);
                                yield LlmTtsItem::Failed { error: format!("tts error: {}", e), code: 1004 };
                                return;
                            }
                            sentence_buf.push_str(&remaining);
                            remaining.clear();
                        }
                    }
                }

                if evt.is_final { break; }
                continue;
            }

            sentence_buf.push_str(&evt.delta);

            // 切出所有完整句。`next_sentence_end` 只返回安全的 UTF-8 字符边界，
            // 取出的 `sent` 立即从缓冲区移除，避免下一轮重复合成。
            while let Some(end) = next_sentence_end(&sentence_buf) {
                let raw_sent: String = sentence_buf[..end].to_string();
                sentence_buf = sentence_buf[end..].to_string();
                let sent = to_tts_text(&raw_sent);
                let Some(sent) = short_sentence_buf.push(&sent) else {
                    continue;
                };
                info!(target: "voice_server.pipeline", stage = "tts_call", session_id = %sid, sentence = %sent, "切出句子送 TTS");

                let mut tts_stream = match tts.synthesize(&sid, &sent, sample_rate_override, voice_override.clone()).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(target: "voice_server.pipeline", stage = "tts_call", session_id = %sid, "TTS 调用失败: {}", e);
                        yield LlmTtsItem::Failed { error: format!("tts error: {}", e), code: 1004 };
                        return;
                    }
                };

                // 阶段 3：为当前句建立 TTS 流。句子级调用可以让前端尽早收到第一句
                // 音频；`sample_rate_override` 和 `voice_override` 只影响本句的 provider 请求。
                fader.begin_sentence();
                while let Some(tts_item) = tts_stream.next().await {
                    let t = match tts_item {
                        Ok(t) => t,
                        Err(e) => {
                            warn!(target: "voice_server.pipeline", stage = "tts_stream", session_id = %sid, "TTS 流错误: {}", e);
                            yield LlmTtsItem::Failed { error: format!("tts stream: {}", e), code: 1005 };
                            return;
                        }
                    };
                    // 每个 chunk 先喂给 crossfader，再由宏完成 seq 编号和 base64 编码。
                    emit_pcm!(fader.feed(&t.data));
                    if t.is_last {
                        // provider 用 is_last 表示当前句结束；外层协议的全局结束标记
                        // 要等所有句子都处理完后才能发送，因此这里仅跳出当前句循环。
                        break;
                    }
                }
                // 刷出当前句被 fader 暂存的尾部 PCM，为下一句的 crossfade 做准备。
                emit_pcm!(fader.end_sentence());
            }

            if evt.is_final {
                // LLM 已明确结束。缓冲区中可能仍有不带标点的尾巴，统一在主循环后冲刷。
                break;
            }
        }

        // 收尾：LLM 结束时仍可能遗留没有句末标点的文本，例如“谢谢你的提问”。
        // 不能丢弃这段内容，因此在末尾 flush；失败时关闭增量会话并返回稳定错误。
        let tail = if ws_session.is_some() {
            sentence_buf.trim().to_string()
        } else {
            to_tts_text(sentence_buf.trim())
        };
        if let Some(tail) = if ws_session.is_some() {
            (!tail.is_empty()).then_some(tail.clone())
        } else {
            short_sentence_buf.push(&tail).or_else(|| short_sentence_buf.flush())
        } {
            info!(target: "voice_server.pipeline", stage = "tts_tail", session_id = %sid, sentence = %tail, "LLM 末尾残余句子送 TTS");
            if let Some(session) = ws_session.as_mut() {
                match flush_incremental_sentence(session.as_mut()).await {
                    Ok(events) => {
                        fader.begin_sentence();
                        for t in events {
                            emit_pcm!(fader.feed(&t.data));
                        }
                        emit_pcm!(fader.end_sentence());
                    }
                    Err(e) => {
                        let _ = session.close().await;
                        warn!(target: "voice_server.pipeline", stage = "tts_tail", session_id = %sid, "TTS 增量尾句失败: {}", e);
                        yield LlmTtsItem::Failed { error: "tts stream failed".into(), code: 1005 };
                        return;
                    }
                }
            } else if let Ok(mut tts_stream) = tts.synthesize(&sid, &tail, sample_rate_override, voice_override.clone()).await {
                fader.begin_sentence();
                while let Some(tts_item) = tts_stream.next().await {
                    if let Ok(t) = tts_item {
                        emit_pcm!(fader.feed(&t.data));
                        if t.is_last { break; }
                    } else {
                        break;
                    }
                }
                emit_pcm!(fader.end_sentence());
            }
        }
        if let Some(session) = ws_session.as_mut() {
            if let Err(e) = session.finish().await {
                warn!(target: "voice_server.pipeline", stage = "tts_finish", session_id = %sid, "TTS 增量会话归还连接失败: {}", e);
            }
        }
        // 最后一句扣留的句尾不再需要淡化，原样下发；finish 之后 fader 不再产生数据。
        emit_pcm!(fader.finish());

        // 结束标记：上面各 chunk 下发时无法预知自己是不是最后一条，
        // 统一在流末尾补一条 audio 为空的 {is_last:true}，前端据此判定结束
        global_seq += 1;
        yield LlmTtsItem::Tts { seq: global_seq, audio: String::new(), is_last: true };

        debug!(target: "voice_server.pipeline", session_id = %sid, "LLM→TTS 管线全部完成");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures_util::stream;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;
    use tokio::time::{timeout, Duration};

    use crate::client::llm::{BoxStream as LlmStream, ChatMessage, LlmClient};
    use crate::client::tts::{BoxStream as TtsStream, TtsClient, TtsInputSession};
    use crate::events::TtsEvent;

    struct BurstLlm {
        emitted: Arc<AtomicUsize>,
        event_count: usize,
    }

    #[async_trait]
    impl LlmClient for BurstLlm {
        async fn chat(
            &self,
            _session_id: &str,
            _prompt: &str,
            _emotion_hint: Option<&str>,
        ) -> Result<LlmStream<Result<LlmEvent, ClientError>>, ClientError> {
            let emitted = Arc::clone(&self.emitted);
            let event_count = self.event_count;
            Ok(Box::pin(stream::iter((0..event_count).map(move |index| {
                emitted.fetch_add(1, Ordering::SeqCst);
                Ok(LlmEvent {
                    delta: if index == 0 {
                        "第一句内容。"
                    } else {
                        "尾"
                    }
                    .to_string(),
                    is_final: index + 1 == event_count,
                })
            }))))
        }

        async fn chat_with_messages(
            &self,
            session_id: &str,
            _messages: &[ChatMessage],
            _emotion_hint: Option<&str>,
        ) -> Result<LlmStream<Result<LlmEvent, ClientError>>, ClientError> {
            self.chat(session_id, "", None).await
        }
    }

    struct DelayedTts {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    struct StaticLlm {
        response: String,
    }

    struct ChunkedLlm {
        chunks: Vec<String>,
    }

    #[async_trait]
    impl LlmClient for ChunkedLlm {
        async fn chat(
            &self,
            _session_id: &str,
            _prompt: &str,
            _emotion_hint: Option<&str>,
        ) -> Result<LlmStream<Result<LlmEvent, ClientError>>, ClientError> {
            let chunks = self.chunks.clone();
            let count = chunks.len();
            Ok(Box::pin(stream::iter(chunks.into_iter().enumerate().map(
                move |(index, delta)| {
                    Ok(LlmEvent {
                        delta,
                        is_final: index + 1 == count,
                    })
                },
            ))))
        }

        async fn chat_with_messages(
            &self,
            session_id: &str,
            _messages: &[ChatMessage],
            _emotion_hint: Option<&str>,
        ) -> Result<LlmStream<Result<LlmEvent, ClientError>>, ClientError> {
            self.chat(session_id, "", None).await
        }
    }

    #[async_trait]
    impl LlmClient for StaticLlm {
        async fn chat(
            &self,
            _session_id: &str,
            _prompt: &str,
            _emotion_hint: Option<&str>,
        ) -> Result<LlmStream<Result<LlmEvent, ClientError>>, ClientError> {
            Ok(Box::pin(stream::iter(vec![Ok(LlmEvent {
                delta: self.response.clone(),
                is_final: true,
            })])))
        }

        async fn chat_with_messages(
            &self,
            session_id: &str,
            _messages: &[ChatMessage],
            _emotion_hint: Option<&str>,
        ) -> Result<LlmStream<Result<LlmEvent, ClientError>>, ClientError> {
            self.chat(session_id, "", None).await
        }
    }

    struct RecordingTts {
        requests: Arc<Mutex<Vec<String>>>,
    }

    struct RecordingIncrementalSession {
        messages: Arc<Mutex<Vec<String>>>,
        flush_count: Arc<AtomicUsize>,
        finish_count: Arc<AtomicUsize>,
        awaiting_done: bool,
    }

    #[async_trait]
    impl TtsInputSession for RecordingIncrementalSession {
        async fn send_text(&mut self, text: &str) -> Result<(), ClientError> {
            assert!(
                !self.awaiting_done,
                "next sentence was sent before session.done"
            );
            self.messages.lock().unwrap().push(format!("text:{text}"));
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), ClientError> {
            assert!(!self.awaiting_done);
            self.awaiting_done = true;
            self.flush_count.fetch_add(1, Ordering::SeqCst);
            self.messages.lock().unwrap().push("input.done".into());
            Ok(())
        }

        async fn next_event(&mut self) -> Option<Result<TtsEvent, ClientError>> {
            if !self.awaiting_done {
                return None;
            }
            self.awaiting_done = false;
            Some(Ok(TtsEvent {
                seq: 1,
                data: vec![1, 0, 2, 0],
                is_last: true,
            }))
        }

        async fn close(&mut self) -> Result<(), ClientError> {
            Ok(())
        }

        async fn finish(&mut self) -> Result<(), ClientError> {
            self.finish_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct RecordingIncrementalTts {
        messages: Arc<Mutex<Vec<String>>>,
        flush_count: Arc<AtomicUsize>,
        finish_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TtsClient for RecordingIncrementalTts {
        async fn open_input_session(
            &self,
            _session_id: &str,
            _sample_rate_override: Option<u32>,
            _voice_override: Option<String>,
        ) -> Result<Option<Box<dyn TtsInputSession>>, ClientError> {
            Ok(Some(Box::new(RecordingIncrementalSession {
                messages: Arc::clone(&self.messages),
                flush_count: Arc::clone(&self.flush_count),
                finish_count: Arc::clone(&self.finish_count),
                awaiting_done: false,
            })))
        }

        async fn synthesize(
            &self,
            _session_id: &str,
            _text: &str,
            _sample_rate_override: Option<u32>,
            _voice_override: Option<String>,
        ) -> Result<TtsStream<Result<TtsEvent, ClientError>>, ClientError> {
            panic!("incremental path should not call synthesize")
        }

        fn default_voice_short(&self) -> &str {
            "test"
        }
    }

    #[async_trait]
    impl TtsClient for RecordingTts {
        async fn synthesize(
            &self,
            _session_id: &str,
            text: &str,
            _sample_rate_override: Option<u32>,
            _voice_override: Option<String>,
        ) -> Result<TtsStream<Result<TtsEvent, ClientError>>, ClientError> {
            self.requests.lock().unwrap().push(text.to_string());
            Ok(Box::pin(stream::iter(vec![Ok(TtsEvent {
                seq: 1,
                data: vec![1, 0, 2, 0],
                is_last: true,
            })])))
        }

        fn default_voice_short(&self) -> &str {
            "test"
        }
    }

    async fn collect_tts_requests(response: &str) -> Vec<String> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut items = Box::pin(llm_tts_items(
            "prompt".into(),
            None,
            "test-session".into(),
            Arc::new(StaticLlm {
                response: response.to_string(),
            }),
            Arc::new(RecordingTts {
                requests: Arc::clone(&requests),
            }),
            None,
            None,
        ));
        while items.next().await.is_some() {}
        Arc::try_unwrap(requests).unwrap().into_inner().unwrap()
    }

    #[tokio::test]
    async fn cleans_markdown_before_calling_tts() {
        let requests = collect_tts_requests("## **你好世界。**").await;
        assert_eq!(requests, vec!["你好世界。"]);
    }

    #[tokio::test]
    async fn merges_short_sentence_with_the_next_tts_request() {
        let requests = collect_tts_requests("好的。我马上处理。").await;
        assert_eq!(requests, vec!["好的。我马上处理。"]);
    }

    #[tokio::test]
    async fn incremental_waits_for_session_done_before_next_sentence() {
        let messages = Arc::new(Mutex::new(Vec::new()));
        let flush_count = Arc::new(AtomicUsize::new(0));
        let finish_count = Arc::new(AtomicUsize::new(0));
        let mut items = Box::pin(llm_tts_items(
            "prompt".into(),
            None,
            "test-session".into(),
            Arc::new(StaticLlm {
                response: "第一句。第二句。".into(),
            }),
            Arc::new(RecordingIncrementalTts {
                messages: Arc::clone(&messages),
                flush_count: Arc::clone(&flush_count),
                finish_count: Arc::clone(&finish_count),
            }),
            None,
            None,
        ));
        while items.next().await.is_some() {}

        assert_eq!(
            *messages.lock().unwrap(),
            vec![
                "text:第一句。".to_string(),
                "input.done".to_string(),
                "text:第二句。".to_string(),
                "input.done".to_string(),
            ]
        );
        assert_eq!(flush_count.load(Ordering::SeqCst), 2);
        assert_eq!(finish_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn incremental_sends_each_delta_without_sentence_buffering() {
        let messages = Arc::new(Mutex::new(Vec::new()));
        let flush_count = Arc::new(AtomicUsize::new(0));
        let finish_count = Arc::new(AtomicUsize::new(0));
        let mut items = Box::pin(llm_tts_items(
            "prompt".into(),
            None,
            "test-session".into(),
            Arc::new(ChunkedLlm {
                chunks: vec!["第一".into(), "句".into(), "。".into()],
            }),
            Arc::new(RecordingIncrementalTts {
                messages: Arc::clone(&messages),
                flush_count: Arc::clone(&flush_count),
                finish_count: Arc::clone(&finish_count),
            }),
            None,
            None,
        ));
        while items.next().await.is_some() {}

        assert_eq!(
            *messages.lock().unwrap(),
            vec![
                "text:第一".to_string(),
                "text:句".to_string(),
                "text:。".to_string(),
                "input.done".to_string(),
            ]
        );
        assert_eq!(flush_count.load(Ordering::SeqCst), 1);
        assert_eq!(finish_count.load(Ordering::SeqCst), 1);
    }

    #[async_trait]
    impl TtsClient for DelayedTts {
        async fn synthesize(
            &self,
            _session_id: &str,
            _text: &str,
            _sample_rate_override: Option<u32>,
            _voice_override: Option<String>,
        ) -> Result<TtsStream<Result<TtsEvent, ClientError>>, ClientError> {
            self.started.notify_one();
            let release = Arc::clone(&self.release);
            Ok(Box::pin(stream! {
                release.notified().await;
                yield Ok(TtsEvent { seq: 1, data: vec![1, 0, 2, 0], is_last: true });
            }))
        }

        fn default_voice_short(&self) -> &str {
            "test"
        }
    }

    #[tokio::test]
    async fn llm_reader_keeps_draining_while_tts_is_slow() {
        let emitted = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let event_count = 128;
        let mut items = Box::pin(llm_tts_items(
            "prompt".into(),
            None,
            "test-session".into(),
            Arc::new(BurstLlm {
                emitted: Arc::clone(&emitted),
                event_count,
            }),
            Arc::new(DelayedTts {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
            }),
            None,
            None,
        ));

        assert!(matches!(items.next().await, Some(LlmTtsItem::Llm { .. })));
        let next_item = tokio::spawn(async move { items.next().await });
        timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("TTS should start for the first complete sentence");

        timeout(Duration::from_secs(1), async {
            while emitted.load(Ordering::SeqCst) != event_count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("LLM reader should drain the response while TTS is waiting");
        assert!(!next_item.is_finished());

        release.notify_one();
        assert!(next_item.await.unwrap().is_some());
    }
}
