//! 完整 pipeline：ASR → LLM → 切句 → TTS（复用 crate::pipeline::llm_tts_items）。
//!
//! 与 /admin/asr_llm_tts 结构对称，差异仅在 wire 侧：
//!   - 下行推 VoicePayload（msgpack 信封）而非 HTTP SSE
//!   - 全程受 CancellationToken 约束（用户打断）
//!   - Tts chunk 直接带原始 PCM 字节（不做 base64）
//!
//! 主流程可以按下面的时序阅读：
//! `客户端 PCM → WAV 封装 → ASR 事件/最终文本 → 标签解析 → LLM delta → TTS PCM → VoicePayload`。
//! 每个阶段之间都有取消检查；取消只结束当前 pipeline，不向客户端伪造“成功完成”事件。

use std::sync::Arc;
use std::time::Instant;

use actix::prelude::Recipient;
use futures_util::StreamExt;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use voice_proto::{AgentPhase, VoicePayload};
use webhttp::websocket::OutMessage;

use crate::client::{asr::wrap_pcm_as_wav, AsrClient, LlmClient, TtsClient};
use crate::events::AsrEvent;
use crate::metrics::{PipelineMetricsGuard, PipelineResult, VoiceMetricsSink};
use crate::pipeline::{llm_tts_items, LlmTtsItem};
use crate::utils::postprocess_utils::{
    format_asr_hint, parse_asr_text, rich_transcription_postprocess,
};

const SAFE_PIPELINE_ERROR_MESSAGE: &str = "服务暂时不可用，请稍后再试。";

/// pipeline 退出时兜底清掉自己在 `current_real_cancel` 中的注册（M4 part B）。
///
/// 只有当"自己仍然是 current"时才清，避免覆盖更新的 pipeline（auto-interrupt 链）的注册。
/// 这样无论 pipeline 是正常完成、被 cancel、还是 panic 早 return，current_real_cancel
/// 都不会留下 stale token 误 cancel 后续 pipeline。
pub(super) struct CurrentCancelGuard {
    current: Arc<Mutex<Option<CancellationToken>>>,
    cancel: CancellationToken,
}

impl Drop for CurrentCancelGuard {
    fn drop(&mut self) {
        let mut g = self.current.lock();
        if g.as_ref().map(|c| c == &self.cancel).unwrap_or(false) {
            *g = None;
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_pipeline(
    session_id: String,
    _request_id: u64,
    message_id: String,
    audio: Vec<u8>,
    sample_rate: u32,
    channels: u16,
    // 端侧 SessionStart 上报的 TTS 音色短名。`None` 让 HttpTtsClient 走 `TtsConfig.voice` 兜底。
    client_voice: Option<String>,
    asr: Arc<dyn AsrClient>,
    llm: Arc<dyn LlmClient>,
    tts: Arc<dyn TtsClient>,
    down_addr: Recipient<OutMessage>,
    cancel: CancellationToken,
    current_real_cancel: Arc<Mutex<Option<CancellationToken>>>,
    metrics: Arc<dyn VoiceMetricsSink>,
    input_started_at: Instant,
    input_ended_at: Instant,
) {
    // 这是一次 utterance（用户一段语音）的完整生命周期。函数本身不返回业务数据，
    // 所有结果都通过 `down_addr` 发送 VoicePayload；因此每个 return 都代表该请求结束。
    // request_id 仅用于服务端内部取消和指标，不进入任何下行事件。
    info!(target: "voice_server.session", session_id = %session_id, "pipeline 开始");
    let mut metric_guard =
        PipelineMetricsGuard::start(metrics.clone(), input_started_at, input_ended_at);

    // ===== 阶段 1：ASR —— 先识别，再决定是否进入后续链路 =====
    // 先通知前端进入“正在理解”，后续 ASR partial 会在拿到最终文本后一次性 flush。
    // 这样可以避免空文本、标签文本或被取消的请求产生误导性的中间消息。
    if !send_agent_status(
        &down_addr,
        &session_id,
        &message_id,
        &cancel,
        AgentPhase::Transcribing,
        false,
    ) {
        metric_guard.set_result(PipelineResult::Cancelled);
        return;
    }

    // 浏览器 / WS pipeline 攒的是裸 PCM 字节；上游 siliconflow / OpenAI 兼容 ASR
    // 按 multipart 文件后缀选解码器，包成 RIFF/WAVE（44 字节头 + data chunk）才能解析
    let wav = wrap_pcm_as_wav(&audio, sample_rate, channels);
    debug!(
        target: "voice_server.session",
        session_id = %session_id,
        pcm_bytes = audio.len(),
        wav_bytes = wav.len(),
        sample_rate, channels,
        "包 WAV 头喂 ASR"
    );
    // recognize 建连（HTTP 请求）本身也进 select：否则建连期间无法被打断，
    // 只能等 HTTP 超时，Interrupt 形同虚设
    // 同时加 tokio::time::timeout：上游挂死/极慢响应 → future 被 drop，不会永久持有
    // Arc client + Mutex + HTTP 连接资源（M4 part A）
    let asr_timeout = std::time::Duration::from_secs(30);
    let asr_started_at = Instant::now();
    metrics.observe_queue(asr_started_at.duration_since(input_ended_at));
    let mut asr_stream = match tokio::select! {
        _ = cancel.cancelled() => {
            warn!(target: "voice_server.session", session_id = %session_id, "ASR 建连阶段被取消");
            metric_guard.set_result(PipelineResult::Cancelled);
            return;
        }
        r = tokio::time::timeout(asr_timeout, asr.recognize(&session_id, None, wav)) => {
            match r {
                Ok(inner) => inner,
                Err(_elapsed) => {
                    metric_guard.set_result(PipelineResult::Timeout);
                    error!(
                        target: "voice_server.session",
                        session_id = %session_id,
                        timeout_secs = asr_timeout.as_secs(),
                        "ASR recognize 超时"
                    );
                    if !send_agent_status(
                        &down_addr,
                        &session_id,
                        &message_id,
                        &cancel,
                        AgentPhase::Error,
                        true,
                    ) {
                        return;
                    }
                    send_down(&down_addr, VoicePayload::Error {
                        code: 1001,
                        message: SAFE_PIPELINE_ERROR_MESSAGE.to_string(),
                        message_id: Some(message_id.clone()),
                    });
                    return;
                }
            }
        }
    } {
        Ok(s) => s,
        Err(e) => {
            metric_guard.set_result(PipelineResult::Failed);
            error!(target: "voice_server.session", session_id = %session_id, "ASR 调用失败: {}", e);
            if !send_agent_status(
                &down_addr,
                &session_id,
                &message_id,
                &cancel,
                AgentPhase::Error,
                true,
            ) {
                return;
            }
            send_down(
                &down_addr,
                VoicePayload::Error {
                    code: 1001,
                    message: SAFE_PIPELINE_ERROR_MESSAGE.to_string(),
                    message_id: Some(message_id.clone()),
                },
            );
            return;
        }
    };

    // 缓冲 ASR 事件，循环结束后根据最终文本决定 flush 还是全部丢弃：
    // 空文本时一条 AsrPartial 都不推到前端，避免前端拿到一条空识别记录
    let mut prompt = String::new();
    let mut asr_events: Vec<AsrEvent> = Vec::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                warn!(target: "voice_server.session", session_id = %session_id, "ASR 阶段被取消");
                metric_guard.set_result(PipelineResult::Cancelled);
                return;
            }
            evt = asr_stream.next() => {
                match evt {
                    Some(Ok(e)) => {
                        // 先统一清洗 ASR 标签；非流式 ASR 客户端只发一个完整结果，
                        // 流式客户端则可能发送多条 partial。prompt 始终保存最近一条非空文本，
                        // 这样上游偶尔发出的空 partial 不会覆盖有效识别结果。
                        let cleaned = rich_transcription_postprocess(&e.text);
                        if !cleaned.is_empty() {
                            prompt = cleaned.clone();
                        }
                        asr_events.push(AsrEvent { text: cleaned, ..e });
                        if let Some(last) = asr_events.last() {
                            if last.is_final {
                                metric_guard.record_asr_output_end(asr_started_at, Instant::now());
                                break;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        metric_guard.set_result(PipelineResult::Failed);
                        error!(target: "voice_server.session", session_id = %session_id, "ASR 流错误: {}", e);
                        if !send_agent_status(
                            &down_addr,
                            &session_id,
                            &message_id,
                            &cancel,
                            AgentPhase::Error,
                            true,
                        ) {
                            return;
                        }
                        send_down(&down_addr, VoicePayload::Error {
                            code: 1001,
                            message: SAFE_PIPELINE_ERROR_MESSAGE.to_string(),
                            message_id: Some(message_id.clone()),
                        });
                        return;
                    }
                    None => {
                        warn!(target: "voice_server.session", session_id = %session_id, "ASR 流提前结束");
                        break;
                    }
                }
            }
        }
    }

    let prompt = prompt.trim().to_string();
    if prompt.is_empty() {
        // 空文本：不推任何 AsrPartial / AsrFinal、不注册为 current real、
        // 不打断任何正在跑的 pipeline、跳过整个 LLM/TTS 链路
        // （VAD 误切、上游返回 ""、纯噪声都属于这一类；省一次 LLM 调用 + 一次 TTS 调用）
        info!(
            target: "voice_server.session",
            session_id = %session_id,
            buffered_events = asr_events.len(),
            "ASR 最终文本为空，跳过 LLM/TTS（不推任何 ASR 事件、不打断其他 pipeline）"
        );
        metric_guard.set_result(PipelineResult::EmptyResponse);
        return;
    }

    // ===== 阶段 1.5：解析 ASR 控制标签 =====
    // ASR 文本已在接收阶段经过 rich 后处理；这里从 rich 输出的 emoji 中提取
    // 情绪/事件，剥掉 emoji 后的正文送 LLM。
    let parsed = parse_asr_text(&prompt);
    let asr_hint = format_asr_hint(parsed.emotion.as_deref(), &parsed.event);
    if asr_hint.is_some() {
        info!(
            target: "voice_server.session",
            session_id = %session_id,
            emotion = ?parsed.emotion,
            events = ?parsed.event,
            "ASR 文本含情绪/事件信号"
        );
    }
    let prompt = parsed.text;
    if prompt.is_empty() {
        // 剥掉标签后是空串（如 ASR 只返回了 "<|zh|><|SAD|>"），按空文本处理
        warn!(
            target: "voice_server.session",
            session_id = %session_id,
            "ASR 文本仅含标签，跳过 LLM/TTS"
        );
        metric_guard.set_result(PipelineResult::EmptyResponse);
        return;
    }
    info!(target: "voice_server.session", session_id = %session_id, asr_text = %prompt, "ASR final 完成，进入 LLM");

    // 非空：把缓冲的 ASR 事件按顺序推给前端 —— 用剥掉 `<|...|>` 标签后的纯文本，
    // 前端不需要关心 ASR 的内部控制 token
    for e in asr_events {
        if cancel.is_cancelled() {
            metric_guard.set_result(PipelineResult::Cancelled);
            return;
        }
        send_down(
            &down_addr,
            VoicePayload::AsrPartial {
                session_id: session_id.clone(),
                text: e.text,
                is_final: e.is_final,
                replace_last: false,
                message_id: message_id.clone(),
            },
        );
    }

    // ===== 阶段 1.8：注册当前请求并打断上一条真实语音请求 =====
    // 空文本的 pipeline 已经早 return，不会污染这条路径。
    // 只 cancel 上一个「已经进入 LLM/TTS」的 pipeline（prev_token），不动当前 pipeline 的 cancel。
    // CurrentCancelGuard 在 pipeline 退出时清掉自己的 token，避免 stale token 误 cancel 后续
    // （M4 part B：drop guard）
    let _guard = CurrentCancelGuard {
        current: current_real_cancel.clone(),
        cancel: cancel.clone(),
    };
    let prev = current_real_cancel.lock().replace(cancel.clone());
    if let Some(prev) = prev {
        info!(
            target: "voice_server.session",
            session_id = %session_id,
            "ASR 拿到文本，打断上一个 LLM/TTS pipeline"
        );
        prev.cancel();
    }

    // ASR 已确认有可处理文本，才进入“正在组织答案”。如果用户在这之前打断，
    // send_agent_status 会因 cancel 返回 false，后续不会再发任何下行事件。
    if !send_agent_status(
        &down_addr,
        &session_id,
        &message_id,
        &cancel,
        AgentPhase::Composing,
        false,
    ) {
        metric_guard.set_result(PipelineResult::Cancelled);
        return;
    }

    // 模型路由由单个 LlmAgent 内部完成；session pipeline 只消费统一的 LLM 流。
    // ===== 阶段 2+3：LLM → 切句 → TTS =====
    // 这里复用共享的 llm_tts_items：它负责网络流、切句、crossfade、base64 和 seq；
    // 本函数只负责把抽象事件映射成 WS 的 VoicePayload，并在每个 await 上响应取消。
    // voice_override：同理，原样透传 → HttpTtsClient 拼 model 前缀后发给 provider。
    let tts_format = tts.output_format();
    let llm_started_at = Instant::now();
    let mut items = Box::pin(llm_tts_items(
        prompt,
        asr_hint,
        session_id.clone(),
        llm,
        tts,
        client_voice,
    ));
    let mut speaking_sent = false;
    let mut llm_first_token_recorded = false;
    let mut llm_completed_recorded = false;
    // 消费共享管线事件。select! 同时监听 cancel 和 items.next()，保证 LLM/TTS 任一
    // 网络 await 期间都能被用户的新语音打断。
    while let Some(item) = {
        tokio::select! {
            _ = cancel.cancelled() => {
                warn!(target: "voice_server.session", session_id = %session_id, "pipeline 被取消");
                metric_guard.set_result(PipelineResult::Cancelled);
                return;
            }
            evt = items.next() => evt,
        }
    } {
        match item {
            LlmTtsItem::Llm { delta, is_final } => {
                // 空 delta 只在 is_final=true 时有意义：它是“文本流结束”的协议通知。
                if !delta.is_empty() || is_final {
                    if !delta.trim().is_empty() && !llm_first_token_recorded {
                        llm_first_token_recorded = true;
                        metric_guard.record_llm_first_text(llm_started_at, Instant::now());
                    }
                    if is_final && !llm_completed_recorded {
                        llm_completed_recorded = true;
                        metrics.observe_llm_complete(llm_started_at.elapsed());
                    }
                    if cancel.is_cancelled() {
                        metric_guard.set_result(PipelineResult::Cancelled);
                        return;
                    }
                    send_down(
                        &down_addr,
                        VoicePayload::LlmDelta {
                            session_id: session_id.clone(),
                            delta,
                            is_final,
                            message_id: message_id.clone(),
                        },
                    );
                }
            }
            LlmTtsItem::Tts {
                seq,
                audio,
                is_last,
            } => {
                let audio =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &audio)
                        .unwrap_or_default();
                let is_first_audio = !audio.is_empty() && !metric_guard.has_first_audio();
                if is_first_audio {
                    let first_audio_at = Instant::now();
                    metric_guard.record_tts_first_frame(first_audio_at);
                }
                // 第一块音频到达才切换到 Speaking，避免 TTS 尚未产出时提前显示“正在回答”。
                if !speaking_sent {
                    if !send_agent_status(
                        &down_addr,
                        &session_id,
                        &message_id,
                        &cancel,
                        AgentPhase::Speaking,
                        false,
                    ) {
                        metric_guard.set_result(PipelineResult::Cancelled);
                        return;
                    }
                    speaking_sent = true;
                }
                if !send_tts_audio(
                    &down_addr,
                    &session_id,
                    &message_id,
                    &cancel,
                    seq,
                    audio,
                    is_last,
                    tts_format.0,
                    Some(tts_format.1),
                ) {
                    metric_guard.set_result(PipelineResult::Cancelled);
                    return;
                }
                if is_first_audio {
                    metric_guard.record_first_audio_ws_sent(Instant::now());
                }
                if is_last
                    && !send_agent_status(
                        &down_addr,
                        &session_id,
                        &message_id,
                        &cancel,
                        AgentPhase::Speaking,
                        true,
                    )
                {
                    return;
                }
            }
            LlmTtsItem::Failed { error, code } => {
                // 共享管线已经把失败阶段映射为稳定错误码；WS 侧统一隐藏内部错误文本，
                // 只记录日志并返回面向用户的安全提示。
                error!(target: "voice_server.session", session_id = %session_id, code, %error, "pipeline 失败");
                metric_guard.set_result(PipelineResult::Failed);
                if !send_agent_status(
                    &down_addr,
                    &session_id,
                    &message_id,
                    &cancel,
                    AgentPhase::Error,
                    true,
                ) {
                    return;
                }
                send_down(
                    &down_addr,
                    VoicePayload::Error {
                        code: code as u32,
                        message: SAFE_PIPELINE_ERROR_MESSAGE.to_string(),
                        message_id: Some(message_id.clone()),
                    },
                );
                return;
            }
        }
    }
    metric_guard.finish(PipelineResult::Success, Instant::now());
    info!(target: "voice_server.session", session_id = %session_id, "pipeline 全部完成");
}

fn agent_label(phase: AgentPhase) -> &'static str {
    match phase {
        AgentPhase::Listening => "正在聆听",
        AgentPhase::Transcribing => "正在理解",
        AgentPhase::Searching => "正在查资料",
        AgentPhase::Composing => "正在组织答案",
        AgentPhase::Speaking => "正在回答",
        AgentPhase::Error => "暂时遇到问题",
    }
}

fn send_agent_status(
    addr: &Recipient<OutMessage>,
    session_id: &str,
    message_id: &str,
    cancel: &CancellationToken,
    phase: AgentPhase,
    done: bool,
) -> bool {
    if cancel.is_cancelled() {
        return false;
    }
    let label = agent_label(phase.clone()).to_string();
    send_down(
        addr,
        VoicePayload::AgentStatus {
            session_id: session_id.to_string(),
            phase,
            label,
            tool: None,
            message_id: message_id.to_string(),
            done,
        },
    );
    true
}

fn send_tts_audio(
    addr: &Recipient<OutMessage>,
    session_id: &str,
    message_id: &str,
    cancel: &CancellationToken,
    seq: u32,
    audio: Vec<u8>,
    is_last: bool,
    sample_rate: Option<u32>,
    channels: Option<u8>,
) -> bool {
    if cancel.is_cancelled() {
        return false;
    }
    send_down(
        addr,
        VoicePayload::TtsAudio {
            session_id: session_id.to_string(),
            seq,
            // admin_api 侧是 base64 字符串（SSE 需要），WS 侧还原为原始字节
            data: audio,
            is_last,
            sample_rate,
            channels,
            message_id: message_id.to_string(),
        },
    )
}

/// 下行推送：msgpack 编码后用 Recipient::try_send；弱网/客户端断连时吞错。
pub(super) fn send_down(addr: &Recipient<OutMessage>, p: VoicePayload) -> bool {
    match voice_proto::encode_indication(&p) {
        Ok(bytes) => {
            let kind = p.type_name();
            let session_id = p.session_id().map(str::to_owned);
            let bytes_len = bytes.len();
            // 注：Recipient::do_send 返回 ()，无法获知失败 —— 用 try_send 接住 SendError
            // （SendError 的 Display 只输出变体名 "receiver is full" / "receiver is gone"），
            // 弱网下能定位"客户端没收到 TTS"这类问题
            match addr.try_send(OutMessage { data: bytes }) {
                Ok(()) => {
                    info!(
                        target: "voice_server.session",
                        direction = "outbound",
                        session_id = ?session_id,
                        kind,
                        bytes = bytes_len,
                        "WS 发送消息"
                    );
                    true
                }
                Err(e) => {
                    warn!(
                        target: "voice_server.session",
                        direction = "outbound",
                        session_id = ?session_id,
                        kind,
                        bytes = bytes_len,
                        "WS 发送消息失败（客户端可能已断开）: {}", e
                    );
                    false
                }
            }
        }
        Err(e) => {
            warn!(
                target: "voice_server.session",
                direction = "outbound",
                session_id = ?p.session_id(),
                kind = p.type_name(),
                "WS 发送消息编码失败: {}", e
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use actix::{Actor, Context, Handler};
    use async_trait::async_trait;
    use futures_util::{stream, Stream};
    use tokio::sync::mpsc;
    use voice_proto::{decode_payload, AgentPhase};

    use crate::client::error::ClientError;
    use crate::client::llm::{BoxStream as LlmStream, ChatMessage};
    use crate::client::{AsrClient, LlmClient, TtsClient};
    use crate::events::{AsrEvent, LlmEvent, TtsEvent};
    use crate::metrics::VoiceMetrics;

    use super::*;

    struct MockAsr;

    #[async_trait]
    impl AsrClient for MockAsr {
        async fn recognize(
            &self,
            _session_id: &str,
            _filename: Option<&str>,
            _audio: Vec<u8>,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<AsrEvent, ClientError>> + Send>>, ClientError>
        {
            Ok(Box::pin(stream::iter([Ok(AsrEvent {
                text: "你好".to_string(),
                is_final: true,
                ..AsrEvent::default()
            })])))
        }
    }

    struct FailingAsr;

    #[async_trait]
    impl AsrClient for FailingAsr {
        async fn recognize(
            &self,
            _session_id: &str,
            _filename: Option<&str>,
            _audio: Vec<u8>,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<AsrEvent, ClientError>> + Send>>, ClientError>
        {
            Err(ClientError::Http(
                "provider-secret-detail https://provider.invalid".to_string(),
            ))
        }
    }

    struct MockLlm;

    impl MockLlm {
        fn response() -> LlmStream<Result<LlmEvent, ClientError>> {
            Box::pin(stream::iter([
                Ok(LlmEvent {
                    delta: " \n ".to_string(),
                    is_final: false,
                }),
                Ok(LlmEvent {
                    delta: "你好。".to_string(),
                    is_final: true,
                }),
            ]))
        }
    }

    #[async_trait]
    impl LlmClient for MockLlm {
        async fn chat(
            &self,
            _session_id: &str,
            _prompt: &str,
            _emotion_hint: Option<&str>,
        ) -> Result<LlmStream<Result<LlmEvent, ClientError>>, ClientError> {
            Ok(Self::response())
        }

        async fn chat_with_messages(
            &self,
            _session_id: &str,
            _messages: &[ChatMessage],
            _emotion_hint: Option<&str>,
        ) -> Result<LlmStream<Result<LlmEvent, ClientError>>, ClientError> {
            Ok(Self::response())
        }
    }

    struct MockTts;

    #[async_trait]
    impl TtsClient for MockTts {
        fn output_format(&self) -> (Option<u32>, u8) {
            (Some(24_000), 1)
        }

        async fn synthesize(
            &self,
            _session_id: &str,
            _text: &str,
            _voice_override: Option<String>,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<TtsEvent, ClientError>> + Send>>, ClientError>
        {
            Ok(Box::pin(stream::iter([Ok(TtsEvent {
                seq: 1,
                data: vec![1, 0, 2, 0],
                is_last: true,
            })])))
        }

        fn default_voice_short(&self) -> &str {
            "mock"
        }
    }

    struct CaptureActor {
        tx: mpsc::UnboundedSender<VoicePayload>,
    }

    impl Actor for CaptureActor {
        type Context = Context<Self>;
    }

    impl Handler<OutMessage> for CaptureActor {
        type Result = ();

        fn handle(&mut self, msg: OutMessage, _ctx: &mut Self::Context) {
            let (_, payload) = decode_payload(&msg.data).expect("downlink payload should decode");
            self.tx.send(payload).expect("test receiver should be open");
        }
    }

    #[test]
    fn agent_labels_are_fixed_user_safe_copy() {
        assert_eq!(agent_label(AgentPhase::Transcribing), "正在理解");
        assert_eq!(agent_label(AgentPhase::Composing), "正在组织答案");
        assert_eq!(agent_label(AgentPhase::Speaking), "正在回答");
        assert_eq!(agent_label(AgentPhase::Error), "暂时遇到问题");
    }

    #[actix::test]
    async fn normal_request_emits_safe_phases_with_one_message_id() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let down_addr = CaptureActor { tx }.start().recipient();
        let request_id = 42;
        let metrics = Arc::new(VoiceMetrics::new());

        run_pipeline(
            "session-1".to_string(),
            request_id,
            "message-1".to_string(),
            vec![0; 6400],
            16_000,
            1,
            None,
            Arc::new(MockAsr),
            Arc::new(MockLlm),
            Arc::new(MockTts),
            down_addr,
            CancellationToken::new(),
            Arc::new(Mutex::new(None)),
            metrics.clone(),
            Instant::now(),
            Instant::now(),
        )
        .await;

        let mut payloads = Vec::new();
        loop {
            let payload = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
                .await
                .expect("pipeline events should arrive")
                .expect("capture actor should remain available");
            let is_terminal_status = matches!(
                payload,
                VoicePayload::AgentStatus {
                    phase: AgentPhase::Speaking,
                    done: true,
                    ..
                }
            );
            payloads.push(payload);
            if is_terminal_status {
                break;
            }
        }

        let phases: Vec<&str> = payloads
            .iter()
            .filter_map(|payload| match payload {
                VoicePayload::AgentStatus { phase, .. } => Some(match phase {
                    AgentPhase::Listening => "listening",
                    AgentPhase::Transcribing => "transcribing",
                    AgentPhase::Searching => "searching",
                    AgentPhase::Composing => "composing",
                    AgentPhase::Speaking => "speaking",
                    AgentPhase::Error => "error",
                }),
                _ => None,
            })
            .collect();
        assert_eq!(
            phases,
            ["transcribing", "composing", "speaking", "speaking"]
        );

        assert!(payloads.iter().all(|payload| match payload {
            VoicePayload::AgentStatus { message_id, .. }
            | VoicePayload::AsrPartial { message_id, .. }
            | VoicePayload::LlmDelta { message_id, .. }
            | VoicePayload::TtsAudio { message_id, .. } => message_id == "message-1",
            _ => true,
        }));
        assert!(payloads.iter().all(|payload| match payload {
            VoicePayload::TtsAudio {
                sample_rate,
                channels,
                ..
            } => *sample_rate == Some(24_000) && *channels == Some(1),
            _ => true,
        }));

        let speaking_index = payloads
            .iter()
            .position(|payload| {
                matches!(
                    payload,
                    VoicePayload::AgentStatus {
                        phase: AgentPhase::Speaking,
                        ..
                    }
                )
            })
            .unwrap();
        let first_audio_index = payloads
            .iter()
            .position(|payload| matches!(payload, VoicePayload::TtsAudio { .. }))
            .unwrap();
        assert_eq!(speaking_index + 1, first_audio_index);

        let last_audio_index = payloads
            .iter()
            .rposition(|payload| matches!(payload, VoicePayload::TtsAudio { is_last: true, .. }))
            .unwrap();
        assert!(matches!(
            payloads.get(last_audio_index + 1),
            Some(VoicePayload::AgentStatus {
                phase: AgentPhase::Speaking,
                done: true,
                message_id,
                ..
            }) if message_id == "message-1"
        ));

        let metric_text = metrics.render();
        for name in [
            "voice_input_end_to_asr_output_end_seconds",
            "voice_input_end_to_llm_first_text_seconds",
            "voice_input_end_to_tts_first_frame_seconds",
            "voice_input_end_to_ws_first_audio_sent_seconds",
            "voice_asr_input_to_output_end_seconds",
            "voice_llm_input_to_first_text_seconds",
            "voice_llm_first_text_to_tts_first_frame_seconds",
            "voice_tts_first_frame_to_ws_first_audio_sent_seconds",
        ] {
            assert!(
                metric_text.contains(&format!("{name}_count 1")),
                "missing {name}"
            );
        }
        assert!(metric_text.contains("voice_requests_success_total 1"));
    }

    #[actix::test]
    async fn provider_error_details_are_not_sent_to_clients() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let down_addr = CaptureActor { tx }.start().recipient();

        run_pipeline(
            "session-1".to_string(),
            9,
            "message-9".to_string(),
            vec![0; 6400],
            16_000,
            1,
            None,
            Arc::new(FailingAsr),
            Arc::new(MockLlm),
            Arc::new(MockTts),
            down_addr,
            CancellationToken::new(),
            Arc::new(Mutex::new(None)),
            Arc::new(VoiceMetrics::new()),
            Instant::now(),
            Instant::now(),
        )
        .await;

        let mut payloads = Vec::new();
        while let Ok(Some(payload)) =
            tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await
        {
            payloads.push(payload);
        }

        let message = payloads
            .iter()
            .find_map(|payload| match payload {
                VoicePayload::Error {
                    code: 1001,
                    message,
                    ..
                } => Some(message.as_str()),
                _ => None,
            })
            .expect("a safe ASR error should be sent");
        assert_eq!(message, "服务暂时不可用，请稍后再试。");
        assert!(!message.contains("provider-secret-detail"));
        assert!(!message.contains("provider.invalid"));
    }

    #[actix::test]
    async fn cancellation_between_speaking_and_audio_suppresses_tts() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let down_addr = CaptureActor { tx }.start().recipient();
        let cancel = CancellationToken::new();

        assert!(send_agent_status(
            &down_addr,
            "session-1",
            "message-7",
            &cancel,
            AgentPhase::Speaking,
            false,
        ));
        cancel.cancel();
        assert!(!send_tts_audio(
            &down_addr,
            "session-1",
            "message-7",
            &cancel,
            1,
            vec![1_u8, 0, 2, 0],
            false,
            Some(24_000),
            Some(1),
        ));

        let status = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("speaking status should arrive")
            .expect("capture actor should remain available");
        assert!(matches!(status, VoicePayload::AgentStatus { .. }));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv())
                .await
                .is_err()
        );
    }
}
