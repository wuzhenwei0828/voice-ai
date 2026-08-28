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

use async_stream::stream;
use futures_util::{Stream, StreamExt};
use tracing::{debug, info, warn};

use crate::client::{ArcLlm, ArcTts};

use super::crossfade::SentenceCrossfader;
use super::sentence::next_sentence_end;

/// `llm_tts_items()` 的输出事件，由各消费方映射成自己的 wire 格式
/// （SSE data event / WS VoicePayload）。`Failed` 是终端事件（产出后流即结束）。
///
/// `pub`：session 的 WS pipeline 也要消费。
#[derive(Debug)]
pub enum LlmTtsItem {
    /// LLM 文本 delta（/admin/llm_tts 不透出，/admin/asr_llm_tts 与 WS 侧透出）
    Llm { delta: String, is_final: bool },
    /// TTS 音频 chunk（audio 为 base64）；最后一条 audio 为空、is_last=true，是结束标记
    Tts { seq: u32, audio: String, is_last: bool },
    /// 管线失败
    Failed { error: String, code: u16 },
}

/// 把 llm + sentence-split + tts 三个阶段串成一条事件流，
/// 供 /admin/llm_tts、/admin/asr_llm_tts 与 session.rs 的 WS pipeline 共用
/// （后两者额外透出 Llm 文本事件）。
/// 接受 Arc 而非 &Arc 是因为 actix-web 的 streaming() 要求 `Stream + 'static`，Arc 便宜 clone。
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
        // 阶段 1: 拉 LLM 流
        let mut llm_stream = match llm.chat(&sid, &prompt, emotion_hint.as_deref()).await {
            Ok(s) => s,
            Err(e) => {
                warn!(target: "voice_server.pipeline", stage = "llm_call", session_id = %sid, "LLM 调用失败: {}", e);
                yield LlmTtsItem::Failed { error: format!("llm error: {}", e), code: 1002 };
                return;
            }
        };

        let mut sentence_buf = String::new();
        let mut global_seq: u32 = 0;
        let mut fader = SentenceCrossfader::default();

        // 把一段 PCM 包成 Tts 事件（空则跳过）
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

        // 阶段 2: 流式消费 LLM delta，切句后调 TTS
        while let Some(item) = llm_stream.next().await {
            let evt = match item {
                Ok(e) => e,
                Err(e) => {
                    warn!(target: "voice_server.pipeline", stage = "llm_stream", session_id = %sid, "LLM 流错误: {}", e);
                    yield LlmTtsItem::Failed { error: format!("llm stream: {}", e), code: 1003 };
                    return;
                }
            };
            sentence_buf.push_str(&evt.delta);
            yield LlmTtsItem::Llm { delta: evt.delta.clone(), is_final: evt.is_final };

            // 切出所有完整句
            while let Some(end) = next_sentence_end(&sentence_buf) {
                let sent: String = sentence_buf[..end].to_string();
                sentence_buf = sentence_buf[end..].to_string();
                info!(target: "voice_server.pipeline", stage = "tts_call", session_id = %sid, sentence = %sent, "切出句子送 TTS");

                let mut tts_stream = match tts.synthesize(&sid, &sent, sample_rate_override, voice_override.clone()).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(target: "voice_server.pipeline", stage = "tts_call", session_id = %sid, "TTS 调用失败: {}", e);
                        yield LlmTtsItem::Failed { error: format!("tts error: {}", e), code: 1004 };
                        return;
                    }
                };

                // 阶段 3: 转发 TTS chunk（过 crossfade，重编号 seq）
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
                    emit_pcm!(fader.feed(&t.data));
                    if t.is_last {
                        // 单句 TTS 已经结束，准备下一句
                        break;
                    }
                }
                emit_pcm!(fader.end_sentence());
            }

            if evt.is_final {
                break;
            }
        }

        // 收尾：剩余 sentence_buf
        let tail = sentence_buf.trim().to_string();
        if !tail.is_empty() {
            info!(target: "voice_server.pipeline", stage = "tts_tail", session_id = %sid, sentence = %tail, "LLM 末尾残余句子送 TTS");
            if let Ok(mut tts_stream) = tts.synthesize(&sid, &tail, sample_rate_override, voice_override.clone()).await {
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
        // 最后一句扣留的句尾不再需要淡化，原样下发
        emit_pcm!(fader.finish());

        // 结束标记：上面各 chunk 下发时无法预知自己是不是最后一条，
        // 统一在流末尾补一条 audio 为空的 {is_last:true}，前端据此判定结束
        global_seq += 1;
        yield LlmTtsItem::Tts { seq: global_seq, audio: String::new(), is_last: true };

        debug!(target: "voice_server.pipeline", session_id = %sid, "LLM→TTS 管线全部完成");
    }
}
