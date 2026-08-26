//! 完整 pipeline：ASR → LLM → 切句 → TTS（复用 crate::pipeline::llm_tts_items）。
//!
//! 与 /admin/asr_llm_tts 结构对称，差异仅在 wire 侧：
//!   - 下行推 VoicePayload（msgpack 信封）而非 NDJSON
//!   - 全程受 CancellationToken 约束（用户打断）
//!   - Tts chunk 直接带原始 PCM 字节（不做 base64）

use std::sync::Arc;

use actix::prelude::Recipient;
use futures_util::StreamExt;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use voice_proto::VoicePayload;
use webhttp::websocket::OutMessage;

use crate::client::{asr::wrap_pcm_as_wav, AsrClient, LlmClient, TtsClient};
use crate::events::AsrEvent;
use crate::pipeline::{llm_tts_items, LlmTtsItem};

use super::text::parse_asr_emotion_tags;

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
    audio: Vec<u8>,
    sample_rate: u32,
    channels: u16,
    // 端侧 SessionStart 上报的 TTS 输出采样率。`None` 让 HttpTtsClient 走 `TtsConfig.sample_rate` 兜底。
    client_tts_sample_rate: Option<u32>,
    // 端侧 SessionStart 上报的 TTS 音色短名。`None` 让 HttpTtsClient 走 `TtsConfig.voice` 兜底。
    client_voice: Option<String>,
    asr: Arc<dyn AsrClient>,
    llm: Arc<dyn LlmClient>,
    tts: Arc<dyn TtsClient>,
    down_addr: Recipient<OutMessage>,
    cancel: CancellationToken,
    current_real_cancel: Arc<Mutex<Option<CancellationToken>>>,
) {
    info!(target: "voice_server.session", session_id = %session_id, "pipeline 开始");

    // ===== 阶段 1: ASR —— 转发识别事件，收最终文本作为 LLM prompt =====
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
    let mut asr_stream = match tokio::select! {
        _ = cancel.cancelled() => {
            warn!(target: "voice_server.session", session_id = %session_id, "ASR 建连阶段被取消");
            return;
        }
        r = tokio::time::timeout(asr_timeout, asr.recognize(&session_id, None, wav)) => {
            match r {
                Ok(inner) => inner,
                Err(_elapsed) => {
                    error!(
                        target: "voice_server.session",
                        session_id = %session_id,
                        timeout_secs = asr_timeout.as_secs(),
                        "ASR recognize 超时"
                    );
                    send_down(&down_addr, VoicePayload::Error {
                        code: 1001,
                        message: format!("asr timeout after {}s", asr_timeout.as_secs()),
                    });
                    return;
                }
            }
        }
    } {
        Ok(s) => s,
        Err(e) => {
            error!(target: "voice_server.session", session_id = %session_id, "ASR 调用失败: {}", e);
            send_down(&down_addr, VoicePayload::Error {
                code: 1001,
                message: format!("asr error: {}", e),
            });
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
                return;
            }
            evt = asr_stream.next() => {
                match evt {
                    Some(Ok(e)) => {
                        // 取最新非空识别文本；非流式 ASR 客户端只发一个 is_final=true 的完整结果
                        if !e.text.is_empty() {
                            prompt = e.text.clone();
                        }
                        asr_events.push(e);
                        if let Some(last) = asr_events.last() {
                            if last.is_final { break; }
                        }
                    }
                    Some(Err(e)) => {
                        error!(target: "voice_server.session", session_id = %session_id, "ASR 流错误: {}", e);
                        send_down(&down_addr, VoicePayload::Error {
                            code: 1001,
                            message: format!("asr stream: {}", e),
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
        warn!(
            target: "voice_server.session",
            session_id = %session_id,
            buffered_events = asr_events.len(),
            "ASR 最终文本为空，跳过 LLM/TTS（不推任何 ASR 事件、不打断其他 pipeline）"
        );
        return;
    }

    // 解析情绪标签：Qwen 等 ASR 会在末尾附加 <|zh|><|SAD|><|Speech|><|woitn|> 这种 token，
    // 第二个 token 是说话人情绪，剥掉标签后纯文本送 LLM，情绪作为参考放进 system prompt。
    let parsed = parse_asr_emotion_tags(&prompt);
    if parsed.emotion.is_some() {
        info!(
            target: "voice_server.session",
            session_id = %session_id,
            emotion = ?parsed.emotion,
            "ASR 文本含情绪标签"
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
        return;
    }
    info!(target: "voice_server.session", session_id = %session_id, asr_text = %prompt, "ASR final 完成，进入 LLM");

    // 非空：把缓冲的 ASR 事件按顺序推给前端 —— 用剥掉 `<|...|>` 标签后的纯文本，
    // 前端不需要关心 ASR 的内部控制 token
    for e in asr_events {
        let cleaned = parse_asr_emotion_tags(&e.text).text;
        send_down(&down_addr, VoicePayload::AsrPartial {
            session_id: session_id.clone(),
            text: cleaned,
            is_final: e.is_final,
            replace_last: false,
        });
    }

    // ===== 注册为 current real 并 cancel 上一个 real pipeline（auto-interrupt on new utterance）=====
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

    // ===== 阶段 2+3: LLM → 切句 → TTS（共享管线，含句间 crossfade / 全局 seq / 结束标记）=====
    // sample_rate_override：把端侧 SessionStart 上报的值原样透传 —— HttpTtsClient 内部决定
    // 用 override 还是配置兜底（sample_rate_override.or(self.sample_rate)）。
    // voice_override：同理，原样透传 → HttpTtsClient 拼 model 前缀后发给 provider。
    let mut items = Box::pin(llm_tts_items(
        prompt,
        parsed.emotion,
        session_id.clone(),
        llm,
        tts,
        client_tts_sample_rate,
        client_voice,
    ));
    while let Some(item) = {
        tokio::select! {
            _ = cancel.cancelled() => {
                warn!(target: "voice_server.session", session_id = %session_id, "pipeline 被取消");
                return;
            }
            evt = items.next() => evt,
        }
    } {
        match item {
            LlmTtsItem::Llm { delta, is_final } => {
                if !delta.is_empty() || is_final {
                    send_down(&down_addr, VoicePayload::LlmDelta {
                        session_id: session_id.clone(),
                        delta,
                        is_final,
                    });
                }
            }
            LlmTtsItem::Tts { seq, audio, is_last } => {
                send_down(&down_addr, VoicePayload::TtsAudio {
                    session_id: session_id.clone(),
                    seq,
                    // admin_api 侧是 base64 字符串（NDJSON 需要），WS 侧还原为原始字节
                    data: base64::Engine::decode(
                        &base64::engine::general_purpose::STANDARD,
                        &audio,
                    )
                    .unwrap_or_default(),
                    is_last,
                });
            }
            LlmTtsItem::Failed { error, code } => {
                error!(target: "voice_server.session", session_id = %session_id, code, %error, "pipeline 失败");
                send_down(&down_addr, VoicePayload::Error {
                    code: code as u32,
                    message: error,
                });
                return;
            }
        }
    }
    info!(target: "voice_server.session", session_id = %session_id, "pipeline 全部完成");
}

/// 下行推送：msgpack 编码后用 Recipient::try_send；弱网/客户端断连时吞错。
pub(super) fn send_down(addr: &Recipient<OutMessage>, p: VoicePayload) {
    match voice_proto::encode_indication(&p) {
        Ok(bytes) => {
            // 注：Recipient::do_send 返回 ()，无法获知失败 —— 用 try_send 接住 SendError
            // （SendError 的 Display 只输出变体名 "receiver is full" / "receiver is gone"），
            // 弱网下能定位"客户端没收到 TTS"这类问题
            if let Err(e) = addr.try_send(OutMessage { data: bytes }) {
                debug!(
                    target: "voice_server.session",
                    session_id = ?p.session_id(),
                    "下行 try_send 失败（客户端可能已断开）: {}", e
                );
            }
        }
        Err(e) => warn!(
            target: "voice_server.session",
            session_id = ?p.session_id(),
            "下行编码失败: {}", e
        ),
    }
}