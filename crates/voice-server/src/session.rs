//! VoiceSession: 每条 WS 连接对应一个会话
//!
//! 状态机：Idle / Listening / Processing / Speaking
//! 关键节点（转换、推流、错误）都打 `tracing::*!` 日志，便于观测。
//!
//! 下行通过 webhttp::websocket::OutMessage 推给客户端。
//!
//! pipeline 复用 test_api::llm_tts_items（LLM → 切句 → TTS → 句间 crossfade），
//! 本文件只负责：上行消息分发、ASR 阶段、事件 → VoicePayload 映射、取消。
//!
//! ## Pipeline 并发策略：**打断旧的**（auto-interrupt on new utterance）
//!
//! 同一会话里若上一条 pipeline 还没跑完，新一条 `AudioChunk{is_last:true}` 触发时
//! 不会排队、不会丢弃，而是**主动 cancel 旧 pipeline** 后立即启动新 pipeline：
//!
//! 1. `trigger_pipeline()` 在 spawn 前先 `self.cancel.cancel()` 把旧 token 置位，
//!    旧 pipeline 的 `tokio::select!` 观察到 cancel 后提前 return；
//! 2. 接着 `self.cancel = CancellationToken::new()` 给新 pipeline 一个干净 token，
//!    旧 pipeline 持有的是已被 cancel 的旧 token，不会被误波及；
//! 3. 旧 pipeline 已通过 `down_addr.do_send` 推出去的消息无法回收，但客户端按
//!    seq 单调递增 + `is_final` / `is_last` 终止标记能识别一次完整回合，丢弃被
//!    截断的后续 chunk 即可。
//!
//! 等价于"`Interrupt` 由新语音自动触发"，是半双工 voice agent 的常规 UX。
//! 排队策略需要单独设计调度器，超出本文件职责。

use std::sync::Arc;
use std::time::Instant;

use actix::prelude::Recipient;
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use voice_proto::VoicePayload;
use webhttp::websocket::OutMessage;

use crate::client::{AsrClient, LlmClient, TtsClient};
use crate::test_api::{llm_tts_items, LlmTtsItem};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Idle,
    Listening,
    Processing,
    Speaking,
}

#[derive(Debug, Clone)]
pub struct AsrEvent {
    pub text: String,
    pub is_final: bool,
}

#[derive(Debug, Clone)]
pub struct LlmEvent {
    pub delta: String,
    pub is_final: bool,
}

#[derive(Debug, Clone)]
pub struct TtsEvent {
    pub seq: u32,
    pub data: Vec<u8>,
    pub is_last: bool,
}

struct AudioAccumulator {
    chunks: Vec<Vec<u8>>,
    total_bytes: usize,
    started_at: Instant,
}

impl AudioAccumulator {
    fn new() -> Self {
        Self {
            chunks: Vec::new(),
            total_bytes: 0,
            started_at: Instant::now(),
        }
    }
    fn push(&mut self, chunk: Vec<u8>) {
        self.total_bytes += chunk.len();
        self.chunks.push(chunk);
    }
    fn drain(&mut self) -> Vec<u8> {
        let mut all = Vec::with_capacity(self.total_bytes);
        for c in self.chunks.drain(..) {
            all.extend_from_slice(&c);
        }
        self.total_bytes = 0;
        all
    }
}

pub struct VoiceSession {
    pub session_id: String,
    pub state: SessionState,
    cancel: CancellationToken,
    audio_buf: AudioAccumulator,
    asr: Arc<dyn AsrClient>,
    llm: Arc<dyn LlmClient>,
    tts: Arc<dyn TtsClient>,
    /// 下行发送目标：actix 的 Recipient，跨任务 clone
    down_addr: Recipient<OutMessage>,
}

impl VoiceSession {
    pub fn new(
        session_id: String,
        asr: Arc<dyn AsrClient>,
        llm: Arc<dyn LlmClient>,
        tts: Arc<dyn TtsClient>,
        down_addr: Recipient<OutMessage>,
    ) -> Self {
        info!(target: "voice_server.session", session_id = %session_id, "VoiceSession 创建");
        Self {
            session_id,
            state: SessionState::Idle,
            cancel: CancellationToken::new(),
            audio_buf: AudioAccumulator::new(),
            asr,
            llm,
            tts,
            down_addr,
        }
    }

    fn transition(&mut self, next: SessionState) {
        if self.state != next {
            info!(
                target: "voice_server.session",
                session_id = %self.session_id,
                from = ?self.state,
                to = ?next,
                "状态转换"
            );
            self.state = next;
        }
    }

    /// 处理上行 payload（同步；可触发异步 pipeline 任务）
    pub fn on_payload(&mut self, p: VoicePayload) {
        match p {
            VoicePayload::SessionStart { .. } => {
                info!(target: "voice_server.session", session_id = %self.session_id, "收到 SessionStart");
                self.transition(SessionState::Listening);
            }
            VoicePayload::AudioChunk {
                seq,
                timestamp_ms,
                data,
                is_last,
                ..
            } => {
                let bytes = data.len();
                debug!(
                    target: "voice_server.session",
                    session_id = %self.session_id,
                    seq,
                    bytes,
                    timestamp_ms,
                    is_last,
                    "收到 AudioChunk"
                );
                if self.state == SessionState::Idle {
                    self.transition(SessionState::Listening);
                }
                self.audio_buf.push(data);
                if is_last {
                    self.trigger_pipeline();
                }
            }
            VoicePayload::Interrupt { .. } => {
                warn!(
                    target: "voice_server.session",
                    session_id = %self.session_id,
                    "收到 Interrupt，取消当前任务"
                );
                self.cancel.cancel();
                self.cancel = CancellationToken::new();
                self.transition(SessionState::Listening);
                send_down(&self.down_addr, VoicePayload::LlmDelta {
                    session_id: self.session_id.clone(),
                    delta: "[已打断]".to_string(),
                    is_final: true,
                });
            }
            VoicePayload::SessionEnd { reason, .. } => {
                info!(
                    target: "voice_server.session",
                    session_id = %self.session_id,
                    reason = %reason,
                    "收到 SessionEnd，关闭会话"
                );
                self.cancel.cancel();
            }
            other => {
                debug!(
                    target: "voice_server.session",
                    session_id = %self.session_id,
                    "忽略非上行 payload: {:?}", other
                );
            }
        }
    }

    fn trigger_pipeline(&mut self) {
        let audio = self.audio_buf.drain();
        if audio.is_empty() {
            warn!(target: "voice_server.session", session_id = %self.session_id, "累积音频为空，跳过");
            return;
        }
        info!(
            target: "voice_server.session",
            session_id = %self.session_id,
            bytes = audio.len(),
            elapsed_ms = self.audio_buf.started_at.elapsed().as_millis() as u64,
            "VAD 句尾，触发 pipeline"
        );

        // ===== 打断旧的 pipeline（auto-interrupt on new utterance）=====
        // 旧 pipeline 已 clone 走的 token 仍指向被 cancel 的那一个，它在下一个
        // tokio::select! 边界看到 cancel 就会提前 return；新 pipeline 拿到的是
        // 全新、未 cancel 的 token，与旧 pipeline 互不影响。
        self.cancel.cancel();
        self.cancel = CancellationToken::new();

        self.transition(SessionState::Processing);

        let cancel = self.cancel.clone();
        let session_id = self.session_id.clone();
        let asr = self.asr.clone();
        let llm = self.llm.clone();
        let tts = self.tts.clone();
        let down_addr = self.down_addr.clone();

        tokio::spawn(async move {
            run_pipeline(session_id, audio, asr, llm, tts, down_addr, cancel).await;
        });

        self.transition(SessionState::Listening);
    }
}

/// 完整 pipeline：ASR → LLM → 切句 → TTS（复用 test_api::llm_tts_items）。
///
/// 与 /test/asr_llm_tts 结构对称，差异仅在 wire 侧：
///   - 下行推 VoicePayload（msgpack 信封）而非 NDJSON
///   - 全程受 CancellationToken 约束（用户打断）
///   - Tts chunk 直接带原始 PCM 字节（不做 base64）
async fn run_pipeline(
    session_id: String,
    audio: Vec<u8>,
    asr: Arc<dyn AsrClient>,
    llm: Arc<dyn LlmClient>,
    tts: Arc<dyn TtsClient>,
    down_addr: Recipient<OutMessage>,
    cancel: CancellationToken,
) {
    info!(target: "voice_server.session", session_id = %session_id, "pipeline 开始");

    // ===== 阶段 1: ASR —— 转发识别事件，收最终文本作为 LLM prompt =====
    let mut asr_stream = match asr.recognize(&session_id, None, audio).await {
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

    let mut prompt = String::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                warn!(target: "voice_server.session", session_id = %session_id, "ASR 阶段被取消");
                return;
            }
            evt = asr_stream.next() => {
                match evt {
                    Some(Ok(e)) => {
                        send_down(&down_addr, VoicePayload::AsrPartial {
                            session_id: session_id.clone(),
                            text: e.text.clone(),
                            is_final: e.is_final,
                        });
                        // 取最新非空识别文本；非流式 ASR 客户端只发一个 is_final=true 的完整结果
                        if !e.text.is_empty() {
                            prompt = e.text;
                        }
                        if e.is_final {
                            break;
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
        warn!(target: "voice_server.session", session_id = %session_id, "ASR 最终文本为空，跳过 LLM/TTS");
        return;
    }
    info!(target: "voice_server.session", session_id = %session_id, asr_text = %prompt, "ASR final 完成，进入 LLM");

    // ===== 阶段 2+3: LLM → 切句 → TTS（共享管线，含句间 crossfade / 全局 seq / 结束标记）=====
    let mut items = Box::pin(llm_tts_items(prompt, session_id.clone(), llm, tts));
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
                    // test_api 侧是 base64 字符串（NDJSON 需要），WS 侧还原为原始字节
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

fn send_down(addr: &Recipient<OutMessage>, p: VoicePayload) {
    match voice_proto::encode_indication(&p) {
        Ok(bytes) => {
            let _ = addr.do_send(OutMessage { data: bytes });
        }
        Err(e) => warn!(
            target: "voice_server.session",
            session_id = ?p.session_id(),
            "下行编码失败: {}", e
        ),
    }
}

/// 在 `buf` 中找下一个"句末标点"，返回该标点之后第一个字节的位置（byte index）
/// pub 供 `test_api` 切句逻辑复用
pub fn next_sentence_end(buf: &str) -> Option<usize> {
    for (i, c) in buf.char_indices() {
        if matches!(c, '。' | '！' | '?' | '.' | '!') {
            return Some(i + c.len_utf8());
        }
    }
    None
}
