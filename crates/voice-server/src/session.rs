//! VoiceSession: 每条 WS 连接对应一个会话
//!
//! 状态机：Idle / Listening / Processing / Speaking
//! 关键节点（转换、推流、错误）都打 `tracing::*!` 日志，便于观测。
//!
//! 下行通过 webhttp::websocket::OutMessage 推给客户端。

use std::sync::Arc;
use std::time::Instant;

use actix::prelude::Recipient;
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use voice_proto::VoicePayload;
use webhttp::websocket::OutMessage;

use crate::clients::{AsrClient, LlmClient, TtsClient};

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
                self.send(VoicePayload::LlmDelta {
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

    fn send(&self, p: VoicePayload) {
        match voice_proto::encode_indication(&p) {
            Ok(bytes) => {
                let _ = self.down_addr.do_send(OutMessage { data: bytes });
                debug!(
                    target: "voice_server.session",
                    session_id = %self.session_id,
                    "下行推 payload"
                );
            }
            Err(e) => warn!(
                target: "voice_server.session",
                session_id = %self.session_id,
                "下行编码失败: {}", e
            ),
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

    let mut asr_stream = match asr.recognize(&session_id, audio).await {
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

    let mut asr_final_text = String::new();
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
                        if e.is_final {
                            asr_final_text = e.text;
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        error!(target: "voice_server.session", session_id = %session_id, "ASR 流错误: {}", e);
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

    if asr_final_text.trim().is_empty() {
        warn!(target: "voice_server.session", session_id = %session_id, "ASR 最终文本为空，跳过 LLM/TTS");
        return;
    }
    info!(target: "voice_server.session", session_id = %session_id, asr_text = %asr_final_text, "ASR final 完成，进入 LLM");

    let mut llm_stream = match llm.chat(&session_id, &asr_final_text).await {
        Ok(s) => s,
        Err(e) => {
            error!(target: "voice_server.session", session_id = %session_id, "LLM 调用失败: {}", e);
            send_down(&down_addr, VoicePayload::Error { code: 1002, message: format!("llm error: {}", e) });
            return;
        }
    };

    let mut sentence_buf = String::new();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                warn!(target: "voice_server.session", session_id = %session_id, "LLM 阶段被取消");
                return;
            }
            evt = llm_stream.next() => {
                match evt {
                    Some(Ok(e)) => {
                        if !e.delta.is_empty() {
                            send_down(&down_addr, VoicePayload::LlmDelta {
                                session_id: session_id.clone(),
                                delta: e.delta.clone(),
                                is_final: false,
                            });
                        }
                        sentence_buf.push_str(&e.delta);

                        while let Some(end) = next_sentence_end(&sentence_buf) {
                            let sent: String = sentence_buf[..end].to_string();
                            sentence_buf = sentence_buf[end..].to_string();
                            info!(target: "voice_server.session", session_id = %session_id, sentence = %sent, "LLM 切出完整句子，送 TTS");
                            if let Err(e) = speak_one_sentence(&session_id, &sent, tts.as_ref(), &down_addr, cancel.clone()).await {
                                error!(target: "voice_server.session", session_id = %session_id, "TTS 失败: {:?}", e);
                                return;
                            }
                        }

                        if e.is_final { break; }
                    }
                    Some(Err(e)) => {
                        error!(target: "voice_server.session", session_id = %session_id, "LLM 流错误: {}", e);
                        return;
                    }
                    None => {
                        warn!(target: "voice_server.session", session_id = %session_id, "LLM 流提前结束");
                        break;
                    }
                }
            }
        }
    }

    let tail = sentence_buf.trim().to_string();
    if !tail.is_empty() {
        info!(target: "voice_server.session", session_id = %session_id, sentence = %tail, "LLM 末尾残余句子，送 TTS");
        if let Err(e) = speak_one_sentence(&session_id, &tail, tts.as_ref(), &down_addr, cancel.clone()).await {
            error!(target: "voice_server.session", session_id = %session_id, "TTS 收尾失败: {:?}", e);
            return;
        }
    }
    info!(target: "voice_server.session", session_id = %session_id, "pipeline 全部完成");
}

async fn speak_one_sentence(
    session_id: &str,
    text: &str,
    tts: &dyn TtsClient,
    down_addr: &Recipient<OutMessage>,
    cancel: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut stream = tts.synthesize(session_id, text).await?;
    while let Some(item) = stream.next().await {
        if cancel.is_cancelled() {
            warn!(target: "voice_server.session", session_id = %session_id, "TTS 阶段被取消");
            return Ok(());
        }
        match item {
            Ok(e) => {
                send_down(down_addr, VoicePayload::TtsAudio {
                    session_id: session_id.to_string(),
                    seq: e.seq,
                    data: e.data,
                    is_last: e.is_last,
                });
                if e.is_last {
                    return Ok(());
                }
            }
            Err(e) => return Err(Box::new(e)),
        }
    }
    Ok(())
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
fn next_sentence_end(buf: &str) -> Option<usize> {
    for (i, c) in buf.char_indices() {
        if matches!(c, '。' | '！' | '?' | '.' | '!') {
            return Some(i + c.len_utf8());
        }
    }
    None
}