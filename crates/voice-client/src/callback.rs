//! VoiceCallback: 业务侧的下行回调 trait

use async_trait::async_trait;
use tracing::{debug, info, warn};
use voice_proto::VoicePayload;

#[async_trait]
pub trait VoiceCallback: Send + Sync {
    async fn on_payload(&self, p: VoicePayload);
}

/// 默认实现：把所有下行 payload 打到日志，ASR/LLM 文本打到 stdout，
/// TTS 音频字节累计到一个 Vec。
pub struct DefaultVoiceCallback {
    pub tts_audio_buf: std::sync::Mutex<Vec<u8>>,
}

impl Default for DefaultVoiceCallback {
    fn default() -> Self {
        Self {
            tts_audio_buf: std::sync::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl VoiceCallback for DefaultVoiceCallback {
    async fn on_payload(&self, p: VoicePayload) {
        match p {
            VoicePayload::AsrPartial { text, is_final, .. } => {
                if is_final {
                    info!(target: "voice_client.cb", "[ASR FINAL] {}", text);
                    println!("[ASR] {}", text);
                } else {
                    debug!(target: "voice_client.cb", "[ASR PARTIAL] {}", text);
                }
            }
            VoicePayload::LlmDelta { delta, is_final, .. } => {
                if is_final {
                    info!(target: "voice_client.cb", "[LLM END]");
                } else if !delta.is_empty() {
                    print!("{}", delta);
                    use std::io::Write;
                    let _ = std::io::stdout().flush();
                }
            }
            VoicePayload::TtsAudio { seq, data, is_last, .. } => {
                debug!(target: "voice_client.cb", seq, bytes = data.len(), is_last, "[TTS CHUNK]");
                if !data.is_empty() {
                    self.tts_audio_buf.lock().unwrap().extend_from_slice(&data);
                }
                if is_last {
                    let total: usize = self.tts_audio_buf.lock().unwrap().len();
                    info!(target: "voice_client.cb", total_bytes = total, "[TTS END] 音频缓冲总字节数");
                    println!("\n[TTS] 共 {} 字节音频（终端 demo 未真实播放）", total);
                }
            }
            VoicePayload::Error { code, message } => {
                warn!(target: "voice_client.cb", code, "[ERROR] {}", message);
                println!("[ERROR {}] {}", code, message);
            }
            _ => {}
        }
    }
}