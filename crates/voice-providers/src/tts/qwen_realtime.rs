//! Qwen-TTS Realtime API adapter
//!
//! 适用模型（详见 `docs/qwen-tts-docs/real-tts-code.md` 中的 Qwen-TTS 章节）：
//! - `qwen3-tts-flash-realtime`
//! - `qwen3-tts-instruct-flash-realtime`
//!
//! 协议（OpenAI Realtime 风格）：
//! - URL：`wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=qwen3-tts-flash-realtime`
//! - 控制面（text JSON）：
//!   - 客户端 → 服务端：
//!     - `session.update`：设置 voice / mode (server_commit|commit) / response_format / sample_rate / language_type
//!     - `input_text_buffer.append`：追加文本片段（server_commit 模式）
//!     - `input_text_buffer.commit`：提交缓冲（commit 模式触发合成）
//!     - `session.finish`：结束会话
//!   - 服务端 → 客户端：
//!     - `session.created` / `session.updated`
//!     - `response.created` / `response.output_item.added` / `response.done`
//!     - `response.audio.delta`（base64 PCM）+ `response.audio.done`
//!     - `input_text_buffer.committed` / `input_text_buffer.cleared`
//!     - `session.finished`
//!     - `error`
//! - 音频：base64 编码在 JSON `delta` 字段里（PCM 16-bit mono）。
//!
//! Auth：握手头 `Authorization: Bearer <api_key>`（与 Qwen-Audio-TTS 同）。

use serde_json::{json, Value};

use crate::asr::ClientError;
use crate::tts::{RealtimeEventHint, TtsModelAdapter, TtsProtocol};

/// Qwen-TTS Realtime adapter
pub struct QwenRealtimeTts {
    /// 模型名（实际 model 在 WSS URL 的 query string 里；本字段仅用于日志 / 标识）
    #[allow(dead_code)]
    model: String,
    voice: String,
    /// "server_commit"（默认，文本由服务端分段合成）或 "commit"（客户端主动 commit）
    mode: String,
    /// 响应音频格式（"pcm" / "wav" / "mp3" 等），默认 "pcm"
    response_format: String,
    /// 采样率，默认 24000
    sample_rate: u32,
    /// 语言提示（"zh" / "en" / "Auto"），默认 "Auto"
    language_type: String,
}

impl QwenRealtimeTts {
    pub fn new(model: String, voice: String) -> Self {
        Self {
            model,
            voice,
            mode: "server_commit".to_string(),
            response_format: "pcm".to_string(),
            sample_rate: 24000,
            language_type: "Auto".to_string(),
        }
    }

    pub fn with_mode(mut self, mode: impl Into<String>) -> Self {
        self.mode = mode.into();
        self
    }

    pub fn with_format(mut self, format: impl Into<String>) -> Self {
        self.response_format = format.into();
        self
    }

    pub fn with_sample_rate(mut self, sr: u32) -> Self {
        self.sample_rate = sr;
        self
    }

    pub fn with_language(mut self, lang: impl Into<String>) -> Self {
        self.language_type = lang.into();
        self
    }
}

impl TtsModelAdapter for QwenRealtimeTts {
    fn model_name(&self) -> &'static str {
        "qwen3-tts-flash-realtime"
    }

    fn voice(&self) -> &str {
        &self.voice
    }

    fn protocol(&self) -> TtsProtocol {
        TtsProtocol::RealtimeSession
    }

    // ===== Realtime 协议实现 =====

    fn session_update_text(&self) -> Result<String, ClientError> {
        let msg = json!({
            "type": "session.update",
            "session": {
                "mode": self.mode,
                "voice": self.voice,
                "response_format": self.response_format,
                "sample_rate": self.sample_rate,
                "language_type": self.language_type,
            },
        });
        serde_json::to_string(&msg).map_err(|e| ClientError::Decode(e.to_string()))
    }

    fn append_text_text(&self, text: &str) -> Result<String, ClientError> {
        let msg = json!({
            "type": "input_text_buffer.append",
            "text": text,
        });
        serde_json::to_string(&msg).map_err(|e| ClientError::Decode(e.to_string()))
    }

    fn commit_text(&self) -> Result<String, ClientError> {
        let msg = json!({ "type": "input_text_buffer.commit" });
        serde_json::to_string(&msg).map_err(|e| ClientError::Decode(e.to_string()))
    }

    fn session_finish_text(&self) -> Result<String, ClientError> {
        let msg = json!({ "type": "session.finish" });
        serde_json::to_string(&msg).map_err(|e| ClientError::Decode(e.to_string()))
    }

    fn parse_realtime_event(&self, text: &str) -> Result<RealtimeEventHint, ClientError> {
        let v: Value = serde_json::from_str(text).map_err(|e| ClientError::Decode(e.to_string()))?;
        let event_type = v
            .get("type")
            .and_then(|x| x.as_str())
            .ok_or_else(|| ClientError::Decode("missing type".into()))?;

        match event_type {
            "session.created" => {
                let session_id = v
                    .get("session")
                    .and_then(|s| s.get("id"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                Ok(RealtimeEventHint::SessionCreated { session_id })
            }
            "session.updated" => Ok(RealtimeEventHint::SessionUpdated),
            "response.created" => {
                let response_id = v
                    .get("response")
                    .and_then(|r| r.get("id"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                Ok(RealtimeEventHint::ResponseCreated { response_id })
            }
            "response.done" => Ok(RealtimeEventHint::ResponseDone),
            "session.finished" => Ok(RealtimeEventHint::SessionFinished),
            "response.audio.delta" => {
                let sample_b64 = v
                    .get("delta")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                Ok(RealtimeEventHint::AudioDelta { sample_b64 })
            }
            "error" => {
                let message = v
                    .get("error")
                    .and_then(|e| e.get("message"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("unknown error")
                    .to_string();
                Ok(RealtimeEventHint::Error { message })
            }
            _ => Ok(RealtimeEventHint::Other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tts::TtsModelAdapter;

    #[test]
    fn model_name_voice_protocol() {
        let a = QwenRealtimeTts::new("qwen3-tts-flash-realtime".into(), "Cherry".into());
        assert_eq!(a.model_name(), "qwen3-tts-flash-realtime");
        assert_eq!(a.voice(), "Cherry");
        assert_eq!(a.protocol(), TtsProtocol::RealtimeSession);
    }

    #[test]
    fn session_update_text_roundtrip() {
        let a = QwenRealtimeTts::new("qwen3-tts-flash-realtime".into(), "Cherry".into())
            .with_sample_rate(24000)
            .with_format("pcm")
            .with_mode("server_commit")
            .with_language("Auto");
        let s = a.session_update_text().unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["type"], "session.update");
        assert_eq!(v["session"]["voice"], "Cherry");
        assert_eq!(v["session"]["mode"], "server_commit");
        assert_eq!(v["session"]["response_format"], "pcm");
        assert_eq!(v["session"]["sample_rate"], 24000);
        assert_eq!(v["session"]["language_type"], "Auto");
    }

    #[test]
    fn append_text_text_has_text() {
        let a = QwenRealtimeTts::new("qwen3-tts-flash-realtime".into(), "Cherry".into());
        let s = a.append_text_text("你好").unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["type"], "input_text_buffer.append");
        assert_eq!(v["text"], "你好");
    }

    #[test]
    fn commit_text_has_correct_type() {
        let a = QwenRealtimeTts::new("qwen3-tts-flash-realtime".into(), "Cherry".into());
        let s = a.commit_text().unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["type"], "input_text_buffer.commit");
    }

    #[test]
    fn session_finish_text_has_correct_type() {
        let a = QwenRealtimeTts::new("qwen3-tts-flash-realtime".into(), "Cherry".into());
        let s = a.session_finish_text().unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["type"], "session.finish");
    }

    #[test]
    fn parse_event_session_created() {
        let a = QwenRealtimeTts::new("qwen3-tts-flash-realtime".into(), "Cherry".into());
        let s = r#"{
            "type": "session.created",
            "event_id": "event_1",
            "session": {"id": "sess_123", "model": "qwen3-tts-flash-realtime"}
        }"#;
        assert_eq!(
            a.parse_realtime_event(s).unwrap(),
            RealtimeEventHint::SessionCreated {
                session_id: "sess_123".into(),
            }
        );
    }

    #[test]
    fn parse_event_response_audio_delta_carries_base64() {
        let a = QwenRealtimeTts::new("qwen3-tts-flash-realtime".into(), "Cherry".into());
        let s = r#"{
            "type": "response.audio.delta",
            "event_id": "event_2",
            "response_id": "resp_1",
            "item_id": "item_1",
            "output_index": 0,
            "content_index": 0,
            "delta": "SGVsbG8="
        }"#;
        match a.parse_realtime_event(s).unwrap() {
            RealtimeEventHint::AudioDelta { sample_b64 } => {
                assert_eq!(sample_b64, "SGVsbG8=");
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn parse_event_session_finished() {
        let a = QwenRealtimeTts::new("qwen3-tts-flash-realtime".into(), "Cherry".into());
        let s = r#"{"type": "session.finished"}"#;
        assert_eq!(
            a.parse_realtime_event(s).unwrap(),
            RealtimeEventHint::SessionFinished
        );
    }

    #[test]
    fn parse_event_error_message() {
        let a = QwenRealtimeTts::new("qwen3-tts-flash-realtime".into(), "Cherry".into());
        let s = r#"{"type": "error", "error": {"message": "rate limit"}}"#;
        match a.parse_realtime_event(s).unwrap() {
            RealtimeEventHint::Error { message } => {
                assert_eq!(message, "rate limit");
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn parse_event_unknown_returns_other() {
        let a = QwenRealtimeTts::new("qwen3-tts-flash-realtime".into(), "Cherry".into());
        let s = r#"{"type": "mystery"}"#;
        assert_eq!(a.parse_realtime_event(s).unwrap(), RealtimeEventHint::Other);
    }
}
