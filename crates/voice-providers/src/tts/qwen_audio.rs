//! Qwen-Audio-TTS / CosyVoice 公共 DashScope WebSocket 协议 adapter
//!
//! 适用模型：
//! - `qwen-audio-3.0-tts-flash` / `qwen-audio-3.0-tts-plus`
//! - `cosyvoice-v2` / `cosyvoice-v3-flash` / `cosyvoice-v3-plus`
//! - `cosyvoice-v3.5-flash` / `cosyvoice-v3.5-plus`
//!
//! 协议（详见 `docs/qwen-tts-docs/qwen-tts-{server,client,ws}-api.md`）：
//!
//! - 控制面（text JSON）：
//!   - 客户端 → 服务端：`run-task` / `continue-task` / `finish-task`
//!   - 服务端 → 客户端：`task-started` / `result-generated` / `task-finished` / `task-failed`
//! - 音频面（binary）：每个 `result-generated` 的 `sentence-synthesis` 子事件后，紧跟一个 binary 帧承载音频数据。
//!
//! URL：`wss://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/api-ws/v1/inference`
//! Auth：握手头 `Authorization: Bearer <api_key>`

use serde_json::{json, Value};

use crate::asr::ClientError;
use crate::tts::{ServerEventHint, TtsModelAdapter, TtsProtocol};

// ===== Adapter =====

/// Qwen-Audio-TTS / CosyVoice 公共协议 adapter。
///
/// 通过 `model` 字段（运行时给定）携带具体模型名（`qwen-audio-3.0-tts-flash` 等）；
/// voice 字段携带具体音色 ID（如 `longanhuan_v3.6` / `longanyang`）。
pub struct QwenAudioTts {
    model: String,
    voice: String,
}

impl QwenAudioTts {
    pub fn new(model: String, voice: String) -> Self {
        Self { model, voice }
    }
}

impl TtsModelAdapter for QwenAudioTts {
    fn model_name(&self) -> &'static str {
        // 返回的 `&'static str` 仅用作日志 / cfg 显示；具体模型名由 self.model 给出
        "qwen-audio-tts"
    }

    fn voice(&self) -> &str {
        &self.voice
    }

    fn protocol(&self) -> TtsProtocol {
        TtsProtocol::JsonDuplex
    }

    // ===== JSON duplex 协议实现 =====

    fn run_task_text(
        &self,
        task_id: &str,
        sample_rate: u32,
        format: &str,
    ) -> Result<String, ClientError> {
        // sample_rate / format 在公共协议里是可选；上层可通过 cfg 调整
        let parameters = json!({
            "text_type": "PlainText",
            "voice": self.voice,
            "format": if format.is_empty() { Value::Null } else { Value::String(format.to_string()) },
            "sample_rate": if sample_rate == 0 { Value::Null } else { Value::Number(sample_rate.into()) },
            "volume": 50,
            "rate": 1.0,
            "pitch": 1.0,
            "enable_ssml": false,
        });

        let msg = json!({
            "header": {
                "action": "run-task",
                "task_id": task_id,
                "streaming": "duplex",
            },
            "payload": {
                "task_group": "audio",
                "task": "tts",
                "function": "SpeechSynthesizer",
                "model": self.model,
                "parameters": parameters,
                "input": {},
            },
        });

        serde_json::to_string(&msg).map_err(|e| ClientError::Decode(e.to_string()))
    }

    fn continue_task_text(&self, task_id: &str, text: &str) -> Result<String, ClientError> {
        let msg = json!({
            "header": {
                "action": "continue-task",
                "task_id": task_id,
                "streaming": "duplex",
            },
            "payload": {
                "input": { "text": text },
            },
        });
        serde_json::to_string(&msg).map_err(|e| ClientError::Decode(e.to_string()))
    }

    fn finish_task_text(&self, task_id: &str) -> Result<String, ClientError> {
        let msg = json!({
            "header": {
                "action": "finish-task",
                "task_id": task_id,
                "streaming": "duplex",
            },
            "payload": {
                "input": {},
            },
        });
        serde_json::to_string(&msg).map_err(|e| ClientError::Decode(e.to_string()))
    }

    fn parse_server_event(&self, text: &str) -> Result<ServerEventHint, ClientError> {
        let v: Value = serde_json::from_str(text).map_err(|e| ClientError::Decode(e.to_string()))?;

        let header = v
            .get("header")
            .ok_or_else(|| ClientError::Decode("missing header".into()))?;
        let event = header
            .get("event")
            .and_then(|x| x.as_str())
            .ok_or_else(|| ClientError::Decode("missing header.event".into()))?;

        match event {
            "task-started" => Ok(ServerEventHint::TaskStarted),
            "task-finished" => {
                let characters = v
                    .get("payload")
                    .and_then(|p| p.get("usage"))
                    .and_then(|u| u.get("characters"))
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0) as u32;
                let request_uuid = header
                    .get("attributes")
                    .and_then(|a| a.get("request_uuid"))
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_string());
                Ok(ServerEventHint::TaskFinished {
                    request_uuid,
                    characters,
                })
            }
            "task-failed" => {
                let code = header
                    .get("error_code")
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_string());
                let message = header
                    .get("error_message")
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_string());
                Ok(ServerEventHint::TaskFailed { code, message })
            }
            "result-generated" => {
                let payload = v.get("payload").cloned().unwrap_or(Value::Null);
                let output = payload.get("output").cloned().unwrap_or(Value::Null);
                let sub_type = output
                    .get("type")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let sentence_index = output
                    .get("sentence")
                    .and_then(|s| s.get("index"))
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0) as u32;
                let original_text = output
                    .get("original_text")
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_string());

                match sub_type.as_str() {
                    "sentence-begin" => Ok(ServerEventHint::SentenceBegin {
                        index: sentence_index,
                        original_text,
                    }),
                    "sentence-synthesis" => Ok(ServerEventHint::SentenceSynthesis {
                        index: sentence_index,
                    }),
                    "sentence-end" => {
                        let characters = payload
                            .get("usage")
                            .and_then(|u| u.get("characters"))
                            .and_then(|x| x.as_u64())
                            .unwrap_or(0) as u32;
                        Ok(ServerEventHint::SentenceEnd {
                            index: sentence_index,
                            characters,
                        })
                    }
                    _ => Ok(ServerEventHint::Other),
                }
            }
            _ => Ok(ServerEventHint::Other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tts::TtsModelAdapter;

    fn make(model: &str, voice: &str) -> QwenAudioTts {
        QwenAudioTts::new(model.to_string(), voice.to_string())
    }

    #[test]
    fn model_name_voice_protocol() {
        let a = make("qwen-audio-3.0-tts-flash", "longanhuan_v3.6");
        assert_eq!(a.model_name(), "qwen-audio-tts");
        assert_eq!(a.voice(), "longanhuan_v3.6");
        assert_eq!(a.protocol(), TtsProtocol::JsonDuplex);
    }

    #[test]
    fn run_task_text_roundtrip() {
        let a = make("qwen-audio-3.0-tts-flash", "longanhuan_v3.6");
        let s = a.run_task_text("task-1", 22050, "mp3").unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["header"]["action"], "run-task");
        assert_eq!(v["header"]["task_id"], "task-1");
        assert_eq!(v["header"]["streaming"], "duplex");
        assert_eq!(v["payload"]["task_group"], "audio");
        assert_eq!(v["payload"]["task"], "tts");
        assert_eq!(v["payload"]["function"], "SpeechSynthesizer");
        assert_eq!(v["payload"]["model"], "qwen-audio-3.0-tts-flash");
        assert_eq!(v["payload"]["parameters"]["voice"], "longanhuan_v3.6");
        assert_eq!(v["payload"]["parameters"]["format"], "mp3");
        assert_eq!(v["payload"]["parameters"]["sample_rate"], 22050);
        assert_eq!(v["payload"]["parameters"]["enable_ssml"], false);
    }

    #[test]
    fn run_task_text_omits_sample_rate_and_format_when_zero_or_empty() {
        let a = make("cosyvoice-v3-flash", "longanyang");
        let s = a.run_task_text("task-x", 0, "").unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert!(v["payload"]["parameters"]["format"].is_null());
        assert!(v["payload"]["parameters"]["sample_rate"].is_null());
    }

    #[test]
    fn continue_task_text_has_text_field() {
        let a = make("cosyvoice-v3-flash", "longanyang");
        let s = a.continue_task_text("task-x", "床前明月光").unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["header"]["action"], "continue-task");
        assert_eq!(v["payload"]["input"]["text"], "床前明月光");
    }

    #[test]
    fn finish_task_text_has_empty_input() {
        let a = make("cosyvoice-v3-flash", "longanyang");
        let s = a.finish_task_text("task-x").unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["header"]["action"], "finish-task");
        assert!(v["payload"]["input"].is_object());
    }

    #[test]
    fn parse_event_task_started() {
        let a = make("qwen-audio-3.0-tts-flash", "v");
        let s = r#"{
            "header": {"task_id": "abc", "event": "task-started", "attributes": {}},
            "payload": {}
        }"#;
        assert_eq!(a.parse_server_event(s).unwrap(), ServerEventHint::TaskStarted);
    }

    #[test]
    fn parse_event_result_generated_sentence_begin() {
        let a = make("qwen-audio-3.0-tts-flash", "v");
        let s = r#"{
            "header": {"task_id": "abc", "event": "result-generated", "attributes": {}},
            "payload": {
                "output": {
                    "sentence": {"index": 0, "words": []},
                    "type": "sentence-begin",
                    "original_text": "床前明月光，"
                }
            }
        }"#;
        match a.parse_server_event(s).unwrap() {
            ServerEventHint::SentenceBegin { index, original_text } => {
                assert_eq!(index, 0);
                assert_eq!(original_text.as_deref(), Some("床前明月光，"));
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn parse_event_result_generated_sentence_synthesis() {
        let a = make("qwen-audio-3.0-tts-flash", "v");
        let s = r#"{
            "header": {"task_id": "abc", "event": "result-generated", "attributes": {}},
            "payload": {
                "output": {
                    "sentence": {"index": 0, "words": []},
                    "type": "sentence-synthesis"
                }
            }
        }"#;
        assert_eq!(
            a.parse_server_event(s).unwrap(),
            ServerEventHint::SentenceSynthesis { index: 0 }
        );
    }

    #[test]
    fn parse_event_result_generated_sentence_end() {
        let a = make("qwen-audio-3.0-tts-flash", "v");
        let s = r#"{
            "header": {"task_id": "abc", "event": "result-generated", "attributes": {}},
            "payload": {
                "output": {
                    "sentence": {"index": 0, "words": []},
                    "type": "sentence-end",
                    "original_text": "床前明月光，"
                },
                "usage": {"characters": 6}
            }
        }"#;
        assert_eq!(
            a.parse_server_event(s).unwrap(),
            ServerEventHint::SentenceEnd {
                index: 0,
                characters: 6,
            }
        );
    }

    #[test]
    fn parse_event_task_finished() {
        let a = make("qwen-audio-3.0-tts-flash", "v");
        let s = r#"{
            "header": {
                "task_id": "abc",
                "event": "task-finished",
                "attributes": {"request_uuid": "req-1"}
            },
            "payload": {"usage": {"characters": 13}}
        }"#;
        match a.parse_server_event(s).unwrap() {
            ServerEventHint::TaskFinished { request_uuid, characters } => {
                assert_eq!(request_uuid.as_deref(), Some("req-1"));
                assert_eq!(characters, 13);
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn parse_event_task_failed() {
        let a = make("qwen-audio-3.0-tts-flash", "v");
        let s = r#"{
            "header": {
                "task_id": "abc",
                "event": "task-failed",
                "error_code": "InvalidParameter",
                "error_message": "boom",
                "attributes": {}
            },
            "payload": {}
        }"#;
        match a.parse_server_event(s).unwrap() {
            ServerEventHint::TaskFailed { code, message } => {
                assert_eq!(code.as_deref(), Some("InvalidParameter"));
                assert_eq!(message.as_deref(), Some("boom"));
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn parse_event_unknown_returns_other() {
        let a = make("qwen-audio-3.0-tts-flash", "v");
        let s = r#"{"header": {"task_id":"abc","event":"mystery","attributes":{}}}"#;
        assert_eq!(a.parse_server_event(s).unwrap(), ServerEventHint::Other);
    }

    #[test]
    fn parse_event_malformed_returns_err() {
        let a = make("qwen-audio-3.0-tts-flash", "v");
        assert!(a.parse_server_event("not json").is_err());
    }
}
