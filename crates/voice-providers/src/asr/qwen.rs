//! Qwen-Audio-3.0-ASR-Flash-Streaming / Fun-ASR-Realtime / Qwen-Paraformer-realtime
//! 适配层 —— DashScope 公共 WebSocket 协议（JSON 文本 + 裸 PCM binary）
//!
//! 协议概览（参考 `crates/voice-providers/docs/qwen-asr-docs/qwen-asr-ws-api.md`）：
//!
//! | 方向            | 通道   | 内容                         | 对应线上形态              |
//! | --------------- | ------ | ---------------------------- | ------------------------- |
//! | C → S run-task  | Text   | `header.action=run-task` JSON | `GaxFrame::text`          |
//! | C → S audio     | Binary | 原始 PCM 字节流              | `GaxFrame::raw_binary`    |
//! | C → S finish    | Text   | `header.action=finish-task` JSON | `GaxFrame::text`      |
//! | S → C task-started     | Text | `{header:{action:task-started}, payload:{task_id}}` | 已 wrap 的 GaxFrame |
//! | S → C result-generated | Text | `{header:{action:result-generated}, payload:{output:{sentence:{text,sentence_end}}}}` | 由 `parse_event` 解析 |
//! | S → C task-finished    | Text | `{header:{action:task-finished}, payload:{task_id}}` | `parse_event` 返回 Ok(None) |
//! | S → C task-failed      | Text | `{header:{action:task-failed, error_message}, payload:{task_id}}` | `parse_event` 返回 Err |
//!
//! 与 `paraformer.rs` 的关键差异：
//! 1. 控制面是 JSON 文本（不是 GAX 4-byte 长度前缀的 protobuf）。
//! 2. 音频是裸 PCM（不是 GAX 包装的 AsrAudioChunk）。
//! 3. 服务端 transcript 字段路径在 `payload.output.sentence.text`，而非顶层 `payload.text`。
//! 4. 终判条件由 `payload.output.sentence.sentence_end` 决定，而非 `is_final` 字段。
//!
//! ## 复用细节
//!
//! `open_request` / `audio_frame` / `stop_frame` 仍返回 `GaxFrame`，但带 `wire` 标记：
//! - `GaxFrame::text(cmd, json_bytes)` —— 控制面 JSON
//! - `GaxFrame::raw_binary(cmd, pcm)` —— 音频
//!
//! 服务端响应在 `TungsteniteWs::recv_frame` 中已被包成 `GaxFrame::text(RESP_TRANSCRIPT, ...)`，
//! `parse_event` 解析 JSON 后根据 `header.action` 区分事件类型。

use serde::Deserialize;

use crate::asr::{AsrEvent, AsrModelAdapter, ClientError};
use crate::codec::{
    GaxFrame, REQ_AUDIO_ASR, REQ_OPEN_ASR, REQ_STOP_ASR,
};

// ===== 配置 =====

/// Qwen 系列实时识别 adapter。支持的模型：
///
/// - `qwen-audio-3.0-asr-flash-streaming`（推荐）
/// - `fun-asr-realtime` / `fun-asr-realtime-2025-11-07` / `fun-asr-realtime-2026-02-28` 等快照版
/// - `fun-asr-flash-8k-realtime` / `fun-asr-flash-8k-realtime-2026-01-28`（8kHz 电话场景）
/// - `paraformer-realtime-v2` / `paraformer-realtime-v1` / `paraformer-realtime-8k-v2` / `paraformer-realtime-8k-v1`
pub struct QwenAsrAdapter {
    /// 实际发给服务端的 model 字段（如 `qwen-audio-3.0-asr-flash-streaming`）
    model: &'static str,
    /// 给上层 `AsrModelAdapter::model_name` 返回的对外名称
    canonical: &'static str,
    /// 音频格式：默认 `pcm`（实时推荐）
    format: &'static str,
    /// `payload.parameters.max_sentence_silence`（VAD 断句静音阈值 ms）。None=服务端默认 800
    max_sentence_silence: Option<u32>,
    /// 是否开启 ITN（数字、日期等转写，如 `123` → `一百二十三`）
    enable_inverse_text_normalization: bool,
    /// 是否开启语义断句（开启后不再返回 `emo_tag` / `emo_confidence`）
    semantic_punctuation_enabled: bool,
}

impl QwenAsrAdapter {
    /// 按 model 名挑预设；采样率 / 声道由 model 自带决定，AudioFormat.sample_rate 走 AsrCfg
    pub fn for_model(model: &str) -> Self {
        match model {
            // 8kHz 系列：sample_rate 必须 8000
            "fun-asr-flash-8k-realtime" | "fun-asr-flash-8k-realtime-2026-01-28" => Self {
                model: "fun-asr-flash-8k-realtime",
                canonical: "fun-asr-flash-8k-realtime",
                format: "pcm",
                max_sentence_silence: Some(800),
                enable_inverse_text_normalization: false,
                semantic_punctuation_enabled: false,
            },
            // Fun-ASR 16kHz 系列
            "fun-asr-realtime"
            | "fun-asr-realtime-2025-11-07"
            | "fun-asr-realtime-2026-02-28"
            | "fun-asr-realtime-2025-09-15" => Self {
                model: "fun-asr-realtime",
                canonical: "fun-asr-realtime",
                format: "pcm",
                max_sentence_silence: Some(800),
                enable_inverse_text_normalization: false,
                semantic_punctuation_enabled: false,
            },
            // Qwen-Audio 3.0 主力
            "qwen-audio-3.0-asr-flash-streaming" | "qwen-audio-3.0" => Self {
                model: "qwen-audio-3.0-asr-flash-streaming",
                canonical: "qwen-audio-3.0-asr-flash-streaming",
                format: "pcm",
                max_sentence_silence: Some(800),
                enable_inverse_text_normalization: false,
                semantic_punctuation_enabled: false,
            },
            // Paraformer 16kHz
            "paraformer-realtime-v2" | "paraformer-realtime-v1" => Self {
                model: "paraformer-realtime-v2",
                canonical: "paraformer-realtime-v2",
                format: "pcm",
                max_sentence_silence: Some(800),
                enable_inverse_text_normalization: false,
                // paraformer 默认 semantic_punctuation_enabled=false 以保留情感字段
                // 见 real-asr-code.md#情感识别
                semantic_punctuation_enabled: false,
            },
            // Paraformer 8kHz（带情感识别）
            "paraformer-realtime-8k-v2" | "paraformer-realtime-8k-v1" => Self {
                model: "paraformer-realtime-8k-v2",
                canonical: "paraformer-realtime-8k-v2",
                format: "pcm",
                max_sentence_silence: Some(800),
                enable_inverse_text_normalization: false,
                // 8k v2 情感识别要求 semantic_punctuation_enabled = false
                semantic_punctuation_enabled: false,
            },
            // fallthrough：best-effort
            _ => Self {
                model: "qwen-audio-3.0-asr-flash-streaming",
                canonical: "qwen-audio-3.0-asr-flash-streaming",
                format: "pcm",
                max_sentence_silence: Some(800),
                enable_inverse_text_normalization: false,
                semantic_punctuation_enabled: false,
            },
        }
    }
}

// ===== JSON 形状 =====

/// run-task / finish-task 顶层 envelope：`{header, payload}`
#[derive(Debug, Serialize)]
struct WsEnvelope<'a> {
    header: WsHeader<'a>,
    payload: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct WsHeader<'a> {
    action: &'a str,
    task_id: &'a str,
    /// `duplex` = 全双工（边发边收）；`half-duplex` = 半双工（发完再收，默认）。
    /// 实时推荐 `duplex`，与 Python/Java SDK 一致。
    streaming: &'a str,
}

/// run-task payload parameters
#[derive(Debug, Serialize)]
struct RunParams {
    sample_rate: u32,
    format: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_sentence_silence: Option<u32>,
    semantic_punctuation_enabled: bool,
    enable_inverse_text_normalization: bool,
}

// ===== 服务端事件 JSON 解析 =====

#[derive(Debug, Deserialize)]
struct ServerEvent {
    header: ServerHeader,
    payload: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ServerHeader {
    action: String,
    #[serde(default)]
    #[allow(dead_code)]
    task_id: String,
    #[serde(default)]
    #[allow(dead_code)]
    event_id: String,
    #[serde(default)]
    error_code: Option<String>,
    #[serde(default)]
    error_message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ResultPayload {
    /// 嵌套结构：`payload.output.sentence.{text, sentence_end, ...}`
    #[serde(default)]
    output: Option<OutputBlock>,
}

#[derive(Debug, Deserialize)]
struct OutputBlock {
    #[serde(default)]
    sentence: Option<Sentence>,
}

#[derive(Debug, Deserialize)]
struct Sentence {
    #[serde(default)]
    text: String,
    #[serde(default)]
    sentence_end: bool,
    #[serde(default)]
    #[allow(dead_code)]
    begin_time: Option<i64>,
    #[serde(default)]
    #[allow(dead_code)]
    end_time: Option<i64>,
}

// ===== 序列化所需 trait =====

use serde::Serialize;

fn build_run_task(
    model: &str,
    task_id: &str,
    _sample_rate: u32,
    _channels: u16,
    params: &RunParams,
) -> Result<Vec<u8>, ClientError> {
    let value = serde_json::json!({
        "task_group": "audio",
        "task": "asr",
        "function": "recognition",
        "model": model,
        "parameters": params,
        "input": {},
    });
    let envelope = WsEnvelope {
        header: WsHeader {
            action: "run-task",
            task_id,
            streaming: "duplex",
        },
        payload: value,
    };
    serde_json::to_vec(&envelope).map_err(|e| ClientError::Decode(format!("encode run-task: {}", e)))
}

fn build_finish_task(task_id: &str) -> Result<Vec<u8>, ClientError> {
    let envelope = WsEnvelope {
        header: WsHeader {
            action: "finish-task",
            task_id,
            streaming: "duplex",
        },
        payload: serde_json::json!({ "input": {} }),
    };
    serde_json::to_vec(&envelope)
        .map_err(|e| ClientError::Decode(format!("encode finish-task: {}", e)))
}

// ===== Adapter 实现 =====

impl AsrModelAdapter for QwenAsrAdapter {
    fn model_name(&self) -> &'static str {
        self.canonical
    }

    fn open_request(&self, session_id: &str, sr: u32, ch: u16) -> GaxFrame {
        let params = RunParams {
            sample_rate: sr,
            format: self.format,
            max_sentence_silence: self.max_sentence_silence,
            semantic_punctuation_enabled: self.semantic_punctuation_enabled,
            enable_inverse_text_normalization: self.enable_inverse_text_normalization,
        };
        let payload = build_run_task(self.model, session_id, sr, ch, &params)
            .expect("encode run-task JSON");
        GaxFrame::text(REQ_OPEN_ASR, payload)
    }

    fn audio_frame(&self, pcm: &[u8]) -> GaxFrame {
        // 持续 PCM 数据流直接作为 binary 帧发出（无 GAX 长度前缀）
        GaxFrame::raw_binary(REQ_AUDIO_ASR, pcm.to_vec())
    }

    fn stop_frame(&self, session_id: &str) -> GaxFrame {
        // Qwen 协议要求 finish-task 与 run-task 使用相同 task_id。
        // session_id 由 StreamingAsrClient 透传过来（即 task_id 直接复用）。
        let payload = build_finish_task(session_id)
            .expect("encode finish-task JSON");
        GaxFrame::text(REQ_STOP_ASR, payload)
    }

    fn parse_event(&self, payload: &[u8]) -> Result<Option<AsrEvent>, ClientError> {
        // 服务端返回 JSON 文本。已知有四种 action：
        //   task-started / task-finished / task-failed / result-generated
        let event: ServerEvent = match serde_json::from_slice(payload) {
            Ok(e) => e,
            // 非法 JSON 视为非 transcript 帧（其它帧类型也会进 parse_event）
            Err(_) => return Ok(None),
        };

        match event.header.action.as_str() {
            "task-started" | "task-finished" => {
                // 控制消息，不产生 AsrEvent
                Ok(None)
            }
            "task-failed" => {
                let code = event.header.error_code.unwrap_or_default();
                let msg = event.header.error_message.unwrap_or_default();
                Err(ClientError::Decode(format!(
                    "task-failed: code={} message={}",
                    code, msg
                )))
            }
            "result-generated" => {
                let pld = event.payload.ok_or_else(|| {
                    ClientError::Decode("result-generated missing payload".into())
                })?;
                let parsed: ResultPayload = serde_json::from_value(pld).map_err(|e| {
                    ClientError::Decode(format!("decode result-generated payload: {}", e))
                })?;
                let sentence = parsed
                    .output
                    .and_then(|o| o.sentence)
                    .ok_or_else(|| ClientError::Decode("missing output.sentence".into()))?;
                Ok(Some(AsrEvent {
                    text: sentence.text,
                    is_final: sentence.sentence_end,
                }))
            }
            // 其它 action（罕见）：忽略，不报错
            _ => Ok(None),
        }
    }
}

// ===== 单元测试 =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{RESP_TRANSCRIPT, WireFormat};

    #[test]
    fn canonical_name_matches_for_supported_models() {
        assert_eq!(
            QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming").model_name(),
            "qwen-audio-3.0-asr-flash-streaming"
        );
        assert_eq!(
            QwenAsrAdapter::for_model("fun-asr-realtime").model_name(),
            "fun-asr-realtime"
        );
        assert_eq!(
            QwenAsrAdapter::for_model("fun-asr-realtime-2026-02-28").model_name(),
            "fun-asr-realtime"
        );
        assert_eq!(
            QwenAsrAdapter::for_model("paraformer-realtime-v2").model_name(),
            "paraformer-realtime-v2"
        );
        assert_eq!(
            QwenAsrAdapter::for_model("paraformer-realtime-8k-v2").model_name(),
            "paraformer-realtime-8k-v2"
        );
    }

    #[test]
    fn open_request_emits_json_text_frame() {
        let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
        let frame = adapter.open_request("task-abc", 16000, 1);

        assert_eq!(frame.cmd, REQ_OPEN_ASR);
        assert_eq!(frame.wire, WireFormat::Text);

        let v: serde_json::Value = serde_json::from_slice(&frame.payload).unwrap();
        assert_eq!(v["header"]["action"], "run-task");
        assert_eq!(v["header"]["task_id"], "task-abc");
        assert_eq!(v["header"]["streaming"], "duplex");
        assert_eq!(v["payload"]["model"], "qwen-audio-3.0-asr-flash-streaming");
        assert_eq!(v["payload"]["parameters"]["sample_rate"], 16000);
        assert_eq!(v["payload"]["parameters"]["format"], "pcm");
        assert_eq!(v["payload"]["parameters"]["max_sentence_silence"], 800);
        assert_eq!(
            v["payload"]["parameters"]["semantic_punctuation_enabled"],
            false
        );
        assert_eq!(v["payload"]["task_group"], "audio");
        assert_eq!(v["payload"]["task"], "asr");
        assert_eq!(v["payload"]["function"], "recognition");
    }

    #[test]
    fn audio_frame_emits_raw_binary() {
        let adapter = QwenAsrAdapter::for_model("fun-asr-realtime");
        let pcm: Vec<u8> = (0..64).collect();
        let frame = adapter.audio_frame(&pcm);

        assert_eq!(frame.cmd, REQ_AUDIO_ASR);
        assert_eq!(frame.wire, WireFormat::RawBinary);
        assert_eq!(frame.payload, pcm);
    }

    #[test]
    fn stop_frame_emits_json_text() {
        let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
        let frame = adapter.stop_frame("task-abc");

        assert_eq!(frame.cmd, REQ_STOP_ASR);
        assert_eq!(frame.wire, WireFormat::Text);
        let v: serde_json::Value = serde_json::from_slice(&frame.payload).unwrap();
        assert_eq!(v["header"]["action"], "finish-task");
        assert_eq!(v["header"]["task_id"], "task-abc");
        assert_eq!(v["header"]["streaming"], "duplex");
    }

    #[test]
    fn parse_event_result_generated_partial() {
        let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
        let evt = serde_json::json!({
            "header": {
                "action": "result-generated",
                "task_id": "task-abc",
                "event_id": "evt-1"
            },
            "payload": {
                "output": {
                    "sentence": {
                        "text": "你好",
                        "sentence_end": false,
                        "begin_time": 0,
                        "end_time": 320,
                        "words": []
                    }
                }
            }
        });
        let bytes = serde_json::to_vec(&evt).unwrap();
        let out = adapter.parse_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "你好");
        assert!(!out.is_final);
    }

    #[test]
    fn parse_event_result_generated_final() {
        let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
        let evt = serde_json::json!({
            "header": {
                "action": "result-generated",
                "task_id": "task-abc",
                "event_id": "evt-2"
            },
            "payload": {
                "output": {
                    "sentence": {
                        "text": "你好世界。",
                        "sentence_end": true,
                        "begin_time": 0,
                        "end_time": 720,
                        "words": [
                            {"text": "你好", "begin_time": 0, "end_time": 320},
                            {"text": "世界", "begin_time": 320, "end_time": 720}
                        ]
                    }
                }
            }
        });
        let bytes = serde_json::to_vec(&evt).unwrap();
        let out = adapter.parse_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "你好世界。");
        assert!(out.is_final);
    }

    #[test]
    fn parse_event_task_started_returns_none() {
        let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
        let evt = serde_json::json!({
            "header": {
                "action": "task-started",
                "task_id": "task-abc",
                "event_id": "evt-0"
            },
            "payload": {"task_id": "task-abc"}
        });
        let bytes = serde_json::to_vec(&evt).unwrap();
        assert!(adapter.parse_event(&bytes).unwrap().is_none());
    }

    #[test]
    fn parse_event_task_finished_returns_none() {
        let adapter = QwenAsrAdapter::for_model("fun-asr-realtime");
        let evt = serde_json::json!({
            "header": {
                "action": "task-finished",
                "task_id": "task-abc",
                "event_id": "evt-9"
            },
            "payload": {"task_id": "task-abc"}
        });
        let bytes = serde_json::to_vec(&evt).unwrap();
        assert!(adapter.parse_event(&bytes).unwrap().is_none());
    }

    #[test]
    fn parse_event_task_failed_returns_err() {
        let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
        let evt = serde_json::json!({
            "header": {
                "action": "task-failed",
                "task_id": "task-abc",
                "event_id": "evt-err",
                "error_code": "BAD_AUDIO_FORMAT",
                "error_message": "sample rate mismatch"
            },
            "payload": {"task_id": "task-abc"}
        });
        let bytes = serde_json::to_vec(&evt).unwrap();
        let r = adapter.parse_event(&bytes);
        assert!(r.is_err());
        let err = r.unwrap_err();
        match err {
            ClientError::Decode(msg) => {
                assert!(msg.contains("BAD_AUDIO_FORMAT"));
                assert!(msg.contains("sample rate mismatch"));
            }
            _ => panic!("expected Decode error"),
        }
    }

    #[test]
    fn parse_event_with_error_in_payload() {
        // 业务错误内嵌在 result-generated payload 中（旧 docs 形式）
        let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
        let evt = serde_json::json!({
            "header": {
                "action": "result-generated",
                "task_id": "task-abc",
                "event_id": "evt-err"
            },
            "payload": {
                "text": "",
                "is_final": false,
                "error_code": "QUOTA_EXCEEDED",
                "error_message": "balance insufficient"
            }
        });
        let bytes = serde_json::to_vec(&evt).unwrap();
        // output.sentence 缺失时按 result-generated 协议应返回 Decode 错误
        let r = adapter.parse_event(&bytes);
        assert!(r.is_err());
    }

    #[test]
    fn parse_event_invalid_json_returns_none() {
        let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
        let r = adapter.parse_event(b"not json at all");
        assert!(r.is_ok());
        assert!(r.unwrap().is_none());
    }

    #[test]
    fn wrong_cmd_text_response_still_parses() {
        // 即便 recv_frame 误把 cmd 传成 RESP_TRANSCRIPT 之类，parse_event 也应通过 JSON 解析识别
        let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
        let evt = serde_json::json!({
            "header": {"action": "result-generated", "task_id": "x", "event_id": "e"},
            "payload": {"output": {"sentence": {"text": "ok", "sentence_end": true}}}
        });
        let bytes = serde_json::to_vec(&evt).unwrap();
        let out = adapter.parse_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "ok");
        assert!(out.is_final);
    }

    #[test]
    fn resp_transcript_marker_in_parse_event() {
        // 防止误把 RESP_TRANSCRIPT 字节值写到 parse_event 路径（占位 cmd 应被忽略）
        let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
        let evt = serde_json::json!({
            "header": {"action": "result-generated", "task_id": "x", "event_id": "e"},
            "payload": {"output": {"sentence": {"text": "hi", "sentence_end": false}}}
        });
        let bytes = serde_json::to_vec(&evt).unwrap();
        let out = adapter.parse_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "hi");
        assert!(!out.is_final);
        // 验证 RESP_TRANSCRIPT 常量值（防回归）
        assert_eq!(RESP_TRANSCRIPT, 0x12);
    }
}
