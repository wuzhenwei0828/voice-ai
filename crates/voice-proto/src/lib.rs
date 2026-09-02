//! voice-proto: 语音链路业务数据类型 + 编解码辅助
//!
//! 定义 `VoicePayload` 枚举（上行音频 / 下行 ASR/LLM/TTS 流式结果 / 控制信令），
//! 并把它接进 webproto 的 `Message<VoicePayload>` 信封里。
//!
//! 用法：
//! ```ignore
//! use voice_proto::{VoicePayload, encode_indication, decode_payload};
//!
//! // 客户端发音频
//! let bytes = encode_indication(&VoicePayload::AudioChunk { ... })?;
//!
//! // 服务端收到
//! let msg: webproto::Message<VoicePayload> = webproto::decode_message(&bytes)?;
//! match msg {
//!     webproto::Message::Indication(i) => { /* i.data */ }
//!     _ => {}
//! }
//! ```

use serde::{Deserialize, Serialize};

/// 服务端向客户端报告的语音智能体处理阶段。
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum AgentPhase {
    Listening,
    Transcribing,
    Searching,
    Composing,
    Speaking,
    Error,
}

/// 语音链路所有消息的统一业务数据类型。
/// 客户端/服务端在 WS 二进制帧里就传这个。
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VoicePayload {
    // ---- 会话控制 ----
    SessionStart {
        session_id: String,
        sample_rate: u32,
        channels: u8,
        codec: String,
        language: String,
        /// **TTS 输出采样率（Hz）** —— 由端侧（浏览器）按 AudioContext 实际能力上报。
        /// 服务端 `tts.sample_rate` 配置项只做兜底：端侧没传（None / 0）时使用配置值。
        /// `#[serde(default)]` 兼容旧客户端 —— 不带这字段的 SessionStart 仍能解码。
        #[serde(default)]
        tts_sample_rate: Option<u32>,
        /// **TTS 音色短名**（如 `"alex"`）—— 由端侧（前端 voice 下拉）选好后带过来。
        /// 服务端 `tts.voice` 配置项只做兜底：端侧没传（None / 空字符串）时使用配置值。
        /// `#[serde(default)]` 兼容旧客户端。
        #[serde(default)]
        voice: Option<String>,
    },
    SessionEnd {
        session_id: String,
        reason: String,
    },
    /// 用户打断当前 TTS 播放（半双工 → 重新 Listening）
    Interrupt {
        session_id: String,
    },
    /// 重放本会话最近一次有效语音请求。服务端为其分配新的 request_id。
    Retry {
        session_id: String,
    },
    /// 客户端在实际开始播放首个可播放 PCM chunk 后回传的相对时延。
    PlaybackStarted {
        session_id: String,
        request_id: u64,
        /// 从服务端首个 TTS 音频到客户端开始播放的毫秒数，服务端限制最大 30 秒。
        delay_ms: u64,
    },

    // ---- 下行：服务端 → 客户端（握手 ack）----
    /// 服务端处理 `SessionStart` 后返回的握手 ack。
    /// 客户端应等收到 `success=true` 的 ack 再开始推 PCM；
    /// `success=false` 时附 `message` 描述失败原因（缺 endpoint / 上游 WSS 失败 / 等）。
    SessionAck {
        session_id: String,
        success: bool,
        message: String,
    },

    // ---- 上行：客户端 → 服务端（音频流）----
    // data 用 serde_bytes 走 msgpack bin8/16/32，比 Vec<u8> 默认的 "array of u8s" 节省 ~2x 体积。
    // 通过 #[serde(with = "serde_bytes")] 包裹即可，无须改字段类型，调用方仍可用 Vec<u8>。
    AudioChunk {
        session_id: String,
        seq: u32,
        timestamp_ms: u64,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
        is_last: bool,
    },

    // ---- 下行：服务端 → 客户端（流式结果）----
    AsrPartial {
        session_id: String,
        /// incremental 增量（FunASR 累积文本 → server 端转 delta 后下发）。
        /// `is_final=true` 时若 `replace_last=false`：携带整个句子文本，作为新一行 final 提交。
        /// `is_final=true` 且 `replace_last=true`：携带 2pass-offline 二次纠错结果，替换最近一条 final 行（不要新增）。
        text: String,
        is_final: bool,
        /// 2pass-offline 二次纠错专用：true 表示用 text 替换最近一条 final 行（不要 append 新行）。
        /// 其它情况为 false。#[serde(default)] 保证旧客户端 / 旧消息可正常解析。
        #[serde(default)]
        replace_last: bool,
        /// 关联一轮用户请求；旧客户端不带该字段时按 0 处理。
        #[serde(default)]
        request_id: u64,
    },
    LlmDelta {
        session_id: String,
        delta: String,
        is_final: bool,
        /// 关联一轮用户请求；旧客户端不带该字段时按 0 处理。
        #[serde(default)]
        request_id: u64,
    },
    TtsAudio {
        session_id: String,
        seq: u32,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
        is_last: bool,
        /// 当前音频 chunk 的采样率（Hz）。旧服务端未提供时为 None。
        #[serde(default)]
        sample_rate: Option<u32>,
        /// 当前音频 chunk 的声道数。旧服务端未提供时为 None。
        #[serde(default)]
        channels: Option<u8>,
        /// 关联一轮用户请求；旧客户端不带该字段时按 0 处理。
        #[serde(default)]
        request_id: u64,
    },

    // ---- 下行：服务端 → 客户端（智能体阶段状态）----
    AgentStatus {
        session_id: String,
        phase: AgentPhase,
        label: String,
        #[serde(default)]
        tool: Option<String>,
        request_id: u64,
        done: bool,
    },
    Error {
        code: u32,
        message: String,
    },
}

impl VoicePayload {
    /// 返回线协议中的消息类型名，用于日志和指标，避免记录完整音频/文本正文。
    pub fn type_name(&self) -> &'static str {
        match self {
            VoicePayload::SessionStart { .. } => "session_start",
            VoicePayload::SessionEnd { .. } => "session_end",
            VoicePayload::Interrupt { .. } => "interrupt",
            VoicePayload::Retry { .. } => "retry",
            VoicePayload::PlaybackStarted { .. } => "playback_started",
            VoicePayload::SessionAck { .. } => "session_ack",
            VoicePayload::AudioChunk { .. } => "audio_chunk",
            VoicePayload::AsrPartial { .. } => "asr_partial",
            VoicePayload::LlmDelta { .. } => "llm_delta",
            VoicePayload::TtsAudio { .. } => "tts_audio",
            VoicePayload::AgentStatus { .. } => "agent_status",
            VoicePayload::Error { .. } => "error",
        }
    }

    /// 取出 `session_id`（如有）。用于日志/路由。
    pub fn session_id(&self) -> Option<&str> {
        match self {
            VoicePayload::SessionStart { session_id, .. }
            | VoicePayload::SessionEnd { session_id, .. }
            | VoicePayload::Interrupt { session_id }
            | VoicePayload::Retry { session_id }
            | VoicePayload::PlaybackStarted { session_id, .. }
            | VoicePayload::AudioChunk { session_id, .. }
            | VoicePayload::AsrPartial { session_id, .. }
            | VoicePayload::LlmDelta { session_id, .. }
            | VoicePayload::TtsAudio { session_id, .. }
            | VoicePayload::AgentStatus { session_id, .. }
            | VoicePayload::SessionAck { session_id, .. } => Some(session_id),
            VoicePayload::Error { .. } => None,
        }
    }
}

/// 把 VoicePayload 包成单向 Indication（音频上行、流式下行都用这个）
pub fn encode_indication(payload: &VoicePayload) -> anyhow::Result<Vec<u8>> {
    webproto::Indication::<VoicePayload>::encode(payload.clone())
        .map_err(|e| anyhow::anyhow!("encode voice indication failed: {:?}", e))
}

/// 把字节流解码成 `Message<VoicePayload>`，再按需拆出 `VoicePayload`
pub fn decode_payload(bytes: &[u8]) -> anyhow::Result<(PayloadKind, VoicePayload)> {
    let msg: webproto::Message<VoicePayload> = webproto::decode_message(&bytes.to_vec())?;
    let (kind, p) = match msg {
        webproto::Message::Indication(i) => (PayloadKind::Indication, i.data),
        webproto::Message::ClientCommand(c) => (PayloadKind::ClientCommand, c.command),
        webproto::Message::ServerCommand(c) => (PayloadKind::ServerCommand, c.command),
    };
    Ok((kind, p))
}

/// 从任意业务消息中提取客户端生成的消息链路 ID。
/// 旧客户端没有该字段时返回 None，保持协议向后兼容。
pub fn decode_message_id(bytes: &[u8]) -> Option<String> {
    let msg: webproto::Message<serde_json::Value> =
        webproto::decode_message(&bytes.to_vec()).ok()?;
    let payload = match msg {
        webproto::Message::Indication(indication) => indication.data,
        webproto::Message::ClientCommand(command) => command.command,
        webproto::Message::ServerCommand(command) => command.command,
    };
    payload
        .get("message_id")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    Indication,
    ClientCommand,
    ServerCommand,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_audio_chunk() {
        let p = VoicePayload::AudioChunk {
            session_id: "abc".into(),
            seq: 1,
            timestamp_ms: 1000,
            data: vec![1, 2, 3, 4],
            is_last: false,
        };
        let bytes = encode_indication(&p).unwrap();
        let (kind, decoded) = decode_payload(&bytes).unwrap();
        assert_eq!(kind, PayloadKind::Indication);
        match decoded {
            VoicePayload::AudioChunk {
                session_id,
                seq,
                data,
                ..
            } => {
                assert_eq!(session_id, "abc");
                assert_eq!(seq, 1);
                assert_eq!(data, vec![1, 2, 3, 4]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn round_trip_retry_request() {
        let payload = VoicePayload::Retry {
            session_id: "retry-session".to_string(),
        };

        let bytes = encode_indication(&payload).expect("retry should encode");
        let (kind, decoded) = decode_payload(&bytes).expect("retry should decode");

        assert_eq!(kind, PayloadKind::Indication);
        assert!(matches!(
            decoded,
            VoicePayload::Retry { session_id } if session_id == "retry-session"
        ));
    }

    #[test]
    fn session_id_accessor() {
        let p = VoicePayload::SessionStart {
            session_id: "x".into(),
            sample_rate: 16000,
            channels: 1,
            codec: "pcm".into(),
            language: "zh-CN".into(),
            tts_sample_rate: None,
            voice: None,
        };
        assert_eq!(p.session_id(), Some("x"));

        let e = VoicePayload::Error {
            code: 1,
            message: "x".into(),
        };
        assert_eq!(e.session_id(), None);
    }

    /// tts_sample_rate 缺省（None / 老客户端不发该字段）应能被 round-trip 解码
    #[test]
    fn session_start_omits_tts_sample_rate_round_trips() {
        // 编码端：None（模拟老客户端）
        let p = VoicePayload::SessionStart {
            session_id: "s".into(),
            sample_rate: 16000,
            channels: 1,
            codec: "pcm".into(),
            language: "zh".into(),
            tts_sample_rate: None,
            voice: None,
        };
        let bytes = encode_indication(&p).unwrap();
        let (_, decoded) = decode_payload(&bytes).unwrap();
        match decoded {
            VoicePayload::SessionStart {
                tts_sample_rate,
                voice,
                ..
            } => {
                assert_eq!(tts_sample_rate, None);
                assert_eq!(voice, None);
            }
            _ => panic!("wrong variant"),
        }

        // 验证 #[serde(default)] 在缺字段时也能容错 —— 直接用 rmp-serde 编码一份
        // 不带 tts_sample_rate / voice 字段的 VoicePayload，再解回，看容错是否生效
        // （注：Msgpack / JSON 在 serde derive 层都遵循同一个 #[serde(default)] 语义，
        //  所以即便 wire envelope 是 Message::Indication，serde 容错行为等价）
        #[derive(serde::Serialize)]
        struct OldWire<'a> {
            #[serde(rename = "type")]
            t: &'a str,
            session_id: &'a str,
            sample_rate: u32,
            channels: u8,
            codec: &'a str,
            language: &'a str,
            // 注意：故意不带 tts_sample_rate / voice —— 模拟老客户端字节
        }
        let old = OldWire {
            t: "session_start",
            session_id: "s",
            sample_rate: 16000,
            channels: 1,
            codec: "pcm",
            language: "zh",
        };
        let raw = rmp_serde::to_vec_named(&old).unwrap();
        let v: VoicePayload = rmp_serde::from_slice(&raw).unwrap();
        match v {
            VoicePayload::SessionStart {
                tts_sample_rate,
                voice,
                ..
            } => {
                assert_eq!(
                    tts_sample_rate, None,
                    "缺省字段应通过 #[serde(default)] 解为 None"
                );
                assert_eq!(voice, None, "缺省字段应通过 #[serde(default)] 解为 None");
            }
            _ => panic!("wrong variant"),
        }
    }

    /// tts_sample_rate = Some(N) 应能正常 round-trip
    #[test]
    fn session_start_with_tts_sample_rate_round_trips() {
        let p = VoicePayload::SessionStart {
            session_id: "s".into(),
            sample_rate: 16000,
            channels: 1,
            codec: "pcm".into(),
            language: "zh".into(),
            tts_sample_rate: Some(24000),
            voice: Some("alex".into()),
        };
        let bytes = encode_indication(&p).unwrap();
        let (_, decoded) = decode_payload(&bytes).unwrap();
        match decoded {
            VoicePayload::SessionStart {
                tts_sample_rate,
                voice,
                ..
            } => {
                assert_eq!(tts_sample_rate, Some(24000));
                assert_eq!(voice.as_deref(), Some("alex"));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn tts_audio_format_fields_round_trip() {
        let payload = VoicePayload::TtsAudio {
            session_id: "s".into(), seq: 1, data: vec![1, 2], is_last: false,
            sample_rate: Some(24_000), channels: Some(1), request_id: 2,
        };
        let (_, decoded) = decode_payload(&encode_indication(&payload).unwrap()).unwrap();
        match decoded { VoicePayload::TtsAudio { sample_rate, channels, .. } => { assert_eq!(sample_rate, Some(24_000)); assert_eq!(channels, Some(1)); }, _ => panic!("wrong variant") }
    }

    #[test]
    fn round_trip_session_ack() {
        let p = VoicePayload::SessionAck {
            session_id: "abc".into(),
            success: true,
            message: String::new(),
        };
        let bytes = encode_indication(&p).unwrap();
        let (_kind, decoded) = decode_payload(&bytes).unwrap();
        match decoded {
            VoicePayload::SessionAck {
                session_id,
                success,
                message,
            } => {
                assert_eq!(session_id, "abc");
                assert!(success);
                assert_eq!(message, "");
            }
            _ => panic!("wrong variant"),
        }

        // 失败 ack
        let p_fail = VoicePayload::SessionAck {
            session_id: "abc".into(),
            success: false,
            message: "missing api_key".into(),
        };
        let bytes = encode_indication(&p_fail).unwrap();
        let (_kind, decoded) = decode_payload(&bytes).unwrap();
        match decoded {
            VoicePayload::SessionAck {
                session_id,
                success,
                message,
            } => {
                assert_eq!(session_id, "abc");
                assert!(!success);
                assert_eq!(message, "missing api_key");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn round_trip_agent_status() {
        let payload = VoicePayload::AgentStatus {
            session_id: "s".into(),
            phase: AgentPhase::Searching,
            label: "正在查资料".into(),
            tool: Some("knowledge_search".into()),
            request_id: 7,
            done: false,
        };
        let bytes = encode_indication(&payload).unwrap();
        let (kind, decoded) = decode_payload(&bytes).unwrap();
        assert_eq!(kind, PayloadKind::Indication);
        match decoded {
            VoicePayload::AgentStatus {
                session_id,
                phase,
                label,
                tool,
                request_id,
                done,
            } => {
                assert_eq!(session_id, "s");
                assert!(matches!(phase, AgentPhase::Searching));
                assert_eq!(label, "正在查资料");
                assert_eq!(tool.as_deref(), Some("knowledge_search"));
                assert_eq!(request_id, 7);
                assert!(!done);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn round_trip_playback_started() {
        let payload = VoicePayload::PlaybackStarted {
            session_id: "s".into(),
            request_id: 7,
            delay_ms: 125,
        };
        let (_, decoded) = decode_payload(&encode_indication(&payload).unwrap()).unwrap();
        match decoded {
            VoicePayload::PlaybackStarted {
                session_id,
                request_id,
                delay_ms,
            } => {
                assert_eq!(session_id, "s");
                assert_eq!(request_id, 7);
                assert_eq!(delay_ms, 125);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn legacy_stream_event_without_request_id_decodes() {
        #[derive(serde::Serialize)]
        struct OldWire<'a> {
            #[serde(rename = "type")]
            t: &'a str,
            session_id: &'a str,
            delta: &'a str,
            is_final: bool,
        }
        let raw = rmp_serde::to_vec_named(&OldWire {
            t: "llm_delta",
            session_id: "s",
            delta: "你好",
            is_final: true,
        })
        .unwrap();
        let decoded: VoicePayload = rmp_serde::from_slice(&raw).unwrap();
        match decoded {
            VoicePayload::LlmDelta { request_id, .. } => assert_eq!(request_id, 0),
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn extracts_message_id_from_indication_payload() {
        let payload = serde_json::json!({
            "type": "interrupt",
            "session_id": "s",
            "message_id": "550e8400-e29b-41d4-a716-446655440000",
        });
        let bytes = webproto::Indication::<serde_json::Value>::encode(payload).unwrap();
        assert_eq!(decode_message_id(&bytes).as_deref(), Some("550e8400-e29b-41d4-a716-446655440000"));
    }

    #[test]
    fn exposes_protocol_type_name_without_serializing_payload() {
        let payload = VoicePayload::Error {
            code: 500,
            message: "provider error".into(),
        };
        assert_eq!(payload.type_name(), "error");
    }
}
