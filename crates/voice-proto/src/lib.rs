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
    },
    SessionEnd {
        session_id: String,
        reason: String,
    },
    /// 用户打断当前 TTS 播放（半双工 → 重新 Listening）
    Interrupt {
        session_id: String,
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
    },
    LlmDelta {
        session_id: String,
        delta: String,
        is_final: bool,
    },
    TtsAudio {
        session_id: String,
        seq: u32,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
        is_last: bool,
    },
    Error {
        code: u32,
        message: String,
    },
}

impl VoicePayload {
    /// 取出 `session_id`（如有）。用于日志/路由。
    pub fn session_id(&self) -> Option<&str> {
        match self {
            VoicePayload::SessionStart { session_id, .. }
            | VoicePayload::SessionEnd { session_id, .. }
            | VoicePayload::Interrupt { session_id }
            | VoicePayload::AudioChunk { session_id, .. }
            | VoicePayload::AsrPartial { session_id, .. }
            | VoicePayload::LlmDelta { session_id, .. }
            | VoicePayload::TtsAudio { session_id, .. }
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
            VoicePayload::AudioChunk { session_id, seq, data, .. } => {
                assert_eq!(session_id, "abc");
                assert_eq!(seq, 1);
                assert_eq!(data, vec![1, 2, 3, 4]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn session_id_accessor() {
        let p = VoicePayload::SessionStart {
            session_id: "x".into(),
            sample_rate: 16000,
            channels: 1,
            codec: "pcm".into(),
            language: "zh-CN".into(),
        };
        assert_eq!(p.session_id(), Some("x"));

        let e = VoicePayload::Error { code: 1, message: "x".into() };
        assert_eq!(e.session_id(), None);
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
            VoicePayload::SessionAck { session_id, success, message } => {
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
            VoicePayload::SessionAck { session_id, success, message } => {
                assert_eq!(session_id, "abc");
                assert!(!success);
                assert_eq!(message, "missing api_key");
            }
            _ => panic!("wrong variant"),
        }
    }
}