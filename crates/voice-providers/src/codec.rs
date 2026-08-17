//! GAX wire codec: 4-byte BE u32 length-prefix || cmd_byte || protobuf_payload
//!
//! DashScope GAX (Ganymede Audio eXchange) 二进制帧格式（基于官方文档与抓包推断）：
//!   offset 0..4:  payload 长度（大端 u32，包含 cmd 字节 + protobuf payload）
//!   offset 4   :  cmd byte（命令码，区分请求/响应类型）
//!   offset 5..  :  protobuf 序列化的消息字节
//!
//! ## ⚠️ cmd byte 暂定表（待用 wscat 抓真实握手后校准）
//!
//! ```text
//! REQ_OPEN_ASR    = 0x01  // 客户端发起 ASR 会话
//! REQ_AUDIO_ASR   = 0x02  // 客户端发送音频分片
//! REQ_STOP_ASR    = 0x03  // 客户端结束 ASR 会话
//! RESP_OPEN_ASR   = 0x11  // 服务端确认 ASR 会话建立
//! RESP_TRANSCRIPT = 0x12  // 服务端返回转写结果（中间或最终）
//! RESP_ERR_ASR    = 0x13  // 服务端返回 ASR 错误
//! REQ_OPEN_TTS    = 0x21  // 客户端发起 TTS 会话
//! REQ_TEXT_TTS    = 0x22  // 客户端发送待合成文本
//! REQ_STOP_TTS    = 0x23  // 客户端结束 TTS 会话
//! RESP_OPEN_TTS   = 0x31  // 服务端确认 TTS 会话建立
//! RESP_AUDIO_TTS  = 0x32  // 服务端返回音频分片
//! RESP_DONE_TTS   = 0x33  // 服务端通知 TTS 完成
//! RESP_ERR_TTS    = 0x34  // 服务端返回 TTS 错误
//! ```
//!
//! 这些 cmd 值是基于通用 GAX 模式的合理默认；与 DashScope 真实字节是否一致需抓包核对。
//! 如果不一致，所有调用方只需替换此文件中的常量即可，无需改其它模块。

// ===== cmd byte 常量 =====
pub const REQ_OPEN_ASR: u8 = 0x01;
pub const REQ_AUDIO_ASR: u8 = 0x02;
pub const REQ_STOP_ASR: u8 = 0x03;
pub const RESP_OPEN_ASR: u8 = 0x11;
pub const RESP_TRANSCRIPT: u8 = 0x12;
pub const RESP_ERR_ASR: u8 = 0x13;

pub const REQ_OPEN_TTS: u8 = 0x21;
pub const REQ_TEXT_TTS: u8 = 0x22;
pub const REQ_STOP_TTS: u8 = 0x23;
pub const RESP_OPEN_TTS: u8 = 0x31;
pub const RESP_AUDIO_TTS: u8 = 0x32;
pub const RESP_DONE_TTS: u8 = 0x33;
pub const RESP_ERR_TTS: u8 = 0x34;

// ===== 帧结构 =====

/// 帧在 WebSocket 上的物理形态。
///
/// 现有 GAX 协议（占位/Paraformer）使用 `BinaryGax` 形态：4-byte BE u32 长度前缀 + cmd + payload。
/// 部分模型（Qwen-Audio-3.0-ASR-Flash-Streaming / Fun-ASR / Qwen-Paraformer-realtime）走
/// 混合协议：JSON 文本帧 + 裸 PCM binary 帧，**没有** GAX 长度前缀。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFormat {
    /// 4-byte BE u32 长度前缀 + cmd + payload（默认，向后兼容）
    BinaryGax,
    /// `Message::Text`，payload 为 UTF-8 文本（不带 GAX 长度前缀）
    Text,
    /// `Message::Binary`，payload 为原始二进制（不带 GAX 长度前缀）
    RawBinary,
}

/// GAX 帧：1 字节 cmd + payload bytes
#[derive(Debug, Clone)]
pub struct GaxFrame {
    pub cmd: u8,
    pub payload: Vec<u8>,
    /// 描述该帧在 WebSocket 上的物理形态。默认 `BinaryGax`（与原行为一致）。
    pub wire: WireFormat,
}

impl GaxFrame {
    pub fn new(cmd: u8, payload: Vec<u8>) -> Self {
        Self {
            cmd,
            payload,
            wire: WireFormat::BinaryGax,
        }
    }

    /// 构造一个文本帧（payload 为 UTF-8 文本）
    pub fn text(cmd: u8, payload: Vec<u8>) -> Self {
        Self {
            cmd,
            payload,
            wire: WireFormat::Text,
        }
    }

    /// 构造一个裸二进制帧（payload 直接作为 binary 发出）
    pub fn raw_binary(cmd: u8, payload: Vec<u8>) -> Self {
        Self {
            cmd,
            payload,
            wire: WireFormat::RawBinary,
        }
    }
}

// ===== codec error =====

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("buffer too short: have {have} bytes, need at least {need}")]
    TooShort { have: usize, need: usize },
    #[error("declared length {declared} exceeds buffer size {have}")]
    LengthOverflow { declared: usize, have: usize },
}

// ===== 编码 / 解码 =====

/// 编码为 GAX 帧：4 字节 BE u32 长度（包含 cmd 字节 + payload）+ cmd + payload
pub fn encode(cmd: u8, payload: &[u8]) -> Vec<u8> {
    // 长度 = 1（cmd 字节）+ payload.len()
    let len = (payload.len() + 1) as u32;
    let mut out = Vec::with_capacity(4 + 1 + payload.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.push(cmd);
    out.extend_from_slice(payload);
    out
}

/// 编码为 GaxFrame
pub fn encode_frame(frame: &GaxFrame) -> Vec<u8> {
    encode(frame.cmd, &frame.payload)
}

/// 从字节流解码出一个 GAX 帧：返回 (cmd, payload, bytes_consumed)
///
/// 失败返回 CodecError；调用方需自行处理（如 EAGAIN / 帧长度不足）
pub fn decode(bytes: &[u8]) -> Result<(u8, Vec<u8>, usize), CodecError> {
    if bytes.len() < 5 {
        return Err(CodecError::TooShort {
            have: bytes.len(),
            need: 5,
        });
    }
    let len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    if bytes.len() < 4 + len {
        return Err(CodecError::TooShort {
            have: bytes.len(),
            need: 4 + len,
        });
    }
    let cmd = bytes[4];
    let payload = bytes[5..4 + len].to_vec();
    Ok((cmd, payload, 4 + len))
}

/// 同 decode，但直接返回 GaxFrame（payload 不复制额外一层）
pub fn decode_frame(bytes: &[u8]) -> Result<(GaxFrame, usize), CodecError> {
    let (cmd, payload, consumed) = decode(bytes)?;
    Ok((
        GaxFrame {
            cmd,
            payload,
            wire: WireFormat::BinaryGax,
        },
        consumed,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_then_decode_roundtrip() {
        let cmd = RESP_TRANSCRIPT;
        let payload = b"hello world";
        let bytes = encode(cmd, payload);
        let (decoded_cmd, decoded_payload, consumed) = decode(&bytes).unwrap();
        assert_eq!(decoded_cmd, cmd);
        assert_eq!(decoded_payload, payload);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn encode_length_includes_cmd_byte() {
        let payload = vec![0u8; 10];
        let bytes = encode(REQ_AUDIO_ASR, &payload);
        // 长度字段 = 1（cmd）+ 10（payload）= 11
        let declared = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(declared, 11);
        assert_eq!(bytes.len(), 4 + 11);
    }

    #[test]
    fn decode_empty_payload() {
        let bytes = encode(REQ_STOP_ASR, b"");
        let (cmd, payload, consumed) = decode(&bytes).unwrap();
        assert_eq!(cmd, REQ_STOP_ASR);
        assert!(payload.is_empty());
        // 长度 = 1（cmd only）
        assert_eq!(consumed, 5);
    }

    #[test]
    fn decode_short_buffer_returns_error() {
        let r = decode(&[1, 2, 3]);
        assert!(matches!(r, Err(CodecError::TooShort { .. })));
    }

    #[test]
    fn decode_partial_frame_returns_error() {
        // 声明 100 字节但只给 10 字节
        let bytes = vec![0, 0, 0, 100, REQ_AUDIO_ASR, 1, 2, 3, 4, 5];
        let r = decode(&bytes);
        assert!(matches!(r, Err(CodecError::TooShort { .. })));
    }

    #[test]
    fn decode_frame_helper_works() {
        let bytes = encode(RESP_DONE_TTS, b"done");
        let (frame, consumed) = decode_frame(&bytes).unwrap();
        assert_eq!(frame.cmd, RESP_DONE_TTS);
        assert_eq!(frame.payload, b"done");
        assert_eq!(consumed, bytes.len());
    }
}