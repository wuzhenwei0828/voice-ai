//! CosyVoice-v2 TTS 模型 adapter（GAX 协议）
//!
//! 实现 TtsModelAdapter trait，把 text / open params / audio bytes 翻译成 protobuf 消息。
//!
//! ## 注意
//!
//! 当前公共 DashScope WebSocket 协议已统一为 JSON duplex（见 `qwen_audio.rs`），
//! GAX 路径仅作为 legacy 入口保留：通过 `cosyvoice-gax` / `cosyvoice-v2-gax` 模型名
//! 显式路由到此 adapter。新接入默认走 `cosyvoice` / `cosyvoice-v2` → `qwen_audio::QwenAudioTts`。

use prost::Message;

use crate::codec::{GaxFrame, REQ_OPEN_TTS, REQ_STOP_TTS, REQ_TEXT_TTS};
use crate::pb;
use crate::tts::{ClientError, TtsModelAdapter, TtsProtocol};

// ===== Adapter 实现 =====

pub struct CosyVoiceV2 {
    voice: String,
}

impl CosyVoiceV2 {
    pub fn new(voice: String) -> Self {
        Self { voice }
    }
}

impl TtsModelAdapter for CosyVoiceV2 {
    fn model_name(&self) -> &'static str {
        "cosyvoice-v2"
    }

    fn voice(&self) -> &str {
        &self.voice
    }

    fn protocol(&self) -> TtsProtocol {
        TtsProtocol::Gax
    }

    fn open_request(&self, sr: u32, format: &str, stream: bool) -> GaxFrame {
        let req = pb::TtsOpenRequest {
            model: self.model_name().to_string(),
            voice: self.voice.clone(),
            format: format.to_string(),
            sample_rate: sr,
            stream,
        };
        let mut buf = Vec::with_capacity(req.encoded_len());
        req.encode(&mut buf).expect("encode TtsOpenRequest");
        GaxFrame::new(REQ_OPEN_TTS, buf)
    }

    fn text_frame(&self, text: &str) -> GaxFrame {
        let req = pb::TtsText {
            text: text.to_string(),
        };
        let mut buf = Vec::with_capacity(req.encoded_len());
        req.encode(&mut buf).expect("encode TtsText");
        GaxFrame::new(REQ_TEXT_TTS, buf)
    }

    fn stop_frame(&self) -> GaxFrame {
        let req = pb::TtsStop {};
        let mut buf = Vec::with_capacity(req.encoded_len());
        req.encode(&mut buf).expect("encode TtsStop");
        GaxFrame::new(REQ_STOP_TTS, buf)
    }

    fn parse_audio(&self, payload: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        // 尝试按 TtsAudioChunk 解；解不开 → Ok(None)（其它帧类型）
        let chunk = match pb::TtsAudioChunk::decode(payload) {
            Ok(c) => c,
            Err(_) => return Ok(None),
        };
        Ok(Some(chunk.payload.to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn open_request_round_trip() {
        let adapter = CosyVoiceV2::new("longxiaochun".to_string());
        let frame = adapter.open_request(16000, "pcm", true);
        assert_eq!(frame.cmd, REQ_OPEN_TTS);

        let req = pb::TtsOpenRequest::decode(&frame.payload[..]).unwrap();
        assert_eq!(req.model, "cosyvoice-v2");
        assert_eq!(req.voice, "longxiaochun");
        assert_eq!(req.format, "pcm");
        assert_eq!(req.sample_rate, 16000);
        assert!(req.stream);
    }

    #[test]
    fn text_frame_round_trip() {
        let adapter = CosyVoiceV2::new("longxiaochun".to_string());
        let frame = adapter.text_frame("你好世界");
        assert_eq!(frame.cmd, REQ_TEXT_TTS);
        let req = pb::TtsText::decode(&frame.payload[..]).unwrap();
        assert_eq!(req.text, "你好世界");
    }

    #[test]
    fn stop_frame_round_trip() {
        let adapter = CosyVoiceV2::new("longxiaochun".to_string());
        let frame = adapter.stop_frame();
        assert_eq!(frame.cmd, REQ_STOP_TTS);
        let _ = pb::TtsStop::decode(&frame.payload[..]).unwrap();
    }

    #[test]
    fn parse_audio_round_trip() {
        let adapter = CosyVoiceV2::new("longxiaochun".to_string());
        let audio_bytes: Vec<u8> = (0..64).collect();
        let chunk = pb::TtsAudioChunk {
            payload: audio_bytes.clone(),
        };
        let mut buf = Vec::new();
        chunk.encode(&mut buf).unwrap();
        let out = adapter.parse_audio(&buf).unwrap().unwrap();
        assert_eq!(out, audio_bytes);
    }

    #[test]
    fn parse_audio_invalid_payload_returns_none() {
        let adapter = CosyVoiceV2::new("longxiaochun".to_string());
        let r = adapter.parse_audio(&[1, 2, 3]).unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn model_name_and_voice() {
        let adapter = CosyVoiceV2::new("longxiaochun".to_string());
        assert_eq!(adapter.model_name(), "cosyvoice-v2");
        assert_eq!(adapter.voice(), "longxiaochun");
    }
}