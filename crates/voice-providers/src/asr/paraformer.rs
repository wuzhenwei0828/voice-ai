//! Paraformer-realtime-v2 ASR 模型 adapter
//!
//! 实现 AsrModelAdapter trait，把 audio bytes / open params / transcript payload 翻译成
//! protobuf 消息。
//!
//! ## 占位 / 待补
//!
//! - model 字符串："paraformer-realtime-v2"
//! - audio 字节：原样塞进 `AsrAudioChunk.payload`
//! - transcript frame：AsrTranscript protobuf 直接映射 AsrEvent

use prost::Message;

use crate::asr::{AsrEvent, AsrModelAdapter, ClientError};
use crate::codec::{GaxFrame, REQ_AUDIO_ASR, REQ_OPEN_ASR, REQ_STOP_ASR};
use crate::pb;

// ===== Adapter 实现 =====

pub struct ParaformerRealtime;

impl ParaformerRealtime {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ParaformerRealtime {
    fn default() -> Self {
        Self::new()
    }
}

impl AsrModelAdapter for ParaformerRealtime {
    fn model_name(&self) -> &'static str {
        "paraformer-realtime-v2"
    }

    fn open_request(&self, _session_id: &str, sr: u32, ch: u16) -> GaxFrame {
        let req = pb::AsrOpenRequest {
            model: self.model_name().to_string(),
            format: "pcm".to_string(),
            sample_rate: sr,
            channels: ch as u32,
            enable_intermediate_result: true,
            enable_punctuation: true,
        };
        let mut buf = Vec::with_capacity(req.encoded_len());
        req.encode(&mut buf).expect("encode AsrOpenRequest");
        GaxFrame::new(REQ_OPEN_ASR, buf)
    }

    fn audio_frame(&self, pcm: &[u8]) -> GaxFrame {
        let req = pb::AsrAudioChunk {
            payload: pcm.to_vec(),
        };
        let mut buf = Vec::with_capacity(req.encoded_len());
        req.encode(&mut buf).expect("encode AsrAudioChunk");
        GaxFrame::new(REQ_AUDIO_ASR, buf)
    }

    fn stop_frame(&self, _session_id: &str) -> GaxFrame {
        let req = pb::AsrStop {};
        let mut buf = Vec::with_capacity(req.encoded_len());
        req.encode(&mut buf).expect("encode AsrStop");
        GaxFrame::new(REQ_STOP_ASR, buf)
    }

    fn parse_event(&self, payload: &[u8]) -> Result<Option<AsrEvent>, ClientError> {
        // 尝试按 AsrTranscript 解；解不开 → Ok(None)（其它类型的帧，不出错）
        let transcript = match pb::AsrTranscript::decode(payload) {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };
        let event = AsrEvent {
            text: transcript.text,
            is_final: transcript.is_final,
        };
        Ok(Some(event))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn open_request_frame_round_trip() {
        let adapter = ParaformerRealtime::new();
        let frame = adapter.open_request("test-session", 16000, 1);
        assert_eq!(frame.cmd, REQ_OPEN_ASR);

        let req = pb::AsrOpenRequest::decode(&frame.payload[..]).unwrap();
        assert_eq!(req.model, "paraformer-realtime-v2");
        assert_eq!(req.format, "pcm");
        assert_eq!(req.sample_rate, 16000);
        assert_eq!(req.channels, 1);
        assert!(req.enable_intermediate_result);
        assert!(req.enable_punctuation);
    }

    #[test]
    fn audio_frame_round_trip() {
        let adapter = ParaformerRealtime::new();
        let pcm = vec![0u8; 100];
        let frame = adapter.audio_frame(&pcm);
        assert_eq!(frame.cmd, REQ_AUDIO_ASR);

        let req = pb::AsrAudioChunk::decode(&frame.payload[..]).unwrap();
        assert_eq!(req.payload.len(), 100);
        assert_eq!(&req.payload[..], &pcm[..]);
    }

    #[test]
    fn stop_frame_round_trip() {
        let adapter = ParaformerRealtime::new();
        let frame = adapter.stop_frame("test-session");
        assert_eq!(frame.cmd, REQ_STOP_ASR);
        let _ = pb::AsrStop::decode(&frame.payload[..]).unwrap();
    }

    #[test]
    fn parse_event_final() {
        let adapter = ParaformerRealtime::new();
        let t = pb::AsrTranscript {
            text: "你好世界".to_string(),
            is_final: true,
            begin_time: 0,
        };
        let mut buf = Vec::new();
        t.encode(&mut buf).unwrap();
        let evt = adapter.parse_event(&buf).unwrap().unwrap();
        assert_eq!(evt.text, "你好世界");
        assert!(evt.is_final);
    }

    #[test]
    fn parse_event_intermediate_empty_text() {
        let adapter = ParaformerRealtime::new();
        let t = pb::AsrTranscript {
            text: String::new(),
            is_final: false,
            begin_time: 0,
        };
        let mut buf = Vec::new();
        t.encode(&mut buf).unwrap();
        // 中间结果且空文本：返回 Ok(Some(AsrEvent{...}))，上层负责跳过
        let evt = adapter.parse_event(&buf).unwrap().unwrap();
        assert_eq!(evt.text, "");
        assert!(!evt.is_final);
    }

    #[test]
    fn parse_event_invalid_payload_returns_none() {
        let adapter = ParaformerRealtime::new();
        // 非法 protobuf → Ok(None)（不作为错误抛出，因为其它帧也会进 parse_event）
        let r = adapter.parse_event(&[1, 2, 3, 4, 5]).unwrap();
        assert!(r.is_none());
    }
}