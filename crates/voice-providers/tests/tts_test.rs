//! TTS cosyvoice 集成测试：hand-built protobuf → parse_audio

use prost::Message;
use voice_providers::pb;
use voice_providers::tts::cosyvoice::CosyVoiceV2;
use voice_providers::tts::TtsModelAdapter;

#[test]
fn parse_audio_hand_built_chunk_roundtrip() {
    let audio_bytes: Vec<u8> = (0..128).collect();
    let chunk = pb::TtsAudioChunk {
        payload: audio_bytes.clone(),
    };
    let mut buf = Vec::new();
    chunk.encode(&mut buf).unwrap();

    let adapter = CosyVoiceV2::new("longxiaochun".to_string());
    let out = adapter.parse_audio(&buf).unwrap().unwrap();
    assert_eq!(out, audio_bytes);
}

#[test]
fn parse_audio_empty_payload_returns_some_empty_vec() {
    let chunk = pb::TtsAudioChunk { payload: Vec::new() };
    let mut buf = Vec::new();
    chunk.encode(&mut buf).unwrap();

    let adapter = CosyVoiceV2::new("longxiaochun".to_string());
    let out = adapter.parse_audio(&buf).unwrap().unwrap();
    assert!(out.is_empty());
}

#[test]
fn parse_audio_invalid_returns_none() {
    let adapter = CosyVoiceV2::new("longxiaochun".to_string());
    let r = adapter.parse_audio(&[0u8; 32]);
    assert!(r.is_ok());
    assert!(r.unwrap().is_none());
}

#[test]
fn parse_audio_preserves_content() {
    // 用明显非 zero-fill 的 payload 验证位级别保真
    let mut payload: Vec<u8> = Vec::with_capacity(256);
    for i in 0..256u16 {
        payload.push((i ^ 0xa5) as u8);
    }
    let chunk = pb::TtsAudioChunk { payload: payload.clone() };
    let mut buf = Vec::new();
    chunk.encode(&mut buf).unwrap();

    let adapter = CosyVoiceV2::new("longxiaochun".to_string());
    let out = adapter.parse_audio(&buf).unwrap().unwrap();
    assert_eq!(out.len(), 256);
    assert_eq!(out, payload);
}