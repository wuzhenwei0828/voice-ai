//! ASR paraformer 集成测试：hand-built protobuf → parse_event

use prost::Message;
use voice_providers::asr::paraformer::ParaformerRealtime;
use voice_providers::asr::AsrModelAdapter;
use voice_providers::pb;

#[test]
fn parse_event_hand_built_transcript_final() {
    let t = pb::AsrTranscript {
        text: "你好，世界".to_string(),
        is_final: true,
        begin_time: 0,
    };
    let mut buf = Vec::new();
    t.encode(&mut buf).unwrap();

    let adapter = ParaformerRealtime::new();
    let evt = adapter.parse_event(&buf).unwrap().unwrap();
    assert_eq!(evt.text, "你好，世界");
    assert!(evt.is_final);
}

#[test]
fn parse_event_intermediate_partial() {
    let t = pb::AsrTranscript {
        text: "你好".to_string(),
        is_final: false,
        begin_time: 0,
    };
    let mut buf = Vec::new();
    t.encode(&mut buf).unwrap();

    let adapter = ParaformerRealtime::new();
    let evt = adapter.parse_event(&buf).unwrap().unwrap();
    assert_eq!(evt.text, "你好");
    assert!(!evt.is_final);
}

#[test]
fn parse_event_intermediate_empty_returns_some_event() {
    // 中间结果但文本为空 → 返回 Some(AsrEvent{text:"", is_final:false})
    let t = pb::AsrTranscript {
        text: String::new(),
        is_final: false,
        begin_time: 0,
    };
    let mut buf = Vec::new();
    t.encode(&mut buf).unwrap();

    let adapter = ParaformerRealtime::new();
    let evt = adapter.parse_event(&buf).unwrap().unwrap();
    assert_eq!(evt.text, "");
    assert!(!evt.is_final);
}

#[test]
fn parse_event_malformed_returns_none_not_err() {
    let adapter = ParaformerRealtime::new();
    // 5 字节无效 protobuf（长度字段超过）
    let r = adapter.parse_event(&[0xff, 0x00, 0x00, 0x00, 0x00]);
    // 期待 Ok(None)（不是错误）—— 因为非 transcript 帧会进 parse_event 但不该报错
    assert!(r.is_ok());
    assert!(r.unwrap().is_none());
}

#[test]
fn parse_event_random_bytes_returns_none() {
    let adapter = ParaformerRealtime::new();
    let r = adapter.parse_event(&[0u8; 64]);
    assert!(r.is_ok());
    assert!(r.unwrap().is_none());
}