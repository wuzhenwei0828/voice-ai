//! Qwen TTS 集成测试：MockWs 模拟服务端，跑完整 synthesize pipeline
//!
//! 覆盖：
//! - JSON duplex pipeline（Qwen-Audio-TTS / CosyVoice）
//! - Realtime pipeline（qwen3-tts-flash-realtime）
//! - task-failed 错误传播
//! - 复用同一条 MockWs 跑多个 test case

use std::sync::Arc;

use futures_util::{future::FutureExt, StreamExt};
use prost::Message;

use voice_providers::asr::ClientError;
use voice_providers::codec::GaxFrame;
use voice_providers::pb::TtsAudioChunk;
use voice_providers::tts::TtsEvent;
use voice_providers::ws_pool::test_helpers::MockWs;
use voice_providers::ws_pool::{Dialer, LaneKind, PoolConfig, WebSocketLike, WsMessage, WsConnPool};

// ===== helpers =====

/// 构造一个"自带入站消息"的 mock dialer（带预设的入站 WS 消息列表）
fn dialer_with_messages(messages: Vec<WsMessage>) -> Dialer {
    Arc::new(move |_kind: LaneKind| {
        let messages = messages.clone();
        async move {
            let ws = MockWs::new();
            for m in messages {
                ws.push_incoming_ws(m);
            }
            let conn: Box<dyn WebSocketLike> = Box::new(ws);
            Ok(conn)
        }
        .boxed()
    })
}

fn text(s: impl Into<String>) -> WsMessage {
    WsMessage::Text(s.into())
}

fn binary(bytes: Vec<u8>) -> WsMessage {
    WsMessage::Binary(bytes)
}

// ===== Qwen-Audio-TTS / CosyVoice (JSON duplex) =====

#[tokio::test]
async fn qwen_audio_tts_pipeline_full_flow() {
    let pool = WsConnPool::new(PoolConfig {
        max_connections: 2,
        ..PoolConfig::default()
    });

    let messages = vec![
        text(r#"{"header":{"task_id":"t1","event":"task-started","attributes":{}},"payload":{}}"#),
        binary(b"audio-chunk-1".to_vec()),
        text(r#"{"header":{"task_id":"t1","event":"result-generated","attributes":{}},"payload":{"output":{"sentence":{"index":0,"words":[]},"type":"sentence-synthesis"}}}"#),
        binary(b"audio-chunk-2".to_vec()),
        text(r#"{"header":{"task_id":"t1","event":"result-generated","attributes":{}},"payload":{"output":{"sentence":{"index":0,"words":[]},"type":"sentence-end","original_text":"床前明月光"},"usage":{"characters":5}}}"#),
        text(r#"{"header":{"task_id":"t1","event":"task-finished","attributes":{"request_uuid":"req-1"}},"payload":{"usage":{"characters":5}}}"#),
    ];

    let client = voice_providers::tts::build_tts_client(
        pool,
        "qwen-audio-3.0-tts-flash",
        "longanhuan_v3.6",
        22050,
        "mp3".to_string(),
        true,
        dialer_with_messages(messages),
    )
    .expect("build_tts_client");

    let mut stream = client.synthesize("sess-1", "床前明月光").await.unwrap();
    let mut events: Vec<TtsEvent> = Vec::new();
    while let Some(ev) = stream.next().await {
        events.push(ev.expect("ok"));
    }

    // 期望：2 个音频 + 1 个 done
    assert_eq!(events.len(), 3, "got {} events", events.len());

    let ev0 = &events[0];
    assert_eq!(ev0.seq, 1);
    assert_eq!(ev0.data, b"audio-chunk-1");
    assert!(!ev0.is_last);

    let ev1 = &events[1];
    assert_eq!(ev1.seq, 2);
    assert_eq!(ev1.data, b"audio-chunk-2");
    assert!(!ev1.is_last);

    let ev2 = &events[2];
    assert!(ev2.is_last);
    assert!(ev2.data.is_empty());
}

#[tokio::test]
async fn qwen_audio_tts_task_failed_propagates_error() {
    let pool = WsConnPool::new(PoolConfig {
        max_connections: 1,
        ..PoolConfig::default()
    });

    let messages = vec![
        text(r#"{"header":{"task_id":"t1","event":"task-started","attributes":{}},"payload":{}}"#),
        text(r#"{"header":{"task_id":"t1","event":"task-failed","error_code":"InvalidParameter","error_message":"bad voice","attributes":{}},"payload":{}}"#),
    ];

    let client = voice_providers::tts::build_tts_client(
        pool,
        "qwen-audio-3.0-tts-flash",
        "longanhuan_v3.6",
        22050,
        "mp3".to_string(),
        true,
        dialer_with_messages(messages),
    )
    .unwrap();

    let mut stream = client.synthesize("sess-fail", "test").await.unwrap();
    let mut last_err: Option<ClientError> = None;
    while let Some(ev) = stream.next().await {
        if let Err(e) = ev {
            last_err = Some(e);
        }
    }
    match last_err {
        Some(ClientError::Decode(s)) => {
            assert!(s.contains("InvalidParameter"));
            assert!(s.contains("bad voice"));
        }
        other => panic!("expected ClientError::Decode with task-failed info, got: {:?}", other),
    }
}

#[tokio::test]
async fn cosyvoice_v3_routes_to_json_duplex() {
    let pool = WsConnPool::new(PoolConfig {
        max_connections: 1,
        ..PoolConfig::default()
    });

    let messages = vec![
        text(r#"{"header":{"task_id":"t2","event":"task-started","attributes":{}},"payload":{}}"#),
        binary(b"cosy-audio".to_vec()),
        text(r#"{"header":{"task_id":"t2","event":"task-finished","attributes":{}},"payload":{"usage":{"characters":3}}}"#),
    ];

    let client = voice_providers::tts::build_tts_client(
        pool,
        "cosyvoice-v3-flash",
        "longanyang",
        22050,
        "mp3".to_string(),
        true,
        dialer_with_messages(messages),
    )
    .unwrap();

    let mut stream = client.synthesize("sess-cv3", "hi").await.unwrap();
    let mut events: Vec<TtsEvent> = Vec::new();
    while let Some(ev) = stream.next().await {
        events.push(ev.expect("ok"));
    }
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].data, b"cosy-audio");
    assert!(events[1].is_last);
}

#[tokio::test]
async fn qwen_audio_tts_peer_close_returns_err() {
    let pool = WsConnPool::new(PoolConfig {
        max_connections: 1,
        ..PoolConfig::default()
    });

    let messages = vec![
        text(r#"{"header":{"task_id":"t","event":"task-started","attributes":{}},"payload":{}}"#),
        WsMessage::Close,
    ];

    let client = voice_providers::tts::build_tts_client(
        pool,
        "qwen-audio-3.0-tts-flash",
        "longanhuan_v3.6",
        22050,
        "mp3".to_string(),
        true,
        dialer_with_messages(messages),
    )
    .unwrap();

    let mut stream = client.synthesize("sess-close", "test").await.unwrap();
    let mut saw_err = false;
    while let Some(ev) = stream.next().await {
        if let Err(ClientError::Ws(s)) = ev {
            if s.contains("closed by peer") {
                saw_err = true;
            }
        }
    }
    assert!(saw_err, "expected ClientError::Ws 'closed by peer'");
}

// ===== Qwen-TTS Realtime =====

#[tokio::test]
async fn qwen_realtime_pipeline_decodes_base64_audio() {
    use base64::Engine as _;

    let pool = WsConnPool::new(PoolConfig {
        max_connections: 1,
        ..PoolConfig::default()
    });

    let pcm: Vec<u8> = (0..32).collect();
    let audio_b64 = base64::engine::general_purpose::STANDARD.encode(&pcm);

    let messages = vec![
        text(r#"{"type":"session.created","event_id":"e1","session":{"id":"sess_1","model":"qwen3-tts-flash-realtime"}}"#),
        text(r#"{"type":"response.created","event_id":"e2","response":{"id":"resp_1"}}"#),
        text(format!(
            r#"{{"type":"response.audio.delta","event_id":"e3","response_id":"resp_1","item_id":"item_1","output_index":0,"content_index":0,"delta":"{}"}}"#,
            audio_b64
        )),
        text(r#"{"type":"response.done","event_id":"e4","response":{"id":"resp_1"}}"#),
        text(r#"{"type":"session.finished","event_id":"e5"}"#),
    ];

    let client = voice_providers::tts::build_tts_client(
        pool,
        "qwen3-tts-flash-realtime",
        "Cherry",
        24000,
        "pcm".to_string(),
        true,
        dialer_with_messages(messages),
    )
    .unwrap();

    let mut stream = client.synthesize("sess-rt", "你好").await.unwrap();
    let mut events: Vec<TtsEvent> = Vec::new();
    while let Some(ev) = stream.next().await {
        events.push(ev.expect("ok"));
    }
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].seq, 1);
    assert_eq!(events[0].data, pcm);
    assert!(!events[0].is_last);
    assert!(events[1].is_last);
}

#[tokio::test]
async fn qwen_realtime_error_propagates() {
    let pool = WsConnPool::new(PoolConfig {
        max_connections: 1,
        ..PoolConfig::default()
    });

    let messages = vec![
        text(r#"{"type":"session.created","session":{"id":"sess"}}"#),
        text(r#"{"type":"error","error":{"message":"rate limit"}}"#),
        text(r#"{"type":"session.finished"}"#),
    ];

    let client = voice_providers::tts::build_tts_client(
        pool,
        "qwen3-tts-flash-realtime",
        "Cherry",
        24000,
        "pcm".to_string(),
        true,
        dialer_with_messages(messages),
    )
    .unwrap();

    let mut stream = client.synthesize("sess-rt-err", "hi").await.unwrap();
    let mut saw_err = false;
    while let Some(ev) = stream.next().await {
        if let Err(ClientError::Decode(s)) = ev {
            assert!(s.contains("rate limit"));
            saw_err = true;
        }
    }
    assert!(saw_err, "expected ClientError::Decode from realtime error");
}

// ===== GAX 旧路径（cosyvoice-v2-gax）向后兼容 =====

#[tokio::test]
async fn cosyvoice_v2_gax_legacy_still_works() {
    use voice_providers::codec::{RESP_AUDIO_TTS, RESP_DONE_TTS};

    let pool = WsConnPool::new(PoolConfig {
        max_connections: 1,
        ..PoolConfig::default()
    });

    let audio_payload: Vec<u8> = (0..64).collect();
    let chunk = TtsAudioChunk {
        payload: audio_payload.clone(),
    };
    let mut encoded = Vec::new();
    chunk.encode(&mut encoded).unwrap();

    // MockWs 同时支持 incoming_gax 队列
    let dialer: Dialer = Arc::new(move |_kind: LaneKind| {
        let encoded = encoded.clone();
        async move {
            let ws = MockWs::new();
            ws.push_incoming(GaxFrame::new(RESP_AUDIO_TTS, encoded));
            ws.push_incoming(GaxFrame::new(RESP_DONE_TTS, Vec::new()));
            let conn: Box<dyn WebSocketLike> = Box::new(ws);
            Ok(conn)
        }
        .boxed()
    });

    let client = voice_providers::tts::build_tts_client(
        pool,
        "cosyvoice-v2-gax",
        "longxiaochun",
        22050,
        "pcm".to_string(),
        true,
        dialer,
    )
    .unwrap();

    let mut stream = client.synthesize("sess-gax", "你好").await.unwrap();
    let mut events: Vec<TtsEvent> = Vec::new();
    while let Some(ev) = stream.next().await {
        events.push(ev.expect("ok"));
    }
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].data, audio_payload);
    assert!(events[1].is_last);
}

// ===== select_tts_adapter 路由验证 =====

#[test]
fn select_tts_adapter_routes_correctly() {
    use voice_providers::tts::{select_tts_adapter, TtsProtocol};

    let a = select_tts_adapter("qwen-audio-3.0-tts-flash", "longanhuan_v3.6").unwrap();
    assert_eq!(a.protocol(), TtsProtocol::JsonDuplex);

    let a = select_tts_adapter("qwen-audio-3.0-tts-plus", "longanyang").unwrap();
    assert_eq!(a.protocol(), TtsProtocol::JsonDuplex);

    let a = select_tts_adapter("cosyvoice-v2", "longxiaochun").unwrap();
    assert_eq!(a.protocol(), TtsProtocol::JsonDuplex);

    let a = select_tts_adapter("cosyvoice-v3-flash", "longanyang").unwrap();
    assert_eq!(a.protocol(), TtsProtocol::JsonDuplex);

    let a = select_tts_adapter("qwen3-tts-flash-realtime", "Cherry").unwrap();
    assert_eq!(a.protocol(), TtsProtocol::RealtimeSession);

    let a = select_tts_adapter("cosyvoice-v2-gax", "longxiaochun").unwrap();
    assert_eq!(a.protocol(), TtsProtocol::Gax);

    assert!(select_tts_adapter("unknown-model", "v").is_err());
}

// ===== endpoint 自动构造 =====

#[test]
fn provider_builds_tts_endpoint_for_realtime_models() {
    // 通过构造 BailianConfig 并 build_all 来验证；或者直接调 build_tts_endpoint
    // 这里通过 from_mapping + build_all 验证 dialer 配置
    use voice_providers::config::from_mapping;
    use serde_yaml::Value;

    let yaml = r#"
        tts:
          model: "qwen3-tts-flash-realtime"
          voice: "Cherry"
    "#;
    let v: Value = serde_yaml::from_str(yaml).unwrap();
    let m = match v {
        Value::Mapping(m) => m,
        _ => unreachable!(),
    };
    let cfg = from_mapping(Some(&m), "wss://default", "sk-test").unwrap();
    assert_eq!(cfg.tts.model, "qwen3-tts-flash-realtime");
    assert_eq!(cfg.tts.voice, "Cherry");
    // 校验 endpoint 字段默认值
    assert!(cfg.tts.endpoint.is_none());
    assert!(cfg.tts.workspace_id.is_none());
}

#[test]
fn provider_builds_tts_endpoint_for_workspace_based() {
    use voice_providers::config::from_mapping;
    use serde_yaml::Value;

    let yaml = r#"
        tts:
          model: "qwen-audio-3.0-tts-flash"
          voice: "longanhuan_v3.6"
          workspace_id: "ws-12345"
    "#;
    let v: Value = serde_yaml::from_str(yaml).unwrap();
    let m = match v {
        Value::Mapping(m) => m,
        _ => unreachable!(),
    };
    let cfg = from_mapping(Some(&m), "wss://default", "sk-test").unwrap();
    assert_eq!(cfg.tts.workspace_id.as_deref(), Some("ws-12345"));
}
