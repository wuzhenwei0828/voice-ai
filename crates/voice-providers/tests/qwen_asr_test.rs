//! Qwen 系列 ASR adapter 端到端集成测试 —— 走 MockWs + WsPool，
//! 验证 `StreamingAsrClient` 在 Qwen JSON 协议下的完整识别流。
//!
//! 覆盖：
//!   - adapter 单元测试：frame cmd / wire format / JSON 形状
//!   - QwenMockWs：Text/Binary 双通道 send + recv_message 行为
//!   - StreamingAsrClient 端到端：open + audio + stop + transcript 解析

use std::collections::VecDeque;
use std::sync::Arc;

use futures_util::future::FutureExt;
use futures_util::StreamExt;
use parking_lot::Mutex;
use voice_providers::asr::{AsrClient, AsrModelAdapter, ClientError};
use voice_providers::codec::{
    GaxFrame, WireFormat, REQ_AUDIO_ASR, REQ_OPEN_ASR, RESP_TRANSCRIPT,
};
use voice_providers::ws_pool::{
    Dialer, LaneKind, PoolConfig, PoolError, WebSocketLike, WsPool, WsMessage,
};

use voice_providers::asr::qwen::QwenAsrAdapter;
use voice_providers::asr::StreamingAsrClient;

// ===== QwenMockWs：分别支持 Text 与 Binary 双向 =====

#[derive(Debug)]
struct QwenMockWs {
    incoming_text: Mutex<VecDeque<String>>,
    outgoing: Arc<Mutex<Vec<GaxFrame>>>,
    healthy: std::sync::atomic::AtomicBool,
}

impl QwenMockWs {
    fn new(outgoing: Arc<Mutex<Vec<GaxFrame>>>) -> Self {
        Self {
            incoming_text: Mutex::new(VecDeque::new()),
            outgoing,
            healthy: std::sync::atomic::AtomicBool::new(true),
        }
    }
    fn push_text(&self, json: &str) {
        self.incoming_text.lock().push_back(json.to_string());
    }
}

#[async_trait::async_trait]
impl WebSocketLike for QwenMockWs {
    async fn send_text(&mut self, text: &str) -> Result<(), PoolError> {
        self.outgoing
            .lock()
            .push(GaxFrame::text(REQ_OPEN_ASR, text.as_bytes().to_vec()));
        Ok(())
    }
    async fn send_binary(&mut self, bytes: Vec<u8>) -> Result<(), PoolError> {
        self.outgoing
            .lock()
            .push(GaxFrame::raw_binary(REQ_AUDIO_ASR, bytes));
        Ok(())
    }
    async fn recv_message(&mut self) -> Result<WsMessage, PoolError> {
        if let Some(t) = self.incoming_text.lock().pop_front() {
            return Ok(WsMessage::Text(t));
        }
        self.healthy.store(false, std::sync::atomic::Ordering::SeqCst);
        Err(PoolError::ClosedByPeer)
    }
    /// 按 WireFormat 分发到 send_text / send_binary（默认 impl 是 GAX 二进制编码）。
    async fn send_frame(&mut self, frame: GaxFrame) -> Result<(), PoolError> {
        match frame.wire {
            WireFormat::Text => {
                let text = String::from_utf8_lossy(&frame.payload).into_owned();
                self.send_text(&text).await
            }
            WireFormat::RawBinary => self.send_binary(frame.payload).await,
            WireFormat::BinaryGax => {
                // 测试不需要 GAX 路径，但 fallback 避免 panic
                self.send_binary(frame.payload).await
            }
        }
    }
    /// Qwen 走 JSON 文本通道：必须 override 返回值（默认 impl 会忽略 text）。
    async fn recv_frame(&mut self) -> Result<GaxFrame, PoolError> {
        loop {
            match self.recv_message().await? {
                WsMessage::Text(text) => {
                    return Ok(GaxFrame::text(RESP_TRANSCRIPT, text.into_bytes()));
                }
                WsMessage::Binary(bytes) => {
                    // 也兜底支持 Binary（如果测试后期混入）
                    return Ok(GaxFrame::new(REQ_AUDIO_ASR, bytes));
                }
                WsMessage::Close => return Err(PoolError::ClosedByPeer),
            }
        }
    }
    async fn close(self: Box<Self>) -> Result<(), PoolError> {
        Ok(())
    }
    fn is_healthy(&self) -> bool {
        self.healthy.load(std::sync::atomic::Ordering::SeqCst)
    }
}

// ===== adapter 单元测试 =====

#[test]
fn qwen_open_request_json_matches_docs_spec() {
    let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
    let frame = adapter.open_request("task-12345", 16000, 1);
    assert_eq!(frame.wire, WireFormat::Text);

    let v: serde_json::Value = serde_json::from_slice(&frame.payload).unwrap();
    assert_eq!(v["header"]["action"], "run-task");
    assert_eq!(v["header"]["task_id"], "task-12345");
    assert_eq!(v["header"]["streaming"], "duplex");
    assert_eq!(v["payload"]["task_group"], "audio");
    assert_eq!(v["payload"]["task"], "asr");
    assert_eq!(v["payload"]["function"], "recognition");
    assert_eq!(v["payload"]["model"], "qwen-audio-3.0-asr-flash-streaming");
    assert_eq!(v["payload"]["parameters"]["sample_rate"], 16000);
    assert_eq!(v["payload"]["parameters"]["format"], "pcm");
    assert_eq!(v["payload"]["parameters"]["max_sentence_silence"], 800);
}

#[test]
fn qwen_audio_frame_is_raw_binary_with_pcm() {
    let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
    let pcm: Vec<u8> = (0..3200).map(|i| (i % 256) as u8).collect();
    let frame = adapter.audio_frame(&pcm);
    assert_eq!(frame.wire, WireFormat::RawBinary);
    assert_eq!(frame.payload, pcm);
}

#[test]
fn qwen_finish_task_json_has_same_task_id() {
    let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
    let frame = adapter.stop_frame("task-12345");
    assert_eq!(frame.wire, WireFormat::Text);
    let v: serde_json::Value = serde_json::from_slice(&frame.payload).unwrap();
    assert_eq!(v["header"]["action"], "finish-task");
    assert_eq!(v["header"]["task_id"], "task-12345");
    assert_eq!(v["header"]["streaming"], "duplex");
}

#[test]
fn qwen_chunks_16000hz_mono_wire_format() {
    let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
    let pcm_100ms = vec![0u8; 3200];
    let f = adapter.audio_frame(&pcm_100ms);
    assert_eq!(f.cmd, REQ_AUDIO_ASR);
    assert_eq!(f.wire, WireFormat::RawBinary);
    assert_eq!(f.payload.len(), 3200);
}

#[test]
fn qwen_task_failed_event_propagates_error() {
    let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
    let err_event = serde_json::json!({
        "header": {
            "action": "task-failed",
            "task_id": "session-x",
            "event_id": "e9",
            "error_code": "BAD_AUDIO_FORMAT",
            "error_message": "audio sample rate does not match"
        },
        "payload": {"task_id": "session-x"}
    });
    let bytes = serde_json::to_vec(&err_event).unwrap();
    let r = adapter.parse_event(&bytes);
    assert!(r.is_err());
    let msg = match r.unwrap_err() {
        ClientError::Decode(m) => m,
        other => panic!("expected Decode error, got {:?}", other),
    };
    assert!(msg.contains("BAD_AUDIO_FORMAT"));
    assert!(msg.contains("audio sample rate does not match"));
}

#[test]
fn qwen_resp_transcript_marker_protected() {
    let adapter = QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming");
    let evt = serde_json::json!({
        "header": {"action": "result-generated", "task_id": "x", "event_id": "e"},
        "payload": {"output": {"sentence": {"text": "hi", "sentence_end": false}}}
    });
    let bytes = serde_json::to_vec(&evt).unwrap();
    let out = adapter.parse_event(&bytes).unwrap().unwrap();
    assert_eq!(out.text, "hi");
    assert!(!out.is_final);
    assert_eq!(RESP_TRANSCRIPT, 0x12);
}

// ===== Mock 单测 =====

#[tokio::test]
async fn qwen_mock_ws_text_round_trip() {
    let outgoing = Arc::new(Mutex::new(Vec::new()));
    let mut ws = QwenMockWs::new(outgoing.clone());
    ws.send_text(r#"{"action":"run-task"}"#).await.unwrap();
    let out = outgoing.lock().clone();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].wire, WireFormat::Text);
    assert_eq!(
        std::str::from_utf8(&out[0].payload).unwrap(),
        r#"{"action":"run-task"}"#
    );
}

#[tokio::test]
async fn qwen_mock_ws_binary_round_trip() {
    let outgoing = Arc::new(Mutex::new(Vec::new()));
    let mut ws = QwenMockWs::new(outgoing.clone());
    ws.send_binary(vec![10, 20, 30]).await.unwrap();
    let out = outgoing.lock().clone();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].wire, WireFormat::RawBinary);
    assert_eq!(out[0].payload, vec![10, 20, 30]);
}

#[tokio::test]
async fn qwen_mock_ws_recv_message_returns_text() {
    let outgoing = Arc::new(Mutex::new(Vec::new()));
    let mut ws = QwenMockWs::new(outgoing);
    ws.push_text("hello");
    let msg = ws.recv_message().await.unwrap();
    match msg {
        WsMessage::Text(s) => assert_eq!(s, "hello"),
        _ => panic!("expected Text"),
    }
}

#[tokio::test]
async fn qwen_mock_ws_recv_message_returns_closed_when_empty() {
    let outgoing = Arc::new(Mutex::new(Vec::new()));
    let mut ws = QwenMockWs::new(outgoing);
    let r = ws.recv_message().await;
    assert!(matches!(r, Err(PoolError::ClosedByPeer)));
}

// ===== 端到端：StreamingAsrClient + QwenMockWs =====

fn dialer_with_responses(
    responses: Vec<String>,
    outgoing: Arc<Mutex<Vec<GaxFrame>>>,
) -> Dialer {
    Arc::new(move |_kind: LaneKind| {
        let responses = responses.clone();
        let outgoing = outgoing.clone();
        async move {
            let ws = QwenMockWs::new(outgoing);
            for r in &responses {
                ws.push_text(r);
            }
            Ok(Box::new(ws) as Box<dyn WebSocketLike>)
        }
        .boxed()
    })
}

#[tokio::test]
async fn qwen_streaming_client_emits_partial_then_final() {
    let responses = vec![
        serde_json::json!({
            "header": {"action": "task-started", "task_id": "s1", "event_id": "e0"},
            "payload": {"task_id": "s1"}
        })
        .to_string(),
        serde_json::json!({
            "header": {"action": "result-generated", "task_id": "s1", "event_id": "e1"},
            "payload": {"output": {"sentence": {"text": "你", "sentence_end": false}}}
        })
        .to_string(),
        serde_json::json!({
            "header": {"action": "result-generated", "task_id": "s1", "event_id": "e2"},
            "payload": {"output": {"sentence": {"text": "你好", "sentence_end": true}}}
        })
        .to_string(),
    ];

    let pool = WsPool::new(PoolConfig {
        max_connections: 2,
        ..PoolConfig::default()
    });
    let outgoing = Arc::new(Mutex::new(Vec::new()));
    let dialer = dialer_with_responses(responses, outgoing.clone());
    let client = StreamingAsrClient::new(
        pool.clone(),
        Box::new(QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming")),
        dialer,
        16000,
        1,
    );

    let mut stream = client.recognize("s1", None, vec![0u8; 3200]).await.unwrap();
    let partial = stream.next().await.expect("partial").expect("ok");
    assert_eq!(partial.text, "你");
    assert!(!partial.is_final);

    let final_evt = stream.next().await.expect("final").expect("ok");
    assert_eq!(final_evt.text, "你好");
    assert!(final_evt.is_final);

    // final 触发 break
    let after = stream.next().await;
    assert!(after.is_none(), "stream should end after final");

    // 验证 3 帧 outgoing
    let out = outgoing.lock().clone();
    assert_eq!(out.len(), 3, "expected 3 frames, got {}", out.len());
    assert_eq!(out[0].wire, WireFormat::Text);
    assert_eq!(out[1].wire, WireFormat::RawBinary);
    assert_eq!(out[2].wire, WireFormat::Text);
}

#[tokio::test]
async fn qwen_streaming_client_breaks_on_final() {
    let responses = vec![
        serde_json::json!({
            "header": {"action": "task-started", "task_id": "s1", "event_id": "e0"},
            "payload": {"task_id": "s1"}
        })
        .to_string(),
        serde_json::json!({
            "header": {"action": "result-generated", "task_id": "s1", "event_id": "e1"},
            "payload": {"output": {"sentence": {"text": "你好世界", "sentence_end": true}}}
        })
        .to_string(),
    ];

    let pool = WsPool::new(PoolConfig::default());
    let outgoing = Arc::new(Mutex::new(Vec::new()));
    let dialer = dialer_with_responses(responses, outgoing.clone());
    let client = StreamingAsrClient::new(
        pool.clone(),
        Box::new(QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming")),
        dialer,
        16000,
        1,
    );

    let mut stream = client.recognize("s1", None, vec![0u8; 3200]).await.unwrap();
    let evt = stream.next().await.expect("final").expect("ok");
    assert_eq!(evt.text, "你好世界");
    assert!(evt.is_final);
    let after = stream.next().await;
    assert!(after.is_none(), "stream should end after final");
}

#[tokio::test]
async fn qwen_streaming_client_propagates_task_failed() {
    let responses = vec![
        serde_json::json!({
            "header": {"action": "task-started", "task_id": "s1", "event_id": "e0"},
            "payload": {"task_id": "s1"}
        })
        .to_string(),
        serde_json::json!({
            "header": {
                "action": "task-failed",
                "task_id": "s1",
                "event_id": "e9",
                "error_code": "BAD_AUDIO_FORMAT",
                "error_message": "sample rate mismatch"
            },
            "payload": {"task_id": "s1"}
        })
        .to_string(),
    ];

    let pool = WsPool::new(PoolConfig::default());
    let outgoing = Arc::new(Mutex::new(Vec::new()));
    let dialer = dialer_with_responses(responses, outgoing);
    let client = StreamingAsrClient::new(
        pool.clone(),
        Box::new(QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming")),
        dialer,
        16000,
        1,
    );

    let mut stream = client.recognize("s1", None, vec![0u8; 3200]).await.unwrap();
    let first = stream.next().await.expect("err item");
    assert!(first.is_err());
    let err = first.unwrap_err();
    match err {
        ClientError::Decode(msg) => {
            assert!(msg.contains("BAD_AUDIO_FORMAT"));
        }
        other => panic!("expected Decode error, got {:?}", other),
    }
}

#[tokio::test]
async fn qwen_streaming_client_outgoing_frames_correct_wire_format() {
    let responses = vec![
        serde_json::json!({
            "header": {"action": "task-started", "task_id": "s1", "event_id": "e0"},
            "payload": {"task_id": "s1"}
        })
        .to_string(),
        serde_json::json!({
            "header": {"action": "result-generated", "task_id": "s1", "event_id": "e9"},
            "payload": {"output": {"sentence": {"text": "x", "sentence_end": true}}}
        })
        .to_string(),
    ];

    let pool = WsPool::new(PoolConfig::default());
    let outgoing = Arc::new(Mutex::new(Vec::new()));
    let dialer = dialer_with_responses(responses, outgoing.clone());
    let client = StreamingAsrClient::new(
        pool.clone(),
        Box::new(QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming")),
        dialer,
        16000,
        1,
    );

    let pcm = vec![0u8; 3200];
    let mut stream = client.recognize("s1", None, pcm).await.unwrap();
    while let Some(item) = stream.next().await {
        let _ = item.expect("ok");
    }

    let out = outgoing.lock().clone();
    assert_eq!(out.len(), 3, "expected 3 outgoing frames, got {:?}", out.len());
    // run-task
    assert_eq!(out[0].wire, WireFormat::Text);
    let run_json: serde_json::Value = serde_json::from_slice(&out[0].payload).unwrap();
    assert_eq!(run_json["header"]["action"], "run-task");
    assert_eq!(run_json["header"]["task_id"], "s1");
    // audio
    assert_eq!(out[1].wire, WireFormat::RawBinary);
    assert_eq!(out[1].payload.len(), 3200);
    // finish-task
    assert_eq!(out[2].wire, WireFormat::Text);
    let fin_json: serde_json::Value = serde_json::from_slice(&out[2].payload).unwrap();
    assert_eq!(fin_json["header"]["action"], "finish-task");
    assert_eq!(fin_json["header"]["task_id"], "s1");
}

#[tokio::test]
async fn qwen_pooled_conn_send_text_and_binary() {
    // 验证 PooledConn 暴露的 send_text / send_binary 走 TungsteniteWs 路径
    let outgoing = Arc::new(Mutex::new(Vec::new()));
    let incoming = vec![serde_json::json!({
        "header": {"action": "task-started", "task_id": "s1", "event_id": "e0"},
        "payload": {"task_id": "s1"}
    })
    .to_string()];

    let pool = WsPool::new(PoolConfig {
        max_connections: 2,
        ..PoolConfig::default()
    });
    let dialer = dialer_with_responses(incoming, outgoing.clone());

    let mut conn = pool
        .acquire_or_dial(LaneKind::Asr, dialer)
        .await
        .expect("acquire");

    // send_text → 应记到 outgoing（WireFormat::Text）
    conn.send_text(r#"{"k":1}"#).await.expect("send_text");
    // send_binary → 应记到 outgoing（WireFormat::RawBinary）
    conn.send_binary(vec![9, 8, 7]).await.expect("send_binary");

    let out = outgoing.lock().clone();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].wire, WireFormat::Text);
    assert_eq!(out[1].wire, WireFormat::RawBinary);

    // recv_message → 应拿到 task-started JSON
    let msg = conn.recv_message().await.expect("recv_message");
    match msg {
        WsMessage::Text(s) => assert!(s.contains("task-started")),
        _ => panic!("expected text"),
    }

    conn.release(true);
}

#[tokio::test]
async fn qwen_streaming_client_handles_multiple_audio_chunks() {
    // 验证 6400 字节音频会被切成 2 个 3200 字节 chunk，发出 2 帧 audio
    let responses = vec![
        serde_json::json!({
            "header": {"action": "task-started", "task_id": "s1", "event_id": "e0"},
            "payload": {"task_id": "s1"}
        })
        .to_string(),
        serde_json::json!({
            "header": {"action": "result-generated", "task_id": "s1", "event_id": "e9"},
            "payload": {"output": {"sentence": {"text": "x", "sentence_end": true}}}
        })
        .to_string(),
    ];

    let pool = WsPool::new(PoolConfig::default());
    let outgoing = Arc::new(Mutex::new(Vec::new()));
    let dialer = dialer_with_responses(responses, outgoing.clone());
    let client = StreamingAsrClient::new(
        pool.clone(),
        Box::new(QwenAsrAdapter::for_model("qwen-audio-3.0-asr-flash-streaming")),
        dialer,
        16000,
        1,
    );

    // 6400 字节 → 2 帧 audio
    let pcm = vec![0u8; 6400];
    let mut stream = client.recognize("s1", None, pcm).await.unwrap();
    while let Some(item) = stream.next().await {
        let _ = item.expect("ok");
    }

    let out = outgoing.lock().clone();
    // 1 run-task + 2 audio + 1 finish-task = 4 帧
    assert_eq!(out.len(), 4, "expected 4 frames, got {}", out.len());
    assert_eq!(out[0].wire, WireFormat::Text); // run-task
    assert_eq!(out[1].wire, WireFormat::RawBinary); // audio chunk 1
    assert_eq!(out[2].wire, WireFormat::RawBinary); // audio chunk 2
    assert_eq!(out[3].wire, WireFormat::Text); // finish-task
    assert_eq!(out[1].payload.len(), 3200);
    assert_eq!(out[2].payload.len(), 3200);
}
