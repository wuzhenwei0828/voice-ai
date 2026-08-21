//! Qwen3-ASR-Flash-Realtime 会话集成测试（无网络，ScriptedServer mock）
//!
//! 覆盖：
//! 1. 多句连续听写：partial→final→（继续）partial→final→finished，
//!    事件流**不**在首个 final 断流（与公共协议 session.rs 的行为差异点）
//! 2. 上行帧序列：session.update → N×input_audio_buffer.append（超长音频按 3200B 切片）→ session.finish
//! 3. abandon：drop 句柄 → 后台任务干净退出 → 连接归还 idle 池、事件流结束
//!
//! 为什么不用 `ws_pool::test_helpers::MockWs`：它空队列时 `recv_message` 立即返回
//! `ClosedByPeer`，与后台任务"等待服务端下一事件"的语义冲突（测试会有竞态）。
//! 这里用 tokio mpsc 写一个空队列阻塞的 ScriptedServer，行为完全确定性。

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::FutureExt;
use futures_util::StreamExt;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use voice_providers::asr::qwen3_realtime::{
    start_realtime_session, RealtimeEvent, Qwen3RealtimeAdapter,
};
use voice_providers::asr::ClientError;
use voice_providers::ws_pool::{Dialer, LaneKind, PoolConfig, PoolError, WebSocketLike, WsMessage, WsConnPool};

// ===== ScriptedServer：mpsc-backed mock WebSocket =====

/// 空队列时阻塞（不是报错）的 mock WS：incoming 走 mpsc，outgoing 全程记录。
/// receiver 存在 slot 里，dialer 首次拨号取走（测试每个 session 只拨一次号）。
#[derive(Clone)]
struct ScriptedServer {
    incoming_tx: mpsc::UnboundedSender<WsMessage>,
    outgoing: Arc<Mutex<Vec<WsMessage>>>,
    incoming_rx_slot: Arc<tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<WsMessage>>>>,
}

impl ScriptedServer {
    fn new() -> Self {
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel::<WsMessage>();
        Self {
            incoming_tx,
            outgoing: Arc::new(Mutex::new(Vec::new())),
            incoming_rx_slot: Arc::new(tokio::sync::Mutex::new(Some(incoming_rx))),
        }
    }

    /// 模拟服务端下发一条 JSON 事件
    fn server_event(&self, json: serde_json::Value) {
        let _ = self.incoming_tx.send(WsMessage::Text(json.to_string()));
    }

    /// 已记录的客户端上行帧（text 形态）
    fn outgoing_texts(&self) -> Vec<String> {
        self.outgoing
            .lock()
            .iter()
            .filter_map(|m| match m {
                WsMessage::Text(s) => Some(s.clone()),
                _ => None,
            })
            .collect()
    }

    /// 等待客户端已发出至少 n 条上行帧（轮询，总超时 2s）
    async fn wait_outgoing_count(&self, n: usize) {
        for _ in 0..200 {
            if self.outgoing.lock().len() >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("等待 {n} 条上行帧超时，当前 {:?}", self.outgoing_texts());
    }
}

struct ScriptedConn {
    server: ScriptedServer,
    incoming_rx: mpsc::UnboundedReceiver<WsMessage>,
}

#[async_trait::async_trait]
impl WebSocketLike for ScriptedConn {
    async fn send_text(&mut self, text: &str) -> Result<(), PoolError> {
        self.server.outgoing.lock().push(WsMessage::Text(text.to_string()));
        Ok(())
    }

    async fn send_binary(&mut self, bytes: Vec<u8>) -> Result<(), PoolError> {
        self.server.outgoing.lock().push(WsMessage::Binary(bytes));
        Ok(())
    }

    async fn recv_message(&mut self) -> Result<WsMessage, PoolError> {
        match self.incoming_rx.recv().await {
            Some(m) => Ok(m),
            None => Err(PoolError::ClosedByPeer),
        }
    }

    async fn send_frame(&mut self, frame: voice_providers::codec::GaxFrame) -> Result<(), PoolError> {
        // Realtime 路径 wire=Text 的帧按文本记录（便于 JSON 断言）
        match frame.wire {
            voice_providers::codec::WireFormat::Text => {
                let text = String::from_utf8_lossy(&frame.payload).into_owned();
                self.server.outgoing.lock().push(WsMessage::Text(text));
            }
            _ => {
                self.server.outgoing.lock().push(WsMessage::Binary(frame.payload));
            }
        }
        Ok(())
    }

    async fn close(self: Box<Self>) -> Result<(), PoolError> {
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        true
    }
}

fn scripted_dialer(server: &ScriptedServer) -> Dialer {
    let server = server.clone();
    Arc::new(move |_kind: LaneKind| {
        let server = server.clone();
        async move {
            let rx = server
                .incoming_rx_slot
                .lock()
                .await
                .take()
                .ok_or_else(|| PoolError::Handshake("scripted server 只支持一次拨号".into()))?;
            Ok(Box::new(ScriptedConn {
                server,
                incoming_rx: rx,
            }) as Box<dyn WebSocketLike>)
        }
        .boxed()
    })
}

// ===== helpers =====

fn realtime_session() -> (Arc<WsConnPool>, ScriptedServer, Dialer) {
    let pool = WsConnPool::new(PoolConfig {
        max_connections: 4,
        ..PoolConfig::default()
    });
    let server = ScriptedServer::new();
    let dialer = scripted_dialer(&server);
    (pool, server, dialer)
}

/// 收集事件流到指定数量或结束
async fn collect_events<S>(stream: &mut S, want: usize) -> Vec<Result<RealtimeEvent, ClientError>>
where
    S: futures_util::Stream<Item = Result<RealtimeEvent, ClientError>> + Unpin,
{
    let mut out = Vec::with_capacity(want);
    while out.len() < want {
        match stream.next().await {
            Some(item) => out.push(item),
            None => break,
        }
    }
    out
}

fn unwrap_events(
    items: Vec<Result<RealtimeEvent, ClientError>>,
) -> Vec<RealtimeEvent> {
    items
        .into_iter()
        .map(|r| r.expect("事件流不应有错误"))
        .collect()
}

// ===== 测试用例 =====

/// 核心回归：多句连续听写，事件流不在首个 final 断流。
/// 服务端脚本：created → partial → final(第一句) → partial → final(第二句) → finished
#[tokio::test]
async fn multi_sentence_stream_not_cut_at_first_final() {
    let (pool, server, dialer) = realtime_session();
    let adapter = Qwen3RealtimeAdapter::for_model("qwen3-asr-flash-realtime");

    let (_session, mut stream) = start_realtime_session(
        pool,
        adapter,
        dialer,
        16000,
        1,
        "task-multi".to_string(),
    )
    .await
    .unwrap();

    // 预置服务端事件脚本（FIFO，循环任务按序消费）
    server.server_event(serde_json::json!({ "type": "session.created", "session": { "id": "s1" } }));
    server.server_event(serde_json::json!({
        "type": "conversation.item.input_audio_transcription.text", "text": "你", "stash": "好"
    }));
    server.server_event(serde_json::json!({
        "type": "conversation.item.input_audio_transcription.completed", "transcript": "你好。"
    }));
    server.server_event(serde_json::json!({
        "type": "conversation.item.input_audio_transcription.text", "text": "世", "stash": "界"
    }));
    server.server_event(serde_json::json!({
        "type": "conversation.item.input_audio_transcription.completed", "transcript": "世界。"
    }));
    server.server_event(serde_json::json!({ "type": "session.finished" }));

    let events = unwrap_events(collect_events(&mut stream, 6).await);
    assert_eq!(
        events,
        vec![
            RealtimeEvent::Partial { text: "你好".into() },
            RealtimeEvent::Final { text: "你好。".into() },
            RealtimeEvent::Partial { text: "世界".into() },
            RealtimeEvent::Final { text: "世界。".into() },
            RealtimeEvent::Finished,
        ],
        "两个 final + Finished 都要收到；若在首个 final 断流，这里只有 1 个 final"
    );

    // Finished 之后流自然结束
    assert!(stream.next().await.is_none());
}

/// 上行帧序列与切片：session.update → append×3（6400B 拆 2 帧 + 1600B 1 帧）→ session.finish
#[tokio::test]
async fn outgoing_frame_sequence_and_chunking() {
    let (pool, server, dialer) = realtime_session();
    let adapter = Qwen3RealtimeAdapter::for_model("qwen3-asr-flash-realtime");

    let (session, mut stream) = start_realtime_session(
        pool,
        adapter,
        dialer,
        16000,
        1,
        "task-frames".to_string(),
    )
    .await
    .unwrap();

    // 6400B = 2×3200B 帧；1600B = 1 帧（尾帧）
    session.send_audio(&vec![0u8; 6400]).unwrap();
    session.send_audio(&vec![1u8; 1600]).unwrap();
    session.finish().unwrap();

    // open(1) + append(3) + finish(1) = 5 条上行
    server.wait_outgoing_count(5).await;
    let frames: Vec<serde_json::Value> = server
        .outgoing_texts()
        .iter()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();

    assert_eq!(frames[0]["type"], "session.update");
    assert_eq!(frames[0]["session"]["sample_rate"], 16000);
    assert_eq!(frames[1]["type"], "input_audio_buffer.append");
    assert_eq!(frames[2]["type"], "input_audio_buffer.append");
    assert_eq!(frames[3]["type"], "input_audio_buffer.append");
    assert_eq!(frames[4]["type"], "session.finish");

    // base64 长度断言：3200B → 4268 字符（ceil(3200/3)*4）；1600B → 2136 字符
    let a1 = frames[1]["audio"].as_str().unwrap().len();
    let a2 = frames[2]["audio"].as_str().unwrap().len();
    let a3 = frames[3]["audio"].as_str().unwrap().len();
    assert_eq!((a1, a2, a3), (4268, 4268, 2136));

    // finish 后让服务端回 finished，会话干净结束
    server.server_event(serde_json::json!({ "type": "session.finished" }));
    let events = unwrap_events(collect_events(&mut stream, 1).await);
    assert_eq!(events, vec![RealtimeEvent::Finished]);
    assert!(stream.next().await.is_none());
}

/// VAD 边沿事件透出 + finish 后残余 final 仍可达
#[tokio::test]
async fn vad_events_and_trailing_final_after_finish() {
    let (pool, server, dialer) = realtime_session();
    let adapter = Qwen3RealtimeAdapter::for_model("qwen3-asr-flash-realtime");

    let (session, mut stream) = start_realtime_session(
        pool,
        adapter,
        dialer,
        16000,
        1,
        "task-vad".to_string(),
    )
    .await
    .unwrap();

    session.send_audio(&vec![0u8; 3200]).unwrap();
    session.finish().unwrap();
    server.wait_outgoing_count(3).await;

    // 服务端：VAD 起止 + finish 之后还吐一个句终 + finished
    server.server_event(serde_json::json!({ "type": "input_audio_buffer.speech_started" }));
    server.server_event(serde_json::json!({ "type": "input_audio_buffer.speech_stopped" }));
    server.server_event(serde_json::json!({
        "type": "conversation.item.input_audio_transcription.completed", "transcript": "尾巴。"
    }));
    server.server_event(serde_json::json!({ "type": "session.finished" }));

    let events = unwrap_events(collect_events(&mut stream, 4).await);
    assert_eq!(
        events,
        vec![
            RealtimeEvent::SpeechStarted,
            RealtimeEvent::SpeechStopped,
            RealtimeEvent::Final { text: "尾巴。".into() },
            RealtimeEvent::Finished,
        ]
    );
}

/// abandon：drop 句柄 → 后台任务退出 → 连接归还 idle、事件流结束
#[tokio::test]
async fn abandon_releases_connection_and_ends_stream() {
    let (pool, _server, dialer) = realtime_session();
    let adapter = Qwen3RealtimeAdapter::for_model("qwen3-asr-flash-realtime");

    let (session, mut stream) = start_realtime_session(
        pool.clone(),
        adapter,
        dialer,
        16000,
        1,
        "task-abandon".to_string(),
    )
    .await
    .unwrap();

    assert_eq!(pool.idle_count(LaneKind::Asr), 0);

    // abandon：drop 句柄
    drop(session);

    // 事件流结束（event_tx drop）——stream! 需要被 poll 才推进？不：
    // event_rx.recv() 在 channel 关闭后返回 None，无需外部驱动
    let mut ended = false;
    for _ in 0..200 {
        if stream.next().await.is_none() {
            ended = true;
            break;
        }
    }
    assert!(ended, "abandon 后事件流应结束");

    // 连接归还 idle 池（release 在 spawn 任务内，稍等生效）
    let mut idle = 0;
    for _ in 0..200 {
        idle = pool.idle_count(LaneKind::Asr);
        if idle == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(idle, 1, "abandon 后连接应 healthy 归还 idle 池");
}

/// 服务端 error 事件 → 事件流以 Err 结束
#[tokio::test]
async fn server_error_terminates_stream_with_err() {
    let (pool, server, dialer) = realtime_session();
    let adapter = Qwen3RealtimeAdapter::for_model("qwen3-asr-flash-realtime");

    let (_session, mut stream) = start_realtime_session(
        pool,
        adapter,
        dialer,
        16000,
        1,
        "task-err".to_string(),
    )
    .await
    .unwrap();

    server.server_event(serde_json::json!({
        "type": "error",
        "error": { "code": "invalid_api_key", "message": "Invalid API key provided" }
    }));

    let item = stream.next().await.unwrap();
    let err = item.expect_err("error 事件应映射为 Err");
    let msg = err.to_string();
    assert!(msg.contains("invalid_api_key"), "msg={msg}");
    assert!(stream.next().await.is_none(), "错误后流应终止");
}
