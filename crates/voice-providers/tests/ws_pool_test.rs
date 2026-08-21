//! WS pool 集成测试：mock WebSocketLike + 真实 Pool 行为

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures_util::future::FutureExt;
use voice_providers::codec::GaxFrame;
use voice_providers::ws_pool::{
    Dialer, LaneKind, PoolConfig, PoolError, WebSocketLike, WsConnPool,
};
use voice_providers::ws_pool::test_helpers::MockWs;

// 由于 MockWs 的 incoming_gax/incoming_ws 都是私有字段，我们写一个 helper 测试版本：
// 暴露 push_incoming 让 dialer 能预设消息
pub struct TestMockWs {
    inner: MockWs,
}

impl TestMockWs {
    pub fn new() -> Self {
        Self { inner: MockWs::new() }
    }
    pub fn push_incoming(&self, f: GaxFrame) {
        self.inner.push_incoming(f);
    }
    pub fn into_inner(self) -> MockWs {
        self.inner
    }
}

#[async_trait::async_trait]
impl WebSocketLike for TestMockWs {
    async fn send_text(&mut self, text: &str) -> Result<(), PoolError> {
        self.inner.send_text(text).await
    }
    async fn send_binary(&mut self, bytes: Vec<u8>) -> Result<(), PoolError> {
        self.inner.send_binary(bytes).await
    }
    async fn recv_message(&mut self) -> Result<voice_providers::ws_pool::WsMessage, PoolError> {
        self.inner.recv_message().await
    }
    async fn send_frame(&mut self, f: GaxFrame) -> Result<(), PoolError> {
        self.inner.send_frame(f).await
    }
    async fn recv_frame(&mut self) -> Result<GaxFrame, PoolError> {
        self.inner.recv_frame().await
    }
    async fn close(self: Box<Self>) -> Result<(), PoolError> {
        // 直接吞掉 close；mock 不需要真实关闭
        Ok(())
    }
    fn is_healthy(&self) -> bool {
        self.inner.is_healthy()
    }
}

// dialer：每次新建 TestMockWs，附带预置若干帧到 incoming
fn test_dialer_with_incoming(
    dialed: Arc<AtomicUsize>,
    prefilled: Vec<GaxFrame>,
) -> Dialer {
    Arc::new(move |_kind: LaneKind| {
        let dialed = dialed.clone();
        let prefilled = prefilled.clone();
        async move {
            dialed.fetch_add(1, Ordering::SeqCst);
            let ws = TestMockWs::new();
            for f in prefilled {
                ws.push_incoming(f);
            }
            Ok(Box::new(ws) as Box<dyn WebSocketLike>)
        }
        .boxed()
    })
}

// ===== 测试用例 =====

#[tokio::test]
async fn acquire_release_returns_to_idle() {
    let pool = WsConnPool::new(PoolConfig {
        max_connections: 4,
        ..PoolConfig::default()
    });
    let dialed = Arc::new(AtomicUsize::new(0));
    let dialer = test_dialer_with_incoming(dialed.clone(), vec![]);

    // 第一次 acquire → dial
    let conn1 = pool.acquire_or_dial(LaneKind::Asr, dialer.clone()).await.unwrap();
    assert_eq!(dialed.load(Ordering::SeqCst), 1);
    assert_eq!(pool.idle_count(LaneKind::Asr), 0);

    // 释放（healthy）
    conn1.release(true);
    assert_eq!(pool.idle_count(LaneKind::Asr), 1);

    // 第二次 acquire → 复用 idle，不应再 dial
    let conn2 = pool.acquire_or_dial(LaneKind::Asr, dialer.clone()).await.unwrap();
    assert_eq!(dialed.load(Ordering::SeqCst), 1); // 没新增 dial
    assert_eq!(pool.idle_count(LaneKind::Asr), 0);
    conn2.release(true);
    assert_eq!(pool.idle_count(LaneKind::Asr), 1);
}

#[tokio::test]
async fn release_unhealthy_dials_new_on_next_acquire() {
    let pool = WsConnPool::new(PoolConfig {
        max_connections: 4,
        ..PoolConfig::default()
    });
    let dialed = Arc::new(AtomicUsize::new(0));
    let dialer = test_dialer_with_incoming(dialed.clone(), vec![]);

    let conn1 = pool.acquire_or_dial(LaneKind::Asr, dialer.clone()).await.unwrap();
    assert_eq!(dialed.load(Ordering::SeqCst), 1);

    // 释放 unhealthy → 不入队
    conn1.release(false);
    assert_eq!(pool.idle_count(LaneKind::Asr), 0);

    // 第二次 acquire → 重新 dial
    let conn2 = pool.acquire_or_dial(LaneKind::Asr, dialer.clone()).await.unwrap();
    assert_eq!(dialed.load(Ordering::SeqCst), 2);
    conn2.release(true);
}

#[tokio::test]
async fn max_connections_blocks_extra_acquire() {
    let pool = WsConnPool::new(PoolConfig {
        max_connections: 2,
        acquire_timeout: std::time::Duration::from_millis(200),
        ..PoolConfig::default()
    });
    let dialed = Arc::new(AtomicUsize::new(0));
    let dialer = test_dialer_with_incoming(dialed.clone(), vec![]);

    // 占满 2 个 slot
    let c1 = pool.acquire_or_dial(LaneKind::Asr, dialer.clone()).await.unwrap();
    let c2 = pool.acquire_or_dial(LaneKind::Asr, dialer.clone()).await.unwrap();
    assert_eq!(dialed.load(Ordering::SeqCst), 2);

    // 第 3 个 acquire 应阻塞至超时 → 报错
    let r = pool.acquire_or_dial(LaneKind::Asr, dialer.clone()).await;
    assert!(matches!(r, Err(PoolError::AcquireTimeout(_))));

    // 释放一个 → 下一个 acquire 应成功
    c1.release(true);
    let c3 = pool.acquire_or_dial(LaneKind::Asr, dialer.clone()).await.unwrap();
    c2.release(true);
    c3.release(true);
}

#[tokio::test]
async fn lanes_are_isolated() {
    let pool = WsConnPool::new(PoolConfig {
        max_connections: 4,
        ..PoolConfig::default()
    });
    let dialed_asr = Arc::new(AtomicUsize::new(0));
    let dialed_tts = Arc::new(AtomicUsize::new(0));

    let asr_dialer = test_dialer_with_incoming(dialed_asr.clone(), vec![]);
    let tts_dialer = test_dialer_with_incoming(dialed_tts.clone(), vec![]);

    // 拿一条 ASR lane 的连接
    let asr_conn = pool.acquire_or_dial(LaneKind::Asr, asr_dialer.clone()).await.unwrap();
    assert_eq!(dialed_asr.load(Ordering::SeqCst), 1);
    assert_eq!(dialed_tts.load(Ordering::SeqCst), 0);

    // 释放回 ASR lane
    asr_conn.release(true);
    assert_eq!(pool.idle_count(LaneKind::Asr), 1);
    assert_eq!(pool.idle_count(LaneKind::Tts), 0);

    // TTS lane 借用：idle 列表是空的，应重新 dial
    let tts_conn = pool.acquire_or_dial(LaneKind::Tts, tts_dialer.clone()).await.unwrap();
    assert_eq!(dialed_asr.load(Ordering::SeqCst), 1); // 没新增
    assert_eq!(dialed_tts.load(Ordering::SeqCst), 1); // 新增 1
    tts_conn.release(true);
}