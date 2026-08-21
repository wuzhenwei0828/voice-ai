//! 增量式流式 ASR 会话。
//!
//! 与 `StreamingAsrClient::recognize`（一次性消费整段音频的批量 API）不同，
//! `StreamingAsrSession` 暴露 start / send_audio / finish 的事件式接口，
//! 适合"浏览器实时麦克风 → 后端 → DashScope"这类边收边发的场景。
//!
//! 用法：
//!   let (session, events) = start_session(pool, adapter, dialer, sr, ch, task_id).await?;
//!   // 后台任务即可并发消费 events 推给前端
//!   session.send_audio(&pcm_chunk)?;
//!   session.send_audio(&chunk2)?;
//!   session.finish()?;
//!   // events 流在 finish 后继续产出 transcript，最终由服务端关闭连接
//!
//! 内部：一次性 acquire 连接 + 发 run-task，然后 spawn 一个后台任务把
//! `cmd_rx` 的 Audio/Finish 命令转成 WS 帧；服务端 events 通过 `event_tx` 推回。
//! 任务结束后连接自动 release。

use std::pin::Pin;
use std::sync::Arc;

use async_stream::stream;
use futures_util::Stream;
use tokio::sync::mpsc;

use crate::asr::{AsrEvent, AsrModelAdapter, ClientError};
use crate::codec::GaxFrame;
use crate::ws_pool::{Dialer, LaneKind, WsConnPool};

/// 客户端 → 服务端的控制命令
#[derive(Debug)]
pub enum SessionCmd {
    Audio(Vec<u8>),
    Finish,
}

/// 流式 ASR 会话句柄。`Clone` 便宜，多个组件可共享。
#[derive(Clone)]
pub struct StreamingAsrSession {
    cmd_tx: mpsc::UnboundedSender<SessionCmd>,
}

impl StreamingAsrSession {
    /// 推一段 PCM 字节给 DashScope。无长度限制（内部按模型期望的 100ms 切片）。
    pub fn send_audio(&self, pcm: &[u8]) -> Result<(), ClientError> {
        self.cmd_tx
            .send(SessionCmd::Audio(pcm.to_vec()))
            .map_err(|_| ClientError::Ws("session already closed".into()))
    }

    /// 通知 DashScope 任务结束。后台任务会发 finish-task 并继续 drain 直到收完所有回包。
    pub fn finish(&self) -> Result<(), ClientError> {
        self.cmd_tx
            .send(SessionCmd::Finish)
            .map_err(|_| ClientError::Ws("session already closed".into()))
    }
}

/// 启动一个流式 ASR 会话。返回 (session 句柄, 事件流)。
///
/// 流程：
/// 1. acquire WSS 连接
/// 2. 发 run-task JSON (Qwen) / protobuf (Paraformer GAX)
/// 3. spawn 后台任务：
///    - 接 cmd_rx 的 Audio 帧 → 转发
///    - 接 cmd_rx 的 Finish 帧 → 发 finish-task，drain 服务端回包到 event_tx
///    - 收到 Err / 连接断开 → 终止
/// 4. 返回的 events 流由调用方消费（典型用法：推到前端 WS）
pub async fn start_session(
    pool: Arc<WsConnPool>,
    adapter: Box<dyn AsrModelAdapter>,
    dialer: Dialer,
    sample_rate: u32,
    channels: u16,
    session_id: String,
) -> Result<
    (
        StreamingAsrSession,
        Pin<Box<dyn Stream<Item = Result<AsrEvent, ClientError>> + Send>>,
    ),
    ClientError,
> {
    // 1. acquire connection
    let mut conn = pool
        .acquire_or_dial(LaneKind::Asr, dialer)
        .await
        .map_err(|e| ClientError::Pool(e.to_string()))?;

    // 2. send run-task
    let open = adapter.open_request(&session_id, sample_rate, channels);
    if let Err(e) = conn.send(open).await {
        // 拨号成功但 run-task 失败：连接不健康，标记 unhealthy
        conn.release(false);
        return Err(ClientError::Ws(e.to_string()));
    }

    // 3. 准备 channels
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<SessionCmd>();
    let (event_tx, event_rx) = mpsc::unbounded_channel::<Result<AsrEvent, ClientError>>();

    // 4. spawn 后台任务
    let session_id_bg = session_id.clone();
    tokio::spawn(async move {
        let mut cmd_rx = cmd_rx;
        let mut conn = conn;
        let adapter = adapter;
        let event_tx = event_tx;
        let mut ever_finished = false;

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                SessionCmd::Audio(pcm) => {
                    let frame = adapter.audio_frame(&pcm);
                    if let Err(e) = conn.send(frame).await {
                        let _ = event_tx.send(Err(ClientError::Ws(e.to_string())));
                        break;
                    }
                }
                SessionCmd::Finish => {
                    ever_finished = true;
                    let stop = adapter.stop_frame(&session_id_bg);
                    if let Err(e) = conn.send(stop).await {
                        let _ = event_tx.send(Err(ClientError::Ws(e.to_string())));
                        // 不 break ——服务端可能已返回剩余事件
                    }
                    // drain 服务端回包直到 is_final=true 或连接断开
                    loop {
                        match conn.recv().await {
                            Ok(frame) => {
                                trace_dbg_recv(&adapter, &frame);
                                match adapter.parse_event(&frame.payload) {
                                    Ok(Some(evt)) => {
                                        let is_final = evt.is_final;
                                        let _ = event_tx.send(Ok(evt));
                                        if is_final {
                                            break;
                                        }
                                    }
                                    Ok(None) => continue,
                                    Err(e) => {
                                        let _ = event_tx.send(Err(e));
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = event_tx.send(Err(ClientError::Ws(e.to_string())));
                                break;
                            }
                        }
                    }
                    break;
                }
            }
        }

        // 关闭连接
        let _ = conn.release(ever_finished);
        // event_tx drop 后 event_rx.next() 返回 None，上游流自然结束
    });

    let session = StreamingAsrSession { cmd_tx };
    let stream = stream! {
        let mut rx = event_rx;
        while let Some(item) = rx.recv().await {
            yield item;
        }
    };
    Ok((session, Box::pin(stream)))
}

// 避免 cfg 太多导致编译期 cfg 警告。把 trace 留给上层 tracing。
fn trace_dbg_recv(_adapter: &Box<dyn AsrModelAdapter>, _frame: &GaxFrame) {}
