//! WS 帧级原语：`FunasrSession` + `FunasrSender` + `FunasrReceiver`
//!
//! 负责把一条 `WebSocketStream` 拆成两端：
//! - `FunasrSender`：发 PCM / send_finish / close；内部 spawn keepalive 后台 task
//! - `FunasrReceiver`：阻塞收下一个 `FunasrEvent`
//!
//! audio 上行（on_audio_chunk）和 transcript 下行（独立后台 task）需要并发跑，
//! 不能用 `&mut FunasrSession` 互斥。

use std::sync::Arc;
use std::time::Duration;

use async_tungstenite::tungstenite::protocol::CloseFrame;
use async_tungstenite::tungstenite::Message;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, info, warn};

use super::protocol::{parse_server_event, FunasrClose, FunasrEvent};
use super::WsStream;
use crate::client::error::ClientError;

/// 单个 WSS 会话（一次 start_session / 多次 send_audio / 一次 send_finish / 多次 recv_event）
///
/// **设计要点**：把 tx / rx **拆成两个独立的 half** —— `FunasrSender`（仅发）和 `FunasrReceiver`（仅收）。
/// 因为 audio 上行（on_audio_chunk）和 transcript 下行（独立后台 task）需要并发跑，
/// 不能用 `&mut FunasrSession` 互斥。先建 `FunasrSession` 再 `split()` 成两半。
pub struct FunasrSession {
    pub(super) tx: SplitSink<WsStream, Message>,
    pub(super) rx: SplitStream<WsStream>,
    pub(super) timeout: Duration,
    pub(super) wav_name: String,
    /// 转交给 `FunasrSender::spawn_keepalive` 用；`Duration::ZERO` = 禁用
    pub(super) keepalive_interval: Duration,
}

impl FunasrSession {
    /// 把 WSS 流拆成发送端 + 接收端，调用方分别 spawn 进独立 task。
    ///
    /// `keepalive_interval` 由 `FunasrConfig` 传入：> 0 时会在 `FunasrSender` 内 spawn 一个
    /// 后台 task，每 N 秒向上游 WSS 发 `Message::Ping(Vec::new())`，避免 FunASR 服务端
    /// 被自己的 idle timeout 误杀（参见 `FunasrConfig::keepalive_interval` 注释）。
    pub fn split(self) -> (FunasrSender, FunasrReceiver) {
        let Self {
            tx,
            rx,
            timeout,
            wav_name,
            keepalive_interval,
        } = self;
        // 共享给用户调用 + keepalive task —— 两者都要拿 `&mut SplitSink`，互斥串行
        let tx = Arc::new(TokioMutex::new(tx));
        let keepalive_handle =
            FunasrSender::spawn_keepalive(tx.clone(), keepalive_interval, wav_name.clone());
        (
            FunasrSender {
                tx,
                timeout,
                wav_name: wav_name.clone(),
                keepalive_handle,
            },
            FunasrReceiver {
                rx,
                timeout,
                wav_name,
            },
        )
    }
}

/// 发送端：send_audio / send_finish / close
/// 可被多个 task 共享（包 `Arc<Mutex<>>` 后），但调用方需自己保证互斥。
///
/// **keepalive**：构造时（`split()` 内）会按 `FunasrConfig::keepalive_interval` 起一个后台
/// ping task。`Drop` impl 会 abort 该 task —— 不会泄漏。
pub struct FunasrSender {
    tx: Arc<TokioMutex<SplitSink<WsStream, Message>>>,
    timeout: Duration,
    wav_name: String,
    keepalive_handle: Option<JoinHandle<()>>,
}

impl FunasrSender {
    /// 构造后台 keepalive task（每 `interval` 发一帧 Ping）。
    /// - `interval == ZERO` 返回 None，不 spawn
    /// - send 失败（连接已断）→ task 自然退出
    fn spawn_keepalive(
        tx: Arc<TokioMutex<SplitSink<WsStream, Message>>>,
        interval: Duration,
        wav_name: String,
    ) -> Option<JoinHandle<()>> {
        if interval.is_zero() {
            return None;
        }
        Some(tokio::spawn(async move {
            let mut tick = time::interval(interval);
            // 第一次 tick 立刻触发有点早 —— 等一个完整 interval 再开始（给业务帧让路）
            tick.tick().await;
            loop {
                tick.tick().await;
                let mut guard = tx.lock().await;
                let res = guard.send(Message::Ping(Vec::new())).await;
                match res {
                    Ok(()) => {
                        debug!(
                            target: "voice_server.funasr",
                            wav_name = %wav_name,
                            "→ keepalive Ping"
                        );
                    }
                    Err(e) => {
                        // 连接已断，task 自然退出（避免错误日志刷屏）
                        debug!(
                            target: "voice_server.funasr",
                            wav_name = %wav_name,
                            error = %e,
                            "keepalive Ping 失败，task 退出（连接已断？）"
                        );
                        break;
                    }
                }
            }
        }))
    }

    /// 发一段裸 PCM binary 帧（chunk 大小由调用方决定）
    pub async fn send_audio(&mut self, pcm: &[u8]) -> Result<(), ClientError> {
        let len = pcm.len();
        let mut guard = self.tx.lock().await;
        let res = tokio::time::timeout(self.timeout, guard.send(Message::Binary(pcm.to_vec())))
            .await
            .map_err(|_| ClientError::Ws("send_audio 超时".into()))?
            .map_err(|e| ClientError::Ws(format!("send_audio: {}", e)));
        if let Err(ref e) = res {
            warn!(
                target: "voice_server.funasr",
                wav_name = %self.wav_name,
                bytes = len,
                error = %e,
                "send_audio 失败"
            );
        } else {
            // debug：实时性问题排查；info 级别对 ~10 帧/秒的浏览器推流会太刷屏
            debug!(
                target: "voice_server.funasr",
                wav_name = %self.wav_name,
                bytes = len,
                "→ audio binary"
            );
        }
        res
    }

    /// 通知服务端本会话音频已发完：发 `{"is_speaking": false}`
    pub async fn send_finish(&mut self) -> Result<(), ClientError> {
        let frame =
            serde_json::to_string(&serde_json::json!({"is_speaking": false})).expect("static json");
        info!(
            target: "voice_server.funasr",
            wav_name = %self.wav_name,
            "→ finish JSON: {}", frame
        );
        let mut guard = self.tx.lock().await;
        let res = tokio::time::timeout(self.timeout, guard.send(Message::Text(frame)))
            .await
            .map_err(|_| ClientError::Ws("send_finish 超时".into()))?
            .map_err(|e| ClientError::Ws(format!("send_finish: {}", e)));
        if let Err(ref e) = res {
            warn!(
                target: "voice_server.funasr",
                wav_name = %self.wav_name,
                error = %e,
                "send_finish 失败"
            );
        }
        res
    }

    /// 关闭 WSS 连接（graceful Close 帧 + best-effort）。
    /// 注意：本方法**消耗** self（`mut self`），让 Drop 跑起来 —— Drop 会先 abort keepalive task，
    /// 然后 inner `Arc<TokioMutex<SplitSink>>` drop，若仍是最后一个 Arc 则 Drop::drop 触发
    /// SplitSink 的清理逻辑（向 ws stream 发 EOF）。这样保证 keepalive task 不会在 close 之后
    /// 还试图往已关闭的 stream 里塞 Ping。
    pub async fn close(mut self) -> Result<(), ClientError> {
        info!(
            target: "voice_server.funasr",
            wav_name = %self.wav_name,
            "→ WSS Close 帧（best-effort）"
        );
        let mut guard = self.tx.lock().await;
        let _ = tokio::time::timeout(self.timeout, guard.send(Message::Close(None))).await;
        // guard 在函数结尾 drop，释放锁；keepalive task 下一 tick 还会试图 lock —— 但此时
        // ws stream 已经走完 close 流程，Ping.send() 会失败 → task 自然 break 退出。
        drop(guard);
        // 显式 abort：避免 keepalive 还要再等一个 interval 才退出
        if let Some(h) = self.keepalive_handle.take() {
            h.abort();
        }
        Ok(())
    }
}

impl Drop for FunasrSender {
    fn drop(&mut self) {
        // 非 close 路径下的兜底：例如 caller 直接 `let _ = sender;` 丢掉 / 走 Arc 路径漏 close
        if let Some(h) = self.keepalive_handle.take() {
            h.abort();
        }
    }
}

/// 接收端：仅 `next_event`，由独立后台 task 持有整个生命周期。
/// FunASR 服务端在所有结果输出完毕后发 Close 帧，`next_event` 返回 `FunasrEvent::Close(_)`。
pub struct FunasrReceiver {
    rx: SplitStream<WsStream>,
    timeout: Duration,
    wav_name: String,
}

impl FunasrReceiver {
    /// 阻塞收下一个 `FunasrEvent`。
    ///   - `FunasrEvent::Message(resp)` — 识别事件（onmessage：mode + text + is_final）
    ///   - `FunasrEvent::Close(c)` — 服务端结束（onclose：FunASR 协议 = 识别完成，**不是错误**）
    ///   - `FunasrEvent::Error(e)` — WS 错误（onerror：超时 / 读失败 / stream 异常）
    ///
    /// 内部循环会**吃掉**以下帧而不返回：服务端的 metadata 帧、`Message::Ping(_)`、
    /// 解析失败的服务端 JSON（warn 后继续）。这些都不应杀掉整轮 recv。
    pub async fn next_event(&mut self) -> FunasrEvent {
        loop {
            let msg = match tokio::time::timeout(self.timeout, self.rx.next()).await {
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(e))) => {
                    // WS 读错误（协议层 fail）—— onerror
                    let err = ClientError::Ws(format!("recv: {}", e));
                    warn!(
                        target: "voice_server.funasr",
                        wav_name = %self.wav_name,
                        error = %err,
                        "← 读 WS 帧失败（onerror）"
                    );
                    return FunasrEvent::Error(err);
                }
                Ok(None) => {
                    // stream 结束但未收到 Close 帧 —— 对齐浏览器 WS code=1006 abnormal closure
                    // **不是错误** —— 调用方应当退出 recv loop，不视作失败。
                    warn!(
                        target: "voice_server.funasr",
                        wav_name = %self.wav_name,
                        "← stream 结束但未见 Close 帧（onclose code=1006 abnormal）"
                    );
                    return FunasrEvent::Close(FunasrClose::abnormal());
                }
                Err(_) => {
                    let err = ClientError::Ws("next_event 超时".into());
                    warn!(
                        target: "voice_server.funasr",
                        wav_name = %self.wav_name,
                        "← recv 超时（onerror）"
                    );
                    return FunasrEvent::Error(err);
                }
            };
            match msg {
                Message::Text(t) => {
                    let bytes = t.as_bytes();
                    debug!(
                        target: "voice_server.funasr",
                        wav_name = %self.wav_name,
                        "← 上游文本帧: {}",
                        String::from_utf8_lossy(bytes)
                    );
                    match parse_server_event(bytes) {
                        Ok(Some(e)) => {
                            info!(
                                target: "voice_server.funasr",
                                wav_name = %self.wav_name,
                                mode = ?e.mode,
                                is_final = e.is_final,
                                text_len = e.text.chars().count(),
                                text = %e.text,
                                "← 识别结果（onmessage）"
                            );
                            return FunasrEvent::Message(e);
                        }
                        Ok(None) => continue, // 服务端偶尔发的 metadata / 未知帧，忽略继续等
                        Err(e) => {
                            // 单帧 parse 失败不能杀掉整轮 recv —— warn 后继续等下一帧
                            warn!(
                                target: "voice_server.funasr",
                                wav_name = %self.wav_name,
                                error = %e,
                                raw = %String::from_utf8_lossy(bytes),
                                "← 解析失败（继续等下一帧，不退出 loop）"
                            );
                            continue;
                        }
                    }
                }
                Message::Ping(_) => {
                    // FunASR server 配 ping_interval=None（funasr_wss_server.py:841），
                    // 理论上不会发 Ping；万一收到时丢弃即可（无 tx 不能 Pong，
                    // WebSocket 协议允许 unsent Pong —— server timeout 通常也很长）
                    debug!(target: "voice_server.funasr", wav_name = %self.wav_name, "← 收到 Ping（已忽略，FunASR server 通常不发）");
                    continue;
                }
                Message::Close(c) => {
                    // FunASR 协议下 Close = 识别完成。`Option<CloseFrame>`：服务端可能发空 Close 帧。
                    let close = c
                        .map(FunasrClose::from_frame)
                        .unwrap_or_else(FunasrClose::normal);
                    info!(
                        target: "voice_server.funasr",
                        wav_name = %self.wav_name,
                        code = close.code,
                        reason = %close.reason,
                        "← 服务端 Close 帧（onclose —— FunASR 识别结束）"
                    );
                    return FunasrEvent::Close(close);
                }
                _ => continue,
            }
        }
    }
}

/// 借 `CloseFrame<'_>` 用 ws 模块 —— 编译期保证字段可见，单独放这里免得引入未用 `CloseFrame`
#[allow(dead_code)]
fn _touch_close_frame(_: CloseFrame<'_>) {}
