//! voice-server 实时流式 ASR WebSocket handler
//!
//! 业务路径：`/ws/live-asr/<actor>/<connid>`
//! 上行 / 下行复用既有 `VoicePayload`（msgpack over binary frame）：
//! - 浏览器 → 服务端：`SessionStart` / `AudioChunk` / `SessionEnd`
//! - 服务端 → 浏览器：`AsrPartial { text, is_final }`（复用既有 variant，无需扩 voice-proto）
//! - 协议详细：`crates/voice-proto/src/lib.rs`
//!
//! ## 协议层
//!
//! 使用 `client::funasr::FunasrClient` 直连本地部署的 FunASR WSS（典型
//! `ws://127.0.0.1:10095/`，无鉴权）。`FunasrClient` 只暴露 WS 帧级原语
//! (`start_session` / `send_audio` / `send_finish` / `next_event` / `close`)，由本模块驱动。
//! `next_event` 返回 `FunasrEvent::{Message, Close, Error}`，对应浏览器 WS 的 onmessage/onclose/onerror。
//!
//! ## 业务层时序（eager：上游连接与"开始识别"同步建立）
//!
//!   `SessionStart`  → 立刻 `client.start_session()` 建上游 WSS + 发首次 JSON
//!   `AudioChunk`    → `session.send_audio(chunk)` 实时推到上游（边录边发）
//!   `AudioChunk{is_last=true}` / `SessionEnd` / `Interrupt`
//!                    → `drive_to_completion`: send_finish + next_event 循环 → close
//!
//! 没有跨浏览器会话的连接复用 —— 上游会话与"浏览器一句话"严格 1:1。
//!
//! ## 模块拆分
//! - [`config`]   `AsrLiveCfg` 配置（YAML `asr_live` 段 + 环境变量覆盖）+ 单元测试
//! - [`runtime`]  懒初始化 `ClientSlot`（配置模板）+ 每会话 `LiveAsrState`
//! - [`mod`]      编排入口 `handle_message` + 业务处理 + 下行工具

pub mod config;
pub mod runtime;

use std::sync::Arc;
use std::time::Duration;

use actix::prelude::Recipient;
use actix_web::rt as actix_rt;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use voice_proto::{decode_payload, encode_indication, PayloadKind, VoicePayload};
use webhttp::websocket::OutMessage;

use crate::client::error::ClientError;
use crate::client::funasr::{
    build_funasr_client, ArcFunasr, FunasrEvent, FunasrReceiver, FunasrSender,
};

use self::runtime::{
    get_state_arc, runtime, runtime_cfg, sessions, ClientSlot, LiveAsrState,
};

// ===== webhttp wsdata 入口 =====

/// service.rs::wsdata 在收到 live-asr 业务消息时调用此函数。
/// 返回 unit，错误以 AsrPartial {is_final:true, text:"[error]..."} 下行回报。
pub fn handle_message(addr: Recipient<OutMessage>, session_id: String, payload: Vec<u8>) {
    actix_rt::spawn(async move {
        let (kind, p) = match decode_payload(&payload) {
            Ok(v) => v,
            Err(e) => {
                warn!(target: "voice_server.live_asr", session_id, error = %e, "解码失败");
                send_error(&addr, &session_id, "decode error");
                return;
            }
        };
        if kind != PayloadKind::Indication {
            debug!(target: "voice_server.live_asr", session_id, ?kind, "忽略非 Indication 消息");
            return;
        }
        dispatch(&addr, session_id, p).await;
    });
}

async fn dispatch(addr: &Recipient<OutMessage>, session_id: String, p: VoicePayload) {
    match p {
        VoicePayload::SessionStart { sample_rate, channels, .. } => {
            on_session_start(addr, session_id, sample_rate, channels as u16).await;
        }
        VoicePayload::AudioChunk { data, is_last, .. } => {
            on_audio_chunk(addr, session_id, data, is_last).await;
        }
        VoicePayload::SessionEnd { .. } => {
            on_session_end(addr, session_id).await;
        }
        VoicePayload::Interrupt { .. } => {
            // live-asr 无 LLM/TTS pipeline；Interrupt 视为"结束当前 ASR"
            on_session_end(addr, session_id).await;
        }
        other => {
            debug!(target: "voice_server.live_asr", session_id, ?other, "忽略非预期消息");
        }
    }
}

// ===== 业务处理 =====

async fn on_session_start(
    addr: &Recipient<OutMessage>,
    session_id: String,
    sample_rate: u32,
    channels: u16,
) {
    if sessions().contains_key(&session_id) {
        warn!(
            target: "voice_server.live_asr",
            session_id,
            "重复 SessionStart"
        );
        send_error(addr, &session_id, "session already started");
        return;
    }

    // 拿配置模板
    let cfg = match runtime() {
        ClientSlot::Ready(c) => c.clone(),
        ClientSlot::Failed(msg) => {
            warn!(
                target: "voice_server.live_asr",
                session_id,
                error = %msg,
                "runtime() 失败：asr_live 运行时未就绪"
            );
            send_error(addr, &session_id, msg.as_str());
            return;
        }
    };

    // 按前端 SessionStart 派生本会话的 FunasrConfig：
    // - sample_rate / channels —— **优先用前端值**（浏览器 AudioContext 实际采样率）；
    //   0 或缺失才回落 cfg 的 fallback。
    // - wav_name —— 默认按 session_id 派生（每浏览器会话唯一），配置里的 wav_name 仅在
    //   业务需要"全局唯一 wav_name"时才显式给。
    let funasr_cfg = match cfg.into_client_config(Some(&session_id), sample_rate, channels) {
        Some(c) => c,
        None => {
            let msg = "asr_live 配置模板 endpoint 为空";
            warn!(target: "voice_server.live_asr", session_id, "{msg}");
            send_error(addr, &session_id, msg);
            return;
        }
    };
    let client = build_funasr_client(funasr_cfg.clone());

    info!(
        target: "voice_server.live_asr",
        session_id,
        browser_sample_rate = sample_rate,
        browser_channels = channels,
        effective_sample_rate = funasr_cfg.sample_rate,
        effective_channels = funasr_cfg.channels,
        effective_wav_name = %funasr_cfg.wav_name,
        "← SessionStart 收到，pending state 插入 map，spawn run_setup"
    );

    // 关键：先把 pending state 插进 map，AudioChunk 立刻能查到。
    // 后续 start_session 还在 await 时到达的帧进 pending_audio 缓冲（关键修复：避免
    // 浏览器推 AudioChunk 时 SessionStart 还在握手而误报"尚未 start"）。
    let state_arc = Arc::new(Mutex::new(LiveAsrState::pending()));
    sessions().insert(session_id.clone(), state_arc.clone());

    // 后台任务跑握手，不阻塞 on_session_start —— 浏览器可以继续推 AudioChunk（缓冲）
    let addr_clone = addr.clone();
    let sid_clone = session_id.clone();
    actix_rt::spawn(async move {
        run_setup(addr_clone, sid_clone, client, state_arc).await;
    });
}

/// 后台建上游 WSS + 发首次 JSON + 启动独立 recv task
///
/// 完成时（成功 / 失败）给前端下行一条 `SessionAck`，让前端能明确等到握手结束再发 PCM。
async fn run_setup(
    addr: Recipient<OutMessage>,
    session_id: String,
    client: ArcFunasr,
    state_arc: Arc<Mutex<LiveAsrState>>,
) {
    info!(
        target: "voice_server.live_asr",
        session_id,
        "run_setup: 开始建上游 WSS + 发首次 JSON"
    );
    let session = match client.start_session(&session_id).await {
        Ok(s) => s,
        Err(e) => {
            warn!(target: "voice_server.live_asr", session_id, error = %e, "上游 start_session 失败");
            let mut g = state_arc.lock().await;
            g.failed = true;
            drop(g);
            sessions().remove(&session_id);
            // 给前端下行失败 ack，前端就能定位到"建连失败"而不是沉默
            send_session_ack(&addr, &session_id, false, &e.to_string());
            return;
        }
    };

    // 拆 session：sender 给业务 handler 用，receiver 给独立 recv task 用
    let (sender, receiver) = session.split();
    let sender_arc = Arc::new(Mutex::new(sender));

    // 取出 pending audio / is_last（在存 sender 前拿，避免 on_audio_chunk 误用空 state）
    let (pending_audio, pending_is_last) = {
        let mut g = state_arc.lock().await;
        let pa = std::mem::take(&mut g.pending_audio);
        let pi = g.pending_is_last;
        (pa, pi)
    };

    info!(
        target: "voice_server.live_asr",
        session_id,
        bytes_pending = pending_audio.len(),
        pending_is_last,
        "run_setup: 上游 WSS 已建，spawn 独立 recv task"
    );

    // 启动独立 recv task（**关键修复**：让服务端 transcript 实时下行，不再被卡到结束）
    let recv_task = actix_rt::spawn({
        let addr = addr.clone();
        let session_id = session_id.clone();
        async move {
            drive_recv_loop(addr, session_id, receiver).await;
        }
    });

    // 给前端下行成功 ack
    send_session_ack(&addr, &session_id, true, "");
    info!(
        target: "voice_server.live_asr",
        session_id,
        "← SessionAck success=true 下行"
    );

    if pending_is_last {
        // is_last 在 pending 期间到达：完整收尾流程 inline 跑完，不把 sender 暴露到 state
        if !pending_audio.is_empty() {
            if let Err(e) = flush_audio_chunks(&sender_arc, &pending_audio).await {
                warn!(target: "voice_server.live_asr", session_id, error = %e, "flush pending audio 失败");
                send_error(&addr, &session_id, e);
                let _ = close_sender(sender_arc).await;
                let _ = recv_task.await;
                sessions().remove(&session_id);
                return;
            }
        }
        // 显式 scope 让 MutexGuard 在 move sender_arc 之前 drop
        let send_finish_result = sender_arc.lock().await.send_finish().await;
        if let Err(e) = send_finish_result {
            warn!(target: "voice_server.live_asr", session_id, error = %e, "send_finish 失败");
            send_error(&addr, &session_id, e);
            let _ = close_sender(sender_arc).await;
            let _ = recv_task.await;
            sessions().remove(&session_id);
            return;
        }
        info!(
            target: "voice_server.live_asr",
            session_id,
            "run_setup: pending_is_last 已发 finish，等 recv_task 排空"
        );
        let _ = recv_task.await;
        let _ = close_sender(sender_arc).await;
        sessions().remove(&session_id);
        return;
    }

    // 否则：把 sender + recv_task 句柄存到 state 给后续 on_audio_chunk / on_session_end 用
    {
        let mut g = state_arc.lock().await;
        g.sender = Some(sender_arc);
        g.recv_task = Some(recv_task);
    }
}

/// 下行握手 ack 给前端。`success=false` 时 `message` 带原因。
fn send_session_ack(addr: &Recipient<OutMessage>, session_id: &str, success: bool, message: &str) {
    info!(
        target: "voice_server.live_asr",
        session_id,
        success,
        message,
        "→ 准备下行 SessionAck"
    );
    send_down(
        addr,
        VoicePayload::SessionAck {
            session_id: session_id.to_string(),
            success,
            message: message.to_string(),
        },
    );
}

async fn on_audio_chunk(
    addr: &Recipient<OutMessage>,
    session_id: String,
    data: Vec<u8>,
    is_last: bool,
) {
    let bytes = data.len();
    debug!(
        target: "voice_server.live_asr",
        session_id,
        bytes,
        is_last,
        "← AudioChunk 收到"
    );
    let state_arc = match get_state_arc(&session_id) {
        Some(arc) => arc,
        None => {
            warn!(
                target: "voice_server.live_asr",
                session_id,
                bytes,
                "AudioChunk 但 session 不存在"
            );
            send_error(addr, &session_id, "尚未 start，音频被丢弃");
            return;
        }
    };

    let mut g = state_arc.lock().await;

    if g.failed || g.finished {
        // start_session 失败 / 已进入收尾阶段 → 静默丢弃（避免重复报错）
        debug!(target: "voice_server.live_asr", session_id, failed = g.failed, finished = g.finished, "session 已结束/失败，迟到 audio chunk 丢弃");
        return;
    }

    if g.sender.is_none() {
        // Pending：start_session 还在跑，缓冲本帧
        g.pending_audio.extend_from_slice(&data);
        if is_last {
            g.pending_is_last = true;
        }
        debug!(
            target: "voice_server.live_asr",
            session_id,
            bytes,
            is_last,
            pending_audio_len = g.pending_audio.len(),
            "Pending：buffer 一帧"
        );
        return;
    }

    // Ready：先 flush pending_audio，再发本帧（锁住 send_audio 是 tokio Mutex，无阻塞问题）
    let pending_audio = std::mem::take(&mut g.pending_audio);
    let pending_chunks = pending_audio.chunks(CHUNK_BYTES).len();
    let sender = g.sender.as_ref().unwrap().clone();
    let mut send_err: Option<ClientError> = None;

    // 先 flush pending（注意：这里只 flush 音频，不发 finish；finish 在 on_session_end / is_last=true 触发）
    if let Err(e) = flush_audio_chunks(&sender, &pending_audio).await {
        send_err = Some(e);
    }
    if send_err.is_none() {
        if let Err(e) = sender.lock().await.send_audio(&data).await {
            send_err = Some(e);
        }
    }

    if let Some(e) = send_err {
        warn!(target: "voice_server.live_asr", session_id, bytes, error = %e, "send_audio 失败");
        send_error(addr, &session_id, e);
        drop(g);
        let close_grace = runtime_cfg()
            .map(|c| c.close_grace())
            .unwrap_or(Duration::from_secs(3));
        finalize_session(&state_arc, addr, &session_id, close_grace).await;
        sessions().remove(&session_id);
        return;
    }

    debug!(
        target: "voice_server.live_asr",
        session_id,
        bytes,
        is_last,
        pending_chunks,
        "Ready：已发本帧（含 flush pending）"
    );

    if is_last {
        // 本帧已发。标记 finished，等 on_session_end 收尾，或 inline 收尾
        g.finished = true;
        drop(g);
        debug!(
            target: "voice_server.live_asr",
            session_id,
            "is_last=true → 触发收尾（send_finish + 等 recv_task 排空 + close）"
        );
        let close_grace = runtime_cfg()
            .map(|c| c.close_grace())
            .unwrap_or(Duration::from_secs(3));
        finalize_session(&state_arc, addr, &session_id, close_grace).await;
        sessions().remove(&session_id);
    }
    // else: sender 留在 state 等下一帧
}

async fn on_session_end(addr: &Recipient<OutMessage>, session_id: String) {
    info!(
        target: "voice_server.live_asr",
        session_id,
        "← SessionEnd / Interrupt 收到"
    );
    let state_arc = match get_state_arc(&session_id) {
        Some(arc) => arc,
        None => {
            debug!(
                target: "voice_server.live_asr",
                session_id,
                "SessionEnd 时会话已不存在（已自动结束）"
            );
            return;
        }
    };

    let mut g = state_arc.lock().await;

    if g.finished || g.failed {
        return; // 已被 is_last / setup_task / start_session 失败收尾
    }

    if g.sender.is_none() {
        // Pending：start_session 还在跑，标 pending_is_last 让 run_setup 走收尾
        g.pending_is_last = true;
        info!(
            target: "voice_server.live_asr",
            session_id,
            "SessionEnd 在 pending 期间到达，等 start_session 完成后再收尾"
        );
        return;
    }

    // Ready：take sender，flush pending，跑收尾
    g.finished = true;
    let sender = g.sender.take().unwrap();
    let pending_audio = std::mem::take(&mut g.pending_audio);
    drop(g);

    if !pending_audio.is_empty() {
        if let Err(e) = flush_audio_chunks(&sender, &pending_audio).await {
            warn!(target: "voice_server.live_asr", session_id, error = %e, "flush pending audio 失败");
            send_error(addr, &session_id, e);
            let _ = close_sender(sender).await;
            sessions().remove(&session_id);
            return;
        }
    }
    let send_finish_result = sender.lock().await.send_finish().await;
    if let Err(e) = send_finish_result {
        warn!(target: "voice_server.live_asr", session_id, error = %e, "send_finish 失败");
        send_error(addr, &session_id, e);
        let _ = close_sender(sender).await;
        sessions().remove(&session_id);
        return;
    }
    // 等 recv_task 排空所有下行 → close sender → 清理 state
    let close_grace = runtime_cfg()
        .map(|c| c.close_grace())
        .unwrap_or(Duration::from_secs(3));
    finalize_with_sender(state_arc, sender, addr.clone(), &session_id, close_grace).await;
    sessions().remove(&session_id);
}

/// 共享收尾逻辑：从 state 取 sender + recv_task，send_finish，等 recv_task，close sender
async fn finalize_session(
    state_arc: &Arc<Mutex<LiveAsrState>>,
    addr: &Recipient<OutMessage>,
    session_id: &str,
    close_grace: Duration,
) {
    // 取走 sender + recv_task
    let (sender, recv_task) = {
        let mut g = state_arc.lock().await;
        let s = g.sender.take();
        let t = g.recv_task.take();
        (s, t)
    };
    let Some(sender) = sender else {
        // 已经收过尾（state.sender 已被 take）
        if let Some(t) = recv_task {
            let _ = t.await;
        }
        return;
    };
    finalize_with_sender_inner(state_arc, sender, recv_task, addr, session_id, close_grace).await;
}

async fn finalize_with_sender(
    state_arc: Arc<Mutex<LiveAsrState>>,
    sender: Arc<Mutex<FunasrSender>>,
    addr: Recipient<OutMessage>,
    session_id: &str,
    close_grace: Duration,
) {
    let recv_task = {
        let mut g = state_arc.lock().await;
        g.recv_task.take()
    };
    finalize_with_sender_inner(&state_arc, sender, recv_task, &addr, session_id, close_grace).await;
}

async fn finalize_with_sender_inner(
    _state_arc: &Arc<Mutex<LiveAsrState>>,
    sender: Arc<Mutex<FunasrSender>>,
    recv_task: Option<actix_rt::task::JoinHandle<()>>,
    _addr: &Recipient<OutMessage>,
    session_id: &str,
    close_grace: Duration,
) {
    if let Err(e) = sender.lock().await.send_finish().await {
        warn!(target: "voice_server.live_asr", session_id, error = %e, "send_finish 失败");
    }
    // 等 recv_task 把已缓冲的所有 transcript 排空 —— 但**最多等 close_grace**：
    // 超时后强制 close sender（让 rx 拿 None → FunasrEvent::Close(abnormal) → 退出）。
    // 否则 FunASR 不发 Close 时会无限等下去（典型场景：FunASR 进程挂死 / 网络异常）。
    if let Some(t) = recv_task {
        match tokio::time::timeout(close_grace, t).await {
            Ok(_) => {
                info!(
                    target: "voice_server.live_asr",
                    session_id,
                    "recv_task 自然退出（FunASR 发 Close 后正常退出）"
                );
            }
            Err(_) => {
                info!(
                    target: "voice_server.live_asr",
                    session_id,
                    close_grace_secs = close_grace.as_secs(),
                    "recv_task 未在 close_grace_secs 内退出（FunASR 未发 Close），强制 close 上游"
                );
            }
        }
    }
    let _ = close_sender(sender).await;
    info!(
        target: "voice_server.live_asr",
        session_id,
        "live ASR WSS 会话结束"
    );
}

/// 从 Arc<Mutex<FunasrSender>> 取出独占所有权再 close。
/// 失败时（还有其它 Arc 引用）fallback 到 drop —— WSS 会在 task 终止时自动回收。
async fn close_sender(sender: Arc<Mutex<FunasrSender>>) -> Result<(), ClientError> {
    match Arc::try_unwrap(sender) {
        Ok(mutex) => mutex.into_inner().close().await,
        Err(arc) => {
            // 还有其它引用（理论上 run_setup 收尾路径不会有，但 is_last + on_session_end 并发可能）
            // drop 后 WSS 由 FunasrSender drop trait 关闭
            drop(arc);
            Ok(())
        }
    }
}

// ===== 共享原语 =====

const CHUNK_BYTES: usize = 3_200; // 100ms @ 16kHz s16le mono

async fn flush_audio_chunks(
    sender: &Arc<Mutex<FunasrSender>>,
    audio: &[u8],
) -> Result<(), ClientError> {
    for chunk in audio.chunks(CHUNK_BYTES) {
        sender.lock().await.send_audio(chunk).await?;
    }
    Ok(())
}

/// 独立后台 recv 任务：从 FunasrReceiver 持续读 transcript 帧，按 mode 分发后下行到浏览器。
///
/// 关键修复：之前这条逻辑嵌在 `drive_to_completion_inner` 里，**只在 is_last / SessionEnd 才启动**，
/// 导致流式期间 FunASR 服务端发的识别结果卡在 WS buffer 等不到消费 —— 用户体验为
/// "只有点结束才一次性看到所有结果"。现在 run_setup 建连后立即 spawn，audio 上行 / transcript
/// 下行真正并发。
async fn drive_recv_loop(
    addr: Recipient<OutMessage>,
    session_id: String,
    mut receiver: FunasrReceiver,
) {
    info!(
        target: "voice_server.live_asr",
        session_id,
        "drive_recv_loop: 启动独立 recv 任务"
    );
    let mut event_count = 0u32;
    let mut final_count = 0u32;
    // 流式增量模式（online / 2pass-online）的 last_online_text 状态：
    // - streaming is_final=false 帧的 text 是**增量字符**（server 推送的就是新字符），透传即可
    // - streaming is_final=true 帧的 text 是**完整句子**（cache 了段首到当前的整段），
    //   要算 delta 让前端把最后几个字补上屏，再清零准备下一句
    // 关键参考：FunASR 官方 HTML5 demo (runtime/html5/static/main.js:362) 直接 `rec_text += text`，
    // 没做任何缓存 —— 因为 server 已经按增量字符推。
    let mut last_online_text = String::new();
    loop {
        match receiver.next_event().await {
            FunasrEvent::Message(resp) => {
                // 模式分发：见上注释
                let (text, is_final, replace_last) = match &resp.mode {
                    crate::client::funasr::FunasrResponseMode::TwoPassOffline => {
                        // 二次纠错：必须用全量替换上一行 final（不要 append 新行）
                        last_online_text.clear();
                        (resp.text.clone(), true, true)
                    }
                    m if m.is_cumulative() && resp.is_final => {
                        // 流式 + 句终：server 推完整句子，去掉已显示部分，剩下的就是要 append 的最后几个字
                        let new_text = &resp.text;
                        let delta: String = if new_text.starts_with(&last_online_text) {
                            new_text[last_online_text.len()..].to_string()
                        } else {
                            // 缓存不一致（理论不会发生，兜底）：发全量
                            warn!(
                                target: "voice_server.live_asr",
                                session_id,
                                last = %last_online_text,
                                new = %new_text,
                                "句终时 last_online_text 与新文本不一致，发全量"
                            );
                            new_text.clone()
                        };
                        last_online_text.clear();
                        // delta 空（句终时也有可能）也照发，前端用 is_final=true 触发 final 上屏
                        (delta, true, false)
                    }
                    m if m.is_cumulative() => {
                        // 流式增量：server 推的就是新字符，直接透传
                        last_online_text.push_str(&resp.text);
                        // 空文本帧（如 VAD begin / metadata） → 不下发
                        if resp.text.is_empty() {
                            continue;
                        }
                        (resp.text.clone(), false, false)
                    }
                    crate::client::funasr::FunasrResponseMode::Offline => {
                        // 离线模式：一次返回完整句子，清零缓存，全量下发
                        last_online_text.clear();
                        (resp.text.clone(), true, false)
                    }
                    _ => {
                        // 未知 mode（服务端偶发）：保守全量下发
                        (resp.text.clone(), resp.is_final, false)
                    }
                };

                event_count += 1;
                if is_final {
                    final_count += 1;
                }
                info!(
                    target: "voice_server.live_asr",
                    session_id,
                    event_count,
                    final_count,
                    mode = ?resp.mode,
                    is_final,
                    replace_last,
                    text_len = text.chars().count(),
                    text = %text,
                    "→ AsrPartial 下行"
                );
                let payload = VoicePayload::AsrPartial {
                    session_id: session_id.to_string(),
                    text,
                    is_final,
                    replace_last,
                    request_id: 0,
                };
                send_down(&addr, payload);
            }
            FunasrEvent::Close(c) => {
                // onclose —— FunASR 协议下 = 识别完成。code=1006 表明 stream 异常结束，
                // 但本地视角都是"不会再有结果了"，同样退出 recv loop。
                info!(
                    target: "voice_server.live_asr",
                    session_id,
                    event_count,
                    final_count,
                    close_code = c.code,
                    close_reason = %c.reason,
                    "drive_recv_loop: FunASR 会话结束（onclose），recv 任务退出"
                );
                return;
            }
            FunasrEvent::Error(e) => {
                // onerror —— WS 读失败 / 超时。向浏览器回报并退出（drive 任务跟会话同生共死）。
                warn!(target: "voice_server.live_asr", session_id, error = %e, "next_event 失败（onerror）");
                send_error(&addr, &session_id, e);
                return;
            }
        }
    }
}

// ===== 下行工具 =====

/// 错误下行：ws error / decode error / 任意 Display 都能格式化
fn send_error<E: std::fmt::Display>(addr: &Recipient<OutMessage>, session_id: &str, message: E) {
    warn!(target: "voice_server.live_asr", session_id, error = %message, "向浏览器回报错误");
    let payload = VoicePayload::AsrPartial {
        session_id: session_id.to_string(),
        text: format!("[error] {}", message),
        is_final: true,
        replace_last: false,
        request_id: 0,
    };
    send_down(addr, payload);
}

fn send_down(addr: &Recipient<OutMessage>, payload: VoicePayload) {
    let kind = match &payload {
        VoicePayload::SessionAck { .. } => "SessionAck",
        VoicePayload::AsrPartial { .. } => "AsrPartial",
        VoicePayload::Error { .. } => "Error",
        _other => "other",
    };
    info!(
        target: "voice_server.live_asr",
        session_id = ?payload.session_id(),
        kind,
        "↓ 下行即将调用 try_send"
    );
    match encode_indication(&payload) {
        Ok(bytes) => {
            let bytes_len = bytes.len();
            match addr.try_send(OutMessage { data: bytes }) {
                Ok(()) => info!(
                    target: "voice_server.live_asr",
                    session_id = ?payload.session_id(),
                    kind,
                    bytes = bytes_len,
                    "↓ 下行 try_send Ok（入 actor 邮箱）"
                ),
                Err(e) => warn!(
                    target: "voice_server.live_asr",
                    session_id = ?payload.session_id(),
                    kind,
                    error = %e,
                    "↓ 下行 try_send Err（recipient 已死 / ws actor 已停）"
                ),
            }
        }
        Err(e) => warn!(
            target: "voice_server.live_asr",
            session_id = ?payload.session_id(),
            kind,
            error = %e,
            "↓ 下行编码失败"
        ),
    }
}
