//! live-asr 运行时：懒初始化的 ClientSlot + 每会话 LiveAsrState

use std::sync::Arc;
use std::sync::OnceLock;

use actix_web::rt as actix_rt;
use dashmap::DashMap;
use tokio::sync::Mutex;
use tracing::info;

use crate::client::funasr::FunasrSender;

use super::config::{load_cfg, AsrLiveCfg};

/// 配置模板懒初始化结果：
/// - Ready(cfg) — 配置模板可用；每会话按前端 SessionStart 的 sample_rate / channels
///   派生新的 FunasrClient
/// - Failed(msg) — 启动失败（缺 endpoint 等）—— 错误延迟到首次 SessionStart 回报浏览器
///   （避免 OnceLock 缓存 panic 后所有后续消息都炸）
///
/// 注意：之前是 `ArcFunasr`，但 FunasrClient 把 sample_rate / channels / wav_name 烤进了
/// 自己持有的 cfg，导致**前端 SessionStart 传的 sample_rate 完全被忽略**。改成持有
/// `Arc<AsrLiveCfg>` 配置模板后，每会话都能按前端值派生自己的 client。
pub(super) enum ClientSlot {
    Ready(Arc<AsrLiveCfg>),
    Failed(String),
}

static RUNTIME: OnceLock<ClientSlot> = OnceLock::new();

pub(super) fn runtime() -> &'static ClientSlot {
    RUNTIME.get_or_init(|| {
        let cfg = Arc::new(load_cfg());
        // 校验 endpoint —— 配置模板自身的 endpoint 必须可用，否则每会话都无法建 client
        if cfg.endpoint().trim().is_empty() {
            let msg = "asr_live 配置异常：endpoint 为空";
            tracing::warn!(target: "voice_server.live_asr", "{msg}");
            return ClientSlot::Failed(msg.to_string());
        }
        info!(
            target: "voice_server.live_asr",
            endpoint = %cfg.endpoint(),
            mode = %cfg.mode().as_str(),
            wav_format = %cfg.wav_format(),
            chunk_size = ?cfg.chunk_size(),
            hotwords_set = cfg.hotwords.is_some(),
            timeout_secs = cfg.timeout_secs.unwrap_or(30),
            keepalive_secs = cfg.keepalive_secs.unwrap_or(20),
            close_grace_secs = cfg.close_grace_secs.unwrap_or(3),
            "asr_live 运行时初始化（AsrLiveCfg 配置模板；每会话按 SessionStart 派生 FunasrClient；sample_rate/channels 由前端传，无 yaml fallback）"
        );
        ClientSlot::Ready(cfg)
    })
}

/// 取出配置模板（如果 runtime 初始化成功）。用于收尾时读 close_grace。
pub(super) fn runtime_cfg() -> Option<&'static AsrLiveCfg> {
    match runtime() {
        ClientSlot::Ready(cfg) => Some(cfg),
        ClientSlot::Failed(_) => None,
    }
}

// ===== 每个浏览器会话的状态 =====

/// 单会话状态机：
/// - Pending (`sender=None, recv_task=None`) — start_session 还在进行中；AudioChunk 进 pending_audio 缓冲
/// - Ready   (`sender=Some`)                  — 上游 WSS 已建，独立 recv task 在跑
/// - Driving (`finished=true`)                — 已进入收尾流程，迟到帧丢弃
/// - Failed  (`failed=true`)                   — start_session 失败，剩余帧丢弃
///
/// **关键设计**：audio 上行（on_audio_chunk）和 transcript 下行（drive_recv_loop 后台 task）
/// 必须并发 —— 否则 FunASR 服务端发的识别结果会卡在 WS buffer 里等不到消费，
/// 表现为"只有用户点结束才一次性看到所有结果"。拆 sender / receiver 解决了这个 bug。
pub(super) struct LiveAsrState {
    /// 发送端：Arc<Mutex<>> 让 on_audio_chunk / on_session_end 都能短暂 lock 进来发 PCM/finish。
    /// recv task 不需要这个 —— 它只读 receiver。
    pub(super) sender: Option<Arc<Mutex<FunasrSender>>>,
    /// 独立后台 recv task 的句柄 —— on_session_end 触发收尾时需要 .await 它确保所有下行都发完。
    pub(super) recv_task: Option<actix_rt::task::JoinHandle<()>>,
    pub(super) pending_audio: Vec<u8>,
    pub(super) pending_is_last: bool,
    pub(super) current_message_id: Option<String>,
    pub(super) failed: bool,
    pub(super) finished: bool,
}

impl LiveAsrState {
    pub(super) fn pending() -> Self {
        Self {
            sender: None,
            recv_task: None,
            pending_audio: Vec::new(),
            pending_is_last: false,
            current_message_id: None,
            failed: false,
            finished: false,
        }
    }
}

type LiveAsrMap = DashMap<String, Arc<Mutex<LiveAsrState>>>;
static SESSIONS: OnceLock<LiveAsrMap> = OnceLock::new();

pub(super) fn sessions() -> &'static LiveAsrMap {
    SESSIONS.get_or_init(DashMap::new)
}

/// 从 map 拿一份独立的 Arc<Mutex<LiveAsrState>>，用完即丢——绝不在 await 路径上持有 DashMap sharding ref。
pub(super) fn get_state_arc(session_id: &str) -> Option<Arc<Mutex<LiveAsrState>>> {
    let state = sessions().get(session_id)?;
    Some(Arc::clone(state.value()))
    // state (Ref) 在此 drop，DashMap shard 释放
}
