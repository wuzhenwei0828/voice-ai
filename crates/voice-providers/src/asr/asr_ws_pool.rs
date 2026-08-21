//! voice-providers ASR 门面：封装"wspool + dialer + adapter + endpoint 解析"
//!
//! 对外只暴露两个东西：
//! - `trait WsPool`（接口） —— "给我一个 asr WS client"
//! - `struct AsrWsPool`（DashScope 公共协议 ASR 的实现）
//!
//! 内部封装：
//! - `WsConnPool`（低层 conn pool 原语）—— `acquire_or_dial` 拨号 / idle 复用
//! - `make_real_dialer`（构造 Authorization + WorkSpace header）—— 业务层看不见
//! - `QwenAsrAdapter::for_model`（按模型选 adapter）—— 业务层看不见
//! - `resolve_endpoint`（workspace_id+region 拼 WSS URL）—— 业务层看不见
//!
//! 与 `voice_providers::asr::session::start_session`（自由函数）的关系：
//! `AsrWsPool::start_session` 内部调 `start_session`，把 dialer/adapter 封装好后传进去。
//! 既有 mock 测试（qwen_asr_test / qwen3_asr_realtime_test）继续用 `start_session` 自由函数。
//!
//! ## 错误分类
//!
//! - `AsrWsError::Config(String)` —— endpoint / api_key / model / workspace_id 缺失或非法
//! - `AsrWsError::Pool(PoolError)` —— WsConnPool 拨号 / 获取失败
//! - `AsrWsError::Session(ClientError)` —— run-task 发送 / stream 初始化失败
//! - `AsrWsError::Handshake(String)` —— 握手后的业务错误（预留）
//!
//! event_stream 内部仍用 `ClientError`（runtime/streaming 错误，与既有契约一致）。

use std::sync::Arc;

use async_trait::async_trait;

use crate::asr::qwen::QwenAsrAdapter;
use crate::asr::session::{start_session, StreamingAsrSession};
use crate::asr::{AsrEvent, BoxStream, ClientError};
use crate::provider::make_real_dialer;
use crate::ws_pool::{Dialer, WsConnPool};

// ===== 错误类型 =====

#[derive(Debug, thiserror::Error)]
pub enum AsrWsError {
    /// 配置缺失或非法（api_key / workspace_id / endpoint）
    #[error("asr ws 配置错误: {0}")]
    Config(String),

    /// WsConnPool 拨号 / 获取连接失败
    #[error("连接池错误: {0}")]
    Pool(#[from] PoolError),

    /// ASR 会话初始化失败（run-task 发送 / stream 建立 / adapter 构造）
    #[error("asr 会话错误: {0}")]
    Session(#[from] ClientError),

    /// 握手后的业务错误（预留）
    #[error("handshake 错误: {0}")]
    Handshake(String),
}

// 复用 ws_pool 的 PoolError 命名空间（不让 AsrWsError 把 ws_pool 也对外暴露）
use crate::ws_pool::PoolError;

// ===== 配置 =====

#[derive(Debug, Clone)]
pub struct AsrPoolConfig {
    /// DashScope 模型名（fun-asr-realtime / qwen-audio-3.0-asr-flash-streaming / paraformer-realtime-v2 等）
    pub model: String,
    /// DashScope API key
    pub api_key: String,
    /// 业务空间 ID（公共协议端点必需，除非显式给 endpoint）
    pub workspace_id: Option<String>,
    /// 地域：cn-beijing（默认）| ap-southeast-1
    pub region: String,
    /// 显式 WSS endpoint（覆盖默认构造）
    pub endpoint: Option<String>,
    /// 采样率（默认 16000）
    pub sample_rate: u32,
    /// 声道数（默认 1，Realtime 协议固定单声道）
    pub channels: u16,
}

impl AsrPoolConfig {
    /// 公共协议 ASR 端点解析：`{WorkspaceId}.{region}.maas.aliyuncs.com/api-ws/v1/inference`
    /// 与 qwen3-asr-flash-realtime 的 `/realtime?model=` 不同。
    pub fn resolve_endpoint(&self) -> Result<String, AsrWsError> {
        if let Some(ep) = self.endpoint.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            return Ok(ep.to_string());
        }
        let ws = self
            .workspace_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AsrWsError::Config(
                    "endpoint 或 workspace_id 至少需要一个（公共协议端点形如 wss://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/api-ws/v1/inference）".into(),
                )
            })?;
        let region = if self.region.trim().is_empty() { "cn-beijing" } else { self.region.trim() };
        Ok(format!(
            "wss://{}.{}.maas.aliyuncs.com/api-ws/v1/inference",
            ws, region
        ))
    }
}

// ===== 接口 =====

#[async_trait]
pub trait WsPool: Send + Sync {
    /// 启动一个流式 ASR 会话。
    ///
    /// 返回 (StreamingAsrSession, BoxStream<Result<AsrEvent, ClientError>>)：
    /// - StreamingAsrSession 提供 send_audio / finish / abandon（句柄便宜 clone）
    /// - event_stream 是 DashScope transcript 事件流；ClientError 是 runtime/streaming 错误
    ///
    /// `session_id` 透传给 DashScope run-task / finish-task 作为 task_id（同一会话内必须稳定）
    async fn start_session(
        &self,
        session_id: String,
    ) -> Result<
        (StreamingAsrSession, BoxStream<Result<AsrEvent, ClientError>>),
        AsrWsError,
    >;
}

// ===== DashScope 公共协议 ASR 实现 =====

pub struct AsrWsPool {
    inner: Arc<WsConnPool>,
    cfg: AsrPoolConfig,
    /// 拨号器（生产 = make_real_dialer；测试 = mock 注入）
    dialer: Dialer,
}

impl std::fmt::Debug for AsrWsPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsrWsPool")
            .field("model", &self.cfg.model)
            .field("workspace_id", &self.cfg.workspace_id)
            .field("region", &self.cfg.region)
            .field("sample_rate", &self.cfg.sample_rate)
            .finish()
    }
}

impl AsrWsPool {
    /// 生产路径：用 `make_real_dialer` 构造 dialer
    pub fn new(cfg: AsrPoolConfig) -> Result<Self, AsrWsError> {
        // 预校验：endpoint / workspace_id 解析必须成功（否则 dialer 拿到错误 URL）
        let endpoint = cfg.resolve_endpoint()?;
        if cfg.api_key.trim().is_empty() {
            return Err(AsrWsError::Config("api_key 未配置".into()));
        }
        let dialer = make_real_dialer(
            endpoint,
            cfg.api_key.clone(),
            cfg.workspace_id
                .as_deref()
                .map(str::to_string)
                .filter(|s| !s.is_empty()),
        );
        let inner = Arc::new(WsConnPool::new(crate::ws_pool::PoolConfig::default()));
        Ok(Self { inner: Arc::clone(&inner), cfg, dialer })
    }

    /// 测试路径：注入自定义 dialer（通常是 mock WebSocket）。
    /// 只校验 api_key；endpoint 由调用方（dialer 自身）负责。
    pub fn with_dialer(cfg: AsrPoolConfig, dialer: Dialer) -> Result<Self, AsrWsError> {
        if cfg.api_key.trim().is_empty() {
            return Err(AsrWsError::Config("api_key 未配置".into()));
        }
        let inner = Arc::new(WsConnPool::new(crate::ws_pool::PoolConfig::default()));
        Ok(Self { inner: Arc::clone(&inner), cfg, dialer })
    }

    /// 访问配置（测试用）
    pub fn config(&self) -> &AsrPoolConfig {
        &self.cfg
    }
}

#[async_trait]
impl WsPool for AsrWsPool {
    async fn start_session(
        &self,
        session_id: String,
    ) -> Result<
        (StreamingAsrSession, BoxStream<Result<AsrEvent, ClientError>>),
        AsrWsError,
    > {
        let adapter: Box<dyn crate::asr::AsrModelAdapter> =
            Box::new(QwenAsrAdapter::for_model(&self.cfg.model));
        let result = start_session(
            self.inner.clone(),
            adapter,
            self.dialer.clone(),
            self.cfg.sample_rate,
            self.cfg.channels,
            session_id,
        )
        .await;
        result.map_err(AsrWsError::from)
    }
}

// ===== 单元测试 =====

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::future::FutureExt;

    fn base_cfg() -> AsrPoolConfig {
        AsrPoolConfig {
            model: "fun-asr-realtime".into(),
            api_key: "sk-test".into(),
            workspace_id: Some("llm-abc".into()),
            region: "cn-beijing".into(),
            endpoint: None,
            sample_rate: 16000,
            channels: 1,
        }
    }

    // ===== AsrWsError Display =====

    #[test]
    fn error_display_config() {
        let e = AsrWsError::Config("api_key 未配置".into());
        assert_eq!(e.to_string(), "asr ws 配置错误: api_key 未配置");
    }

    #[test]
    fn error_display_pool_chains_inner() {
        let pe = PoolError::AcquireTimeout(std::time::Duration::from_millis(100));
        let e = AsrWsError::Pool(pe);
        // #[from] 保证 inner error 的 Display 自动嵌入
        assert!(e.to_string().starts_with("连接池错误:"));
    }

    #[test]
    fn error_display_session_chains_inner() {
        let ce = ClientError::Ws("dial timeout".into());
        let e = AsrWsError::Session(ce);
        assert!(e.to_string().starts_with("asr 会话错误:"));
    }

    #[test]
    fn error_display_handshake() {
        let e = AsrWsError::Handshake("unexpected task-failed".into());
        assert_eq!(e.to_string(), "handshake 错误: unexpected task-failed");
    }

    #[test]
    fn error_from_pool_error() {
        let pe = PoolError::ClosedByPeer;
        let e: AsrWsError = pe.into();
        assert!(matches!(e, AsrWsError::Pool(PoolError::ClosedByPeer)));
    }

    #[test]
    fn error_from_client_error() {
        let ce = ClientError::Decode("bad json".into());
        let e: AsrWsError = ce.into();
        assert!(matches!(e, AsrWsError::Session(ClientError::Decode(_))));
    }

    // ===== AsrPoolConfig::resolve_endpoint =====

    #[test]
    fn endpoint_from_workspace_default_region() {
        let cfg = base_cfg();
        assert_eq!(
            cfg.resolve_endpoint().unwrap(),
            "wss://llm-abc.cn-beijing.maas.aliyuncs.com/api-ws/v1/inference"
        );
    }

    #[test]
    fn endpoint_from_workspace_sg_region() {
        let cfg = AsrPoolConfig {
            region: "ap-southeast-1".into(),
            ..base_cfg()
        };
        assert_eq!(
            cfg.resolve_endpoint().unwrap(),
            "wss://llm-abc.ap-southeast-1.maas.aliyuncs.com/api-ws/v1/inference"
        );
    }

    #[test]
    fn endpoint_explicit_wins() {
        let cfg = AsrPoolConfig {
            endpoint: Some("wss://custom.example.com/inference".into()),
            ..base_cfg()
        };
        assert_eq!(
            cfg.resolve_endpoint().unwrap(),
            "wss://custom.example.com/inference"
        );
    }

    #[test]
    fn endpoint_missing_workspace_and_endpoint_is_config_err() {
        let cfg = AsrPoolConfig {
            workspace_id: None,
            endpoint: None,
            ..base_cfg()
        };
        let err = cfg.resolve_endpoint().unwrap_err();
        assert!(matches!(err, AsrWsError::Config(_)));
        assert!(err.to_string().contains("workspace_id"));
    }

    #[test]
    fn endpoint_empty_strings_treated_as_missing() {
        let cfg = AsrPoolConfig {
            workspace_id: Some("   ".into()),
            endpoint: Some("".into()),
            ..base_cfg()
        };
        assert!(cfg.resolve_endpoint().is_err());
    }

    // ===== AsrWsPool::new 配置预校验 =====

    #[test]
    fn new_missing_api_key_is_config_err() {
        let cfg = AsrPoolConfig {
            api_key: "".into(),
            ..base_cfg()
        };
        let err = AsrWsPool::new(cfg).unwrap_err();
        assert!(matches!(err, AsrWsError::Config(_)));
        assert!(err.to_string().contains("api_key"));
    }

    #[test]
    fn new_missing_workspace_is_config_err() {
        let cfg = AsrPoolConfig {
            workspace_id: None,
            endpoint: None,
            ..base_cfg()
        };
        let err = AsrWsPool::new(cfg).unwrap_err();
        assert!(matches!(err, AsrWsError::Config(_)));
    }

    #[test]
    fn new_succeeds_with_valid_cfg() {
        let cfg = base_cfg();
        let pool = AsrWsPool::new(cfg).expect("valid cfg");
        assert_eq!(pool.config().model, "fun-asr-realtime");
    }

    #[test]
    fn with_dialer_constructs_without_endpoint_resolution_failure() {
        let cfg = base_cfg();
        let dialer: Dialer = Arc::new(|_kind| {
            async { Err(PoolError::Handshake("mock".into())) }.boxed()
        });
        let pool = AsrWsPool::with_dialer(cfg, dialer).unwrap();
        assert_eq!(pool.config().model, "fun-asr-realtime");
    }
}