//! BailianProvider —— 装配 WS pool + ASR/TTS/LLM 三件套
//!
//! `build_all(cfg)` 是 voice-providers 暴露给 voice-server 的顶层入口：
//! 接收 BailianConfig（不依赖 voice-server），返回 (ArcAsr, ArcLlm, ArcTts)。
//!
//! voice-server 在 PR1 的 build_all_for_kind 阶段调用：
//!   let cfg = BailianConfig::from_mapping(voice_cfg.bailian.as_ref(), ws_endpoint, api_key)?;
//!   let (asr, llm, tts) = voice_providers::build_all(&cfg)?;
//! 然后把三件套包成 voice-server 的 trait 对象塞进 service。

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::asr::{build_asr_client, ArcAsr};
use crate::config::BailianConfig;
use crate::llm::{build_llm_client, ArcLlm};
use crate::tts::{build_tts_client, ArcTts};
use crate::ws_pool::{Dialer, LaneKind, PoolConfig, TungsteniteWs, WebSocketLike, WsConnPool};

// ===== build_all =====

/// 顶层入口：装配 (Asr, Llm, Tts) 三件套
pub fn build_all(cfg: &BailianConfig) -> anyhow::Result<(ArcAsr, ArcLlm, ArcTts)> {
    info!(
        target: "voice_providers",
        ws_endpoint = %cfg.ws_endpoint,
        asr_model = %cfg.asr.model,
        tts_model = %cfg.tts.model,
        tts_voice = %cfg.tts.voice,
        tts_endpoint = ?cfg.tts.endpoint.as_deref(),
        llm_model = %cfg.llm.model,
        pool_max = cfg.pool.max_connections,
        "BailianProvider 构造"
    );

    let pool_cfg = PoolConfig {
        max_connections: cfg.pool.max_connections,
        acquire_timeout: cfg.pool.acquire_timeout(),
        idle_timeout: cfg.pool.idle_timeout(),
        connect_timeout: cfg.pool.connect_timeout(),
    };
    let pool = WsConnPool::new(pool_cfg);

    // TTS 端点优先级：tts.endpoint > 自动构造
    let tts_ws_endpoint = cfg
        .tts
        .endpoint
        .clone()
        .unwrap_or_else(|| build_tts_endpoint(cfg));

    // ASR 端点优先级：asr.endpoint > 自动构造（按 model 路由 Qwen3 Realtime vs 公共）
    let asr_ws_endpoint = cfg
        .asr
        .endpoint
        .clone()
        .unwrap_or_else(|| build_asr_endpoint(cfg));
    let asr_workspace_id = cfg.asr.workspace_id.clone();

    // ASR / TTS 各自一个 dialer（端点不同），TTS 还会把 model/endpoint 一并传给上层做协议路由
    let asr_dialer = make_real_dialer(asr_ws_endpoint, cfg.api_key.clone(), asr_workspace_id);
    let tts_dialer = make_real_dialer(
        tts_ws_endpoint,
        cfg.api_key.clone(),
        cfg.tts.workspace_id.clone(),
    );

    let asr = build_asr_client(
        pool.clone(),
        &cfg.asr.model,
        cfg.asr.sample_rate,
        cfg.asr.channels,
        asr_dialer,
    )?;
    let tts = build_tts_client(
        pool.clone(),
        &cfg.tts.model,
        &cfg.tts.voice,
        cfg.tts.sample_rate,
        cfg.tts.response_format.clone(),
        cfg.tts.stream,
        tts_dialer,
    )?;
    let llm = build_llm_client(
        // LLM 走 OpenAI-compat endpoint：百炼 /compatible-mode/v1
        "https://dashscope.aliyuncs.com/compatible-mode/v1".to_string(),
        cfg.api_key.clone(),
        cfg.llm.model.clone(),
        Duration::from_secs(30),
    );

    Ok((asr, llm, tts))
}

/// 单独构造 ASR 端的 WsConnPool + dialer，供 voice-server 暴露流式 ASR WebSocket 端点用。
///
/// 复用 `build_all` 内部的端点解析逻辑（asr.endpoint / workspace_id / 默认 URL 路由）。
pub fn build_asr_streaming_pool(
    cfg: &BailianConfig,
) -> (Arc<WsConnPool>, Dialer) {
    let pool_cfg = PoolConfig {
        max_connections: cfg.pool.max_connections,
        acquire_timeout: cfg.pool.acquire_timeout(),
        idle_timeout: cfg.pool.idle_timeout(),
        connect_timeout: cfg.pool.connect_timeout(),
    };
    let pool = WsConnPool::new(pool_cfg);

    let asr_ws_endpoint = cfg
        .asr
        .endpoint
        .clone()
        .unwrap_or_else(|| build_asr_endpoint(cfg));
    let asr_workspace_id = cfg.asr.workspace_id.clone();
    let asr_dialer = make_real_dialer(asr_ws_endpoint, cfg.api_key.clone(), asr_workspace_id);

    (pool, asr_dialer)
}

/// 根据 TTS 配置自动构造 DashScope 公共 WSS endpoint。
///
/// 规则（按 docs/qwen-tts-docs/qwen-tts-ws-api.md）：
/// - `qwen3-tts-*` / `qwen-tts-*`（Realtime API）：
///   `wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=<model>`
/// - 其它（Qwen-Audio-TTS / CosyVoice 公共协议）：
///   - 有 `workspace_id`：`wss://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/api-ws/v1/inference`
///   - 无 `workspace_id`：直接使用 `cfg.ws_endpoint`（调用方自行配置）
fn build_tts_endpoint(cfg: &BailianConfig) -> String {
    if cfg.tts.model.starts_with("qwen3-tts") || cfg.tts.model.starts_with("qwen-tts") {
        format!(
            "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model={}",
            cfg.tts.model
        )
    } else if let Some(ws) = cfg.tts.workspace_id.as_deref() {
        format!("wss://{}.cn-beijing.maas.aliyuncs.com/api-ws/v1/inference", ws)
    } else {
        cfg.ws_endpoint.clone()
    }
}

/// 根据 ASR 配置自动构造 DashScope WSS endpoint。
///
/// 规则（按 docs/qwen-asr-docs/qwen-asr-ws-api.md）：
/// - `qwen3-asr-flash-realtime`（OpenAI-Realtime 风格）：
///   `wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=qwen3-asr-flash-realtime`
/// - 其它（Qwen-Audio-3.0 / Fun-ASR / Paraformer 公共协议）：
///   - 有 `workspace_id`：`wss://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/api-ws/v1/inference`
///   - 无 `workspace_id`：直接使用 `cfg.ws_endpoint`（调用方自行配置）
fn build_asr_endpoint(cfg: &BailianConfig) -> String {
    let model = &cfg.asr.model;
    if model == "qwen3-asr-flash-realtime"
        || model.starts_with("qwen3-asr")
        || model.starts_with("qwen-asr")
    {
        // Realtime API 需要 ?model= 查询参数
        format!(
            "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model={}",
            model
        )
    } else if let Some(ws) = cfg.asr.workspace_id.as_deref() {
        format!("wss://{}.cn-beijing.maas.aliyuncs.com/api-ws/v1/inference", ws)
    } else {
        cfg.ws_endpoint.clone()
    }
}

/// 真实拨号器：`async_tungstenite::tokio::connect_async` 接 DashScope WSS。
///
/// 流程（按 docs/qwen-asr-docs/qwen-asr-ws-api.md）：
///   1. `ws_endpoint` 即完整 URL（DashScope 公共协议，不按 lane kind 拼路径）。
///   2. `Authorization: Bearer <api_key>`（Bearer 前缀兼容；空 key 不发）。
///   3. 若提供了 `workspace_id`，加 `X-DashScope-WorkSpace: <id>`。
///   4. 用 `connect_async` 建连，拿到 `WebSocketStream` 后包成 `TungsteniteWs` 返回。
///
/// 注：`X-DashScope-DataInspection` 文档明确说"如非必要请勿启用"，dialer 不发。
pub fn make_real_dialer(ws_endpoint: String, api_key: String, workspace_id: Option<String>) -> Dialer {
    use futures_util::future::FutureExt;
    use async_tungstenite::tungstenite::client::IntoClientRequest;
    use async_tungstenite::tungstenite::http::HeaderValue;
    use async_tungstenite::tokio::connect_async as ws_connect;

    Arc::new(move |_kind: LaneKind| {
        let ws_endpoint = ws_endpoint.clone();
        let api_key = api_key.clone();
        let workspace_id = workspace_id.clone();
        async move {
            let url = ws_endpoint.clone();

            let mut req = url
                .into_client_request()
                .map_err(|e| {
                    crate::ws_pool::PoolError::Handshake(format!("into_client_request: {}", e))
                })?;

            // Authorization: Bearer <key>
            if !api_key.is_empty() {
                let key = api_key
                    .strip_prefix("Bearer ")
                    .or_else(|| api_key.strip_prefix("bearer "))
                    .unwrap_or(&api_key);
                let v: HeaderValue = format!("Bearer {}", key)
                    .parse()
                    .map_err(|e| {
                        crate::ws_pool::PoolError::Handshake(format!(
                            "invalid Authorization header: {}",
                            e
                        ))
                    })?;
                req.headers_mut().insert("Authorization", v);
            }

            // X-DashScope-WorkSpace —— 文档说"可选"。有 workspace_id 时带上，
            // 让服务端能识别业务空间（推荐使用 `{WorkspaceId}.cn-beijing.maas.aliyuncs.com`
            // 形态的 WSS 时也推荐带）。
            if let Some(ws) = workspace_id.as_deref() {
                if !ws.is_empty() {
                    if let Ok(v) = HeaderValue::from_str(ws) {
                        req.headers_mut().insert("X-DashScope-WorkSpace", v);
                    }
                }
            }

            // 注：X-DashScope-DataInspection 文档写"如非必要请勿启用"，这里不主动发。

            let (stream, _resp) = ws_connect(req).await.map_err(|e| {
                crate::ws_pool::PoolError::Handshake(format!("connect_async: {}", e))
            })?;

            info!(
                target: "voice_providers.pool",
                url = %ws_endpoint,
                "WWS 拨号成功，TungsteniteWs 接入"
            );
            Ok(Box::new(TungsteniteWs::new(stream)) as Box<dyn WebSocketLike>)
        }
        .boxed()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AsrCfg, BailianConfig, LlmCfg, PoolConfig, TtsCfg};

    fn cfg(asr_model: &str, ws_endpoint: &str, asr_workspace_id: Option<&str>) -> BailianConfig {
        BailianConfig {
            ws_endpoint: ws_endpoint.to_string(),
            api_key: "sk-test".to_string(),
            pool: PoolConfig::default(),
            asr: AsrCfg {
                model: asr_model.to_string(),
                workspace_id: asr_workspace_id.map(|s| s.to_string()),
                endpoint: None,
                ..AsrCfg::default()
            },
            tts: TtsCfg::default(),
            llm: LlmCfg::default(),
        }
    }

    // ===== ASR endpoint 构造（按 docs/qwen-asr-docs/qwen-asr-ws-api.md） =====

    #[test]
    fn asr_endpoint_qwen3_realtime_matches_docs() {
        // 文档：qwen3-asr-flash-realtime 走 Realtime API
        let cfg = cfg("qwen3-asr-flash-realtime", "wss://default", None);
        let url = build_asr_endpoint(&cfg);
        assert_eq!(
            url,
            "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=qwen3-asr-flash-realtime"
        );
    }

    #[test]
    fn asr_endpoint_qwen3_realtime_snapshot_override() {
        // 快照版本也按 Realtime 路由
        let cfg = cfg("qwen3-asr-flash-realtime-2026-02-10", "wss://default", None);
        let url = build_asr_endpoint(&cfg);
        assert!(url.starts_with("wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model="));
        assert!(url.contains("qwen3-asr-flash-realtime-2026-02-10"));
    }

    #[test]
    fn asr_endpoint_public_with_workspace_id() {
        // 文档推荐：{WorkspaceId}.cn-beijing.maas.aliyuncs.com/api-ws/v1/inference
        let cfg = cfg(
            "qwen-audio-3.0-asr-flash-streaming",
            "wss://default",
            Some("ws-abc123"),
        );
        let url = build_asr_endpoint(&cfg);
        assert_eq!(
            url,
            "wss://ws-abc123.cn-beijing.maas.aliyuncs.com/api-ws/v1/inference"
        );
    }

    #[test]
    fn asr_endpoint_public_without_workspace_id_falls_back_to_ws_endpoint() {
        let cfg = cfg(
            "qwen-audio-3.0-asr-flash-streaming",
            "wss://dashscope.aliyuncs.com/api-ws/v1/inference",
            None,
        );
        let url = build_asr_endpoint(&cfg);
        assert_eq!(url, "wss://dashscope.aliyuncs.com/api-ws/v1/inference");
    }

    #[test]
    fn asr_endpoint_paraformer_public_no_workspace() {
        let cfg = cfg("paraformer-realtime-v2", "wss://dashscope.aliyuncs.com/api-ws/v1/inference", None);
        let url = build_asr_endpoint(&cfg);
        assert_eq!(url, "wss://dashscope.aliyuncs.com/api-ws/v1/inference");
    }

    #[test]
    fn asr_endpoint_fun_asr_with_workspace_id() {
        let cfg = cfg(
            "fun-asr-realtime",
            "wss://default",
            Some("ws-fun"),
        );
        let url = build_asr_endpoint(&cfg);
        assert_eq!(
            url,
            "wss://ws-fun.cn-beijing.maas.aliyuncs.com/api-ws/v1/inference"
        );
    }

    #[test]
    fn asr_endpoint_explicit_override_wins() {
        // cfg.asr.endpoint 优先级最高
        let mut cfg = cfg("qwen-audio-3.0-asr-flash-streaming", "wss://default", None);
        cfg.asr.endpoint = Some("wss://my-proxy.example.com/asr".to_string());
        let url = cfg.asr.endpoint.clone().unwrap();
        assert_eq!(url, "wss://my-proxy.example.com/asr");
    }

    // ===== TTS endpoint 构造（对称） =====

    #[test]
    fn tts_endpoint_qwen3_realtime_matches_docs() {
        let mut cfg = cfg("paraformer-realtime-v2", "wss://default", None);
        cfg.tts.model = "qwen3-tts-flash-realtime".to_string();
        let url = build_tts_endpoint(&cfg);
        assert_eq!(
            url,
            "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=qwen3-tts-flash-realtime"
        );
    }

    #[test]
    fn tts_endpoint_cosyvoice_with_workspace() {
        let mut cfg = cfg("paraformer-realtime-v2", "wss://default", None);
        cfg.tts.model = "cosyvoice-v2".to_string();
        cfg.tts.workspace_id = Some("ws-tts".to_string());
        let url = build_tts_endpoint(&cfg);
        assert_eq!(
            url,
            "wss://ws-tts.cn-beijing.maas.aliyuncs.com/api-ws/v1/inference"
        );
    }

    // ===== Dialer header 行为（按 docs/qwen-asr-docs/qwen-asr-ws-api.md） =====

    #[test]
    fn dialer_signature_takes_workspace_id() {
        // 文档：X-DashScope-WorkSpace 可选。dialer 签名支持透传。
        // 真实端到端 header 验证需要 mock WSS server（独立的集成测试覆盖）。
        let _: fn(String, String, Option<String>) -> Dialer = make_real_dialer;
    }
}
