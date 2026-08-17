//! 流式实时 ASR（qwen3-asr-flash-realtime）WebSocket 端点
//!
//! `GET /stream/asr`（WS upgrade）——浏览器麦克风 ↔ voice-providers Realtime 会话。
//! 与 `/ws/voice/*`（VoicePayload 全流程 pipeline）完全独立，互不影响。
//!
//! ## 浏览器 ↔ voice_server 协议
//!
//! 上行（浏览器 → 服务端）：
//! - text `{"type":"start"}`            建连 DashScope、发 session.update；成功回 `started`
//! - binary（raw PCM s16le 16k mono）    转发给 Realtime 会话（内部按 3200B 切片）
//! - text `{"type":"finish"}`            发 session.finish，等剩余事件 + session.finished
//! - text `{"type":"stop"}`              放弃当前会话（drop 句柄，连接归还池）
//!
//! 下行（服务端 → 浏览器，JSON text）：
//! - `{"type":"started","session_id":...}`
//! - `{"type":"partial","text":...}`     增量识别（text+stash）
//! - `{"type":"final","text":...}`       句终识别（VAD 断句）
//! - `{"type":"speech_started"}` / `{"type":"speech_stopped"}`   服务端 VAD 边沿
//! - `{"type":"finished"}`               会话终态（session.finished 或事件流结束）
//! - `{"type":"error","message":...}`    任何阶段错误
//!
//! ## 配置
//!
//! 同一份 voice_server YAML 的可选 `asr_stream:` 段 + 环境变量（优先级 env > YAML > 默认）：
//!
//! ```yaml
//! asr_stream:
//!   model: "qwen3-asr-flash-realtime"
//!   api_key: "sk-..."                  # 或 env DASHSCOPE_API_KEY / VOICE_ASR_STREAM_API_KEY
//!   workspace_id: "llm-xxxxxx"         # 必填（除非显式给 endpoint）—— ASR realtime 专属域名需要
//!   region: "cn-beijing"               # cn-beijing | ap-southeast-1
//!   endpoint: null                     # 显式覆盖（完整 URL，缺 ?model= 自动追加）
//! ```

use std::sync::OnceLock;

use actix_web::web;
use actix_ws::{Message, MessageStream, Session};
use futures_util::StreamExt;
use serde_json::json;
use tokio::sync::mpsc;
use tracing::{info, warn};

use voice_providers::asr::qwen3_realtime::{
    make_realtime_dialer, resolve_realtime_endpoint, start_realtime_session, RealtimeAsrSession,
    RealtimeEvent, Qwen3RealtimeAdapter, CANONICAL_MODEL,
};
use voice_providers::asr::ClientError;
use voice_providers::ws_pool::{PoolConfig, WsPool};

// ===== 配置 =====

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
pub struct AsrStreamCfg {
    /// 模型名（稳定版 / 快照版）
    pub model: Option<String>,
    /// DashScope API key
    pub api_key: Option<String>,
    /// 业务空间 ID（endpoint 未配置时必填）
    pub workspace_id: Option<String>,
    /// 地域：cn-beijing（默认）| ap-southeast-1
    pub region: Option<String>,
    /// 显式 WSS endpoint（完整 URL；缺 ?model= 自动追加）
    pub endpoint: Option<String>,
    /// 采样率（Realtime 会话 session.update 用；浏览器侧固定 16k 采集）
    #[serde(deserialize_with = "de_opt_u32")]
    pub sample_rate: Option<u32>,
}

/// Option<u32> 的宽松反序列化（YAML 数字默认 i64）
fn de_opt_u32<'de, D>(de: D) -> Result<Option<u32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize as _;
    let v: Option<serde_yaml::Value> = Option::<serde_yaml::Value>::deserialize(de)?;
    match v {
        None | Some(serde_yaml::Value::Null) => Ok(None),
        Some(serde_yaml::Value::Number(n)) => Ok(n.as_u64().map(|x| x as u32)),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expect number, got {other:?}"
        ))),
    }
}

impl AsrStreamCfg {
    pub fn model(&self) -> &str {
        self.model.as_deref().filter(|s| !s.is_empty()).unwrap_or(CANONICAL_MODEL)
    }

    pub fn sample_rate(&self) -> u32 {
        self.sample_rate.unwrap_or(16000)
    }
}

/// 环境变量覆盖（优先级最高）。
/// 测试注入用；生产由 [`load_cfg`] 调用。
fn apply_env_overrides(cfg: &mut AsrStreamCfg) {
    fn take(var: &str) -> Option<String> {
        std::env::var(var).ok().filter(|s| !s.trim().is_empty())
    }
    // DASHSCOPE_API_KEY 兜底 + VOICE_ASR_STREAM_API_KEY 精确覆盖
    if let Some(v) = take("DASHSCOPE_API_KEY") {
        cfg.api_key = Some(v);
    }
    for (var, slot) in [
        ("VOICE_ASR_STREAM_API_KEY", &mut cfg.api_key),
        ("VOICE_ASR_STREAM_MODEL", &mut cfg.model),
        ("VOICE_ASR_STREAM_WORKSPACE_ID", &mut cfg.workspace_id),
        ("VOICE_ASR_STREAM_REGION", &mut cfg.region),
        ("VOICE_ASR_STREAM_ENDPOINT", &mut cfg.endpoint),
    ] {
        if let Some(v) = take(var) {
            *slot = Some(v);
        }
    }
}

/// 解析运行时配置：YAML `asr_stream:` 段 + 环境变量覆盖。
/// YAML 来源与 voice_server 主配置同一份文件（VOICE_CONFIG / 标准搜索路径）。
pub fn load_cfg() -> AsrStreamCfg {
    let mut cfg = read_yaml_section();
    apply_env_overrides(&mut cfg);
    cfg
}

/// 从 voice_server 主配置 YAML 读 `asr_stream:` 段（缺省 = 空）
fn read_yaml_section() -> AsrStreamCfg {
    let default_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("config")
        .join("config.yaml");
    let path = crate::resolve_config_path("voice_server", None, Some(&default_path));
    if !path.exists() {
        return AsrStreamCfg::default();
    }
    let Ok(text) = std::fs::read_to_string(&path) else {
        return AsrStreamCfg::default();
    };
    let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(&text) else {
        warn!(target: "voice_server.asr_stream", path = %path.display(), "配置文件解析失败，asr_stream 段按缺省处理");
        return AsrStreamCfg::default();
    };
    match value.get("asr_stream") {
        Some(section) => serde_yaml::from_value::<AsrStreamCfg>(section.clone()).unwrap_or_default(),
        None => AsrStreamCfg::default(),
    }
}

// ===== Realtime 事件 → 浏览器 JSON 行 =====

/// 把 provider 的 Realtime 事件序列化成发给浏览器的 JSON 文本行。
pub fn realtime_event_line(evt: &RealtimeEvent) -> String {
    match evt {
        RealtimeEvent::Partial { text } => json!({ "type": "partial", "text": text }),
        RealtimeEvent::Final { text } => json!({ "type": "final", "text": text }),
        RealtimeEvent::SpeechStarted => json!({ "type": "speech_started" }),
        RealtimeEvent::SpeechStopped => json!({ "type": "speech_stopped" }),
        RealtimeEvent::Finished => json!({ "type": "finished" }),
    }
    .to_string()
}

/// 错误行
pub fn error_line(message: impl std::fmt::Display) -> String {
    json!({ "type": "error", "message": message.to_string() }).to_string()
}

// ===== 运行时（配置 + 连接池，懒初始化） =====

struct AsrStreamRuntime {
    cfg: AsrStreamCfg,
    pool: std::sync::Arc<WsPool>,
}

static RUNTIME: OnceLock<AsrStreamRuntime> = OnceLock::new();

fn runtime() -> &'static AsrStreamRuntime {
    RUNTIME.get_or_init(|| {
        let cfg = load_cfg();
        info!(
            target: "voice_server.asr_stream",
            model = cfg.model(),
            workspace_id = ?cfg.workspace_id,
            region = ?cfg.region,
            endpoint = ?cfg.endpoint,
            "asr_stream 运行时初始化（实时流式 ASR）"
        );
        AsrStreamRuntime {
            cfg,
            pool: WsPool::new(PoolConfig::default()),
        }
    })
}

// ===== WS handler =====

/// `GET /stream/asr` 的 WS upgrade 入口（路由注册见 service.rs::api_init）
pub async fn ws_asr_stream(
    req: actix_web::HttpRequest,
    payload: web::Payload,
) -> Result<actix_web::HttpResponse, actix_web::Error> {
    let (response, session, msg_stream) = actix_ws::handle(&req, payload)?;
    // MessageStream 非 Send（内含 actix payload 的 Rc），用 actix 本地任务 spawn
    actix_web::rt::spawn(run_ws_session(session, msg_stream));
    Ok(response)
}

/// provider 事件桥（forwarder → 主循环）
enum BridgeItem {
    Event(Result<RealtimeEvent, ClientError>),
    /// provider 事件流结束（Finished / Err / 连接断开之后的兜底）
    Ended,
}

async fn run_ws_session(mut ws: Session, mut msg_stream: MessageStream) {
    // evt_tx 由主循环长期持有（保证 evt_rx.recv() 在无 provider 时阻塞而非返回 None），
    // 每次 start 时 clone 给 forwarder
    let (evt_tx, mut evt_rx) = mpsc::unbounded_channel::<BridgeItem>();

    let mut provider: Option<RealtimeAsrSession> = None;
    let mut provider_ended = false;
    let mut session_seq: u64 = 0;
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    info!(target: "voice_server.asr_stream", "WS /stream/asr 连接建立");

    loop {
        tokio::select! {
            // provider 事件下行（会话结束/未开始时禁用该分支，避免空转）
            Some(item) = evt_rx.recv(), if !provider_ended => {
                match item {
                    BridgeItem::Event(Ok(evt)) => {
                        let terminal = matches!(evt, RealtimeEvent::Finished);
                        let _ = ws.text(realtime_event_line(&evt)).await;
                        if terminal {
                            provider = None;
                            provider_ended = true;
                        }
                    }
                    BridgeItem::Event(Err(e)) => {
                        warn!(target: "voice_server.asr_stream", error = %e, "Realtime 会话错误");
                        let _ = ws.text(error_line(e)).await;
                        provider = None;
                        provider_ended = true;
                    }
                    BridgeItem::Ended => {
                        // 事件流收尾但没见到 Finished（如连接断开）：补一个 finished 让前端收敛
                        provider = None;
                        provider_ended = true;
                        let _ = ws.text(json!({ "type": "finished" }).to_string()).await;
                    }
                }
            }
            // 心跳：浏览器自动回 pong，保持中间层不掐空闲连接
            _ = tick.tick() => {
                let _ = ws.ping(&[]).await;
            }
            // 浏览器上行
            msg = msg_stream.next() => {
                let Some(msg) = msg else { break };
                match msg {
                    Ok(Message::Text(text)) => {
                        handle_client_text(
                            &mut ws, &evt_tx, &mut provider, &mut session_seq, &mut provider_ended,
                            text.to_string(),
                        )
                        .await;
                    }
                    Ok(Message::Binary(bytes)) => {
                        if let Some(ps) = &provider {
                            if let Err(e) = ps.send_audio(&bytes) {
                                let _ = ws.text(error_line(e)).await;
                            }
                        } else {
                            let _ = ws
                                .text(error_line("尚未 start，音频帧被丢弃"))
                                .await;
                        }
                    }
                    Ok(Message::Ping(p)) => {
                        let _ = ws.pong(&p).await;
                    }
                    Ok(Message::Close(_)) => break,
                    Ok(_) => {}
                    Err(e) => {
                        warn!(target: "voice_server.asr_stream", error = %e, "WS 读错误，断开");
                        break;
                    }
                }
            }
        }
    }

    // 断开：drop provider 句柄 → Realtime 会话 abandon → 连接归还池
    drop(provider);
    info!(target: "voice_server.asr_stream", "WS /stream/asr 连接关闭");
}

/// 处理浏览器上行 JSON 文本消息（start / finish / stop）
async fn handle_client_text(
    ws: &mut Session,
    evt_tx: &mpsc::UnboundedSender<BridgeItem>,
    provider: &mut Option<RealtimeAsrSession>,
    session_seq: &mut u64,
    provider_ended: &mut bool,
    text: String,
) {
    let cmd: serde_json::Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            let _ = ws.text(error_line(format!("invalid json: {e}"))).await;
            return;
        }
    };
    match cmd.get("type").and_then(|t| t.as_str()) {
        Some("start") => {
            if provider.is_some() {
                let _ = ws
                    .text(error_line("会话已在进行中，先 stop 或 finish"))
                    .await;
                return;
            }
            *session_seq += 1;
            let sid = format!("asr-stream-{}", session_seq);
            match start_provider_session(evt_tx, &sid).await {
                Ok(ps) => {
                    *provider = Some(ps);
                    *provider_ended = false;
                    let _ = ws
                        .text(json!({ "type": "started", "session_id": sid }).to_string())
                        .await;
                }
                Err(e) => {
                    let _ = ws.text(error_line(e)).await;
                }
            }
        }
        Some("finish") => {
            if let Some(ps) = provider.as_ref() {
                if let Err(e) = ps.finish() {
                    let _ = ws.text(error_line(e)).await;
                }
            } else {
                let _ = ws.text(error_line("尚未 start，无法 finish")).await;
            }
        }
        Some("stop") => {
            if provider.take().is_some() {
                *provider_ended = true;
                let _ = ws.text(json!({ "type": "stopped" }).to_string()).await;
            }
        }
        other => {
            let _ = ws
                .text(error_line(format!(
                    "未知消息 type: {}",
                    other.unwrap_or("<missing>")
                )))
                .await;
        }
    }
}

/// 建一个 provider Realtime 会话，并 spawn forwarder 把事件流桥到主循环
async fn start_provider_session(
    evt_tx: &mpsc::UnboundedSender<BridgeItem>,
    sid: &str,
) -> Result<RealtimeAsrSession, String> {
    let rt = runtime();
    let cfg = &rt.cfg;

    if cfg.api_key.as_deref().map(str::trim).unwrap_or("").is_empty() {
        return Err(
            "asr_stream.api_key 未配置（YAML asr_stream: 段或 env DASHSCOPE_API_KEY / VOICE_ASR_STREAM_API_KEY）"
                .to_string(),
        );
    }
    let endpoint = resolve_realtime_endpoint(
        cfg.model(),
        cfg.workspace_id.as_deref(),
        cfg.region.as_deref(),
        cfg.endpoint.as_deref(),
    )
    .map_err(|e| e.to_string())?;

    let dialer = make_realtime_dialer(endpoint, cfg.api_key.clone().unwrap_or_default());
    let adapter = Qwen3RealtimeAdapter::for_model(cfg.model());

    let (ps, stream) =
        start_realtime_session(rt.pool.clone(), adapter, dialer, cfg.sample_rate(), 1, sid.to_string())
            .await
            .map_err(|e| e.to_string())?;

    // forwarder：provider 事件流 → bridge channel；流结束补 Ended
    let tx = evt_tx.clone();
    tokio::spawn(async move {
        let mut stream = stream;
        while let Some(item) = stream.next().await {
            if tx.send(BridgeItem::Event(item)).is_err() {
                break; // 主循环已退出
            }
        }
        let _ = tx.send(BridgeItem::Ended);
    });

    Ok(ps)
}

// ===== 单元测试 =====

#[cfg(test)]
mod tests {
    use super::*;

    // ===== B3: 事件映射 =====

    #[test]
    fn realtime_event_line_shapes() {
        assert_eq!(
            realtime_event_line(&RealtimeEvent::Partial { text: "你好".into() }),
            r#"{"text":"你好","type":"partial"}"#
        );
        assert_eq!(
            realtime_event_line(&RealtimeEvent::Final { text: "你好。".into() }),
            r#"{"text":"你好。","type":"final"}"#
        );
        assert_eq!(
            realtime_event_line(&RealtimeEvent::SpeechStarted),
            r#"{"type":"speech_started"}"#
        );
        assert_eq!(
            realtime_event_line(&RealtimeEvent::SpeechStopped),
            r#"{"type":"speech_stopped"}"#
        );
        assert_eq!(
            realtime_event_line(&RealtimeEvent::Finished),
            r#"{"type":"finished"}"#
        );
    }

    #[test]
    fn error_line_shape() {
        assert_eq!(
            error_line("boom"),
            r#"{"message":"boom","type":"error"}"#
        );
    }

    // ===== B2: YAML 段解析 =====

    #[test]
    fn yaml_section_parse_full() {
        let yaml = r#"
model: "qwen3-asr-flash-realtime-2026-02-10"
api_key: "sk-test"
workspace_id: "llm-abc"
region: "ap-southeast-1"
endpoint: null
sample_rate: 16000
"#;
        let v: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        let cfg: AsrStreamCfg = serde_yaml::from_value(v).unwrap();
        assert_eq!(cfg.model.as_deref(), Some("qwen3-asr-flash-realtime-2026-02-10"));
        assert_eq!(cfg.api_key.as_deref(), Some("sk-test"));
        assert_eq!(cfg.workspace_id.as_deref(), Some("llm-abc"));
        assert_eq!(cfg.region.as_deref(), Some("ap-southeast-1"));
        assert_eq!(cfg.endpoint, None);
        assert_eq!(cfg.sample_rate, Some(16000));
        // helper
        assert_eq!(cfg.model(), "qwen3-asr-flash-realtime-2026-02-10");
        assert_eq!(cfg.sample_rate(), 16000);
    }

    #[test]
    fn yaml_section_defaults() {
        // 空段 / 缺字段：全部 None，model() 回落 canonical
        let cfg: AsrStreamCfg = serde_yaml::from_value(serde_yaml::Value::Null).unwrap();
        assert_eq!(cfg.model(), CANONICAL_MODEL);
        assert_eq!(cfg.sample_rate(), 16000);
        assert_eq!(cfg.api_key, None);
    }

    #[test]
    fn yaml_section_unknown_keys_ignored() {
        let yaml = "model: qwen3-asr-flash-realtime\nfuture_option: 42\n";
        let v: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        let cfg: AsrStreamCfg = serde_yaml::from_value(v).unwrap();
        assert_eq!(cfg.model(), "qwen3-asr-flash-realtime");
    }

    // ===== B2: 端到端配置 → endpoint 解析 =====

    #[test]
    fn cfg_with_workspace_resolves_endpoint() {
        let cfg = AsrStreamCfg {
            workspace_id: Some("llm-abc".into()),
            ..Default::default()
        };
        let url = resolve_realtime_endpoint(
            cfg.model(),
            cfg.workspace_id.as_deref(),
            cfg.region.as_deref(),
            cfg.endpoint.as_deref(),
        )
        .unwrap();
        assert_eq!(
            url,
            "wss://llm-abc.cn-beijing.maas.aliyuncs.com/api-ws/v1/realtime?model=qwen3-asr-flash-realtime"
        );
    }

    // ===== B2: env 覆盖（单测内串行，避免 env 竞态） =====

    #[test]
    fn env_overrides_yaml() {
        let vars = [
            ("DASHSCOPE_API_KEY", "sk-dash"),
            ("VOICE_ASR_STREAM_API_KEY", "sk-precise"),
            ("VOICE_ASR_STREAM_MODEL", "qwen3-asr-flash-realtime-2026-02-10"),
            ("VOICE_ASR_STREAM_WORKSPACE_ID", "llm-env"),
            ("VOICE_ASR_STREAM_REGION", "ap-southeast-1"),
        ];
        // 记录旧值并设置
        let saved: Vec<_> = vars
            .iter()
            .map(|(k, _)| (*k, std::env::var(k).ok()))
            .collect();
        for (k, v) in vars {
            std::env::set_var(k, v);
        }

        let mut cfg = AsrStreamCfg {
            api_key: Some("sk-yaml".into()),
            model: Some("qwen3-asr-flash-realtime".into()),
            ..Default::default()
        };
        apply_env_overrides(&mut cfg);

        // 恢复 env（顺序无关，逐个还原）
        for (k, old) in saved {
            match old {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }

        // 精确 VOICE_ASR_STREAM_API_KEY 覆盖 DASHSCOPE_API_KEY 覆盖 YAML
        assert_eq!(cfg.api_key.as_deref(), Some("sk-precise"));
        assert_eq!(cfg.model.as_deref(), Some("qwen3-asr-flash-realtime-2026-02-10"));
        assert_eq!(cfg.workspace_id.as_deref(), Some("llm-env"));
        assert_eq!(cfg.region.as_deref(), Some("ap-southeast-1"));
    }
}
