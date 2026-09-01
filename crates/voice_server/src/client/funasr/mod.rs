//! FunASR 本地部署 WebSocket 客户端 —— 底层原语
//!
//! 对接本地部署的 FunASR 服务（`runtime/python/funasr_wss_server.py` 或 docker
//! `funasr-runtime-sdk` 镜像）。协议见
//! `docs/FunASR/runtime/docs/websocket_protocol_zh.md`：
//!
//!   - URL: `ws://<host>:<port>/`（本地部署常用 10095 端口，明文 ws://，无鉴权）
//!   - **WebSocket Subprotocol** —— FunASR server 在 `websockets.serve(..., subprotocols=["binary"], ...)`
//!     只接受 `binary` subprotocol，其它值会让 WS upgrade 返回 **400 Bad Request**。
//!     握手必须带 `Sec-WebSocket-Protocol: binary`。
//!   - C → S 首次（文本 JSON）：`{"mode": "2pass", "wav_name": "...", "is_speaking": true, "wav_format": "pcm", "audio_fs": 16000, ...}`
//!   - C → S 音频：裸 PCM s16le binary 帧（无任何 wrapper / header）
//!   - C → S 结束（文本 JSON）：`{"is_speaking": false}`
//!   - S → C 识别（文本 JSON）：`{"mode": "2pass-online"|"2pass-offline"|"offline", "text": "...", "is_final": ...}`
//!   - S → C 结束：服务端在所有结果输出完毕后关闭连接（**无 task-finished 等控制消息** ——
//!     仅靠 Close 帧表示本会话结束）
//!
//! Wire 协议汇总：
//!   - 协议层只暴露 WS 帧级原语：
//!     `FunasrClient::start_session`   → 建连 + 发首次 JSON（隐式 = onopen）
//!     `FunasrSender::send_audio`      → 发一段裸 PCM binary
//!     `FunasrSender::send_finish`     → 发 `{"is_speaking": false}`
//!     `FunasrReceiver::next_event`    → 阻塞收下一个 `FunasrEvent`（Message / Close / Error）
//!     `FunasrSender::close`           → 关闭连接
//!   - 编排（音频切片 / finish / 收 transcript 流）由上层（如 `live_asr`）负责。
//!   - `next_event` 返回 `FunasrEvent` 枚举，对应浏览器 WS 的 onmessage / onclose / onerror ——
//!     调用方 `match` 一下即可，**不再**需要记 `Ok(Some/None/Err)` 三种语义的差别。
//!   - `Close` 在 FunASR 协议下表示"识别完成"，**不是错误**（即便 code=1006 abnormal 也是退出信号）。
//!
//! 关键差异（vs Qwen GAX 协议 —— 旧的 `asr_realtime.rs` 已删除，仅作历史参考）：
//!   - 无 `header.action` envelope（flat JSON）
//!   - 无 `task_id`（会话身份仅靠 `wav_name`）
//!   - 服务端靠 Close 帧表示识别结束（**没有** task-finished 等价物）
//!   - offline 模式 `is_final` 永远为 false（语义上服务端只回一次结果，靠 Close 收尾）
//!   - 2pass-online + is_final=true = 句子边界；2pass-offline = 二次纠错结果（视为最终）
//!
//! ## 模块拆分
//! - [`mod`]      公共 API：`FunasrClient` + `FunasrConfig` + `FunasrMode` + `build_funasr_client` + 首次帧 + 握手
//! - [`ws`]       WS 帧级原语：`FunasrSession` + `FunasrSender` + `FunasrReceiver`（带 keepalive 后台 task）
//! - [`protocol`] 服务端响应 wire 格式：`FunasrResponseMode` / `FunasrResponse` / `FunasrClose` / `FunasrEvent` + 解析

pub mod protocol;
pub mod ws;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_tungstenite::tokio::connect_async;
use async_tungstenite::tungstenite::{client::IntoClientRequest, Message};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tracing::{info, warn};

use crate::client::error::ClientError;
use crate::trace_context::current_trace_id;

pub use protocol::{FunasrClose, FunasrEvent, FunasrResponse, FunasrResponseMode};
pub use ws::{FunasrReceiver, FunasrSender, FunasrSession};

/// WS 连接类型别名
pub type WsStream = async_tungstenite::WebSocketStream<
    async_tungstenite::tokio::ClientStream<tokio::net::TcpStream>,
>;
pub type ArcFunasr = Arc<FunasrClient>;

// ===== 配置 =====

/// 推理模式
///
/// - `Offline` —— 离线文件转写（一次性返回结果，无流式增量）
/// - `Online` —— 实时语音识别（仅流式，无 2-pass 纠错）
/// - `TwoPass` —— 实时识别 + 句尾 2-pass 纠错（推荐；需要 2pass 模型）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunasrMode {
    Offline,
    Online,
    TwoPass,
}

impl FunasrMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            FunasrMode::Offline => "offline",
            FunasrMode::Online => "online",
            FunasrMode::TwoPass => "2pass",
        }
    }
}

/// FunASR 客户端配置
#[derive(Debug, Clone)]
pub struct FunasrConfig {
    /// WSS / WS 端点，例：`ws://127.0.0.1:10095/`（本地部署常见明文 ws://）
    pub endpoint: String,
    /// 推理模式（offline / online / 2pass）
    pub mode: FunasrMode,
    /// 音频文件名（仅用于服务端日志关联，不强制唯一）
    pub wav_name: String,
    /// 音频格式（pcm / mp3 / wav / ...）
    pub wav_format: String,
    /// PCM 采样率（FunASR 支持 8000 / 16000）
    pub sample_rate: u32,
    /// 声道数（FunASR 通常 1）
    pub channels: u16,
    /// 2pass / online 模式下的流式 latency 配置 `[左看, 当前, 右看]`（1 chunk = 60ms）；
    /// 默认 `[5, 10, 5]` = 当前 600ms，回看 300ms，前看 300ms。`None` = 不传（服务端默认）。
    pub chunk_size: Option<Vec<u32>>,
    /// 热词 JSON 字符串，例：`{"阿里巴巴":20,"通义实验室":30}`。`None` = 不传。
    /// 注：字段值是**已经 JSON 序列化的字符串**（按 docs 要求），不要二次序列化。
    pub hotwords: Option<String>,
    /// ITN（文本规范化：数字、日期等转写）。默认 `true`
    pub itn: bool,
    /// SenseVoiceSmall 模型语种。默认 `None`（让服务端走 auto）
    pub svs_lang: Option<String>,
    /// SenseVoiceSmall 是否开启标点 / ITN。默认 `true`
    pub svs_itn: bool,
    /// 附加 header（一般不用；本地部署无鉴权）
    pub extra_headers: HashMap<String, String>,
    /// send / recv 单次超时
    pub timeout: Duration,
    /// **应用层 keepalive ping 间隔**（仅在 `FunasrSender` 持有期间生效）。
    /// 后台 task 每 N 秒通过上游 WSS 发一帧 `Message::Ping(Vec::new())`，让 FunASR 服务端
    /// （Python `websockets` 库）持续收到心跳、不被自己的 idle timeout 杀掉。
    ///
    ///   - 默认 `20s` —— 与常见反向代理 / WS 服务端的 idle timeout 对齐
    ///   - `Duration::ZERO` = 禁用 keepalive
    ///
    /// 注：本参数**不**改 `next_event` 的 30s recv 超时 —— 那是"真卡了"兜底，
    /// keepalive 只防 FunASR 服务端的 idle 误杀，不掩盖协议层故障。
    pub keepalive_interval: Duration,
}

impl Default for FunasrConfig {
    fn default() -> Self {
        Self {
            endpoint: "ws://127.0.0.1:10095/".to_string(),
            mode: FunasrMode::TwoPass,
            wav_name: "default".to_string(),
            wav_format: "pcm".to_string(),
            sample_rate: 16000,
            channels: 1,
            chunk_size: Some(vec![5, 10, 5]),
            hotwords: None,
            itn: true,
            svs_lang: None,
            svs_itn: true,
            extra_headers: HashMap::new(),
            timeout: Duration::from_secs(30),
            keepalive_interval: Duration::from_secs(20),
        }
    }
}

// ===== Client =====

/// FunASR WebSocket 客户端（长生命周期；多次 start_session 复用）
pub struct FunasrClient {
    cfg: FunasrConfig,
}

impl FunasrClient {
    pub fn new(cfg: FunasrConfig) -> Self {
        Self { cfg }
    }

    /// 构造握手请求（URL + `Sec-WebSocket-Protocol: binary` + extra_headers；本地部署无 Authorization）
    ///
    /// FunASR server `websockets.serve(..., subprotocols=["binary"], ...)` 会拒绝任何不带
    /// `Sec-WebSocket-Protocol: binary` 的握手请求（直接返回 400 Bad Request）。
    /// 这里无条件注入；如果用户 `extra_headers` 也带同名 header，后者覆盖前者。
    pub(crate) fn build_handshake_request(
        &self,
    ) -> Result<async_tungstenite::tungstenite::handshake::client::Request, ClientError> {
        let mut req = self
            .cfg
            .endpoint
            .as_str()
            .into_client_request()
            .map_err(|e| ClientError::Http(format!("invalid ws url: {}", e)))?;
        // 必带 Sec-WebSocket-Protocol: binary —— FunASR server 要求
        req.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            async_tungstenite::tungstenite::http::HeaderValue::from_static("binary"),
        );
        for (k, v) in &self.cfg.extra_headers {
            if let (Ok(name), Ok(value)) = (
                async_tungstenite::tungstenite::http::HeaderName::from_bytes(k.as_bytes()),
                async_tungstenite::tungstenite::http::HeaderValue::from_str(v),
            ) {
                req.headers_mut().insert(name, value);
            }
        }
        Ok(req)
    }

    /// 建连 + 发首次 JSON 配置。返回 `FunasrSession` 供 send_audio / send_finish / recv_event。
    ///
    /// 注：FunASR 协议**没有** Qwen 那种 task-started 控制消息 —— 建连成功 = TCP+WS upgrade 通过。
    /// 后续首次 `recv_event` 通常会立刻收到一段空 + is_final=false（VAD 还没触发 / 静音段）。
    ///
    /// `session_id` 仅用于日志关联；协议层会话身份由 `cfg.wav_name` 决定。
    pub async fn start_session(&self, session_id: &str) -> Result<FunasrSession, ClientError> {
        info!(
            target: "voice_server.funasr",
            session_id,
            endpoint = %self.cfg.endpoint,
            mode = %self.cfg.mode.as_str(),
            wav_name = %self.cfg.wav_name,
            sample_rate = self.cfg.sample_rate,
            channels = self.cfg.channels,
            wav_format = %self.cfg.wav_format,
            extra_headers = self.cfg.extra_headers.len(),
            timeout_secs = self.cfg.timeout.as_secs(),
            "FunASR WSS 建连开始"
        );

        let mut req = self.build_handshake_request()?;
        if let Some(trace_id) = current_trace_id() {
            if let Ok(value) = async_tungstenite::tungstenite::http::HeaderValue::from_str(&trace_id)
            {
                req.headers_mut().insert(
                    async_tungstenite::tungstenite::http::HeaderName::from_static("trace_id"),
                    value,
                );
            }
        }
        let (ws, _resp) = connect_async(req)
            .await
            .map_err(|e| ClientError::Ws(format!("ws handshake: {}", e)))?;
        info!(
            target: "voice_server.funasr",
            session_id,
            "WSS 握手成功 (TCP+WS upgrade 通过), 即将发首次 JSON"
        );

        let (mut tx, rx) = ws.split();

        // 首次 JSON：包含 mode / wav_name / is_speaking / wav_format / audio_fs / 可选 chunk_size+hotwords 等
        let first = build_first_frame(&self.cfg);
        // WARN 级别：排查上游拒包时一定要看见这条 JSON
        warn!(
            target: "voice_server.funasr.req",
            session_id,
            wav_name = %self.cfg.wav_name,
            "→ first JSON: {}",
            first
        );
        tx.send(Message::Text(first))
            .await
            .map_err(|e| ClientError::Ws(format!("send first JSON: {}", e)))?;

        info!(
            target: "voice_server.funasr",
            session_id,
            "start_session 完成, FunasrSession 就绪 (可发 audio / recv_event)"
        );

        Ok(FunasrSession {
            tx,
            rx,
            timeout: self.cfg.timeout,
            wav_name: self.cfg.wav_name.clone(),
            keepalive_interval: self.cfg.keepalive_interval,
        })
    }
}

// ===== 协议帧构造 =====

/// 构造首次通信 JSON（按 docs/websocket_protocol_zh.md "首次通信"）
pub(crate) fn build_first_frame(cfg: &FunasrConfig) -> String {
    let mut value = json!({
        "mode": cfg.mode.as_str(),
        "wav_name": cfg.wav_name,
        "is_speaking": true,
        "wav_format": cfg.wav_format,
        "audio_fs": cfg.sample_rate,
        "itn": cfg.itn,
        "svs_itn": cfg.svs_itn,
    });
    // chunk_size / hotwords / svs_lang 是 optional —— None 时跳过（不要输出 null）
    if let Some(cs) = &cfg.chunk_size {
        value["chunk_size"] = json!(cs);
    }
    if let Some(hw) = &cfg.hotwords {
        // 文档要求传字符串（已经是 JSON 序列化的字符串），不要重复 serialize
        value["hotwords"] = json!(hw);
    }
    if let Some(lang) = &cfg.svs_lang {
        value["svs_lang"] = json!(lang);
    }
    serde_json::to_string(&value).expect("static json")
}

// ===== 工厂 =====

pub fn build_funasr_client(cfg: FunasrConfig) -> ArcFunasr {
    Arc::new(FunasrClient::new(cfg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::{
        classify_mode, parse_server_event, FunasrClose, FunasrEvent, FunasrResponse,
        FunasrResponseMode,
    };

    // ===== FunasrMode =====

    #[test]
    fn mode_as_str_matches_doc() {
        // 按 docs/websocket_protocol_zh.md §首次通信 的 mode 字段值
        assert_eq!(FunasrMode::Offline.as_str(), "offline");
        assert_eq!(FunasrMode::Online.as_str(), "online");
        assert_eq!(FunasrMode::TwoPass.as_str(), "2pass");
    }

    // ===== FunasrConfig 默认值 =====

    #[test]
    fn default_config_is_local_two_pass_16k() {
        let c = FunasrConfig::default();
        assert_eq!(c.endpoint, "ws://127.0.0.1:10095/");
        assert_eq!(c.mode, FunasrMode::TwoPass);
        assert_eq!(c.wav_format, "pcm");
        assert_eq!(c.sample_rate, 16000);
        assert_eq!(c.channels, 1);
        assert_eq!(c.itn, true);
        assert_eq!(c.svs_itn, true);
        assert!(c.chunk_size.is_some());
        assert!(c.hotwords.is_none());
        assert!(c.svs_lang.is_none());
    }

    // ===== 首次通信 JSON 帧形态 =====

    #[test]
    fn first_frame_two_pass_with_hotwords_and_svs() {
        let cfg = FunasrConfig {
            endpoint: "ws://127.0.0.1:10095/".into(),
            mode: FunasrMode::TwoPass,
            wav_name: "live-1".into(),
            wav_format: "pcm".into(),
            sample_rate: 16000,
            channels: 1,
            chunk_size: Some(vec![5, 10, 5]),
            hotwords: Some(r#"{"阿里巴巴":20,"通义实验室":30}"#.to_string()),
            itn: true,
            svs_lang: Some("zh".into()),
            svs_itn: true,
            extra_headers: Default::default(),
            timeout: Duration::from_secs(30),
            keepalive_interval: Duration::from_secs(20),
        };
        let s = build_first_frame(&cfg);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["mode"], "2pass");
        assert_eq!(v["wav_name"], "live-1");
        assert_eq!(v["is_speaking"], true);
        assert_eq!(v["wav_format"], "pcm");
        assert_eq!(v["audio_fs"], 16000);
        assert_eq!(v["chunk_size"], json!([5, 10, 5]));
        // hotwords 必须保持字符串形态（服务端要解析的 JSON 字符串，不能二次序列化）
        assert_eq!(v["hotwords"], r#"{"阿里巴巴":20,"通义实验室":30}"#);
        assert_eq!(v["svs_lang"], "zh");
        assert_eq!(v["itn"], true);
        assert_eq!(v["svs_itn"], true);
    }

    #[test]
    fn first_frame_offline_omits_chunk_size() {
        let cfg = FunasrConfig {
            mode: FunasrMode::Offline,
            chunk_size: None,
            ..Default::default()
        };
        let s = build_first_frame(&cfg);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["mode"], "offline");
        // offline 模式不需要 chunk_size（音频一次性发完，无 latency 概念）
        assert!(v.get("chunk_size").is_none());
    }

    #[test]
    fn first_frame_omits_none_optional_fields() {
        let cfg = FunasrConfig {
            chunk_size: None,
            hotwords: None,
            svs_lang: None,
            ..Default::default()
        };
        let s = build_first_frame(&cfg);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert!(v.get("chunk_size").is_none());
        assert!(v.get("hotwords").is_none());
        assert!(v.get("svs_lang").is_none());
    }

    #[test]
    fn first_frame_8k_for_telephony() {
        // 8kHz 是文档明确支持的采样率
        let cfg = FunasrConfig {
            sample_rate: 8000,
            ..Default::default()
        };
        let s = build_first_frame(&cfg);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["audio_fs"], 8000);
    }

    // ===== 握手请求 =====

    #[test]
    fn handshake_request_accepts_plain_ws_scheme() {
        // 本地部署常用 ws://（明文），必须能解析 URL —— 之前如果某处只接受 wss:// 就会爆
        let client = FunasrClient::new(FunasrConfig {
            endpoint: "ws://127.0.0.1:10095/".into(),
            ..Default::default()
        });
        let req = client.build_handshake_request().unwrap();
        assert_eq!(
            req.uri().scheme_str(),
            Some("ws"),
            "本地 FunASR 必须是 ws scheme"
        );
    }

    #[test]
    fn handshake_request_accepts_wss_scheme() {
        // 通过反向代理暴露时可能用 wss://
        let client = FunasrClient::new(FunasrConfig {
            endpoint: "wss://funasr.example.com/ws".into(),
            ..Default::default()
        });
        let req = client.build_handshake_request().unwrap();
        assert_eq!(req.uri().scheme_str(), Some("wss"));
    }

    #[test]
    fn handshake_request_invalid_url_errors() {
        let client = FunasrClient::new(FunasrConfig {
            endpoint: "not a url".into(),
            ..Default::default()
        });
        assert!(client.build_handshake_request().is_err());
    }

    #[test]
    fn handshake_request_applies_extra_headers() {
        let mut headers = HashMap::new();
        headers.insert("X-Custom".into(), "value".into());
        let client = FunasrClient::new(FunasrConfig {
            endpoint: "ws://127.0.0.1:10095/".into(),
            extra_headers: headers,
            ..Default::default()
        });
        let req = client.build_handshake_request().unwrap();
        assert_eq!(req.headers().get("X-Custom").unwrap(), "value");
    }

    /// 回归测试：FunASR server (funasr_wss_server.py:841) 只接受 `binary` subprotocol，
    /// 握手不注入这个 header 会拿到 400 Bad Request。
    #[test]
    fn handshake_request_sets_binary_subprotocol() {
        let client = FunasrClient::new(FunasrConfig {
            endpoint: "ws://127.0.0.1:10095/".into(),
            ..Default::default()
        });
        let req = client.build_handshake_request().unwrap();
        let sub = req
            .headers()
            .get("Sec-WebSocket-Protocol")
            .expect("Sec-WebSocket-Protocol 必须存在（FunASR server 要求 binary）");
        assert_eq!(sub, "binary");
    }

    /// 用户的 extra_headers 同名 header 可以覆盖默认 binary —— 便于对接自定义 server。
    #[test]
    fn handshake_request_user_subprotocol_overrides_default() {
        let mut headers = HashMap::new();
        headers.insert("Sec-WebSocket-Protocol".into(), "custom-proto".into());
        let client = FunasrClient::new(FunasrConfig {
            endpoint: "ws://127.0.0.1:10095/".into(),
            extra_headers: headers,
            ..Default::default()
        });
        let req = client.build_handshake_request().unwrap();
        assert_eq!(
            req.headers().get("Sec-WebSocket-Protocol").unwrap(),
            "custom-proto"
        );
    }

    // ===== 借用 protocol 模块的测试 sanity check =====

    #[test]
    fn protocol_module_linked() {
        // 占位测试：确认 mod.rs 能正确引用 protocol 模块的类型
        // （若 protocol 模块路径变更 / 编译失败，这条测试会先 fail）
        assert!(matches!(
            classify_mode("online"),
            FunasrResponseMode::Online
        ));
        // FunasrResponse / FunasrClose / FunasrEvent 构造可见
        let _ = FunasrResponse {
            mode: FunasrResponseMode::Online,
            text: "x".into(),
            is_final: false,
        };
        let _ = FunasrClose::normal();
        let _ = FunasrEvent::Close(FunasrClose::normal());
        // parse_server_event 可调用
        let bytes = serde_json::to_vec(&json!({"mode": "online", "text": "x"})).unwrap();
        let _ = parse_server_event(&bytes);
    }
}
