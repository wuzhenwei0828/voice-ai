//! mock-asr: 模拟 ASR 服务
//!
//! 协议：
//!   POST /recognize
//!     Headers: X-Session-Id: <string>
//!     Body: 原始音频字节流（chunked）
//!     Response: chunked JSON 流
//!       {"text": "...", "is_final": false}
//!       {"text": "完整识别结果", "is_final": true}
//!
//! 行为：
//!   收到第一个 chunk 后立刻回一个 partial 文本（标识已收到音频）
//!   每收到一个 chunk 累积字节数
//!   收到最后一个 chunk（Connection: close 或 trailing chunk）后回 final
//!   整个过程打印 tracing 日志

use std::convert::Infallible;
use std::time::Duration;

use axum::{
    body::Body,
    extract::Request,
    http::HeaderMap,
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::{debug, error, info};
use voice_config::{init_logging, load_yaml, resolve_config_path, LogConfig};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct MockAsrConfig {
    #[serde(default)]
    log: LogConfig,
    #[serde(default)]
    server: MockServerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MockServerConfig {
    #[serde(default = "default_port")]
    port: u16,
}

impl Default for MockServerConfig {
    fn default() -> Self {
        Self { port: default_port() }
    }
}

fn default_port() -> u16 { 7001 }

#[derive(Parser, Debug)]
struct Args {
    /// 配置文件路径（YAML）；不传则按 VOICE_CONFIG / 标准路径搜索
    #[arg(long)]
    config: Option<std::path::PathBuf>,

    /// 监听端口（覆盖配置文件）
    #[arg(long)]
    port: Option<u16>,

    /// 日志级别（覆盖配置文件）
    #[arg(long)]
    log_level: Option<String>,

    /// 日志文件路径（覆盖配置文件）
    #[arg(long)]
    log_file: Option<String>,
}

#[derive(Serialize)]
struct AsrChunk {
    text: String,
    is_final: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // 1. 解析配置路径
    let cfg_path = resolve_config_path("mock-asr", args.config.as_deref());
    if !cfg_path.exists() {
        tracing::warn!(
            target: "mock_asr",
            path = %cfg_path.display(),
            "未找到配置文件，使用内置默认"
        );
    }

    // 2. 加载配置
    let mut cfg: MockAsrConfig = load_yaml(&cfg_path)?;
    cfg.log.apply_env_overrides();
    if let Some(parent) = cfg_path.parent() {
        cfg.log.resolve_relative_paths(parent);
    }

    // 2. CLI 覆盖
    if let Some(p) = args.port { cfg.server.port = p; }
    if let Some(l) = args.log_level { cfg.log.level = l; }
    if let Some(f) = args.log_file { cfg.log.file = f; }

    // 3. 初始化日志
    init_logging(&cfg.log)?;

    info!(
        target: "mock_asr",
        config_file = %cfg_path.display(),
        port = cfg.server.port,
        log_level = %cfg.log.level,
        log_file = %cfg.log.file,
        "mock-asr 配置加载完成"
    );

    let port = cfg.server.port;
    info!(target: "mock_asr", "启动 mock-asr，监听 0.0.0.0:{}", port);

    let app = Router::new().route("/recognize", post(handle_recognize));
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    info!(target: "mock_asr", "ready: POST http://0.0.0.0:{}/recognize", port);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_recognize(headers: HeaderMap, req: Request) -> Response {
    let session_id = headers
        .get("x-session-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();

    info!(target: "mock_asr", session_id = %session_id, "收到 /recognize 请求，开始接收音频流");

    // 把 body 完整读出来（mock 简化版，不真正流式）
    let body_bytes = match axum::body::to_bytes(req.into_body(), 100 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            error!(target: "mock_asr", session_id = %session_id, "读 body 失败: {}", e);
            return (axum::http::StatusCode::BAD_REQUEST, "bad body").into_response();
        }
    };
    let total_bytes = body_bytes.len();

    info!(target: "mock_asr", session_id = %session_id, total_bytes, "音频接收完成，开始识别");

    // 模拟识别延迟
    sleep(Duration::from_millis(200)).await;

    // 生成 partial 结果
    let partial_text = format!("[mock-asr 识别中... 已收 {} 字节]", total_bytes);
    info!(target: "mock_asr", session_id = %session_id, partial = %partial_text, "推送 ASR partial");
    let partial_json = serde_json::to_string(&AsrChunk {
        text: partial_text,
        is_final: false,
    })
    .unwrap();

    sleep(Duration::from_millis(150)).await;

    // 生成 final 结果
    let final_text = format!(
        "你好世界（mock 识别，共 {} 字节 / session={}）",
        total_bytes, session_id
    );
    info!(target: "mock_asr", session_id = %session_id, final_text = %final_text, "推送 ASR final");
    let final_json = serde_json::to_string(&AsrChunk {
        text: final_text,
        is_final: true,
    })
    .unwrap();

    // 拼成 chunked 响应
    let body = format!("{}\n{}\n", partial_json, final_json);
    debug!(target: "mock_asr", session_id = %session_id, "响应完成");

    Response::builder()
        .status(200)
        .header("content-type", "application/x-ndjson")
        .body(Body::from(body))
        .unwrap()
}

// 抑制 unused 警告
#[allow(dead_code)]
fn _suppress(_: Infallible) {}