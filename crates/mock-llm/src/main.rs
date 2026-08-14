//! mock-llm: 模拟 LLM 服务
//!
//! 协议：
//!   POST /chat
//!     Headers: X-Session-Id
//!     Body: {"prompt": "..."}
//!     Response: chunked JSON 流
//!       {"delta": "...", "is_final": false}
//!       {"delta": "...", "is_final": true}
//!
//! 行为：
//!   收到 prompt 后，按"句号/问号"切分成多个句子，逐句流式返回
//!   模拟思考延迟（每句 100~300ms）

use std::time::Duration;

use axum::{
    extract::Request,
    http::HeaderMap,
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::{error, info, warn};
use voice_config::{init_logging, load_yaml, resolve_config_path, LogConfig};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct MockLlmConfig {
    #[serde(default)]
    log: LogConfig,
    #[serde(default)]
    server: MockLlmServerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MockLlmServerConfig {
    #[serde(default = "default_port")]
    port: u16,
}

impl Default for MockLlmServerConfig {
    fn default() -> Self {
        Self { port: default_port() }
    }
}

fn default_port() -> u16 { 7002 }

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

#[derive(Deserialize)]
struct ChatRequest {
    prompt: String,
}

#[derive(Serialize)]
struct LlmDelta {
    delta: String,
    is_final: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let cfg_path = resolve_config_path("mock-llm", args.config.as_deref());
    if !cfg_path.exists() {
        tracing::warn!(
            target: "mock_llm",
            path = %cfg_path.display(),
            "未找到配置文件，使用内置默认"
        );
    }

    let mut cfg: MockLlmConfig = load_yaml(&cfg_path)?;
    cfg.log.apply_env_overrides();
    if let Some(parent) = cfg_path.parent() {
        cfg.log.resolve_relative_paths(parent);
    }
    if let Some(p) = args.port { cfg.server.port = p; }
    if let Some(l) = args.log_level { cfg.log.level = l; }
    if let Some(f) = args.log_file { cfg.log.file = f; }

    init_logging(&cfg.log)?;

    info!(
        target: "mock_llm",
        config_file = %cfg_path.display(),
        port = cfg.server.port,
        log_level = %cfg.log.level,
        log_file = %cfg.log.file,
        "mock-llm 配置加载完成"
    );

    let port = cfg.server.port;
    info!(target: "mock_llm", "启动 mock-llm，监听 0.0.0.0:{}", port);

    let app = Router::new().route("/chat", post(handle_chat));
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    info!(target: "mock_llm", "ready: POST http://0.0.0.0:{}/chat", port);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_chat(headers: HeaderMap, req: Request) -> Response {
    let session_id = headers
        .get("x-session-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();

    let body: ChatRequest = match axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
    {
        Some(b) => b,
        None => {
            error!(target: "mock_llm", session_id = %session_id, "解析 chat body 失败");
            return axum::http::StatusCode::BAD_REQUEST.into_response();
        }
    };

    info!(target: "mock_llm", session_id = %session_id, prompt = %body.prompt, "收到 prompt，开始流式生成");

    // 固定模板响应（按句切分，模拟真实 LLM 的多句输出）
    let response = "你好！我是 mock-llm。\
        我已经收到了你的问题。\
        让我来给你一个详细的回答。\
        这是第一句完整的话。\
        这是第二句完整的话。\
        希望这个回答对你有帮助！";

    // 按"。"切句（含中文句号和英文句号）
    let sentences: Vec<String> = split_sentences(response);

    info!(target: "mock_llm", session_id = %session_id, sentence_count = sentences.len(), "切句完成，开始逐句输出");

    // 构造 NDJSON 流
    let mut stream_body = String::new();

    for (i, sent) in sentences.iter().enumerate() {
        // 模拟思考延迟
        sleep(Duration::from_millis(120)).await;
        let delta = if i == sentences.len() - 1 {
            sent.clone()
        } else {
            // 流式：每个句子再切成小段模拟 token
            format!("{}{}", sent, "。")
        };
        info!(target: "mock_llm", session_id = %session_id, idx = i, delta = %delta, "推送 LLM delta");
        let json = serde_json::to_string(&LlmDelta {
            delta,
            is_final: false,
        })
        .unwrap();
        stream_body.push_str(&format!("{}\n", json));
    }

    sleep(Duration::from_millis(50)).await;
    // 最终标记
    let json = serde_json::to_string(&LlmDelta {
        delta: String::new(),
        is_final: true,
    })
    .unwrap();
    stream_body.push_str(&format!("{}\n", json));
    info!(target: "mock_llm", session_id = %session_id, "推送 LLM end-of-stream");

    axum::response::IntoResponse::into_response((
        axum::http::StatusCode::OK,
        [("content-type", "application/x-ndjson")],
        stream_body,
    ))
}

fn split_sentences(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for ch in text.chars() {
        cur.push(ch);
        if ch == '！' || ch == '!' || ch == '?' || ch == '?' || ch == '。' || ch == '.' {
            let s = cur.trim().to_string();
            if !s.is_empty() {
                out.push(s);
            }
            cur.clear();
        }
    }
    let tail = cur.trim().to_string();
    if !tail.is_empty() {
        out.push(tail);
    }
    if out.is_empty() {
        warn!("split_sentences 返回空，可能输入无标点");
    }
    out
}