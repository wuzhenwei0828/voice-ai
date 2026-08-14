//! mock-tts: 模拟 TTS 服务
//!
//! 协议：
//!   POST /synthesize
//!     Headers: X-Session-Id
//!     Body: {"text": "..."}
//!     Response: 二进制流（每行一个 JSON 头 + 紧跟音频字节）
//!       {"bytes": 320, "is_last": false} + <320 bytes PCM>
//!       ...
//!       {"bytes": 0, "is_last": true}
//!
//! 简化：把"行格式"换成 NDJSON 风格，每行 base64 编码音频字节：
//!   {"seq": 0, "audio_b64": "...", "is_last": false}
//!   {"seq": 1, "audio_b64": "...", "is_last": false}
//!   {"seq": N, "audio_b64": "",   "is_last": true}
//!
//! 行为：
//!   按 text 长度生成 ~N 个 chunk（每 10 字一个 chunk）
//!   每个 chunk 是 320 字节伪 PCM（s16le, 16kHz, mono，~10ms 静音）
//!   每个 chunk 间 sleep 30ms 模拟合成延迟

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
use tracing::{error, info};
use voice_config::{init_logging, load_yaml, resolve_config_path, LogConfig};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct MockTtsConfig {
    #[serde(default)]
    log: LogConfig,
    #[serde(default)]
    server: MockTtsServerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MockTtsServerConfig {
    #[serde(default = "default_port")]
    port: u16,
}

impl Default for MockTtsServerConfig {
    fn default() -> Self {
        Self { port: default_port() }
    }
}

fn default_port() -> u16 { 7003 }

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
struct TtsRequest {
    text: String,
}

#[derive(Serialize)]
struct TtsChunk {
    seq: u32,
    #[serde(with = "base64_bytes")]
    audio: Vec<u8>,
    is_last: bool,
}

mod base64_bytes {
    use base64::Engine;
    use serde::{Serializer};

    pub fn serialize<S: Serializer>(b: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        let enc = base64::engine::general_purpose::STANDARD.encode(b);
        s.serialize_str(&enc)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let cfg_path = resolve_config_path("mock-tts", args.config.as_deref());
    if !cfg_path.exists() {
        tracing::warn!(
            target: "mock_tts",
            path = %cfg_path.display(),
            "未找到配置文件，使用内置默认"
        );
    }

    let mut cfg: MockTtsConfig = load_yaml(&cfg_path)?;
    cfg.log.apply_env_overrides();
    if let Some(parent) = cfg_path.parent() {
        cfg.log.resolve_relative_paths(parent);
    }
    if let Some(p) = args.port { cfg.server.port = p; }
    if let Some(l) = args.log_level { cfg.log.level = l; }
    if let Some(f) = args.log_file { cfg.log.file = f; }

    init_logging(&cfg.log)?;

    info!(
        target: "mock_tts",
        config_file = %cfg_path.display(),
        port = cfg.server.port,
        log_level = %cfg.log.level,
        log_file = %cfg.log.file,
        "mock-tts 配置加载完成"
    );

    let port = cfg.server.port;
    info!(target: "mock_tts", "启动 mock-tts，监听 0.0.0.0:{}", port);

    let app = Router::new().route("/synthesize", post(handle_synthesize));
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    info!(target: "mock_tts", "ready: POST http://0.0.0.0:{}/synthesize", port);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_synthesize(headers: HeaderMap, req: Request) -> Response {
    let session_id = headers
        .get("x-session-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();

    let body: TtsRequest = match axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
    {
        Some(b) => b,
        None => {
            error!(target: "mock_tts", session_id = %session_id, "解析 synthesize body 失败");
            return axum::http::StatusCode::BAD_REQUEST.into_response();
        }
    };

    info!(target: "mock_tts", session_id = %session_id, text = %body.text, text_len = body.text.chars().count(), "收到合成请求");

    // 估算 chunk 数：每 10 个字符一个 chunk，最少 1 个
    let char_count = body.text.chars().count();
    let chunk_count = std::cmp::max(1, char_count / 10);

    // 每个 chunk 320 字节 PCM（s16le, 16kHz, mono, ~10ms）
    let mut stream_body = String::new();
    for i in 0..chunk_count {
        sleep(Duration::from_millis(40)).await;

        // 生成 320 字节的"伪 PCM"（正弦波 pattern，便于辨识）
        let audio = make_fake_pcm_chunk(320, i as u32);

        info!(
            target: "mock_tts",
            session_id = %session_id,
            seq = i,
            bytes = audio.len(),
            "推送 TTS audio chunk"
        );

        let json = serde_json::to_string(&TtsChunk {
            seq: i as u32,
            audio,
            is_last: false,
        })
        .unwrap();
        stream_body.push_str(&format!("{}\n", json));
    }

    // end-of-stream 标记
    let json = serde_json::to_string(&TtsChunk {
        seq: chunk_count as u32,
        audio: vec![],
        is_last: true,
    })
    .unwrap();
    stream_body.push_str(&format!("{}\n", json));
    info!(target: "mock_tts", session_id = %session_id, total_chunks = chunk_count, "推送 TTS end-of-stream");

    axum::response::IntoResponse::into_response((
        axum::http::StatusCode::OK,
        [("content-type", "application/x-ndjson")],
        stream_body,
    ))
}

fn make_fake_pcm_chunk(n_bytes: usize, seq: u32) -> Vec<u8> {
    // 生成有辨识度的正弦波：频率随 seq 变化
    let freq = 200.0 + (seq as f32) * 50.0;
    let sample_rate = 16000.0;
    let mut buf = Vec::with_capacity(n_bytes);
    for i in 0..(n_bytes / 2) {
        let t = i as f32 / sample_rate;
        let sample = ((2.0 * std::f32::consts::PI * freq * t).sin() * 8000.0) as i16;
        buf.extend_from_slice(&sample.to_le_bytes());
    }
    buf
}