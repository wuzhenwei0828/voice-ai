//! voice_server 启动入口
//!
//! 配置来源（优先级从高到低）：
//!   1. CLI 参数（--config、--port、--log-level）
//!   2. 环境变量（VOICE_LOG_LEVEL / VOICE_PORT / VOICE_*_URL 等）
//!   3. 配置文件（默认 ./voice-voice_server.yaml，可用 --config 指定）
//!   4. 内置默认值
//!
//! 启动日志会打印"最终生效的配置项"，便于确认。

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tracing::info;

use voice_config::{init_logging, load_yaml, resolve_config_path};
use voice_server::{
    build_asr_client, build_llm_client, build_tts_client, VoiceConfig, VoiceService,
};

#[derive(Parser, Debug)]
#[command(name = "voice_server", about = "Voice server entry point")]
struct Cli {
    /// 配置文件路径（YAML）；不传则按 VOICE_CONFIG / 标准路径搜索
    #[arg(long)]
    config: Option<PathBuf>,

    /// 监听端口（覆盖配置文件）
    #[arg(long)]
    port: Option<u16>,

    /// 日志级别（覆盖配置文件）：trace|debug|info|warn|error
    #[arg(long)]
    log_level: Option<String>,

    /// 日志文件（覆盖配置文件）
    #[arg(long)]
    log_file: Option<String>,

    /// 静态文件目录（web demo）；环境变量 VOICE_WEB_STATIC_DIR 也可
    #[arg(long)]
    web_static_dir: Option<String>,
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // 1. 解析配置文件路径（CLI > VOICE_CONFIG > 标准搜索）
    let cfg_path = resolve_config_path("voice_server", cli.config.as_deref());
    if !cfg_path.exists() {
        tracing::warn!(
            target: "voice_server",
            path = %cfg_path.display(),
            "未找到配置文件，使用内置默认 + 环境变量"
        );
    }

    let mut cfg: VoiceConfig = load_yaml(&cfg_path)?;
    cfg.apply_env_overrides();

    // 1.5 log.file 等相对路径以配置文件所在目录为基准
    if let Some(parent) = cfg_path.parent() {
        cfg.log.resolve_relative_paths(parent);
    }

    // 2. CLI 参数覆盖（最高优先级）
    if let Some(p) = cli.port {
        cfg.server.port = p;
    }
    if let Some(l) = cli.log_level {
        cfg.log.level = l;
    }
    if let Some(f) = cli.log_file {
        cfg.log.file = f;
    }

    // 3. 初始化日志（基于 cfg.log）
    init_logging(&cfg.log)?;

    // 4. 打印最终生效的配置
    info!(
        target: "voice_server",
        config_file = %cfg_path.display(),
        server_port = cfg.server.port,
        worker_num = cfg.server.worker_num,
        log_level = %cfg.log.level,
        log_file = %cfg.log.file,
        log_format = %cfg.log.format,
        asr_kind = %cfg.asr.kind,
        asr_endpoint = %cfg.asr.endpoint,
        llm_kind = %cfg.llm.kind,
        llm_endpoint = %cfg.llm.endpoint,
        tts_kind = %cfg.tts.kind,
        tts_endpoint = %cfg.tts.endpoint,
        "voice_server 配置加载完成"
    );

    // 5. 构造客户端
    let asr = build_asr_client(&cfg.asr)?;
    let llm = build_llm_client(&cfg.llm)?;
    let tts = build_tts_client(&cfg.tts)?;

    let static_dir = voice_config::resolve_web_static_dir(cli.web_static_dir.as_deref())
        .unwrap_or_else(|| std::path::PathBuf::from("./static"));
    info!(target: "voice_server.web", static_dir = %static_dir.display(), "web demo 静态目录解析结果");

    let service = Arc::new(
        VoiceService::new(asr, llm, tts)
            .with_web_static_dir(&static_dir),
    );

    // 启动摘要：端口 + 注册的路由
    info!(
        target: "voice_server",
        "=========================================================启动摘要========================================================="
    );
    info!(
        target: "voice_server",
        port = cfg.server.port,
        worker_num = cfg.server.worker_num,
        "HTTP 监听端口"
    );
    info!(
        target: "voice_server",
        web_api = format!("http://127.0.0.1:{}/health", cfg.server.port),
        metrics = format!("http://127.0.0.1:{}/metrics", cfg.server.port),
        static_files = format!("http://127.0.0.1:{}/", cfg.server.port),
        "HTTP 路由"
    );
    info!(
        target: "voice_server",
        ws_url = format!("ws://127.0.0.1:{}/ws/voice/web/demo", cfg.server.port),
        ws_format = "ws://host:port/ws/{business}/{actor}/{connid}",
        "WebSocket 路由"
    );
    info!(
        target: "voice_server",
        asr = format!("{} ({})", cfg.asr.endpoint, cfg.asr.kind),
        llm = format!("{} ({})", cfg.llm.endpoint, cfg.llm.kind),
        tts = format!("{} ({})", cfg.tts.endpoint, cfg.tts.kind),
        "后端服务"
    );
    info!(
        target: "voice_server",
        "=========================================================WebSocket 端到端调试模式========================================================="
    );

    webhttp::start(
        "voice_server".to_string(),
        cfg.server.port,
        None,
        Some(service.clone() as Arc<dyn webhttp::ServiceCallback>),
        Some("ws".to_string()),
        Some(cfg.server.worker_num),
        |_svc| {},
        None,
        None,
        None,
        None,
        None,
        Some("/".to_string()),
    )
    .await?;

    Ok(())
}