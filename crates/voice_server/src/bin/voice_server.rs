//! voice_server 启动入口
//!
//! 配置来源（优先级从高到低）：
//!   1. CLI 参数（--config、--port、--log-level）
//!   2. 环境变量（VOICE_CONFIG / VOICE_LOG_LEVEL / VOICE_PORT / VOICE_*_URL 等）
//!   3. 配置文件：默认 `<crate>/src/config/config.yaml`（in-crate），可用 --config / VOICE_CONFIG 覆盖
//!   4. 内置默认值
//!
//! 启动日志会打印"最终生效的配置项"，便于确认。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use tracing::info;

use voice_server::{init_logging, load_yaml, resolve_config_path, resolve_web_static_dir, VoiceConfig, VoiceService};

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

    /// 静态文件目录（web admin）；环境变量 VOICE_WEB_STATIC_DIR 也可
    #[arg(long)]
    web_static_dir: Option<String>,
}

/// 默认配置文件路径：`<voice-server crate 根>/src/config/config.yaml`
/// 编译期由 CARGO_MANIFEST_DIR 锚定，不依赖 CWD
fn default_config_path() -> &'static Path {
    static PATH: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    PATH.get_or_init(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("config")
            .join("config.yaml")
    })
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // 1. 解析配置文件路径（CLI > VOICE_CONFIG > 标准搜索）
    let cfg_path = resolve_config_path(
        "voice_server",
        cli.config.as_deref(),
        Some(default_config_path()),
    );
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

    let provider_cfg = cfg.provider.as_ref();

    // 4. 打印最终生效的配置
    info!(
        target: "voice_server",
        config_file = %cfg_path.display(),
        server_port = cfg.server.port,
        worker_num = cfg.server.worker_num,
        log_level = %cfg.log.level,
        log_file = %cfg.log.file,
        log_format = %cfg.log.format,
        asr_kind = "http",
        asr_endpoint = %cfg.asr.resolved(cfg.provider.as_ref()).api_base,
        llm_kind = "http",
        llm_endpoint = %cfg.llm.resolved(cfg.provider.as_ref()).api_base,
        tts_kind = cfg.tts.transport_kind(),
        tts_endpoint = %cfg.tts.resolved_endpoint(provider_cfg),
        "voice_server 配置加载完成"
    );

    // 5. 构造客户端
    let asr = voice_server::build_asr_client(&cfg.asr, provider_cfg)?;
    let llm = voice_server::build_llm_client(&cfg.llm, provider_cfg)?;
    let tts = voice_server::build_tts_client(&cfg.tts, provider_cfg)?;

    // 5.5 LlmAgent：根据 cfg.agent.memory_backend 选择 in-memory 或 redis store
    //   redis 配置在顶层 cfg.redis（URL / 全局前缀 / 默认 TTL），
    //   agent 的 namespace 后缀 "memory:" 在代码里写死（不用配置）。
    let backend = cfg.agent.memory_backend.as_str();
    let store: std::sync::Arc<dyn voice_server::MemoryStore> = match backend {
        "in_memory" => {
            info!(
                target: "voice_server.factory",
                backend,
                window_size = cfg.agent.memory_window,
                "Agent 短期记忆后端 = 进程内 DashMap"
            );
            std::sync::Arc::new(voice_server::InMemoryStore::new(cfg.agent.memory_window))
        }
        "redis" => {
            let url = cfg.redis.url.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "agent.memory_backend=redis 但 redis.url 未配置（yaml redis.url 或 env VOICE_REDIS_URL）"
                )
            })?;
            // 完整 key 前缀 = REDIS_KEY_PREFIX 常量 + 代码写死的 "memory:"
            // → "voice:memory:{session_id}"
            let full_prefix = format!("{}memory:", voice_server::REDIS_KEY_PREFIX);
            let ttl = cfg.agent.memory_ttl_secs.unwrap_or(cfg.redis.default_ttl_secs);
            info!(
                target: "voice_server.factory",
                backend,
                url = %url,
                window_size = cfg.agent.memory_window,
                key_prefix = %full_prefix,
                ttl_secs = ttl,
                "Agent 短期记忆后端 = Redis（集群共享）"
            );
            let store = voice_server::RedisStore::connect_with_prefix(
                &url,
                cfg.agent.memory_window,
                full_prefix,
                ttl,
            )
            .await?;
            std::sync::Arc::new(store)
        }
        other => {
            return Err(anyhow::anyhow!(
                "未知 agent.memory_backend: {}（合法值：in_memory | redis）",
                other
            ));
        }
    };
    let agent = std::sync::Arc::new(voice_server::LlmAgent::with_store(llm.clone(), store));

    let static_dir =
        resolve_web_static_dir(cli.web_static_dir.as_deref()).unwrap_or_else(|| std::path::PathBuf::from("./static"));
    info!(target: "voice_server.web", static_dir = %static_dir.display(), "web demo 静态目录解析结果");

    let service = Arc::new(
        VoiceService::new(asr, llm, agent, tts)
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
        ws_url = format!("ws://127.0.0.1:{}/ws/voice/web/admin", cfg.server.port),
        ws_format = "ws://host:port/ws/{business}/{actor}/{connid}",
        "WebSocket 路由"
    );
    info!(
        target: "voice_server",
        asr = format!("{} (http)", cfg.asr.resolved(cfg.provider.as_ref()).api_base),
        llm = format!("{} (http)", cfg.llm.resolved(cfg.provider.as_ref()).api_base),
        tts = format!(
            "{} ({})",
            cfg.tts.resolved_endpoint(provider_cfg),
            cfg.tts.transport_kind()
        ),
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
