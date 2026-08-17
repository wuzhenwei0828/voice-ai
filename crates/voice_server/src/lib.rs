//! Voice server: 在 webhttp 之上挂载语音 pipeline
//!
//! 关键模块：
//!   - `clients`     ASR / LLM / TTS 客户端 trait + HTTP 实现（指向 mock-* 服务）
//!   - `config`      VoiceConfig：从 TOML 加载并应用环境变量覆盖
//!   - `session`     VoiceSession: 每连接一个 Actor，管理状态机 + pipeline
//!   - `service`     VoiceService: 实现 webhttp::ServiceCallback
//!   - `bin/voice_server.rs` 启动入口

pub mod client;
pub mod config;
pub mod logging;
pub mod service;
pub mod session;
pub mod admin_api;

pub use client::{
    build_asr_client, build_llm_client, build_tts_client, AsrClient, HttpAsrClient,
    HttpLlmClient, HttpTtsClient, LlmClient, TtsClient,
};
pub use config::{AsrConfig, LlmConfig, ProviderConfig, ServerConfig, TtsConfig, VoiceConfig};
pub use logging::{
    candidate_paths, default_config_path, init_logging, load_yaml, resolve_config_path,
    resolve_web_static_dir, LogConfig, LogFormat,
};
pub use service::VoiceService;
pub use session::{SessionState, VoiceSession};