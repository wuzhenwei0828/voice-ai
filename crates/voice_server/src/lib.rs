//! Voice server: 在 webhttp 之上挂载语音 pipeline
//!
//! 关键模块：
//!   - `client`       ASR / LLM / TTS 客户端 trait + HTTP 实现
//!   - `agent`        LlmAgent：短期记忆 + 多轮对话 + 提示词注入
//!   - `events`       wire-event 共享类型（AsrEvent / LlmEvent / TtsEvent / AsrSegment）
//!   - `session`      VoiceSession: 每连接一个 Actor，管理状态机 + pipeline
//!   - `service`      VoiceService: 实现 webhttp::ServiceCallback
//!   - `pipeline`     共享 LLM→切句→TTS→crossfade→emit 流水线（admin_api 与 session 共用）
//!   - `admin_api`    单能力验证 HTTP 接口（/admin/asr 等 5 个端点）
//!   - `live_asr`    FunASR 实时流式 ASR WebSocket 端点
//!   - `config`       VoiceConfig：从 YAML 加载并应用环境变量覆盖
//!   - `bin/voice_server.rs` 启动入口

pub mod admin_api;
pub mod agent;
pub mod client;
pub mod config;
pub mod events;
pub mod live_asr;
pub mod logging;
pub mod pipeline;
pub mod service;
pub mod session;

pub use agent::{LlmAgent, RedisStore, InMemoryStore, MemoryStore};
pub use client::{
    build_asr_client, build_llm_client, build_tts_client, AsrClient, HttpAsrClient,
    HttpLlmClient, HttpTtsClient, LlmClient, TtsClient,
};
pub use config::{AsrConfig, LlmConfig, ProviderConfig, REDIS_KEY_PREFIX, ServerConfig, TtsConfig, VoiceConfig};
pub use events::{AsrEvent, AsrSegment, LlmEvent, TtsEvent};
pub use logging::{
    candidate_paths, default_config_path, init_logging, load_yaml, resolve_config_path,
    resolve_web_static_dir, LogConfig, LogFormat,
};
pub use service::VoiceService;
pub use session::{SessionState, VoiceSession};
