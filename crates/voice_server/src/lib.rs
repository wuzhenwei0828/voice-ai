//! Voice server: 在 webhttp 之上挂载语音 pipeline
//!
//! ## 新手阅读顺序
//! 1. 先看 [`config`]，了解 YAML 如何生成各客户端配置。
//! 2. 再看 [`service`]，了解 HTTP 与 WebSocket 路由如何组装。
//! 3. 普通对话从 [`session::VoiceSession`] 进入，具体编排在 [`session::pipeline`]。
//! 4. LLM 到 TTS 的可复用逻辑在 [`pipeline::llm_tts`]，客户端实现位于 [`client`]。
//! 5. 领域事件统一定义在 [`events`]，便于追踪模块之间传递的数据形状。
//!
//! 关键模块：
//!   - `client`       ASR / LLM / TTS 客户端 trait + HTTP 实现
//!   - `agent`        LlmAgent：短期记忆 + 多轮对话 + fast/strong 模型路由
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
pub mod metrics;
pub mod pipeline;
pub mod service;
pub mod trace_context;
pub mod session;
pub mod utils;

pub use agent::{
    InMemoryStore, KnowledgeSearch, LlmAgent, MemoryStore, ModelRouter, NoopKnowledgeSearch,
    RedisStore, SearchError, SearchResult, Source, DEFAULT_STRONG_MIN_CHARS,
};
pub use client::{
    build_asr_client, build_llm_client, build_llm_client_with_prompt, build_tts_client,
    build_tts_client_with_metrics, AsrClient, HttpAsrClient, HttpLlmClient, HttpTtsClient,
    LlmClient, LlmPromptTemplates, ModelTier, TtsClient, TtsInputSession, TtsWsClient, TtsWsConfig,
};
pub use config::{
    AsrConfig, LlmConfig, ProviderConfig, ServerConfig, TtsConfig, VoiceConfig, REDIS_KEY_PREFIX,
};
pub use events::{AsrEvent, AsrSegment, LlmEvent, TtsEvent};
pub use logging::{
    candidate_paths, default_config_path, init_logging, load_yaml, resolve_config_path,
    resolve_web_static_dir, LogConfig, LogFormat,
};
pub use metrics::{
    EscalationReason, NoopMetricsSink, PipelineResult, PrometheusMetricsSink, VoiceMetrics,
    VoiceMetricsSink,
};
pub use service::VoiceService;
pub use session::{SessionState, VoiceSession};
