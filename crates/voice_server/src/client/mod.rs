//! ASR / LLM / TTS 客户端
//!
//! 每种能力都先定义一个 trait，再提供 HTTP 或 WebSocket 实现；上层 pipeline 只依赖
//! trait，因此替换 provider 不需要改会话编排代码。工厂函数负责把 [`VoiceConfig`]
//! 转成具体客户端，启动入口只需注入这些 trait 对象。
//!
//! ASR / LLM / TTS 都直接使用 reqwest，以支持私有字段、自定义鉴权和可立即取消的流。
//! provider 通过 `ProviderConfig::api_base` 切换
//!（siliconflow、OpenAI 官方、自建 ASR 等）。
//!
//! 模块组织：
//!   - `asr.rs`         HttpAsrClient（手搓 reqwest multipart，支持 FunASR 私有扩展 spk/tags）
//!   - `funasr.rs`      FunasrClient（直连本地 FunASR 部署，docs/FunASR/runtime/docs/websocket_protocol_zh.md）
//!   - `llm.rs`         HttpLlmClient
//!   - `prompt.yaml`     fast / strong 两份完整 System Prompt
//!   - `tts.rs`         HttpTtsClient（手搓 reqwest：Voice 枚举不兼容 siliconflow 自定义 voice 字符串）
//!   - `error.rs`       ClientError
//!   - `factory`        build_asr/llm/tts_client
//!
//! 配置文件 schema 见 `voice-voice_server.yaml`，与 `config::VoiceConfig` 严格对应。

pub mod asr;
pub mod error;
pub mod funasr;
pub mod llm;
pub mod prompt;
pub mod tts;
pub mod tts_ws;

pub use asr::{build_asr_client, ArcAsr, AsrClient, HttpAsrClient};
pub use error::ClientError;
pub use funasr::{
    build_funasr_client, ArcFunasr, FunasrClient, FunasrConfig, FunasrMode, FunasrSession,
};
pub use llm::{
    build_llm_client, build_llm_client_with_prompt, ArcLlm, HttpLlmClient, LlmClient, ModelTier,
};
pub use prompt::LlmPromptTemplates;
pub use tts::{build_tts_client, build_tts_client_with_metrics, ArcTts, HttpTtsClient, TtsClient, TtsInputSession};
pub use tts_ws::{TtsWsClient, TtsWsConfig};

/// Attach the current inbound trace ID to an outbound HTTP request when one is
/// available. The helper is intentionally a no-op for background calls.
pub fn apply_trace_header(req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    match crate::trace_context::current_trace_id() {
        Some(trace_id) => req.header("trace_id", trace_id),
        None => req,
    }
}
