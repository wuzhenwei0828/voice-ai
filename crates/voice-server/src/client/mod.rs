//! ASR / LLM / TTS 客户端
//!
//! 三个 client 都基于 `async-openai` 的 `Client<OpenAIConfig>`，provider 通过
//! `OpenAIConfig::with_api_base()` 切换（siliconflow、OpenAI 官方等 OpenAI-兼容服务）。
//!
//! 模块组织：
//!   - `asr.rs`  HttpAsrClient
//!   - `llm.rs`  HttpLlmClient
//!   - `tts.rs`  HttpTtsClient（手搓 reqwest：Voice 枚举不兼容 siliconflow 自定义 voice 字符串）
//!   - `error.rs` ClientError
//!   - `factory` build_asr/llm/tts_client
//!
//! 配置文件 schema 见 `voice-voice_server.yaml`，与 `config::VoiceConfig` 严格对应。

pub mod asr;
pub mod error;
pub mod llm;
pub mod tts;

pub use asr::{build_asr_client, ArcAsr, AsrClient, HttpAsrClient};
pub use error::ClientError;
pub use llm::{build_llm_client, ArcLlm, LlmClient, HttpLlmClient};
pub use tts::{build_tts_client, ArcTts, TtsClient, HttpTtsClient};
