//! ASR / LLM / TTS 客户端
//!
//! ASR / TTS 走手搓 reqwest（绕开 async-openai 的限制 —— FunASR 私有字段、自定义 voice 字符串），
//! LLM 仍用 async-openai SDK。provider 通过 `ProviderConfig::api_base` 切换
//!（siliconflow、OpenAI 官方、自建 ASR 等）。
//!
//! 模块组织：
//!   - `asr.rs`         HttpAsrClient（手搓 reqwest multipart，支持 FunASR 私有扩展 punc/spk/tags）
//!   - `funasr.rs`      FunasrClient（直连本地 FunASR 部署，docs/FunASR/runtime/docs/websocket_protocol_zh.md）
//!   - `llm.rs`         HttpLlmClient
//!   - `tts.rs`         HttpTtsClient（手搓 reqwest：Voice 枚举不兼容 siliconflow 自定义 voice 字符串）
//!   - `error.rs`       ClientError
//!   - `factory`        build_asr/llm/tts_client
//!
//! 配置文件 schema 见 `voice-voice_server.yaml`，与 `config::VoiceConfig` 严格对应。

pub mod asr;
pub mod error;
pub mod funasr;
pub mod llm;
pub mod tts;

pub use asr::{build_asr_client, ArcAsr, AsrClient, HttpAsrClient};
pub use error::ClientError;
pub use funasr::{
    build_funasr_client, ArcFunasr, FunasrClient, FunasrConfig, FunasrMode, FunasrResponse,
    FunasrResponseMode, FunasrSession,
};
pub use llm::{build_llm_client, ArcLlm, LlmClient, HttpLlmClient};
pub use tts::{build_tts_client, ArcTts, TtsClient, HttpTtsClient};
