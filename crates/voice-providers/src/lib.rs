//! voice-providers
//!
//! 百炼（DashScope）GAX ASR/TTS + OpenAI-compat LLM 的 provider 实现。
//!
//! ## 模块组织
//!
//! - `codec`        GAX 4-byte BE u32 length-prefix 帧编解码
//! - `ws_pool`      WS 连接池（双 lane：Asr / Tts），bearer concurrency via Semaphore
//! - `asr`          AsrClient trait + StreamingAsrClient + AsrModelAdapter
//! - `tts`          TtsClient trait + StreamingTtsClient + TtsModelAdapter
//! - `llm`          HttpLlmClient（OpenAI-compat，手搓 reqwest）
//! - `config`       BailianConfig + 从 serde_yaml::Mapping 解析
//! - `provider`     BailianProvider + build_all() 顶层入口
//!
//! ## 设计要点
//!
//! - **不依赖 voice-server**：voice-server → voice-providers 是唯一方向。
//!   voice-providers 自带 AsrClient / LlmClient / TtsClient / AsrEvent / LlmEvent / TtsEvent
//!   与 voice-server 对应类型同构（字段、语义、trait 方法签名一致）。voice-server PR1
//!   在 voice-server 侧用 `impl voice_server::AsrClient for Wrapper{...}` 把 voice-providers
//!   流包成 voice-server 期望的 trait 对象。
//! - **协议细节**：Qwen-Audio / Fun-ASR 等公共协议用 JSON 文本 + 裸 PCM binary，
//!   通过 `codec::WireFormat` 描述（`Text` / `RawBinary` / `BinaryGax`）；
//!   GAX 占位实现保留兼容。
//! - **真实拨号**：`make_real_dialer` 直接用 `async-tungstenite` 接 DashScope WSS，
//!   URL 通过 `build_asr_endpoint` / `build_tts_endpoint` 按 model 路由。
//!
//! ## 编译验证
//!
//! ```bash
//! cargo check -p voice-providers
//! cargo test  -p voice-providers
//! ```

pub mod asr;
pub mod codec;
pub mod config;
pub mod llm;
pub mod provider;
pub mod tts;
pub mod ws_pool;

// ===== prost-build 自动生成的类型（pub，方便外部 / 测试 import） =====
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/bailian_providers.rs"));
}

// ===== 重新导出最常用类型 =====
pub use asr::{
    ArcAsr, AsrClient, AsrEvent, AsrModelAdapter, ClientError, BoxStream,
};
pub use codec::{
    CodecError, GaxFrame, REQ_AUDIO_ASR, REQ_OPEN_ASR, REQ_OPEN_TTS,
    REQ_STOP_ASR, REQ_STOP_TTS, REQ_TEXT_TTS, RESP_AUDIO_TTS, RESP_DONE_TTS,
    RESP_ERR_ASR, RESP_ERR_TTS, RESP_OPEN_ASR, RESP_OPEN_TTS, RESP_TRANSCRIPT,
    decode, decode_frame, encode, encode_frame,
};
pub use config::{from_mapping, AsrCfg, BailianConfig, LlmCfg, PoolConfig, TtsCfg};
pub use llm::{ArcLlm, HttpLlmClient, LlmClient, LlmEvent};
pub use provider::{build_all, build_asr_streaming_pool};
pub use tts::{
    cosyvoice::CosyVoiceV2, qwen_audio::QwenAudioTts, qwen_realtime::QwenRealtimeTts,
    ArcTts, RealtimeEventHint, ServerEventHint, TtsClient, TtsEvent, TtsModelAdapter, TtsProtocol,
};
pub use ws_pool::{
    Dialer, LaneKind, PoolConfig as WsPoolConfig, PoolError, WebSocketLike, WsPool,
};