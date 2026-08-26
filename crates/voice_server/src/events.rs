//! Wire-event 类型统一归属
//!
//! 这些是 domain 复用的 wire-event 数据结构：
//! - `client/{asr,llm,tts}.rs` 各客户端的 stream 输出类型
//! - `agent/llmagent.rs` 透传到下游的事件
//! - `session.rs` 的 WS pipeline 编排
//! - `admin_api.rs` 的 HTTP NDJSON 输出
//! - `pipeline.rs` 的 LLM→TTS 共享流水线
//!
//! 统一在一个地方避免反向依赖：以前这些类型定义在 `session.rs`，被 `client/` 和
//! `agent/` 反向 import 形成 4 个 import 循环。归到这里后：
//!
//! ```text
//! client/{asr,llm,tts} ─┐
//! agent/llmagent        ├─→ events ←─ session
//! admin_api             ┘              │
//!                                     ↓
//!                                pipeline (共享)
//! ```
//!
//! 注：`AsrEvent.language` / `duration` / `segments` 和 `AsrSegment` 是
//! `response_format=verbose_json` 的预留形状字段，当前 voice-server 不消费、不序列化，
//! 仅占位；后续要做语种识别 / 按语种路由 LLM system prompt 时再启用。

use serde::{Deserialize, Serialize};

/// ASR 客户端的流式输出事件。
#[derive(Debug, Clone, Default)]
pub struct AsrEvent {
    pub text: String,
    pub is_final: bool,
    /// `response_format=verbose_json` 时上游返回的语种（如 `"zh"` / `"en"`）。
    /// **预留字段** —— 目前 voice-server 不消费、不序列化，只是把形状先占住，
    /// 等后续要做语种识别 / 按语种路由 LLM system prompt 时再启用。
    pub language: Option<String>,
    /// `response_format=verbose_json` 时上游返回的音频总时长（秒）。
    /// **预留字段**，同上，暂不消费。
    pub duration: Option<f64>,
    /// `response_format=verbose_json` 时按句/段切分的时间对齐分段。
    /// **预留字段**，同上，暂不消费。spk=true 时 `AsrSegment::speaker` 也会被填充。
    pub segments: Option<Vec<AsrSegment>>,
}

/// `verbose_json` 响应 `segments[]` 里每个元素的形状（参见 yapi.md §1）。
/// **预留结构**，目前 voice-server 不消费，仅用于未来扩展。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsrSegment {
    pub id: u32,
    pub start: f64,
    pub end: f64,
    pub text: String,
    /// `spk=true` 时上游会带这个字段（`"spk0"` / `"spk1"` ...）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speaker: Option<String>,
    /// SenseVoice 不输出词级时间戳，固定为空数组；保留字段兼容 OpenAI 形态
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub words: Option<serde_json::Value>,
}

/// LLM 客户端的流式输出事件。
#[derive(Debug, Clone)]
pub struct LlmEvent {
    pub delta: String,
    pub is_final: bool,
}

/// TTS 客户端的流式输出事件。
#[derive(Debug, Clone)]
pub struct TtsEvent {
    pub seq: u32,
    pub data: Vec<u8>,
    pub is_last: bool,
}
