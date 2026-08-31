//! 共享"文本/输入 → TTS"流水线工具集
//!
//! 整个模块是**多模态扩展点**：当前只有 [`llm_tts`]（LLM → TTS），未来加
//! [`image_tts`]（图片描述 → TTS）、[`text_tts`]（直接文本 → TTS）时各自
//! 独立模块，共享 [`crossfade`] 和 [`sentence`] 这两个工具。
//!
//! ## 当前模块
//! - [`crossfade`] 句间 crossfade 状态机（[`SentenceCrossfader`]）—— 任何"文本→TTS
//!   切句拼接"流水线都用
//! - [`sentence`] 句末判定（[`next_sentence_end`]）—— 任何 pipeline 切句都用
//! - [`llm_tts`]  LLM→TTS 完整流水线（[`llm_tts_items`] + [`LlmTtsItem`] 事件）
//!
//! ## 未来扩展指引
//!
//! 加新模态的 pipeline（例：`image → TTS`）：
//!
//! ```text
//! pipeline/
//!   mod.rs         ← re-exports + 文档（本文件）
//!   crossfade.rs   ← 已有
//!   sentence.rs    ← 已有
//!   llm_tts.rs     ← 已有
//!   image_tts.rs   ← 新建：图片 → caption → sentence split → TTS → crossfade
//!   text_tts.rs    ← 新建：直接 text → sentence split → TTS → crossfade
//! ```
//!
//! **为什么每条 pipeline 独立模块而不是抽通用 trait**：
//! - 各 pipeline 的 "source" 阶段语义差异大（流式 vs 一次性 vs 无）
//! - 错误模式不同（LLM 流错误 vs caption 失败 vs 无上游错误）
//! - emit 模式不同（LLM 边发文本边发音频；image/text 没有 mid-flight 文本）
//! - 强行抽通用 trait 反而要管 Source关联类型 + 多态分发，不如独立函数清晰
//!
//! 共享部分（句末判定 + TTS 文本清洗/短句合并 + crossfade + seq 编号 + 结束标记）
//! 保持在 [`crossfade`]、[`sentence`] 和 [`text`] 里，所有 pipeline 直接复用即可。
//!
//! 事件类型（[`LlmTtsItem`] 这种）每个 pipeline 独立定义 —— 不要试图抽统一的
//! `PipelineEvent` 枚举，否则 enum 变体会膨胀且与各 pipeline 语义强耦合。
//! 各 wire 消费方（HTTP/WS）按需 `match` 自己关心的那一个事件类型即可。

pub mod crossfade;
pub mod llm_tts;
pub mod sentence;
pub mod text;

// 公共 re-export：调用方 `use crate::pipeline::*` 拿到主要 API
pub use crossfade::SentenceCrossfader;
pub use llm_tts::{llm_tts_items, LlmTtsItem};
pub use sentence::next_sentence_end;
pub use text::{to_tts_text, TtsSentenceBuffer};
