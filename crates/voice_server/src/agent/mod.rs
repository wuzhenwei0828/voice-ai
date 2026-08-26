//! LLM Agent 层：在 [`LlmClient`] 之上加短期记忆（滑动窗口）+ 多轮对话
//!
//! 关键模块：
//!   - [`memory`]      短期记忆抽象 = [`MemoryStore`] trait + [`InMemoryStore`]（默认）+ [`RedisStore`]（集群）
//!   - [`prompts`]     LLM 提示词模板（编译期嵌入 `prompts.yaml`）—— [`AgentPrompts`] + 默认实例懒加载
//!   - [`llmagent`]    `LlmAgent`：封装 [`crate::client::llm::LlmClient`]，自动拼历史、调底层、写记忆
//!
//! 当前默认窗口 = 20（见 [`memory::DEFAULT_WINDOW`]），可调 [`llmagent::LlmAgent::with_window`]。

pub mod llmagent;
pub mod memory;
pub mod prompts;
pub mod redis_store;

pub use llmagent::LlmAgent;
pub use memory::{InMemoryStore, MemoryStore, Message, Role, ShortTermMemory, DEFAULT_WINDOW};
pub use prompts::AgentPrompts;
pub use redis_store::RedisStore;
