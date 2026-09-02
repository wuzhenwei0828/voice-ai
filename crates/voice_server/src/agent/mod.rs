//! LLM Agent 层：在 [`LlmClient`] 之上加短期记忆（滑动窗口）+ 多轮对话
//!
//! 关键模块：
//!   - [`memory`]      短期记忆抽象 = [`MemoryStore`] trait + [`InMemoryStore`]（默认）+ [`RedisStore`]（集群）
//!   - [`llmagent`]    `LlmAgent`：封装 [`crate::client::llm::LlmClient`]，自动拼历史、调底层、写记忆
//!
//! 当前默认窗口 = 20（见 [`memory::DEFAULT_WINDOW`]），可调 [`llmagent::LlmAgent::with_window`]。

pub mod knowledge_search;
pub mod llmagent;
pub mod memory;
pub mod redis_store;
pub mod router;

pub use knowledge_search::{
    KnowledgeSearch, NoopKnowledgeSearch, SearchError, SearchResult, Source,
};
pub use llmagent::LlmAgent;
pub use memory::{InMemoryStore, MemoryStore, Message, Role, ShortTermMemory, DEFAULT_WINDOW};
pub use redis_store::RedisStore;
pub use router::{ModelRouter, DEFAULT_STRONG_MIN_CHARS};
