//! 短期记忆抽象 + InMemory 实现
//!
//! ## 抽象层
//! [`MemoryStore`] trait 描述"按 session 拉历史 / 追加 / 清空"这组操作，
//! 不同后端（进程内 [`InMemoryStore`]、跨进程 [`RedisStore`]）共享同一接口。
//! 上层 [`crate::agent::LlmAgent`] 通过 `Arc<dyn MemoryStore>` 调用，无关后端选型。
//!
//! ## 滑动窗口
//! 窗口容量 N 由实现持有；`append` 超容时丢弃最旧，自动保留最新 N 条。
//! 默认 N = 20（[`DEFAULT_WINDOW`]）。
//!
//! ## 为什么不用长记忆
//! 用户问"刚才说了啥"——长记忆是 summary / 实体抽取，与本模块（保留逐条原文、滑动窗口）
//! 不是同一层。本模块专为最近对话上下文设计。

use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

/// 滑动窗口默认容量（最近 20 条对话记录）
pub const DEFAULT_WINDOW: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    System,
    User,
    Assistant,
}

impl Role {
    /// 序列化为 `role` 字段（OpenAI chat completions 协议）
    pub fn as_str(self) -> &'static str {
        match self {
            Role::System => "system",
            Role::User => "user",
            Role::Assistant => "assistant",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: String,
}

/// 短期记忆后端抽象。所有方法都是异步 —— 进程内实现是零成本 async 包装，
/// Redis 实现真正跨进程。
#[async_trait]
pub trait MemoryStore: Send + Sync {
    /// 拉取该 session 当前窗口内全部历史（最旧 → 最新）。新 session 返回 `Vec::new()`。
    async fn history(&self, session_id: &str) -> Vec<Message>;

    /// 追加一条；超过窗口容量时由实现内部驱逐最旧。
    async fn append(&self, session_id: &str, msg: Message);

    /// 清空该 session 的记忆。
    async fn clear(&self, session_id: &str);

    /// 当前窗口条数（用于调试 / 监控）。
    async fn len(&self, session_id: &str) -> usize;

    /// session 是否无记录（`history().is_empty()` 的快捷版）
    async fn is_empty(&self, session_id: &str) -> bool {
        self.len(session_id).await == 0
    }
}

// =============================================================================
// InMemory 实现：DashMap<session_id, Arc<Mutex<ShortTermMemory>>>
// 单进程、零网络。生产可用 sticky session 模式。
// =============================================================================

pub struct InMemoryStore {
    window_size: usize,
    map: DashMap<String, Arc<Mutex<ShortTermMemory>>>,
}

impl InMemoryStore {
    pub fn new(window_size: usize) -> Self {
        Self {
            window_size: window_size.max(1),
            map: DashMap::new(),
        }
    }

    pub fn window_size(&self) -> usize {
        self.window_size
    }

    fn mem_for(&self, session_id: &str) -> Arc<Mutex<ShortTermMemory>> {
        self.map
            .entry(session_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(ShortTermMemory::new(self.window_size))))
            .clone()
    }
}

#[async_trait]
impl MemoryStore for InMemoryStore {
    async fn history(&self, session_id: &str) -> Vec<Message> {
        if let Some(m) = self.map.get(session_id) {
            m.lock().history()
        } else {
            Vec::new()
        }
    }

    async fn append(&self, session_id: &str, msg: Message) {
        self.mem_for(session_id).lock().push(msg);
    }

    async fn clear(&self, session_id: &str) {
        if let Some(m) = self.map.get(session_id) {
            m.lock().clear();
        }
    }

    async fn len(&self, session_id: &str) -> usize {
        self.map
            .get(session_id)
            .map(|m| m.lock().len())
            .unwrap_or(0)
    }
}

/// 滑动窗口数据结构（内部使用，不暴露给 MemoryStore 之外）。
/// 单元测试仍在外部可访问，便于测试纯数据逻辑。
pub struct ShortTermMemory {
    capacity: usize,
    items: VecDeque<Message>,
}

impl ShortTermMemory {
    /// 构造。`capacity = 0` 会被当作 1（保留至少 1 条，避免无意义的 0 容量）。
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            capacity: cap,
            items: VecDeque::with_capacity(cap),
        }
    }

    /// 推入一条消息。超容时丢弃最旧。
    pub fn push(&mut self, msg: Message) {
        if self.items.len() == self.capacity {
            self.items.pop_front();
        }
        self.items.push_back(msg);
    }

    /// 当前窗口内全部条目，按时间顺序（最旧 → 最新）。
    pub fn history(&self) -> Vec<Message> {
        self.items.iter().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(role: Role, content: &str) -> Message {
        Message {
            role,
            content: content.to_string(),
        }
    }

    // ----- ShortTermMemory 数据结构测试（保留） -----

    #[test]
    fn capacity_zero_is_clamped_to_one() {
        let mut m = ShortTermMemory::new(0);
        m.push(msg(Role::User, "a"));
        m.push(msg(Role::User, "b"));
        assert_eq!(m.len(), 1);
        assert_eq!(m.history()[0].content, "b");
    }

    #[test]
    fn push_evicts_oldest_when_full() {
        let mut m = ShortTermMemory::new(3);
        for i in 1..=5 {
            m.push(msg(Role::User, &format!("u{i}")));
        }
        let h = m.history();
        assert_eq!(h.len(), 3);
        assert_eq!(h[0].content, "u3");
        assert_eq!(h[1].content, "u4");
        assert_eq!(h[2].content, "u5");
    }

    #[test]
    fn history_preserves_role() {
        let mut m = ShortTermMemory::new(5);
        m.push(msg(Role::System, "sys"));
        m.push(msg(Role::User, "u1"));
        m.push(msg(Role::Assistant, "a1"));
        let h = m.history();
        assert_eq!(h[0].role, Role::System);
        assert_eq!(h[1].role, Role::User);
        assert_eq!(h[2].role, Role::Assistant);
    }

    #[test]
    fn clear_empties_memory() {
        let mut m = ShortTermMemory::new(5);
        m.push(msg(Role::User, "x"));
        m.clear();
        assert!(m.is_empty());
    }

    #[test]
    fn role_as_str_matches_openai_protocol() {
        assert_eq!(Role::System.as_str(), "system");
        assert_eq!(Role::User.as_str(), "user");
        assert_eq!(Role::Assistant.as_str(), "assistant");
    }

    #[test]
    fn message_serde_roundtrip() {
        let m = msg(Role::Assistant, "你好");
        let s = serde_json::to_string(&m).unwrap();
        let back: Message = serde_json::from_str(&s).unwrap();
        assert_eq!(back.role, Role::Assistant);
        assert_eq!(back.content, "你好");
    }
}
