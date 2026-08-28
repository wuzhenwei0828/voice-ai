//! LlmAgent: 在 [`LlmClient`] 上封装短期记忆 + 多轮对话
//!
//! ## 设计要点
//! - 每次 `chat()` 时从 [`MemoryStore`] 拉 session 的最近 N 条历史，拼到 messages 头部送上游
//! - 流收尾时（`is_final = true`）把 `user prompt` + 完整 `assistant 回复` 写入 store
//! - ASR 参考信号（情绪 / 事件）作为 system message 放在 messages 最前面（模板在 `prompts.yaml` 里，可改）
//! - 记忆后端可换：默认 [`InMemoryStore`]（单进程），集群可换 [`RedisStore`]（跨进程）
//! - 滑动窗口默认 20 条（见 [`crate::agent::memory::DEFAULT_WINDOW`]）
//!
//! ## 与 [`LlmClient`] trait 的关系
//!
//! `LlmAgent` 实现 `LlmClient`：调用方拿到 `Arc<dyn LlmClient>` 时可以无缝切换底层是
//! `HttpLlmClient`（无记忆）还是 `LlmAgent`（带记忆）。
//! - `chat()`：含情绪 + 历史 + 记忆写入 —— 语音 pipeline 用这个
//! - `chat_with_messages()`：raw 转发，不参与记忆 —— 给已经自己管历史的调用方留个口子

use std::pin::Pin;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use futures_util::StreamExt;
use tracing::info;

use crate::agent::memory::{InMemoryStore, MemoryStore, Message, Role, DEFAULT_WINDOW};
use crate::agent::prompts::AgentPrompts;
use crate::client::error::ClientError;
use crate::client::llm::{ArcLlm, ChatMessage, LlmClient};
use crate::events::LlmEvent;

/// 这里的 `BoxStream` 就是 `crate::client::llm::BoxStream`，但 trait 返回类型要求写在 impl 里。
type BoxStream<T> = Pin<Box<dyn futures_util::Stream<Item = T> + Send>>;

pub struct LlmAgent {
    llm: ArcLlm,
    store: Arc<dyn MemoryStore>,
    prompts: Arc<AgentPrompts>,
}

impl LlmAgent {
    /// 默认后端 = [`InMemoryStore`]，窗口 = 20，提示词 = `prompts.yaml` 编译期嵌入
    pub fn new(llm: ArcLlm) -> Self {
        Self::with_store(llm, Arc::new(InMemoryStore::new(DEFAULT_WINDOW)))
    }

    /// 自定义窗口的默认后端
    pub fn with_window(llm: ArcLlm, window_size: usize) -> Self {
        Self::with_store(llm, Arc::new(InMemoryStore::new(window_size)))
    }

    /// 通用入口：注入任意 store（InMemory / Redis / 自定义 mock）。
    /// 提示词走默认 yaml —— 想换提示词用 [`Self::with_prompts`]。
    pub fn with_store(llm: ArcLlm, store: Arc<dyn MemoryStore>) -> Self {
        Self {
            llm,
            store,
            prompts: crate::agent::prompts::default_prompts(),
        }
    }

    /// 完整注入：自定义 store + 自定义提示词模板（测试 / 灰度用）。
    pub fn with_prompts(
        llm: ArcLlm,
        store: Arc<dyn MemoryStore>,
        prompts: Arc<AgentPrompts>,
    ) -> Self {
        Self { llm, store, prompts }
    }

    /// 主动清空某会话的短期记忆（外部触发，如 session 结束 / 用户主动重置）
    pub async fn clear_memory(&self, session_id: &str) {
        self.store.clear(session_id).await;
    }

    /// 查询某会话当前记忆条数（用于调试 / 监控）
    pub async fn memory_len(&self, session_id: &str) -> usize {
        self.store.len(session_id).await
    }

    pub fn store(&self) -> &Arc<dyn MemoryStore> {
        &self.store
    }

    pub fn prompts(&self) -> &Arc<AgentPrompts> {
        &self.prompts
    }
}

#[async_trait]
impl LlmClient for LlmAgent {
    /// 主入口：拼 static system + emotion system + 历史 + 当前 user prompt → 调底层 LLM → 流收尾后写记忆。
    async fn chat(
        &self,
        session_id: &str,
        prompt: &str,
        emotion_hint: Option<&str>,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
        // ===== 1. 构造 messages：static system → emotion system → 历史 → 当前 user =====
        let history: Vec<Message> = self.store.history(session_id).await;

        let mut messages: Vec<ChatMessage> = Vec::with_capacity(3 + history.len());
        // 1a. 静态提示词（每次 chat 都注入；role + guidelines 拼成一条 system message）
        if let Some(static_msg) = self.prompts.static_system_message() {
            messages.push(ChatMessage {
                role: "system".to_string(),
                content: static_msg,
            });
        }
        // 1b. 动态提示词（ASR 参考信号非空时插入）
        if let Some(emotion_text) = emotion_hint.and_then(|e| self.prompts.render_emotion_hint(e)) {
            messages.push(ChatMessage {
                role: "system".to_string(),
                content: emotion_text,
            });
        }
        // 1c. 历史
        for h in &history {
            messages.push(ChatMessage {
                role: h.role.as_str().to_string(),
                content: h.content.clone(),
            });
        }
        // 1d. 当前 user prompt
        let prompt_owned = prompt.to_string();
        let history_len = history.len();
        messages.push(ChatMessage {
            role: "user".to_string(),
            content: prompt_owned.clone(),
        });

        info!(
            target: "voice_server.agent",
            session_id,
            history_len,
            has_asr_hint = emotion_hint.is_some(),
            has_static_prompts = self.prompts.static_system_message().is_some(),
            total_messages = messages.len(),
            prompt_chars = prompt_owned.chars().count(),
            "Agent 拼装 messages，调底层 LLM"
        );

        // ===== 2. 调底层 =====
        let inner = self.llm.chat_with_messages(session_id, &messages).await?;

        // ===== 3. 包装流：第一次 yield 前写 user；is_final 时写完整 assistant =====
        let store = self.store.clone();
        let session_id_owned = session_id.to_string();
        let prompt_for_stream = prompt_owned;

        let wrapped = try_stream! {
            let mut assistant_buf = String::new();
            let mut recorded_user = false;
            let mut stream = Box::pin(inner);
            while let Some(evt_res) = stream.next().await {
                let evt = evt_res?;
                if !recorded_user {
                    store.append(&session_id_owned, Message {
                        role: Role::User,
                        content: prompt_for_stream.clone(),
                    }).await;
                    recorded_user = true;
                }
                assistant_buf.push_str(&evt.delta);
                let is_final = evt.is_final;
                yield evt;
                if is_final {
                    if !assistant_buf.is_empty() {
                        let assistant_text = std::mem::take(&mut assistant_buf);
                        store.append(&session_id_owned, Message {
                            role: Role::Assistant,
                            content: assistant_text,
                        }).await;
                    }
                    let mem_len = store.len(&session_id_owned).await;
                    info!(
                        target: "voice_server.agent",
                        session_id = %session_id_owned,
                        memory_len = mem_len,
                        "Agent 已写入 user+assistant 到短期记忆"
                    );
                }
            }
        };

        Ok(Box::pin(wrapped))
    }

    /// Raw 转发：不参与记忆。给"自己管历史的调用方"留的逃生口。
    /// 等价于 `self.llm.chat_with_messages(...)`。
    async fn chat_with_messages(
        &self,
        session_id: &str,
        messages: &[ChatMessage],
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
        self.llm.chat_with_messages(session_id, messages).await
    }
}
