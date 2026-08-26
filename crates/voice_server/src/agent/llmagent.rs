//! LlmAgent: 在 [`LlmClient`] 上封装短期记忆 + 多轮对话
//!
//! ## 设计要点
//! - 每次 `chat()` 时从 [`MemoryStore`] 拉 session 的最近 N 条历史，拼到 messages 头部送上游
//! - 流收尾时（`is_final = true`）把 `user prompt` + 完整 `assistant 回复` 写入 store
//! - emotion_hint 作为 system message 放在 messages 最前面
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
use crate::client::error::ClientError;
use crate::client::llm::{ArcLlm, ChatMessage, LlmClient};
use crate::session::LlmEvent;

/// 这里的 `BoxStream` 就是 `crate::client::llm::BoxStream`，但 trait 返回类型要求写在 impl 里。
type BoxStream<T> = Pin<Box<dyn futures_util::Stream<Item = T> + Send>>;

pub struct LlmAgent {
    llm: ArcLlm,
    store: Arc<dyn MemoryStore>,
}

impl LlmAgent {
    /// 默认后端 = [`InMemoryStore`]，窗口 = 20
    pub fn new(llm: ArcLlm) -> Self {
        Self::with_store(llm, Arc::new(InMemoryStore::new(DEFAULT_WINDOW)))
    }

    /// 自定义窗口的默认后端
    pub fn with_window(llm: ArcLlm, window_size: usize) -> Self {
        Self::with_store(llm, Arc::new(InMemoryStore::new(window_size)))
    }

    /// 通用入口：注入任意 store（InMemory / Redis / 自定义 mock）
    pub fn with_store(llm: ArcLlm, store: Arc<dyn MemoryStore>) -> Self {
        Self { llm, store }
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
}

#[async_trait]
impl LlmClient for LlmAgent {
    /// 主入口：拼 emotion system + 历史 + 当前 user prompt → 调底层 LLM → 流收尾后写记忆。
    async fn chat(
        &self,
        session_id: &str,
        prompt: &str,
        emotion_hint: Option<&str>,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
        // ===== 1. 构造 messages：emotion system → 历史 → 当前 user =====
        let history: Vec<Message> = self.store.history(session_id).await;

        let mut messages: Vec<ChatMessage> = Vec::with_capacity(2 + history.len());
        if let Some(e) = emotion_hint {
            messages.push(ChatMessage {
                role: "system".to_string(),
                content: format!(
                    "[情绪参考] 请据此调整回复的语气和措辞，但**不要在回复中提及、复述或暗示用户当前的情绪**，\
                     也不要解释你是如何判断的；直接、自然地回答用户的问题即可。\
                     （用户当前说话的情绪可能是：{e}，仅供你参考，可能不准确）"
                ),
            });
        }
        for h in &history {
            messages.push(ChatMessage {
                role: h.role.as_str().to_string(),
                content: h.content.clone(),
            });
        }
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
            has_emotion_hint = emotion_hint.is_some(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_stream::stream;

    /// mock LlmClient：返回一个固定回复 + is_final 的流（不入网）
    struct MockLlm {
        reply: String,
    }

    #[async_trait]
    impl LlmClient for MockLlm {
        async fn chat(
            &self,
            _session_id: &str,
            _prompt: &str,
            _emotion_hint: Option<&str>,
        ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
            self.chat_with_messages(_session_id, &[]).await
        }
        async fn chat_with_messages(
            &self,
            _session_id: &str,
            _messages: &[ChatMessage],
        ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
            let reply = self.reply.clone();
            let s = stream! {
                yield Ok::<LlmEvent, ClientError>(LlmEvent { delta: reply, is_final: true });
            };
            Ok(Box::pin(s) as Pin<Box<dyn futures_util::Stream<Item = Result<LlmEvent, ClientError>> + Send>>)
        }
    }

    /// 捕获 messages 的 mock —— 验证 agent 把历史拼对了
    struct CaptureLlm {
        captured: tokio::sync::Mutex<Vec<Vec<ChatMessage>>>,
    }
    #[async_trait]
    impl LlmClient for CaptureLlm {
        async fn chat(
            &self,
            _session_id: &str,
            _prompt: &str,
            _emotion_hint: Option<&str>,
        ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
            self.chat_with_messages(_session_id, &[]).await
        }
        async fn chat_with_messages(
            &self,
            _session_id: &str,
            messages: &[ChatMessage],
        ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
            self.captured.lock().await.push(messages.to_vec());
            let s = stream! {
                yield Ok::<LlmEvent, ClientError>(LlmEvent { delta: "ok".to_string(), is_final: true });
            };
            Ok(Box::pin(s) as Pin<Box<dyn futures_util::Stream<Item = Result<LlmEvent, ClientError>> + Send>>)
        }
    }

    #[tokio::test]
    async fn chat_records_user_and_assistant_to_memory() {
        let mock: ArcLlm = Arc::new(MockLlm {
            reply: "你好！".to_string(),
        });
        let agent = LlmAgent::new(mock);

        let mut s = agent
            .chat("sid-1", "今天天气不错", None)
            .await
            .unwrap();
        while let Some(_) = s.next().await {}

        assert_eq!(agent.memory_len("sid-1").await, 2);
        let _ = s; // silence unused
    }

    #[tokio::test]
    async fn second_chat_includes_history_in_messages() {
        let cap = Arc::new(CaptureLlm {
            captured: tokio::sync::Mutex::new(Vec::new()),
        });
        let agent = LlmAgent::new(cap.clone());

        let mut s1 = agent.chat("sid-2", "first", None).await.unwrap();
        while let Some(_) = s1.next().await {}

        let mut s2 = agent.chat("sid-2", "second", None).await.unwrap();
        while let Some(_) = s2.next().await {}

        let captured = cap.captured.lock().await;
        assert_eq!(captured.len(), 2);
        let msgs = &captured[1];
        assert!(msgs.len() >= 3);
        assert_eq!(msgs[0].role, "user");
        assert_eq!(msgs[0].content, "first");
        assert_eq!(msgs[1].role, "assistant");
        assert_eq!(msgs[2].role, "user");
        assert_eq!(msgs[2].content, "second");
    }

    #[tokio::test]
    async fn emotion_hint_inserted_before_history() {
        let cap = Arc::new(CaptureLlm {
            captured: tokio::sync::Mutex::new(Vec::new()),
        });
        let agent = LlmAgent::new(cap.clone());
        let mut s = agent
            .chat("sid-3", "你说啥", Some("开心"))
            .await
            .unwrap();
        while let Some(_) = s.next().await {}

        let captured = cap.captured.lock().await;
        let msgs = &captured[0];
        assert_eq!(msgs[0].role, "system");
        assert!(msgs[0].content.contains("开心"));
        assert_eq!(msgs[1].role, "user");
        assert_eq!(msgs[1].content, "你说啥");
    }

    #[tokio::test]
    async fn clear_memory_empties_session() {
        let mock: ArcLlm = Arc::new(MockLlm {
            reply: "ok".to_string(),
        });
        let agent = LlmAgent::new(mock);
        let mut s = agent.chat("sid-4", "x", None).await.unwrap();
        while let Some(_) = s.next().await {}
        assert_eq!(agent.memory_len("sid-4").await, 2);
        agent.clear_memory("sid-4").await;
        assert_eq!(agent.memory_len("sid-4").await, 0);
    }

    #[tokio::test]
    async fn custom_store_is_used() {
        // 注入一个自定义 store，确认 agent 走 store 而不是内部 DashMap
        let store: Arc<dyn MemoryStore> = Arc::new(InMemoryStore::new(5));
        let mock: ArcLlm = Arc::new(MockLlm {
            reply: "hello".to_string(),
        });
        let agent = LlmAgent::with_store(mock, store.clone());

        let mut s = agent.chat("custom", "x", None).await.unwrap();
        while let Some(_) = s.next().await {}

        assert_eq!(store.len("custom").await, 2);
    }
}