//! LlmAgent: 在 [`LlmClient`] 上封装短期记忆 + 多轮对话
//!
//! ## 设计要点
//! - 每次 `chat()` 时从 [`MemoryStore`] 拉 session 的最近 N 条历史，拼到 messages 头部送上游
//! - 流收尾时（`is_final = true`）把 `user prompt` + 完整 `assistant 回复` 写入 store
//! - 每轮按当前用户输入长度选择 fast / strong client
//! - System Prompt 是底层 client 的内部属性，Agent 不感知
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
use std::time::Instant;

use async_stream::try_stream;
use async_trait::async_trait;
use futures_util::StreamExt;
use tracing::info;

use crate::agent::memory::{InMemoryStore, MemoryStore, Message, Role, DEFAULT_WINDOW};
use crate::agent::router::ModelRouter;
use crate::client::error::ClientError;
use crate::client::llm::{ArcLlm, ChatMessage, LlmClient, ModelTier};
use crate::events::LlmEvent;
use crate::metrics::{EscalationReason, NoopMetricsSink, VoiceMetricsSink};

/// 这里的 `BoxStream` 就是 `crate::client::llm::BoxStream`，但 trait 返回类型要求写在 impl 里。
type BoxStream<T> = Pin<Box<dyn futures_util::Stream<Item = T> + Send>>;

pub struct LlmAgent {
    fast_llm: ArcLlm,
    strong_llm: ArcLlm,
    store: Arc<dyn MemoryStore>,
    router: ModelRouter,
    metrics: Arc<dyn VoiceMetricsSink>,
}

impl LlmAgent {
    /// 默认后端 = [`InMemoryStore`]，窗口 = 20；兼容单模型调用方。
    pub fn new(llm: ArcLlm) -> Self {
        Self::with_store(llm, Arc::new(InMemoryStore::new(DEFAULT_WINDOW)))
    }

    /// 自定义窗口的默认后端
    pub fn with_window(llm: ArcLlm, window_size: usize) -> Self {
        Self::with_store(llm, Arc::new(InMemoryStore::new(window_size)))
    }

    /// 通用入口：注入任意 store（InMemory / Redis / 自定义 mock）。
    /// 兼容单模型调用方：fast / strong 都指向同一个 client。
    pub fn with_store(llm: ArcLlm, store: Arc<dyn MemoryStore>) -> Self {
        Self {
            fast_llm: llm.clone(),
            strong_llm: llm,
            store,
            router: ModelRouter,
            metrics: Arc::new(NoopMetricsSink),
        }
    }

    pub fn with_models(fast_llm: ArcLlm, strong_llm: ArcLlm, store: Arc<dyn MemoryStore>) -> Self {
        Self::with_models_and_metrics(fast_llm, strong_llm, store, Arc::new(NoopMetricsSink))
    }

    pub fn with_models_and_metrics(
        fast_llm: ArcLlm,
        strong_llm: ArcLlm,
        store: Arc<dyn MemoryStore>,
        metrics: Arc<dyn VoiceMetricsSink>,
    ) -> Self {
        Self {
            fast_llm,
            strong_llm,
            store,
            router: ModelRouter,
            metrics,
        }
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

    fn client_for(&self, tier: ModelTier) -> ArcLlm {
        match tier {
            ModelTier::Fast => self.fast_llm.clone(),
            ModelTier::Strong => self.strong_llm.clone(),
        }
    }
}

fn escalation_reason_for(error: &ClientError) -> EscalationReason {
    match error {
        ClientError::Http(message)
            if message.to_ascii_lowercase().contains("timeout")
                || message.to_ascii_lowercase().contains("timed out") =>
        {
            EscalationReason::Timeout
        }
        _ => EscalationReason::ProviderError,
    }
}

fn empty_response_error() -> ClientError {
    ClientError::Decode("LLM returned an empty response".into())
}

#[async_trait]
impl LlmClient for LlmAgent {
    /// 主入口：读取历史、选择模型、流收尾后写记忆。
    async fn chat(
        &self,
        session_id: &str,
        user_input: &str,
        emotion_hint: Option<&str>,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
        let route_started_at = Instant::now();
        let tier = self.router.route(user_input);
        self.metrics
            .observe_llm_route(tier, route_started_at.elapsed());
        let history: Vec<Message> = self.store.history(session_id).await;
        let mut messages: Vec<ChatMessage> = Vec::with_capacity(1 + history.len());
        for h in &history {
            messages.push(ChatMessage {
                role: h.role.as_str().to_string(),
                content: h.content.clone(),
            });
        }
        let user_input_owned = user_input.to_string();
        let history_len = history.len();
        messages.push(ChatMessage {
            role: "user".to_string(),
            content: user_input_owned.clone(),
        });
        info!(
            target: "voice_server.agent",
            session_id,
            route = tier.as_str(),
            history_len,
            has_asr_hint = emotion_hint.is_some(),
            total_messages = messages.len(),
            input_chars = user_input_owned.trim().chars().count(),
            "Agent 选择模型并拼装历史消息"
        );

        let store = self.store.clone();
        let fast_llm = self.fast_llm.clone();
        let strong_llm = self.strong_llm.clone();
        let metrics = self.metrics.clone();
        let session_id_owned = session_id.to_string();
        let user_input_for_stream = user_input_owned;
        let emotion_hint_owned = emotion_hint.map(str::to_owned);

        let wrapped = try_stream! {
            let mut assistant_buf = String::new();
            let mut recorded_user = false;
            let mut current_tier = tier;
            let mut current_client = match current_tier {
                ModelTier::Fast => fast_llm.clone(),
                ModelTier::Strong => strong_llm.clone(),
            };

            loop {
                let attempt = current_client
                    .chat_with_messages(
                        &session_id_owned,
                        &messages,
                        emotion_hint_owned.as_deref(),
                    )
                    .await;
                let mut stream = match attempt {
                    Ok(stream) => Box::pin(stream),
                    Err(error) if current_tier == ModelTier::Fast => {
                        let reason = escalation_reason_for(&error);
                        metrics.llm_escalated(reason);
                        info!(
                            target: "voice_server.agent",
                            session_id = %session_id_owned,
                            from = "fast",
                            to = "strong",
                            ?reason,
                            "LLM fast 请求在输出前失败，升级 strong"
                        );
                        current_tier = ModelTier::Strong;
                        current_client = strong_llm.clone();
                        continue;
                    }
                    Err(error) => Err(error)?,
                };

                let mut visible = false;
                let mut escalation = None;
                while let Some(event_result) = stream.next().await {
                    let event = match event_result {
                        Ok(event) => event,
                        Err(error) if current_tier == ModelTier::Fast && !visible => {
                            escalation = Some(escalation_reason_for(&error));
                            break;
                        }
                        Err(error) => Err(error)?,
                    };

                    if !event.delta.is_empty() {
                        if !recorded_user {
                            store.append(&session_id_owned, Message {
                                role: Role::User,
                                content: user_input_for_stream.clone(),
                            }).await;
                            recorded_user = true;
                        }
                        visible = true;
                        assistant_buf.push_str(&event.delta);
                    }

                    if event.is_final {
                        if !visible {
                            if current_tier == ModelTier::Fast {
                                escalation = Some(EscalationReason::EmptyResponse);
                                break;
                            }
                            Err(empty_response_error())?;
                        }

                        let assistant_text = std::mem::take(&mut assistant_buf);
                        store.append(&session_id_owned, Message {
                            role: Role::Assistant,
                            content: assistant_text,
                        }).await;
                        let mem_len = store.len(&session_id_owned).await;
                        info!(
                            target: "voice_server.agent",
                            session_id = %session_id_owned,
                            memory_len = mem_len,
                            "Agent 已写入 user+assistant 到短期记忆"
                        );
                        yield event;
                        return;
                    }

                    if visible {
                        yield event;
                    }
                }

                if let Some(reason) = escalation {
                    metrics.llm_escalated(reason);
                    info!(
                        target: "voice_server.agent",
                        session_id = %session_id_owned,
                        from = "fast",
                        to = "strong",
                        ?reason,
                        "LLM fast 流在输出前失败，升级 strong"
                    );
                    current_tier = ModelTier::Strong;
                    current_client = strong_llm.clone();
                    continue;
                }

                if visible {
                    return;
                }

                if current_tier == ModelTier::Fast {
                    let reason = EscalationReason::EmptyResponse;
                    metrics.llm_escalated(reason);
                    current_tier = ModelTier::Strong;
                    current_client = strong_llm.clone();
                    continue;
                }

                Err(empty_response_error())?;
            }
        };

        Ok(Box::pin(wrapped))
    }

    /// Raw 转发：不参与记忆。给"自己管历史的调用方"留的逃生口。
    /// 根据最后一条 user message 路由后，转发给选中的底层 client。
    async fn chat_with_messages(
        &self,
        session_id: &str,
        messages: &[ChatMessage],
        emotion_hint: Option<&str>,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
        let route_started_at = Instant::now();
        let tier = messages
            .iter()
            .rev()
            .find(|message| message.role == "user")
            .map(|message| self.router.route(&message.content))
            .unwrap_or(ModelTier::Fast);
        self.metrics
            .observe_llm_route(tier, route_started_at.elapsed());
        self.client_for(tier)
            .chat_with_messages(session_id, messages, emotion_hint)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{stream, StreamExt};
    use std::sync::Mutex;

    #[derive(Clone, Debug)]
    struct RecordedCall {
        messages: Vec<ChatMessage>,
        emotion_hint: Option<String>,
    }

    #[derive(Clone)]
    enum Behavior {
        Respond(String),
        EmptyFinal,
        EmptyStream,
        StartError,
        StreamError,
        TextThenError(String),
    }

    struct RecordingLlm {
        behavior: Behavior,
        calls: Mutex<Vec<RecordedCall>>,
    }

    impl RecordingLlm {
        fn responding(response: &str) -> Self {
            Self {
                behavior: Behavior::Respond(response.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn with_behavior(behavior: Behavior) -> Self {
            Self {
                behavior,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<RecordedCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl LlmClient for RecordingLlm {
        async fn chat(
            &self,
            session_id: &str,
            user_input: &str,
            emotion_hint: Option<&str>,
        ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
            self.chat_with_messages(
                session_id,
                &[ChatMessage {
                    role: "user".into(),
                    content: user_input.into(),
                }],
                emotion_hint,
            )
            .await
        }

        async fn chat_with_messages(
            &self,
            _session_id: &str,
            messages: &[ChatMessage],
            emotion_hint: Option<&str>,
        ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
            self.calls.lock().unwrap().push(RecordedCall {
                messages: messages.to_vec(),
                emotion_hint: emotion_hint.map(str::to_owned),
            });
            let events = match &self.behavior {
                Behavior::Respond(response) => vec![Ok(LlmEvent {
                    delta: response.clone(),
                    is_final: true,
                })],
                Behavior::EmptyFinal => vec![Ok(LlmEvent {
                    delta: String::new(),
                    is_final: true,
                })],
                Behavior::EmptyStream => Vec::new(),
                Behavior::StartError => {
                    return Err(ClientError::Http("provider failed".into()));
                }
                Behavior::StreamError => vec![Err(ClientError::Http("stream failed".into()))],
                Behavior::TextThenError(text) => vec![
                    Ok(LlmEvent {
                        delta: text.clone(),
                        is_final: false,
                    }),
                    Err(ClientError::Http("stream failed".into())),
                ],
            };
            Ok(Box::pin(stream::iter(events)))
        }
    }

    async fn collect_text(
        mut stream: BoxStream<Result<LlmEvent, ClientError>>,
    ) -> Result<String, ClientError> {
        let mut text = String::new();
        while let Some(event) = stream.next().await {
            text.push_str(&event?.delta);
        }
        Ok(text)
    }

    #[tokio::test]
    async fn one_agent_routes_each_turn_and_shares_history_without_system_messages() {
        let fast = Arc::new(RecordingLlm::responding("fast-answer"));
        let strong = Arc::new(RecordingLlm::responding("strong-answer"));
        let store = Arc::new(InMemoryStore::new(DEFAULT_WINDOW));
        let agent = LlmAgent::with_models(fast.clone(), strong.clone(), store);

        let first = agent.chat("s", "你好", Some("开心")).await.unwrap();
        assert_eq!(collect_text(first).await.unwrap(), "fast-answer");
        let second = agent
            .chat("s", "帮我比较两个方案并给出详细执行计划", Some("认真"))
            .await
            .unwrap();
        assert_eq!(collect_text(second).await.unwrap(), "strong-answer");

        let fast_calls = fast.calls();
        assert_eq!(fast_calls.len(), 1);
        assert_eq!(fast_calls[0].emotion_hint.as_deref(), Some("开心"));
        assert!(fast_calls[0]
            .messages
            .iter()
            .all(|message| message.role != "system"));

        let strong_calls = strong.calls();
        assert_eq!(strong_calls.len(), 1);
        assert_eq!(strong_calls[0].emotion_hint.as_deref(), Some("认真"));
        assert_eq!(
            strong_calls[0]
                .messages
                .iter()
                .map(|message| (message.role.as_str(), message.content.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("user", "你好"),
                ("assistant", "fast-answer"),
                ("user", "帮我比较两个方案并给出详细执行计划"),
            ]
        );
    }

    #[tokio::test]
    async fn fast_empty_response_retries_strong_before_yielding() {
        let fast = Arc::new(RecordingLlm::with_behavior(Behavior::EmptyFinal));
        let strong = Arc::new(RecordingLlm::responding("我来处理。"));
        let store = Arc::new(InMemoryStore::new(DEFAULT_WINDOW));
        let metrics = Arc::new(crate::metrics::VoiceMetrics::new());
        let agent =
            LlmAgent::with_models_and_metrics(fast.clone(), strong.clone(), store, metrics.clone());

        let result = agent.chat("s", "你好", None).await.unwrap();

        assert_eq!(collect_text(result).await.unwrap(), "我来处理。");
        assert_eq!(fast.calls().len(), 1);
        assert_eq!(strong.calls().len(), 1);
        assert_eq!(agent.memory_len("s").await, 2);
        assert!(metrics.render().contains(
            "voice_llm_escalation_total{from=\"fast\",reason=\"empty_response\",to=\"strong\"} 1"
        ));
    }

    #[tokio::test]
    async fn fast_start_and_first_stream_errors_retry_strong_once() {
        for behavior in [Behavior::StartError, Behavior::StreamError] {
            let fast = Arc::new(RecordingLlm::with_behavior(behavior));
            let strong = Arc::new(RecordingLlm::responding("strong"));
            let agent = LlmAgent::with_models(
                fast.clone(),
                strong.clone(),
                Arc::new(InMemoryStore::new(DEFAULT_WINDOW)),
            );

            let result = agent.chat("s", "你好", None).await.unwrap();

            assert_eq!(collect_text(result).await.unwrap(), "strong");
            assert_eq!(fast.calls().len(), 1);
            assert_eq!(strong.calls().len(), 1);
        }
    }

    #[tokio::test]
    async fn fast_empty_stream_retries_strong_once() {
        let fast = Arc::new(RecordingLlm::with_behavior(Behavior::EmptyStream));
        let strong = Arc::new(RecordingLlm::responding("strong"));
        let agent = LlmAgent::with_models(
            fast.clone(),
            strong.clone(),
            Arc::new(InMemoryStore::new(DEFAULT_WINDOW)),
        );

        let result = agent.chat("s", "你好", None).await.unwrap();

        assert_eq!(collect_text(result).await.unwrap(), "strong");
        assert_eq!(strong.calls().len(), 1);
    }

    #[tokio::test]
    async fn does_not_retry_after_fast_yields_visible_text() {
        let fast = Arc::new(RecordingLlm::with_behavior(Behavior::TextThenError(
            "部分回答".into(),
        )));
        let strong = Arc::new(RecordingLlm::responding("重复回答"));
        let agent = LlmAgent::with_models(
            fast,
            strong.clone(),
            Arc::new(InMemoryStore::new(DEFAULT_WINDOW)),
        );

        let result = agent.chat("s", "你好", None).await.unwrap();
        let error = collect_text(result).await.unwrap_err();

        assert!(matches!(error, ClientError::Http(_)));
        assert_eq!(strong.calls().len(), 0);
        assert_eq!(agent.memory_len("s").await, 1);
    }
}
