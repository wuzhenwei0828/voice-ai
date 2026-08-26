//! LLM 客户端：手搓 reqwest 调 OpenAI-compat SSE 聊天接口
//!
//! 之所以不用 async-openai：`async-openai::chat().create_stream()` 内部把
//! `reqwest_eventsource::EventSource`（持有 `reqwest::Response`）放进独立
//! `tokio::spawn` 任务，再用 mpsc 把事件转发给消费者。当上游 pipeline 被
//! `CancellationToken` 取消时，外层 stream 被 drop，但 spawned 任务要等下一次
//! 事件 send 失败后才会调用 `event_source.close()` —— **HTTP 连接不会立刻关闭**，
//! LLM 服务端会继续生成到自然结束。auto-interrupt 就表现为"打断不起作用"。
//!
//! 这里直接 `await reqwest::Response::bytes_stream()` 解析 SSE，stream 被 drop
//! 时立刻 abort HTTP 连接，cancel 才真正生效。
//!
//! Wire format（OpenAI-compat）：JSON `{model, messages, stream: true}` + SSE 响应

use async_stream::stream;
use async_trait::async_trait;
use futures_util::{Stream, StreamExt};
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::sync::Arc;
use tracing::{info, warn};

use crate::client::error::{parse_openai_error, ClientError};
use crate::config::{LlmConfig, ProviderConfig};
use crate::session::LlmEvent;

pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;
pub type ArcLlm = Arc<dyn LlmClient>;

/// 单条 chat 消息（OpenAI-compat 协议）。`role` 取 `"system"` / `"user"` / `"assistant"`。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[async_trait]
pub trait LlmClient: Send + Sync {
    /// 便利方法：单轮对话。`emotion_hint` 会作为 system message 拼到 prompt 前面。
    /// 内部会构造 `[emotion_system?, user]` 两/一条消息后转给 [`chat_with_messages`]。
    async fn chat(
        &self,
        session_id: &str,
        prompt: &str,
        emotion_hint: Option<&str>,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError>;

    /// 原始入口：调用方（典型场景：agent 多轮对话）预构造消息数组（含历史 + system + 当前 user），
    /// 直接发给上游 LLM；本方法不做 emotion system 等封装。
    async fn chat_with_messages(
        &self,
        session_id: &str,
        messages: &[ChatMessage],
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError>;
}

pub struct HttpLlmClient {
    api_base: String,
    api_key: String,
    model: String,
    headers: reqwest::header::HeaderMap,
    http: Client,
}

impl HttpLlmClient {
    pub fn new(resolved: ProviderConfig, model: String) -> Self {
        let api_base = resolved.api_base.clone();
        let api_key = resolved.api_key.clone();
        let headers = resolved.to_header_map();
        let http = Client::builder()
            .timeout(resolved.timeout())
            .build()
            .expect("reqwest client builder");
        Self {
            api_base,
            api_key,
            model,
            headers,
            http,
        }
    }
}

#[derive(Serialize)]
struct ChatReq<'a> {
    model: &'a str,
    messages: &'a [ChatMessage],
    stream: bool,
}

#[derive(Debug, Deserialize)]
struct ChatChunk {
    #[serde(default)]
    #[allow(dead_code)]
    id: String,
    #[serde(default)]
    #[allow(dead_code)]
    model: String,
    #[serde(default)]
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    #[serde(default)]
    delta: ChatDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ChatDelta {
    #[serde(default)]
    content: Option<String>,
}

#[async_trait]
impl LlmClient for HttpLlmClient {
    /// 便利方法：单轮对话。构造 emotion system + user 两条消息后转给 [`chat_with_messages`]。
    async fn chat(
        &self,
        session_id: &str,
        prompt: &str,
        emotion_hint: Option<&str>,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
        // 情绪提示：仅用于调整回复的语气/措辞，禁止 LLM 主动告诉用户情绪是什么
        let mut messages: Vec<ChatMessage> = Vec::with_capacity(2);
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
        messages.push(ChatMessage {
            role: "user".to_string(),
            content: prompt.to_string(),
        });

        self.chat_with_messages(session_id, &messages).await
    }

    /// 原始入口：调用方预构造消息数组，直接发给上游 LLM。
    /// 内部负责：拼请求体 → 发 HTTP → 解析 SSE → 返回 stream。
    async fn chat_with_messages(
        &self,
        session_id: &str,
        messages: &[ChatMessage],
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
        let url = format!(
            "{}/chat/completions",
            self.api_base.trim_end_matches('/')
        );

        let body = ChatReq {
            model: &self.model,
            messages,
            stream: true,
        };

        let messages_count = messages.len();
        let has_system = messages.iter().any(|m| m.role == "system");
        // 找最后一条 user message 作为 prompt 预览
        let user_prompt = messages
            .iter()
            .rev()
            .find(|m| m.role == "user")
            .map(|m| m.content.as_str())
            .unwrap_or("");
        let prompt_preview: String = user_prompt.chars().take(500).collect();
        info!(
            target: "voice_server.llm",
            session_id,
            method = "POST",
            url = %url,
            model = %self.model,
            stream = true,
            messages_count,
            has_system,
            prompt_chars = user_prompt.chars().count(),
            prompt_preview = %prompt_preview,
            "LLM 请求即将发送"
        );

        // ===== 请求体 JSON：方便对照实际发出的 payload =====
        match serde_json::to_string(&body) {
            Ok(s) => info!(
                target: "voice_server.llm.req",
                session_id,
                body = %s,
                "LLM 请求体"
            ),
            Err(e) => tracing::warn!(
                target: "voice_server.llm.req",
                session_id,
                "LLM 请求体序列化失败: {}",
                e
            ),
        }

        let mut req = self
            .http
            .request(Method::POST, &url)
            .header("Content-Type", "application/json")
            .json(&body);
        // 透传 provider/llm 自定义 headers
        for (k, v) in self.headers.iter() {
            req = req.header(k, v);
        }
        if !self.api_key.is_empty() {
            let key = self
                .api_key
                .strip_prefix("Bearer ")
                .or_else(|| self.api_key.strip_prefix("bearer "))
                .unwrap_or(&self.api_key);
            req = req.header("Authorization", format!("Bearer {}", key));
        }

        let resp = req
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let status_u16 = status.as_u16();
            // 抓 body 一次：log + 尝试解析 OpenAI 信封
            let body = match resp.text().await {
                Ok(t) => t,
                Err(e) => {
                    warn!(
                        target: "voice_server.llm.err",
                        session_id,
                        status = status_u16,
                        error = %e,
                        "LLM 非 2xx body 读取失败"
                    );
                    return Err(ClientError::Status(status_u16));
                }
            };
            let body_preview: String = if body.chars().count() > 2048 {
                let s: String = body.chars().take(2048).collect();
                format!("{}…<truncated, total {} chars>", s, body.chars().count())
            } else {
                body.clone()
            };
            warn!(
                target: "voice_server.llm.err",
                session_id,
                status = status_u16,
                body = %body_preview,
                "LLM 返回非 2xx"
            );
            if let Some(api_err) = parse_openai_error(&body) {
                return Err(ClientError::Api {
                    status: status_u16,
                    error: api_err,
                });
            }
            return Err(ClientError::Status(status_u16));
        }
        info!(
            target: "voice_server.llm",
            session_id,
            "LLM 连接建立，stream 已开始"
        );

        let ct = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let is_sse = ct.contains("text/event-stream");

        let session = session_id.to_string();
        let stream: Pin<Box<dyn Stream<Item = Result<LlmEvent, ClientError>> + Send>> = if is_sse {
            let mut sse_buf: Vec<u8> = Vec::new();
            let mut byte_stream = Box::pin(resp.bytes_stream());
            Box::pin(stream! {
                let mut chunk_count: u32 = 0;
                let mut total_delta_chars: usize = 0;
                while let Some(chunk_res) = byte_stream.next().await {
                    let chunk = match chunk_res {
                        Ok(c) => c,
                        Err(e) => { yield Err(ClientError::Http(e.to_string())); return; }
                    };
                    sse_buf.extend_from_slice(&chunk);
                    // 按行切分（SSE 事件以 \n 分隔）；同时支持 \r\n
                    while let Some(pos) = sse_buf.iter().position(|&b| b == b'\n') {
                        let mut line: Vec<u8> = sse_buf.drain(..=pos).collect();
                        // 去掉换行符
                        line.pop();
                        if line.last() == Some(&b'\r') { line.pop(); }
                        if line.starts_with(b"data: ") {
                            let payload = &line[6..];
                            if payload == b"[DONE]" || payload.is_empty() { continue; }
                            match serde_json::from_slice::<ChatChunk>(payload) {
                                Ok(c) => {
                                    if let Some(choice) = c.choices.into_iter().next() {
                                        let delta = choice.delta.content.unwrap_or_default();
                                        let is_final = choice.finish_reason.is_some();
                                        chunk_count += 1;
                                        total_delta_chars += delta.chars().count();
                                        info!(
                                            target: "voice_server.llm",
                                            session_id = %session,
                                            seq = chunk_count,
                                            delta_len = delta.chars().count(),
                                            is_final,
                                            delta = %delta,
                                            "收到 LLM delta"
                                        );
                                        if !delta.is_empty() || is_final {
                                            yield Ok(LlmEvent { delta, is_final });
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        target: "voice_server.llm.err",
                                        session_id = %session,
                                        error = %e,
                                        payload = %String::from_utf8_lossy(payload),
                                        "LLM SSE 解析失败"
                                    );
                                    yield Err(ClientError::Decode(e.to_string()));
                                    return;
                                }
                            }
                        }
                        // 其他 SSE 字段（event: / id: / retry: / 空行）忽略
                    }
                }
                info!(
                    target: "voice_server.llm",
                    session_id = %session,
                    chunk_count,
                    total_delta_chars,
                    "LLM 流结束"
                );
            })
        } else {
            // 非 SSE：尝试单段 JSON
            let bytes = resp
                .bytes()
                .await
                .map_err(|e| ClientError::Http(e.to_string()))?;
            match serde_json::from_slice::<ChatChunk>(&bytes) {
                Ok(c) => {
                    if let Some(choice) = c.choices.into_iter().next() {
                        let delta = choice.delta.content.unwrap_or_default();
                        let is_final = choice.finish_reason.is_some();
                        Box::pin(stream! {
                            yield Ok(LlmEvent { delta, is_final });
                        })
                    } else {
                        return Err(ClientError::Decode("no choices in response".into()));
                    }
                }
                Err(e) => return Err(ClientError::Decode(e.to_string())),
            }
        };

        Ok(stream)
    }
}

pub fn build_llm_client(
    cfg: &LlmConfig,
    provider: Option<&ProviderConfig>,
) -> anyhow::Result<Arc<dyn LlmClient>> {
    let resolved = cfg.resolved(provider);
    let timeout_ms = resolved.timeout_ms;

    tracing::info!(
        target: "voice_server.factory",
        kind = "http",
        api_base = %resolved.api_base,
        model = %cfg.model,
        timeout_ms,
        "构造 HttpLlmClient（手搓 reqwest，drop stream 立即关闭 HTTP 连接）"
    );

    Ok(Arc::new(HttpLlmClient::new(resolved, cfg.model.clone())))
}
