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
use reqwest::{Client, Method, RequestBuilder};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::sync::Arc;
use tracing::{info, warn};

use crate::client::apply_trace_header;
use crate::client::error::{parse_openai_error, ClientError};
use crate::config::{LlmConfig, ProviderConfig};
use crate::events::LlmEvent;

pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;
pub type ArcLlm = Arc<dyn LlmClient>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelTier {
    Fast,
    Strong,
}

impl ModelTier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fast => "fast",
            Self::Strong => "strong",
        }
    }
}

/// 单条 chat 消息（OpenAI-compat 协议）。`role` 取 `"system"` / `"user"` / `"assistant"`。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[async_trait]
pub trait LlmClient: Send + Sync {
    /// 便利方法：单轮对话。内部构造 user message 后转给 [`chat_with_messages`]。
    async fn chat(
        &self,
        session_id: &str,
        prompt: &str,
        emotion_hint: Option<&str>,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError>;

    /// 原始入口：调用方预构造历史和当前 user。具体 client 负责注入其固定 System Prompt。
    async fn chat_with_messages(
        &self,
        session_id: &str,
        messages: &[ChatMessage],
        emotion_hint: Option<&str>,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError>;
}

pub struct HttpLlmClient {
    system_prompt_template: Option<Arc<str>>,
    api_base: String,
    api_key: String,
    model: String,
    max_completion_tokens: Option<u32>,
    temperature: Option<f64>,
    top_p: Option<f64>,
    top_k: Option<u32>,
    reasoning_effort: Option<String>,
    include_usage: bool,
    headers: reqwest::header::HeaderMap,
    http: Client,
}

impl HttpLlmClient {
    pub fn new(resolved: ProviderConfig, model: String) -> Self {
        Self::from_config(
            resolved,
            &LlmConfig {
                model,
                ..LlmConfig::default()
            },
        )
    }

    pub fn from_config(resolved: ProviderConfig, cfg: &LlmConfig) -> Self {
        Self::from_config_with_prompt(resolved, cfg, None)
    }

    pub fn from_config_with_prompt(
        resolved: ProviderConfig,
        cfg: &LlmConfig,
        system_prompt_template: Option<Arc<str>>,
    ) -> Self {
        let api_base = resolved.api_base.clone();
        let api_key = resolved.api_key.clone();
        let headers = resolved.to_header_map();
        let http = Client::builder()
            // `timeout` is a total deadline and would terminate a healthy long-lived
            // SSE response while downstream TTS is still processing earlier text.
            // Keep the same limit for connection and stalled reads instead.
            .connect_timeout(resolved.timeout())
            .read_timeout(resolved.timeout())
            .build()
            .expect("reqwest client builder");
        Self {
            system_prompt_template,
            api_base,
            api_key,
            model: cfg.model.clone(),
            max_completion_tokens: cfg.max_completion_tokens,
            temperature: cfg.temperature,
            top_p: cfg.top_p,
            top_k: cfg.top_k,
            reasoning_effort: cfg.reasoning_effort.clone(),
            include_usage: cfg.include_usage,
            headers,
            http,
        }
    }

    fn messages_with_system_prompt(
        &self,
        messages: &[ChatMessage],
        emotion_hint: Option<&str>,
    ) -> Vec<ChatMessage> {
        let Some(template) = &self.system_prompt_template else {
            return messages.to_vec();
        };
        let emotion = emotion_hint
            .filter(|value| !value.is_empty())
            .unwrap_or("无");
        let mut rendered = Vec::with_capacity(messages.len() + 1);
        rendered.push(ChatMessage {
            role: "system".into(),
            content: template.replace("{emotion}", emotion),
        });
        rendered.extend_from_slice(messages);
        rendered
    }

    fn request_body<'a>(&'a self, messages: &'a [ChatMessage]) -> ChatReq<'a> {
        ChatReq {
            model: &self.model,
            messages,
            stream: true,
            max_completion_tokens: self.max_completion_tokens,
            temperature: self.temperature,
            top_p: self.top_p,
            top_k: self.top_k,
            reasoning_effort: self.reasoning_effort.as_deref(),
            stream_options: self.include_usage.then_some(ChatStreamOptions {
                include_usage: true,
            }),
        }
    }
}

fn apply_auth_headers(
    mut req: RequestBuilder,
    api_key: &str,
    headers: &reqwest::header::HeaderMap,
) -> RequestBuilder {
    for (name, value) in headers {
        req = req.header(name, value);
    }
    // `headers.x-api-key` is authoritative. Keep `api_key` as a compatibility
    // fallback for deployments that have not moved the value into headers yet.
    if !headers.contains_key("x-api-key") && !api_key.is_empty() {
        let key = api_key
            .strip_prefix("Bearer ")
            .or_else(|| api_key.strip_prefix("bearer "))
            .unwrap_or(api_key);
        req = req.header("x-api-key", key);
    }
    req
}

#[derive(Serialize)]
struct ChatReq<'a> {
    model: &'a str,
    messages: &'a [ChatMessage],
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_completion_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_k: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_effort: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_options: Option<ChatStreamOptions>,
}

#[derive(Serialize)]
struct ChatStreamOptions {
    include_usage: bool,
}

#[derive(Debug, Deserialize)]
struct ChatChunk {
    #[serde(default)]
    #[allow(dead_code)]
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[allow(dead_code)]
    created: Option<u64>,
    #[serde(default)]
    #[allow(dead_code)]
    model: String,
    #[serde(default)]
    choices: Vec<ChatChoice>,
    #[serde(default)]
    usage: Option<ChatUsage>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    #[serde(default)]
    #[allow(dead_code)]
    index: Option<u64>,
    #[serde(default)]
    delta: ChatDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ChatDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    refusal: Option<String>,
    #[serde(default)]
    reasoning_content: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ChatUsage {
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
}

#[derive(Debug)]
struct ParsedChatOutput {
    delta: String,
    reasoning_content: Option<String>,
    is_final: bool,
    usage: Option<ChatUsage>,
}

#[derive(Debug, Deserialize)]
struct ChatResponse {
    choices: Vec<ChatResponseChoice>,
    #[serde(default)]
    usage: Option<ChatUsage>,
}

#[derive(Debug, Deserialize)]
struct ChatResponseChoice {
    message: ChatResponseMessage,
}

#[derive(Debug, Deserialize)]
struct ChatResponseMessage {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    refusal: Option<String>,
    #[serde(default)]
    reasoning_content: Option<String>,
}

fn response_text(content: Option<String>, refusal: Option<String>) -> String {
    match (content, refusal) {
        (Some(content), Some(refusal)) if !content.is_empty() && !refusal.is_empty() => {
            format!("{content}\n{refusal}")
        }
        (Some(content), _) if !content.is_empty() => content,
        (_, Some(refusal)) => refusal,
        _ => String::new(),
    }
}

fn non_blank_text(value: Option<String>) -> Option<String> {
    value.filter(|text| !text.trim().is_empty())
}

const THINK_OPEN_TAG: &str = "<think>";
const THINK_CLOSE_TAG: &str = "</think>";

#[derive(Debug, Default)]
struct ThinkTagFilter {
    pending: String,
    in_think: bool,
}

impl ThinkTagFilter {
    fn push(&mut self, delta: &str) -> String {
        self.pending.push_str(delta);
        let mut visible = String::new();

        loop {
            let tag = if self.in_think {
                THINK_CLOSE_TAG
            } else {
                THINK_OPEN_TAG
            };
            if let Some(position) = find_ascii_case_insensitive(&self.pending, tag) {
                if !self.in_think {
                    visible.push_str(&self.pending[..position]);
                }
                self.pending.drain(..position + tag.len());
                self.in_think = !self.in_think;
                continue;
            }

            let retained = matching_tag_prefix_suffix_len(&self.pending, tag);
            let consumed = self.pending.len() - retained;
            if !self.in_think {
                visible.push_str(&self.pending[..consumed]);
            }
            self.pending.drain(..consumed);
            break;
        }

        visible
    }

    fn finish(&mut self) -> String {
        if self.in_think {
            self.pending.clear();
            return String::new();
        }
        std::mem::take(&mut self.pending)
    }
}

fn find_ascii_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    haystack.to_ascii_lowercase().find(needle)
}

fn matching_tag_prefix_suffix_len(value: &str, tag: &str) -> usize {
    (1..tag.len())
        .rev()
        .find(|&length| {
            value
                .get(value.len().saturating_sub(length)..)
                .is_some_and(|suffix| suffix.eq_ignore_ascii_case(&tag[..length]))
        })
        .unwrap_or(0)
}

fn parse_stream_payload(payload: &[u8]) -> Result<Option<ParsedChatOutput>, ClientError> {
    if payload.is_empty() || payload == b"[DONE]" {
        return Ok(None);
    }

    let chunk: ChatChunk =
        serde_json::from_slice(payload).map_err(|e| ClientError::Decode(e.to_string()))?;
    let usage = chunk.usage;
    if let Some(choice) = chunk.choices.into_iter().next() {
        let delta = choice.delta;
        return Ok(Some(ParsedChatOutput {
            delta: response_text(delta.content, delta.refusal),
            reasoning_content: non_blank_text(delta.reasoning_content),
            is_final: choice.finish_reason.is_some(),
            usage,
        }));
    }

    Ok(usage.map(|usage| ParsedChatOutput {
        delta: String::new(),
        reasoning_content: None,
        is_final: false,
        usage: Some(usage),
    }))
}

fn parse_sse_data_line(line: &[u8]) -> Result<Option<ParsedChatOutput>, ClientError> {
    let Some(payload) = line.strip_prefix(b"data:") else {
        return Ok(None);
    };
    parse_stream_payload(payload.strip_prefix(b" ").unwrap_or(payload))
}

fn parse_non_stream_response(body: &[u8]) -> Result<ParsedChatOutput, ClientError> {
    let response: ChatResponse =
        serde_json::from_slice(body).map_err(|e| ClientError::Decode(e.to_string()))?;
    let choice = response
        .choices
        .into_iter()
        .next()
        .ok_or_else(|| ClientError::Decode("no choices in response".into()))?;

    let message = choice.message;
    Ok(ParsedChatOutput {
        delta: response_text(message.content, message.refusal),
        reasoning_content: non_blank_text(message.reasoning_content),
        is_final: true,
        usage: response.usage,
    })
}

#[async_trait]
impl LlmClient for HttpLlmClient {
    /// 便利方法：单轮对话。构造 user message 后转给 [`chat_with_messages`]。
    async fn chat(
        &self,
        session_id: &str,
        prompt: &str,
        emotion_hint: Option<&str>,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
        let messages = vec![ChatMessage {
            role: "user".to_string(),
            content: prompt.to_string(),
        }];

        self.chat_with_messages(session_id, &messages, emotion_hint)
            .await
    }

    /// 原始入口：调用方预构造消息数组，直接发给上游 LLM。
    /// 内部负责：拼请求体 → 发 HTTP → 解析 SSE → 返回 stream。
    async fn chat_with_messages(
        &self,
        session_id: &str,
        messages: &[ChatMessage],
        emotion_hint: Option<&str>,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
        let url = format!("{}/chat/completions", self.api_base.trim_end_matches('/'));

        let messages = self.messages_with_system_prompt(messages, emotion_hint);
        let body = self.request_body(&messages);

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

        let req = self
            .http
            .request(Method::POST, &url)
            .header("Content-Type", "application/json")
            .json(&body);
        let req = apply_trace_header(apply_auth_headers(req, &self.api_key, &self.headers));

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
                let mut think_filter = ThinkTagFilter::default();
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
                        if line.starts_with(b"data:") {
                            match parse_sse_data_line(&line) {
                                Ok(Some(parsed)) => {
                                    if let Some(usage) = &parsed.usage {
                                        info!(
                                            target: "voice_server.llm",
                                            session_id = %session,
                                            prompt_tokens = usage.prompt_tokens,
                                            completion_tokens = usage.completion_tokens,
                                            total_tokens = usage.total_tokens,
                                            "收到 LLM token usage"
                                        );
                                    }
                                    if let Some(reasoning_content) = &parsed.reasoning_content {
                                        info!(
                                            target: "voice_server.llm",
                                            session_id = %session,
                                            reasoning_content = %reasoning_content,
                                            "收到 LLM reasoning_content"
                                        );
                                    }
                                    let mut delta = think_filter.push(&parsed.delta);
                                    let is_final = parsed.is_final;
                                    if is_final {
                                        delta.push_str(&think_filter.finish());
                                    }
                                    if !delta.is_empty() || is_final {
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
                                        yield Ok(LlmEvent { delta, is_final });
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    tracing::warn!(
                                        target: "voice_server.llm.err",
                                        session_id = %session,
                                        error = %e,
                                        payload = %String::from_utf8_lossy(&line),
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
            let parsed = parse_non_stream_response(&bytes)?;
            if let Some(usage) = &parsed.usage {
                info!(
                    target: "voice_server.llm",
                    session_id,
                    prompt_tokens = usage.prompt_tokens,
                    completion_tokens = usage.completion_tokens,
                    total_tokens = usage.total_tokens,
                    "收到 LLM token usage"
                );
            }
            if let Some(reasoning_content) = &parsed.reasoning_content {
                info!(
                    target: "voice_server.llm",
                    session_id,
                    reasoning_content = %reasoning_content,
                    "收到 LLM reasoning_content"
                );
            }
            let delta = parsed.delta;
            let is_final = parsed.is_final;
            let mut think_filter = ThinkTagFilter::default();
            let mut delta = think_filter.push(&delta);
            if is_final {
                delta.push_str(&think_filter.finish());
            }
            Box::pin(stream! {
                yield Ok(LlmEvent { delta, is_final });
            })
        };

        Ok(stream)
    }
}

pub fn build_llm_client(
    cfg: &LlmConfig,
    provider: Option<&ProviderConfig>,
) -> anyhow::Result<Arc<dyn LlmClient>> {
    build_llm_client_inner(cfg, provider, None)
}

pub fn build_llm_client_with_prompt(
    cfg: &LlmConfig,
    provider: Option<&ProviderConfig>,
    system_prompt_template: Arc<str>,
) -> anyhow::Result<Arc<dyn LlmClient>> {
    build_llm_client_inner(cfg, provider, Some(system_prompt_template))
}

fn build_llm_client_inner(
    cfg: &LlmConfig,
    provider: Option<&ProviderConfig>,
    system_prompt_template: Option<Arc<str>>,
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

    Ok(Arc::new(HttpLlmClient::from_config_with_prompt(
        resolved,
        cfg,
        system_prompt_template,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn think_filter_removes_inline_reasoning_from_visible_content() {
        let mut filter = ThinkTagFilter::default();

        let mut visible = filter.push("<think>内部推理</think>最终答案");
        visible.push_str(&filter.finish());

        assert_eq!(visible, "最终答案");
    }

    #[test]
    fn think_filter_handles_tags_split_across_stream_deltas() {
        let mut filter = ThinkTagFilter::default();
        let mut visible = String::new();

        for delta in ["<thi", "nk>内部", "推理</th", "ink>答", "案"] {
            visible.push_str(&filter.push(delta));
        }
        visible.push_str(&filter.finish());

        assert_eq!(visible, "答案");
    }

    #[test]
    fn think_filter_handles_case_insensitive_split_tags() {
        let mut filter = ThinkTagFilter::default();
        let mut visible = String::new();

        for delta in ["<THI", "NK>内部", "</TH", "INK>答案"] {
            visible.push_str(&filter.push(delta));
        }
        visible.push_str(&filter.finish());

        assert_eq!(visible, "答案");
    }

    #[test]
    fn think_filter_drops_unclosed_reasoning_at_stream_end() {
        let mut filter = ThinkTagFilter::default();

        let mut visible = filter.push("开场<think>尚未结束的内部推理");
        visible.push_str(&filter.finish());

        assert_eq!(visible, "开场");
    }

    #[test]
    fn client_prepends_fixed_prompt_and_renders_emotion() {
        let client = HttpLlmClient::from_config_with_prompt(
            ProviderConfig::default(),
            &LlmConfig {
                model: "fast".into(),
                ..LlmConfig::default()
            },
            Some(Arc::<str>::from("固定提示。推断信息：{emotion}")),
        );
        let messages = vec![ChatMessage {
            role: "user".into(),
            content: "你好".into(),
        }];

        let rendered = client.messages_with_system_prompt(&messages, Some("开心"));

        assert_eq!(rendered[0].role, "system");
        assert_eq!(rendered[0].content, "固定提示。推断信息：开心");
        assert_eq!(rendered[1], messages[0]);
    }

    #[test]
    fn client_renders_missing_emotion_as_none() {
        let client = HttpLlmClient::from_config_with_prompt(
            ProviderConfig::default(),
            &LlmConfig {
                model: "fast".into(),
                ..LlmConfig::default()
            },
            Some(Arc::<str>::from("推断信息：{emotion}")),
        );

        let rendered = client.messages_with_system_prompt(&[], None);

        assert_eq!(rendered[0].content, "推断信息：无");
    }

    #[test]
    fn client_without_prompt_preserves_caller_messages() {
        let client = HttpLlmClient::from_config(ProviderConfig::default(), &LlmConfig::default());
        let messages = vec![ChatMessage {
            role: "user".into(),
            content: "你好".into(),
        }];

        let rendered = client.messages_with_system_prompt(&messages, Some("开心"));

        assert_eq!(rendered, messages);
    }

    #[test]
    fn parses_standard_stream_content_and_finish_reason() {
        let payload = br#"{
            "id":"chatcmpl-1",
            "object":"chat.completion.chunk",
            "created":1,
            "model":"test-model",
            "choices":[{
                "index":0,
                "delta":{"content":"hello"},
                "logprobs":null,
                "finish_reason":"stop"
            }]
        }"#;

        let parsed = parse_stream_payload(payload).unwrap().unwrap();
        assert_eq!(parsed.delta, "hello");
        assert!(parsed.is_final);
        assert!(parsed.usage.is_none());
    }

    #[test]
    fn parses_openai_sse_data_line() {
        let line = br#"data: {"id":"chatcmpl-sse","object":"chat.completion.chunk","created":1,"model":"test-model","choices":[{"index":0,"delta":{"content":"chunk"},"logprobs":null,"finish_reason":null}]}"#;

        let parsed = parse_sse_data_line(line).unwrap().unwrap();

        assert_eq!(parsed.delta, "chunk");
        assert!(!parsed.is_final);
    }

    #[test]
    fn preserves_stream_reasoning_content_for_logging() {
        let payload = br#"{
            "id":"chatcmpl-reasoning-stream",
            "object":"chat.completion.chunk",
            "created":1,
            "model":"test-model",
            "choices":[{
                "index":0,
                "delta":{"content":null,"reasoning_content":"checking the answer"},
                "finish_reason":null
            }]
        }"#;

        let parsed = parse_stream_payload(payload).unwrap().unwrap();
        assert_eq!(
            parsed.reasoning_content.as_deref(),
            Some("checking the answer")
        );
        assert!(parsed.delta.is_empty());
    }

    #[test]
    fn drops_blank_stream_reasoning_content() {
        let payload = br#"{
            "choices":[{
                "delta":{"reasoning_content":"  \n  "},
                "finish_reason":null
            }]
        }"#;

        let parsed = parse_stream_payload(payload).unwrap().unwrap();
        assert!(parsed.reasoning_content.is_none());
    }

    #[test]
    fn parses_stream_refusal_as_visible_text() {
        let payload = br#"{
            "id":"chatcmpl-2",
            "object":"chat.completion.chunk",
            "created":1,
            "model":"test-model",
            "choices":[{
                "index":0,
                "delta":{"content":"","refusal":"cannot comply"},
                "logprobs":null,
                "finish_reason":"content_filter"
            }]
        }"#;

        let parsed = parse_stream_payload(payload).unwrap().unwrap();
        assert_eq!(parsed.delta, "cannot comply");
        assert!(parsed.is_final);
    }

    #[test]
    fn preserves_stream_content_and_refusal_when_both_are_present() {
        let payload = br#"{
            "id":"chatcmpl-2b",
            "object":"chat.completion.chunk",
            "created":1,
            "model":"test-model",
            "choices":[{
                "index":0,
                "delta":{"content":"partial answer","refusal":"cannot continue"},
                "logprobs":null,
                "finish_reason":"content_filter"
            }]
        }"#;

        let parsed = parse_stream_payload(payload).unwrap().unwrap();
        assert_eq!(parsed.delta, "partial answer\ncannot continue");
        assert!(parsed.is_final);
    }

    #[test]
    fn parses_usage_only_stream_chunk_without_a_choice() {
        let payload = br#"{
            "id":"chatcmpl-3",
            "object":"chat.completion.chunk",
            "created":1,
            "model":"test-model",
            "choices":[],
            "usage":{"prompt_tokens":12,"completion_tokens":4,"total_tokens":16}
        }"#;

        let parsed = parse_stream_payload(payload).unwrap().unwrap();
        assert_eq!(parsed.delta, "");
        assert!(!parsed.is_final);
        let usage = parsed.usage.unwrap();
        assert_eq!(usage.prompt_tokens, 12);
        assert_eq!(usage.completion_tokens, 4);
        assert_eq!(usage.total_tokens, 16);
    }

    #[test]
    fn parses_standard_non_stream_response_message_content() {
        let body = br#"{
            "id":"chatcmpl-4",
            "object":"chat.completion",
            "created":1,
            "model":"test-model",
            "choices":[{
                "index":0,
                "message":{"role":"assistant","content":"complete answer","refusal":null},
                "logprobs":null,
                "finish_reason":"stop"
            }],
            "usage":{"prompt_tokens":8,"completion_tokens":2,"total_tokens":10}
        }"#;

        let parsed = parse_non_stream_response(body).unwrap();
        assert_eq!(parsed.delta, "complete answer");
        assert!(parsed.is_final);
        assert_eq!(parsed.usage.unwrap().total_tokens, 10);
    }

    #[test]
    fn preserves_non_stream_reasoning_content_for_logging() {
        let body = br#"{
            "choices":[{
                "message":{
                    "role":"assistant",
                    "content":"complete answer",
                    "reasoning_content":"checking the answer"
                },
                "finish_reason":"stop"
            }]
        }"#;

        let parsed = parse_non_stream_response(body).unwrap();
        assert_eq!(
            parsed.reasoning_content.as_deref(),
            Some("checking the answer")
        );
        assert_eq!(parsed.delta, "complete answer");
    }

    #[test]
    fn parses_non_stream_refusal_when_content_is_empty() {
        let body = br#"{
            "choices":[{
                "message":{"role":"assistant","content":"","refusal":"cannot comply"},
                "finish_reason":"content_filter"
            }]
        }"#;

        let parsed = parse_non_stream_response(body).unwrap();
        assert_eq!(parsed.delta, "cannot comply");
        assert!(parsed.is_final);
    }

    #[test]
    fn preserves_non_stream_content_and_refusal_when_both_are_present() {
        let body = br#"{
            "choices":[{
                "message":{
                    "role":"assistant",
                    "content":"partial answer",
                    "refusal":"cannot continue"
                },
                "finish_reason":"content_filter"
            }]
        }"#;

        let parsed = parse_non_stream_response(body).unwrap();
        assert_eq!(parsed.delta, "partial answer\ncannot continue");
        assert!(parsed.is_final);
    }

    #[test]
    fn request_body_serializes_configured_openai_options() {
        let cfg = LlmConfig {
            model: "test-model".into(),
            max_completion_tokens: Some(256),
            temperature: Some(0.2),
            top_p: Some(0.85),
            top_k: Some(20),
            reasoning_effort: Some("low".into()),
            include_usage: true,
            ..LlmConfig::default()
        };
        let client = HttpLlmClient::from_config(ProviderConfig::default(), &cfg);
        let messages = vec![ChatMessage {
            role: "user".into(),
            content: "hello".into(),
        }];

        let body = serde_json::to_value(client.request_body(&messages)).unwrap();

        assert_eq!(
            body,
            serde_json::json!({
                "model": "test-model",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": true,
                "max_completion_tokens": 256,
                "temperature": 0.2,
                "top_p": 0.85,
                "top_k": 20,
                "reasoning_effort": "low",
                "stream_options": {"include_usage": true}
            })
        );
    }

    #[test]
    fn request_body_omits_unconfigured_optional_fields() {
        let cfg = LlmConfig {
            model: "test-model".into(),
            ..LlmConfig::default()
        };
        let client = HttpLlmClient::new(ProviderConfig::default(), cfg.model.clone());
        let messages = vec![ChatMessage {
            role: "user".into(),
            content: "hello".into(),
        }];

        let body = serde_json::to_value(client.request_body(&messages)).unwrap();

        assert_eq!(
            body,
            serde_json::json!({
                "model": "test-model",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": true
            })
        );
    }

    #[test]
    fn llm_auth_uses_x_api_key_without_authorization() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-api-key", "x-key".parse().unwrap());
        let request = apply_auth_headers(
            Client::new().post("http://localhost/chat/completions"),
            "legacy-key",
            &headers,
        )
        .build()
        .unwrap();

        assert_eq!(request.headers().get("x-api-key").unwrap(), "x-key");
        assert!(request.headers().get("authorization").is_none());
    }

    #[test]
    fn llm_api_key_falls_back_to_x_api_key() {
        let request = apply_auth_headers(
            Client::new().post("http://localhost/chat/completions"),
            "Bearer legacy-key",
            &reqwest::header::HeaderMap::new(),
        )
        .build()
        .unwrap();

        assert_eq!(request.headers().get("x-api-key").unwrap(), "legacy-key");
        assert!(request.headers().get("authorization").is_none());
    }
}
