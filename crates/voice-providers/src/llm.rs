//! LLM wrapper
//!
//! 百炼 `/compatible-mode/v1` 是 OpenAI-compat，但 voice-server 不依赖 voice-providers，
//! 所以这里用 reqwest 手搓一个最小的 OpenAI-compat SSE 流式 LLM client。
//!
//! 注：与 voice-server 的 HttpLlmClient（基于 async-openai）行为等价：JSON 请求 + SSE 响应，
//! 吐出 `LlmEvent { delta, is_final }`。voice-server PR1 不需要触碰。

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::asr::{BoxStream, ClientError};

// ===== 公共类型 =====

#[derive(Debug, Clone)]
pub struct LlmEvent {
    pub delta: String,
    pub is_final: bool,
}

pub type ArcLlm = Arc<dyn LlmClient>;

// ===== Client trait =====

#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn chat(
        &self,
        session_id: &str,
        prompt: &str,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError>;
}

// ===== HttpLlmClient =====

#[derive(Clone)]
pub struct HttpLlmClient {
    api_base: String,
    api_key: String,
    model: String,
    timeout: Duration,
}

impl HttpLlmClient {
    pub fn new(api_base: String, api_key: String, model: String, timeout: Duration) -> Self {
        Self {
            api_base,
            api_key,
            model,
            timeout,
        }
    }
}

#[derive(Serialize)]
struct ChatReq<'a> {
    model: &'a str,
    messages: Vec<ChatMsg<'a>>,
    stream: bool,
}

#[derive(Serialize)]
struct ChatMsg<'a> {
    role: &'a str,
    content: &'a str,
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
    async fn chat(
        &self,
        session_id: &str,
        prompt: &str,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
        let url = format!("{}/chat/completions", self.api_base.trim_end_matches('/'));
        let body = ChatReq {
            model: &self.model,
            messages: vec![ChatMsg { role: "user", content: prompt }],
            stream: true,
        };
        info!(
            target: "voice_providers.llm",
            session_id,
            url = %url,
            model = %self.model,
            prompt_chars = prompt.chars().count(),
            "LLM POST 请求即将发送（voice-providers 直连）"
        );

        // 手搓 reqwest 客户端（与 voice-server::HttpLlmClient 同语义）
        let client = reqwest::Client::builder()
            .timeout(self.timeout)
            .build()
            .map_err(|e| ClientError::Http(e.to_string()))?;
        let mut req = client
            .request(reqwest::Method::POST, &url)
            .header("Content-Type", "application/json")
            .json(&body);
        if !self.api_key.is_empty() {
            let key = self.api_key.strip_prefix("Bearer ")
                .or_else(|| self.api_key.strip_prefix("bearer "))
                .unwrap_or(&self.api_key);
            req = req.header("Authorization", format!("Bearer {}", key));
        }

        let resp = req.send().await.map_err(|e| ClientError::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(ClientError::Status(status.as_u16()));
        }

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
                while let Some(chunk_res) = byte_stream.next().await {
                    let chunk = match chunk_res {
                        Ok(c) => c,
                        Err(e) => { yield Err(ClientError::Http(e.to_string())); return; }
                    };
                    sse_buf.extend_from_slice(&chunk);
                    while let Some(pos) = sse_buf.iter().position(|&b| b == b'\n') {
                        let line: Vec<u8> = sse_buf.drain(..=pos).collect();
                        let line = &line[..line.len() - 1];
                        let line = if line.last() == Some(&b'\r') { &line[..line.len()-1] } else { line };
                        if line.starts_with(b"data: ") {
                            let payload = &line[6..];
                            if payload == b"[DONE]" || payload.is_empty() { continue; }
                            let parsed: serde_json::Result<ChatChunk> = serde_json::from_slice(payload);
                            if let Ok(c) = parsed {
                                if let Some(choice) = c.choices.into_iter().next() {
                                    let delta = choice.delta.content.unwrap_or_default();
                                    let is_final = choice.finish_reason.is_some();
                                    if !delta.is_empty() || is_final {
                                        debug!(
                                            target: "voice_providers.llm",
                                            session_id = %session,
                                            delta_len = delta.chars().count(),
                                            is_final,
                                            "LLM delta"
                                        );
                                        yield Ok(LlmEvent { delta, is_final });
                                    }
                                }
                            }
                        }
                    }
                }
            })
        } else {
            // 非 SSE：尝试单段 JSON
            let bytes: Bytes = resp.bytes().await.map_err(|e| ClientError::Http(e.to_string()))?;
            let parsed: Result<ChatChunk, _> = serde_json::from_slice(&bytes);
            Box::pin(stream! {
                match parsed {
                    Ok(c) => {
                        if let Some(choice) = c.choices.into_iter().next() {
                            let delta = choice.delta.content.unwrap_or_default();
                            let is_final = choice.finish_reason.is_some();
                            yield Ok(LlmEvent { delta, is_final });
                        }
                    }
                    Err(e) => yield Err(ClientError::Decode(e.to_string())),
                }
            })
        };

        Ok(stream)
    }
}

// ===== factory =====

pub fn build_llm_client(
    api_base: String,
    api_key: String,
    model: String,
    timeout: Duration,
) -> ArcLlm {
    Arc::new(HttpLlmClient::new(api_base, api_key, model, timeout))
}