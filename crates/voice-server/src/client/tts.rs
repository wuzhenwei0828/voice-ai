//! TTS 客户端：手搓 reqwest（不走 async-openai）
//!
//! ## 为什么不用 async-openai
//! SDK 的 `Voice` 是 enum（Alloy / Echo / Fable / Onyx / Nova / Shimmer / Sage / Verse），
//! 不支持 siliconflow 的 `"fnlp/MOSS-TTSD-v0.5:alex"` 之类自定义 voice 字符串；
//! SDK 的 `post_raw` 是 `pub(crate)` 没法绕过。手搓以传任意 voice 字符串。
//!
//! ## Wire format
//! 请求：JSON `{input, model, voice, response_format, stream?}`（OpenAI-compat）
//! 响应：可能是
//!   - SSE（`content-type: text/event-stream`），每条 `data: {"data":"<base64>","finish_reason":null|"stop"}`
//!   - 单段二进制音频（`content-type: audio/...`），siliconflow 当前即使发 `stream: true` 也走这种
//! 按 content-type 自动分支。

use async_trait::async_trait;
use base64::Engine;
use futures_util::StreamExt;
use reqwest::header::HeaderMap;
use serde::Deserialize;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::client::error::ClientError;
use crate::config::{ProviderConfig, TtsConfig};
use crate::session::TtsEvent;

pub type BoxStream<T> = Pin<Box<dyn futures_util::Stream<Item = T> + Send>>;
pub type ArcTts = Arc<dyn TtsClient>;

#[async_trait]
pub trait TtsClient: Send + Sync {
    async fn synthesize(
        &self,
        session_id: &str,
        text: &str,
    ) -> Result<BoxStream<Result<TtsEvent, ClientError>>, ClientError>;
}

pub struct HttpTtsClient {
    base_url: String,
    path: String,
    api_key: Option<String>, // None = 不发 Authorization
    model: String,
    voice: String,
    response_format: String,
    stream: bool,
    extra_headers: HeaderMap,
    timeout: Duration,
}

impl HttpTtsClient {
    pub fn new(
        base_url: String,
        path: String,
        api_key: Option<String>,
        model: String,
        voice: String,
        response_format: String,
        stream: bool,
        extra_headers: HeaderMap,
        timeout: Duration,
    ) -> Self {
        Self { base_url, path, api_key, model, voice, response_format, stream, extra_headers, timeout }
    }
}

#[derive(Deserialize)]
struct TtsStreamChunk {
    #[serde(default)]
    data: Option<String>,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[async_trait]
impl TtsClient for HttpTtsClient {
    async fn synthesize(
        &self,
        session_id: &str,
        text: &str,
    ) -> Result<BoxStream<Result<TtsEvent, ClientError>>, ClientError> {
        let url = if self.path.is_empty() {
            self.base_url.clone()
        } else {
            format!("{}{}", self.base_url, self.path)
        };

        let client = reqwest::Client::builder()
            .timeout(self.timeout)
            .build()
            .map_err(|e| ClientError::Http(e.to_string()))?;

        #[derive(serde::Serialize)]
        struct Req<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            input: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            model: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            voice: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none", rename = "response_format")]
            response_format: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            stream: Option<bool>,
        }
        let body = Req {
            input: Some(text),
            model: if self.model.is_empty() { None } else { Some(self.model.clone()) },
            voice: if self.voice.is_empty() { None } else { Some(self.voice.clone()) },
            response_format: if self.response_format.is_empty() {
                None
            } else {
                Some(self.response_format.clone())
            },
            stream: if self.stream { Some(true) } else { None },
        };
        let mut req = client
            .request(reqwest::Method::POST, &url)
            .header("x-session-id", session_id)
            .json(&body);
        if let Some(key) = &self.api_key {
            // 如果用户已经在 key 里写了 "Bearer xxx"，原样发；
            // 否则 SDK 习惯是只发 token，由服务端加 Bearer —— 我们这里也加
            if key.starts_with("Bearer ") || key.starts_with("bearer ") {
                req = req.header("Authorization", key);
            } else {
                req = req.header("Authorization", format!("Bearer {}", key));
            }
        }
        for (name, value) in &self.extra_headers {
            req = req.header(name, value);
        }

        // ===== 请求体 JSON：方便对照实际发出的 payload =====
        match serde_json::to_string(&body) {
            Ok(body_json) => info!(
                target: "voice_server.tts.req",
                session_id,
                body = %body_json,
                "TTS 请求体"
            ),
            Err(e) => warn!(
                target: "voice_server.tts.req",
                session_id,
                "TTS 请求体序列化失败: {}",
                e
            ),
        }
        info!(
            target: "voice_server.tts",
            session_id,
            method = "POST",
            url = %url,
            text_chars = text.chars().count(),
            text_preview_len = text.chars().take(200).count(),
            text_preview = %text.chars().take(200).collect::<String>(),
            model = %self.model,
            voice = %self.voice,
            response_format = %self.response_format,
            stream = self.stream,
            api_key_present = self.api_key.is_some(),
            api_key_len = self.api_key.as_deref().map(|k| k.len()).unwrap_or(0),
            extra_headers_count = self.extra_headers.len(),
            timeout_ms = self.timeout.as_millis() as u64,
            "TTS POST 请求即将发送"
        );

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                let status = e.status().map(|s| s.as_u16()).unwrap_or(0);
                warn!(
                    target: "voice_server.tts.err",
                    session_id,
                    url = %url,
                    method = "POST",
                    status,
                    is_timeout = e.is_timeout(),
                    is_connect = e.is_connect(),
                    is_request = e.is_request(),
                    is_body = e.is_body(),
                    is_decode = e.is_decode(),
                    error = %e,
                    "TTS 请求发送失败（连接/传输层）"
                );
                return Err(ClientError::Http(e.to_string()));
            }
        };

        let status = resp.status();
        if !status.is_success() {
            let status_u16 = status.as_u16();
            // 先抓 headers（在 resp.text() 消费 resp 之前）
            let content_type = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            let request_id = resp
                .headers()
                .get("x-request-id")
                .or_else(|| resp.headers().get("x-trace-id"))
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            let headers_dump: String = resp
                .headers()
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}: {}",
                        k,
                        v.to_str().unwrap_or("<binary>")
                    )
                })
                .collect::<Vec<_>>()
                .join(" | ");
            // 抓 body 预览（截断到 2KB，避免日志爆炸）
            let body_preview = match resp.text().await {
                Ok(t) => {
                    let s: String = t.chars().take(2048).collect();
                    if t.chars().count() > 2048 {
                        format!("{}…<truncated, total {} chars>", s, t.chars().count())
                    } else {
                        s
                    }
                }
                Err(e) => format!("<body 读取失败: {}>", e),
            };
            warn!(
                target: "voice_server.tts.err",
                session_id,
                url = %url,
                status = status_u16,
                content_type = %content_type,
                request_id = %request_id,
                headers = %headers_dump,
                body = %body_preview,
                "TTS 返回非 2xx"
            );
            return Err(ClientError::Status(status_u16));
        }

        // 检测 content-type：流式 (text/event-stream) 走 SSE，否则按单段 blob
        let ct = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let is_sse = ct.contains("text/event-stream") || ct.contains("application/x-ndjson");

        let session = session_id.to_string();
        if is_sse {
            // 流式：每条 data: {"data":"<base64>","finish_reason":null|"stop"}
            // 内联迷你 SSE 解析器（仅服务于 TTS，不抽公共模块）
            let mut sse_buf: Vec<u8> = Vec::new();
            let mut byte_stream = Box::pin(resp.bytes_stream());
            let stream = async_stream::stream! {
                let mut seq: u32 = 0;
                while let Some(chunk_res) = byte_stream.next().await {
                    let chunk = match chunk_res {
                        Ok(c) => c,
                        Err(e) => { yield Err(ClientError::Http(e.to_string())); break; }
                    };
                    sse_buf.extend_from_slice(&chunk);
                    while let Some(pos) = sse_buf.iter().position(|&b| b == b'\n') {
                        let line: Vec<u8> = sse_buf.drain(..=pos).collect();
                        let line = &line[..line.len() - 1];
                        let line = if line.last() == Some(&b'\r') { &line[..line.len()-1] } else { line };
                        if line.starts_with(b"data: ") {
                            let payload = &line[6..];
                            if payload == b"[DONE]" || payload.is_empty() { continue; }
                            let parsed: serde_json::Result<TtsStreamChunk> = serde_json::from_slice(payload);
                            if let Ok(chunk) = parsed {
                                let b64 = chunk.data.unwrap_or_default();
                                if b64.is_empty() { continue; }
                                let bytes = base64::engine::general_purpose::STANDARD
                                    .decode(&b64)
                                    .unwrap_or_default();
                                if bytes.is_empty() { continue; }
                                seq += 1;
                                let is_last = chunk.finish_reason.is_some();
                                debug!(
                                    target: "voice_server.tts",
                                    session_id = %session, seq, bytes = bytes.len(), is_last,
                                    "TTS 流式 chunk"
                                );
                                yield Ok(TtsEvent { seq, data: bytes, is_last });
                                if is_last { break; }
                            }
                        }
                    }
                }
            };
            Ok(Box::pin(stream))
        } else {
            // 非流式：单段二进制音频
            let audio_bytes = resp
                .bytes()
                .await
                .map_err(|e| ClientError::Http(e.to_string()))?;
            debug!(
                target: "voice_server.tts",
                session_id,
                bytes = audio_bytes.len(),
                "TTS 收到完整音频"
            );
            let stream = async_stream::stream! {
                yield Ok(TtsEvent { seq: 1, data: audio_bytes.to_vec(), is_last: true });
            };
            Ok(Box::pin(stream))
        }
    }
}

pub fn build_tts_client(
    cfg: &TtsConfig,
    provider: Option<&ProviderConfig>,
) -> anyhow::Result<Arc<dyn TtsClient>> {
    let (resolved, path) = cfg.resolved(provider);
    let base_url = resolved.api_base.clone();
    let api_key = resolved.api_key.clone();
    let timeout = resolved.timeout();
    let headers = resolved.to_header_map();

    tracing::info!(
        target: "voice_server.factory",
        kind = "http",
        base_url = %base_url,
        path = %path,
        model = %cfg.model,
        voice = %cfg.voice,
        response_format = %cfg.response_format,
        "构造 HttpTtsClient"
    );

    Ok(Arc::new(HttpTtsClient::new(
        base_url,
        path,
        Some(api_key),
        cfg.model.clone(),
        cfg.voice.clone(),
        cfg.response_format.clone(),
        cfg.stream,
        headers,
        timeout,
    )))
}
