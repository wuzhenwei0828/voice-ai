//! ASR / LLM / TTS 客户端抽象 + HTTP 实现
//!
//! 真实生产里应该拆成不同 trait + 不同 impl（阿里云/腾讯/OpenAI 等）。
//! MVP 阶段：所有 client 都用 HTTP + chunked response 接 mock 服务或真实服务。
//!
//! HTTP 客户端支持的字段：
//!   - method (POST/GET/PUT)
//!   - model (作为 X-Model header)
//!   - authorization (完整 Authorization 值)
//!   - headers (任意额外 header HashMap)
//!   - timeout_ms
//!   - path (请求路径；空 = 直接用 endpoint 作为完整 URL)

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_stream::try_stream;
use async_trait::async_trait;
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::config::ClientConfig;
use crate::session::{AsrEvent, LlmEvent, TtsEvent};

/// 通用错误
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("http error: {0}")]
    Http(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("service returned status {0}")]
    Status(u16),
}

pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

pub type ArcAsr = Arc<dyn AsrClient>;
pub type ArcLlm = Arc<dyn LlmClient>;
pub type ArcTts = Arc<dyn TtsClient>;

// ====== ASR ======

#[async_trait]
pub trait AsrClient: Send + Sync {
    /// 输入 session_id + 整段 PCM 字节（mock 简化版一次性发送），输出流式识别事件
    async fn recognize(
        &self,
        session_id: &str,
        audio: Vec<u8>,
    ) -> Result<BoxStream<Result<AsrEvent, ClientError>>, ClientError>;
}

pub struct HttpAsrClient {
    pub endpoint: String,
    pub path: String,
    pub method: String,
    pub model: String,
    pub authorization: String,
    pub headers: HashMap<String, String>,
    pub timeout: Duration,
}

impl HttpAsrClient {
    pub fn from_config(cfg: &ClientConfig) -> Self {
        let path = if cfg.path.is_empty() {
            "/recognize".to_string()
        } else {
            cfg.path.clone()
        };
        Self {
            endpoint: cfg.endpoint.clone(),
            path,
            method: cfg.method.clone(),
            model: cfg.model.clone(),
            authorization: cfg.authorization.clone(),
            headers: cfg.headers.clone(),
            timeout: Duration::from_millis(cfg.timeout_ms),
        }
    }
}

#[async_trait]
impl AsrClient for HttpAsrClient {
    async fn recognize(
        &self,
        session_id: &str,
        audio: Vec<u8>,
    ) -> Result<BoxStream<Result<AsrEvent, ClientError>>, ClientError> {
        let url = if self.path.is_empty() {
            self.endpoint.clone()
        } else {
            format!("{}{}", self.endpoint, self.path)
        };

        let method = reqwest::Method::from_bytes(self.method.as_bytes())
            .unwrap_or(reqwest::Method::POST);
        let client = reqwest::Client::builder()
            .timeout(self.timeout)
            .build()
            .map_err(|e| ClientError::Http(e.to_string()))?;
        let mut req = client
            .request(method.clone(), &url)
            .header("x-session-id", session_id);
        if !self.authorization.is_empty() {
            req = req.header("Authorization", &self.authorization);
        }
        if !self.model.is_empty() {
            req = req.header("X-Model", &self.model);
        }
        for (k, v) in &self.headers {
            req = req.header(k.as_str(), v.as_str());
        }

        info!(
            target: "voice_server.asr",
            session_id,
            method = %method,
            url = %url,
            bytes = audio.len(),
            has_auth = !self.authorization.is_empty(),
            model = %self.model,
            "ASR {} 请求 ({} bytes)", method, audio.len()
        );

        let resp = req
            .body(audio)
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            warn!(target: "voice_server.asr", session_id, "ASR 返回非 2xx: {}", status);
            return Err(ClientError::Status(status.as_u16()));
        }

        let text_stream = ndjson_helper::parse_ndjson::<AsrPartialJson>(resp).await;

        let session = session_id.to_string();
        let stream = try_stream! {
            tokio::pin!(text_stream);
            while let Some(item) = text_stream.next().await {
                match item {
                    Ok(p) => {
                        info!(
                            target: "voice_server.asr",
                            session_id = %session,
                            text = %p.text,
                            is_final = p.is_final,
                            "收到 ASR partial/final"
                        );
                        yield AsrEvent {
                            text: p.text,
                            is_final: p.is_final,
                        };
                    }
                    Err(e) => {
                        warn!(target: "voice_server.asr", session_id = %session, "解析 ASR JSON 失败: {}", e);
                        Err(ClientError::Decode(e.to_string()))?;
                    }
                }
            }
            debug!(target: "voice_server.asr", session_id = %session, "ASR 流结束");
        };

        Ok(Box::pin(stream))
    }
}

#[derive(Deserialize)]
struct AsrPartialJson {
    text: String,
    is_final: bool,
}

// ====== LLM ======

#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn chat(
        &self,
        session_id: &str,
        prompt: &str,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError>;
}

pub struct HttpLlmClient {
    pub endpoint: String,
    pub path: String,
    pub method: String,
    pub model: String,
    pub authorization: String,
    pub headers: HashMap<String, String>,
    pub timeout: Duration,
}

impl HttpLlmClient {
    pub fn from_config(cfg: &ClientConfig) -> Self {
        let path = if cfg.path.is_empty() {
            "/chat".to_string()
        } else {
            cfg.path.clone()
        };
        Self {
            endpoint: cfg.endpoint.clone(),
            path,
            method: cfg.method.clone(),
            model: cfg.model.clone(),
            authorization: cfg.authorization.clone(),
            headers: cfg.headers.clone(),
            timeout: Duration::from_millis(cfg.timeout_ms),
        }
    }
}

#[derive(Serialize)]
struct LlmRequest<'a> {
    prompt: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
}

#[derive(Deserialize)]
struct LlmDeltaJson {
    delta: String,
    is_final: bool,
}

#[async_trait]
impl LlmClient for HttpLlmClient {
    async fn chat(
        &self,
        session_id: &str,
        prompt: &str,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
        let url = if self.path.is_empty() {
            self.endpoint.clone()
        } else {
            format!("{}{}", self.endpoint, self.path)
        };

        let method = reqwest::Method::from_bytes(self.method.as_bytes())
            .unwrap_or(reqwest::Method::POST);
        let client = reqwest::Client::builder()
            .timeout(self.timeout)
            .build()
            .map_err(|e| ClientError::Http(e.to_string()))?;
        let body = LlmRequest {
            prompt,
            model: if self.model.is_empty() { None } else { Some(self.model.clone()) },
        };
        let mut req = client
            .request(method.clone(), &url)
            .header("x-session-id", session_id)
            .json(&body);
        if !self.authorization.is_empty() {
            req = req.header("Authorization", &self.authorization);
        }
        if !self.model.is_empty() {
            req = req.header("X-Model", &self.model);
        }
        for (k, v) in &self.headers {
            req = req.header(k.as_str(), v.as_str());
        }

        info!(
            target: "voice_server.llm",
            session_id,
            method = %method,
            url = %url,
            prompt_len = prompt.chars().count(),
            model = %self.model,
            has_auth = !self.authorization.is_empty(),
            "LLM {} 请求", method
        );

        let resp = req
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            warn!(target: "voice_server.llm", session_id, "LLM 返回非 2xx: {}", status);
            return Err(ClientError::Status(status.as_u16()));
        }

        let text_stream = ndjson_helper::parse_ndjson::<LlmDeltaJson>(resp).await;

        let session = session_id.to_string();
        let stream = try_stream! {
            tokio::pin!(text_stream);
            while let Some(item) = text_stream.next().await {
                match item {
                    Ok(d) => {
                        info!(
                            target: "voice_server.llm",
                            session_id = %session,
                            delta_len = d.delta.chars().count(),
                            is_final = d.is_final,
                            delta = %d.delta,
                            "收到 LLM delta"
                        );
                        yield LlmEvent {
                            delta: d.delta,
                            is_final: d.is_final,
                        };
                    }
                    Err(e) => {
                        warn!(target: "voice_server.llm", session_id = %session, "解析 LLM JSON 失败: {}", e);
                        Err(ClientError::Decode(e.to_string()))?;
                    }
                }
            }
            debug!(target: "voice_server.llm", session_id = %session, "LLM 流结束");
        };

        Ok(Box::pin(stream))
    }
}

// ====== TTS ======

#[async_trait]
pub trait TtsClient: Send + Sync {
    async fn synthesize(
        &self,
        session_id: &str,
        text: &str,
    ) -> Result<BoxStream<Result<TtsEvent, ClientError>>, ClientError>;
}

pub struct HttpTtsClient {
    pub endpoint: String,
    pub path: String,
    pub method: String,
    pub model: String,
    pub authorization: String,
    pub headers: HashMap<String, String>,
    pub timeout: Duration,
    pub voice: String,
    pub response_format: String,
}

impl HttpTtsClient {
    pub fn from_config(cfg: &ClientConfig) -> Self {
        let path = if cfg.path.is_empty() {
            "/synthesize".to_string()
        } else {
            cfg.path.clone()
        };
        Self {
            endpoint: cfg.endpoint.clone(),
            path,
            method: cfg.method.clone(),
            model: cfg.model.clone(),
            authorization: cfg.authorization.clone(),
            headers: cfg.headers.clone(),
            timeout: Duration::from_millis(cfg.timeout_ms),
            voice: cfg.voice.clone(),
            response_format: cfg.response_format.clone(),
        }
    }
}

#[derive(Serialize)]
struct TtsRequest<'a> {
    /// mock-tts 用
    text: &'a str,
    /// OpenAI / SiliconFlow 等 OpenAI-兼容 API 用（与 text 同值）
    #[serde(skip_serializing_if = "Option::is_none")]
    input: Option<&'a str>,
    /// 模型
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    /// 发音人/音色（OpenAI: alloy/echo/fable/onyx/nova/shimmer；CosyVoice: zhitian_emo）
    #[serde(skip_serializing_if = "Option::is_none")]
    voice: Option<String>,
    /// 输出格式（OpenAI: mp3/opus/aac/flac/wav/pcm）
    #[serde(skip_serializing_if = "Option::is_none", rename = "response_format")]
    response_format: Option<String>,
}

#[derive(Deserialize)]
struct TtsChunkJson {
    seq: u32,
    #[serde(with = "base64_audio")]
    audio: Vec<u8>,
    is_last: bool,
}

mod base64_audio {
    use base64::Engine;
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        base64::engine::general_purpose::STANDARD
            .decode(&s)
            .map_err(serde::de::Error::custom)
    }
}

#[async_trait]
impl TtsClient for HttpTtsClient {
    async fn synthesize(
        &self,
        session_id: &str,
        text: &str,
    ) -> Result<BoxStream<Result<TtsEvent, ClientError>>, ClientError> {
        let url = if self.path.is_empty() {
            self.endpoint.clone()
        } else {
            format!("{}{}", self.endpoint, self.path)
        };

        let method = reqwest::Method::from_bytes(self.method.as_bytes())
            .unwrap_or(reqwest::Method::POST);
        let client = reqwest::Client::builder()
            .timeout(self.timeout)
            .build()
            .map_err(|e| ClientError::Http(e.to_string()))?;
        let body = TtsRequest {
            text,
            input: Some(text), // OpenAI-compat: also send as `input`
            model: if self.model.is_empty() { None } else { Some(self.model.clone()) },
            voice: if self.voice.is_empty() { None } else { Some(self.voice.clone()) },
            response_format: if self.response_format.is_empty() {
                None
            } else {
                Some(self.response_format.clone())
            },
        };
        let mut req = client
            .request(method.clone(), &url)
            .header("x-session-id", session_id)
            .json(&body);
        if !self.authorization.is_empty() {
            req = req.header("Authorization", &self.authorization);
        }
        if !self.model.is_empty() {
            req = req.header("X-Model", &self.model);
        }
        for (k, v) in &self.headers {
            req = req.header(k.as_str(), v.as_str());
        }

        info!(
            target: "voice_server.tts",
            session_id,
            method = %method,
            url = %url,
            text_len = text.chars().count(),
            model = %self.model,
            voice = %self.voice,
            response_format = %self.response_format,
            has_auth = !self.authorization.is_empty(),
            "TTS {} 请求", method
        );

        let resp = req
            .send()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            warn!(target: "voice_server.tts", session_id, "TTS 返回非 2xx: {}", status);
            return Err(ClientError::Status(status.as_u16()));
        }

        let text_stream = ndjson_helper::parse_ndjson::<TtsChunkJson>(resp).await;

        let session = session_id.to_string();
        let stream = try_stream! {
            tokio::pin!(text_stream);
            while let Some(item) = text_stream.next().await {
                match item {
                    Ok(c) => {
                        info!(
                            target: "voice_server.tts",
                            session_id = %session,
                            seq = c.seq,
                            bytes = c.audio.len(),
                            is_last = c.is_last,
                            "收到 TTS chunk"
                        );
                        yield TtsEvent {
                            seq: c.seq,
                            data: c.audio,
                            is_last: c.is_last,
                        };
                    }
                    Err(e) => {
                        warn!(target: "voice_server.tts", session_id = %session, "解析 TTS JSON 失败: {}", e);
                        Err(ClientError::Decode(e.to_string()))?;
                    }
                }
            }
            debug!(target: "voice_server.tts", session_id = %session, "TTS 流结束");
        };

        Ok(Box::pin(stream))
    }
}

/// 便捷 trait：把 reqwest::Response 转成 NDJSON 流
#[allow(dead_code)]
trait NdjsonExt {
    async fn into_ndjson<T>(self) -> BoxStream<Result<T, String>>
    where
        T: for<'de> Deserialize<'de> + Send + 'static;
}

mod ndjson_helper {
    use super::*;
    use futures_util::{Stream, StreamExt};
    use std::pin::Pin;

    pub async fn parse_ndjson<T>(resp: reqwest::Response) -> Pin<Box<dyn Stream<Item = Result<T, String>> + Send>>
    where
        T: for<'de> Deserialize<'de> + Send + 'static,
    {
        let byte_stream = resp.bytes_stream();
        let lines = byte_stream
            .map(|chunk| match chunk {
                Ok(b) => Ok(b),
                Err(e) => Err(e.to_string()),
            })
            .scan(Vec::<u8>::new(), |buf, chunk| {
                futures::future::ready(match chunk {
                    Ok(bytes) => {
                        buf.extend_from_slice(&bytes);
                        // 按 \n 切
                        let mut items = Vec::new();
                        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                            let line: Vec<u8> = buf.drain(..=pos).collect();
                            let line = line[..line.len() - 1].to_vec(); // strip \n
                            if !line.is_empty() {
                                items.push(Ok(line));
                            }
                        }
                        Some(futures::stream::iter(items))
                    }
                    Err(e) => Some(futures::stream::iter(vec![Err(e)])),
                })
            })
            .flat_map(|s| s);

        let parsed = lines.filter_map(|line_res| async move {
            match line_res {
                Ok(line_bytes) => match serde_json::from_slice::<T>(&line_bytes) {
                    Ok(v) => Some(Ok(v)),
                    Err(e) => Some(Err(format!("serde_json: {}", e))),
                },
                Err(e) => Some(Err(e)),
            }
        });

        Box::pin(parsed)
    }
}

// =========================================================================
// 客户端工厂（根据 config 构造 Arc<dyn ...Client>）
// =========================================================================

/// 根据 config 构造 ASR 客户端；目前只支持 kind="http"
pub fn build_asr_client(cfg: &ClientConfig) -> anyhow::Result<Arc<dyn AsrClient>> {
    match cfg.kind.as_str() {
        "http" => {
            if cfg.endpoint.is_empty() {
                anyhow::bail!("[asr] endpoint 未配置");
            }
            tracing::info!(
                target: "voice_server.factory",
                kind = "http",
                endpoint = %cfg.endpoint,
                path = %cfg.path,
                method = %cfg.method,
                model = %cfg.model,
                has_auth = !cfg.authorization.is_empty(),
                extra_headers = cfg.headers.len(),
                "构造 HttpAsrClient"
            );
            Ok(Arc::new(HttpAsrClient::from_config(cfg)))
        }
        other => anyhow::bail!("[asr] 不支持的 kind: {}（当前仅支持 http）", other),
    }
}

pub fn build_llm_client(cfg: &ClientConfig) -> anyhow::Result<Arc<dyn LlmClient>> {
    match cfg.kind.as_str() {
        "http" => {
            if cfg.endpoint.is_empty() {
                anyhow::bail!("[llm] endpoint 未配置");
            }
            tracing::info!(
                target: "voice_server.factory",
                kind = "http",
                endpoint = %cfg.endpoint,
                path = %cfg.path,
                method = %cfg.method,
                model = %cfg.model,
                has_auth = !cfg.authorization.is_empty(),
                extra_headers = cfg.headers.len(),
                "构造 HttpLlmClient"
            );
            Ok(Arc::new(HttpLlmClient::from_config(cfg)))
        }
        other => anyhow::bail!("[llm] 不支持的 kind: {}（当前仅支持 http）", other),
    }
}

pub fn build_tts_client(cfg: &ClientConfig) -> anyhow::Result<Arc<dyn TtsClient>> {
    match cfg.kind.as_str() {
        "http" => {
            if cfg.endpoint.is_empty() {
                anyhow::bail!("[tts] endpoint 未配置");
            }
            tracing::info!(
                target: "voice_server.factory",
                kind = "http",
                endpoint = %cfg.endpoint,
                path = %cfg.path,
                method = %cfg.method,
                model = %cfg.model,
                voice = %cfg.voice,
                response_format = %cfg.response_format,
                has_auth = !cfg.authorization.is_empty(),
                extra_headers = cfg.headers.len(),
                "构造 HttpTtsClient"
            );
            Ok(Arc::new(HttpTtsClient::from_config(cfg)))
        }
        other => anyhow::bail!("[tts] 不支持的 kind: {}（当前仅支持 http）", other),
    }
}