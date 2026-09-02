//! TTS 客户端：手搓 reqwest（不走 async-openai）
//!
//! ## 为什么不用 async-openai
//! vLLM-Omni 扩展了 OpenAI TTS 请求字段，且 voice 名称由加载的模型决定；
//! 手搓 reqwest 可以透传这些字段和任意模型 voice 名称。
//!
//! ## Wire format
//! 请求：JSON `{input, model, voice, response_format, stream?}`（OpenAI-compat）
//! 响应：可能是
//!   - SSE（`content-type: text/event-stream`），OpenAI `speech.audio.*` 事件
//!   - 单段二进制音频（`content-type: audio/...`），siliconflow 当前即使发 `stream: true` 也走这种
//! 按 content-type 自动分支。

use async_trait::async_trait;
use base64::Engine;
use futures_util::StreamExt;
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tracing::{info, warn};

use crate::client::error::{parse_openai_error, ClientError};
use crate::client::apply_trace_header;
use crate::client::tts_ws::{TtsWsClient, TtsWsConfig};
use crate::config::{ProviderConfig, TtsConfig};
use crate::events::TtsEvent;

pub type BoxStream<T> = Pin<Box<dyn futures_util::Stream<Item = T> + Send>>;
pub type ArcTts = Arc<dyn TtsClient>;

/// 可持续写入输入文本并按需 flush 的增量 TTS 会话。
///
/// HTTP TTS 不提供这种会话能力；支持增量输入的传输（例如 WebSocket）可以通过
/// [`TtsClient::open_input_session`] 返回具体实现。
#[async_trait]
pub trait TtsInputSession: Send {
    /// 发送一段增量文本，不结束当前 utterance。
    async fn send_text(&mut self, text: &str) -> Result<(), ClientError>;

    /// 结束当前 utterance，并触发上游开始生成。
    async fn flush(&mut self) -> Result<(), ClientError>;

    /// 读取下一条 TTS 音频或控制事件；流结束时返回 `None`。
    async fn next_event(&mut self) -> Option<Result<TtsEvent, ClientError>>;

    /// 关闭当前增量会话并释放上游资源。
    async fn close(&mut self) -> Result<(), ClientError>;

    /// 完成整轮增量会话并归还可复用的上游连接。
    ///
    /// 默认退化为 `close`，以兼容只实现旧接口的客户端；WebSocket 会话会覆写
    /// 此方法，在不关闭物理连接的情况下将当前租约标记为空闲。
    async fn finish(&mut self) -> Result<(), ClientError> {
        self.close().await
    }
}

#[async_trait]
pub trait TtsClient: Send + Sync {
    fn output_format(&self) -> (Option<u32>, u8) { (None, 1) }

    /// 打开增量输入会话。
    ///
    /// 默认返回 `None`，表示客户端仅支持 [`TtsClient::synthesize`] 的完整文本请求。
    async fn open_input_session(
        &self,
        _session_id: &str,
        _sample_rate_override: Option<u32>,
        _voice_override: Option<String>,
    ) -> Result<Option<Box<dyn TtsInputSession>>, ClientError> {
        Ok(None)
    }

    /// 列出当前 TTS provider 已加载的音色。
    ///
    /// 只有 HTTP provider 支持 `/v1/audio/voices`；其他实现返回错误，让调用方
    /// 回退到本地兼容列表。
    async fn list_voices(&self) -> Result<Vec<String>, ClientError> {
        Err(ClientError::Config(
            "this TTS transport does not support listing voices".to_string(),
        ))
    }

    /// 合成一段文本到音频流。
    ///
    /// `sample_rate_override`：端侧（浏览器）SessionStart 上报的 TTS 输出采样率。
    /// - `Some(n)` —— 优先用端侧值（**覆盖** `TtsConfig.sample_rate`）
    /// - `None` —— 用配置 `TtsConfig.sample_rate`（兜底）
    ///
    /// `voice_override`：端侧（前端下拉框）选中的音色**短名**（如 `alex`）。
    /// - `Some("alex")` —— 用该短名（拼上 `model` 前缀后发给 provider）
    /// - `None` —— 用配置 `TtsConfig.voice` 默认（拼上 `model` 前缀）
    ///
    /// 调用方（`session.rs` / admin handler）应把"端侧值（可能为 None）"原样透传。
    async fn synthesize(
        &self,
        session_id: &str,
        text: &str,
        sample_rate_override: Option<u32>,
        voice_override: Option<String>,
    ) -> Result<BoxStream<Result<TtsEvent, ClientError>>, ClientError>;

    /// 默认音色**短名**（如 `"alex"`）。
    ///
    /// 给 `/admin/voices` 端点 / 前端下拉框默认值用。**不含**模型前缀。
    fn default_voice_short(&self) -> &str;
}

pub struct HttpTtsClient {
    base_url: String,
    path: String,
    api_key: Option<String>, // None = 不发 Authorization
    model: String,
    /// 配置默认音色名称（由模型决定，例如 `vivian`）。
    voice_short: String,
    response_format: String,
    stream: bool,
    /// 输出采样率（Hz）。None = 不在请求里发 `sample_rate`，由 provider 自行决定。
    sample_rate: Option<u32>,
    channels: u8,
    speed: Option<f32>,
    task_type: Option<String>,
    language: Option<String>,
    instructions: Option<String>,
    max_new_tokens: Option<u32>,
    initial_codec_chunk_frames: Option<u32>,
    non_streaming_mode: Option<bool>,
    stream_format: Option<String>,
    ref_audio: Option<String>,
    ref_text: Option<String>,
    x_vector_only_mode: Option<bool>,
    extra_headers: HeaderMap,
    timeout: Duration,
    client: reqwest::Client,
}

impl HttpTtsClient {
    /// 用配置里的默认音色构造 HttpTtsClient。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        base_url: String,
        path: String,
        api_key: Option<String>,
        model: String,
        voice_short: String,
        response_format: String,
        stream: bool,
        sample_rate: Option<u32>,
        channels: u8,
        speed: Option<f32>,
        task_type: Option<String>,
        language: Option<String>,
        instructions: Option<String>,
        max_new_tokens: Option<u32>,
        initial_codec_chunk_frames: Option<u32>,
        non_streaming_mode: Option<bool>,
        stream_format: Option<String>,
        ref_audio: Option<String>,
        ref_text: Option<String>,
        x_vector_only_mode: Option<bool>,
        extra_headers: HeaderMap,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        let voice_short = if voice_short.is_empty() {
            "vivian".to_string()
        } else {
            voice_short
        };
        let client = reqwest::Client::builder().timeout(timeout).build()?;
        Ok(Self {
            base_url,
            path,
            api_key,
            model,
            voice_short,
            response_format,
            stream,
            sample_rate,
            channels,
            speed,
            task_type,
            language,
            instructions,
            max_new_tokens,
            initial_codec_chunk_frames,
            non_streaming_mode,
            stream_format,
            ref_audio,
            ref_text,
            x_vector_only_mode,
            extra_headers,
            timeout,
            client,
        })
    }

    // 注：发请求时的完整 voice 字符串由 [`synthesize`] 直接查 [`SUPPORTED_VOICES`]
    // 取 [`VoiceEntry::wire_voice`]，不再用 `format!("{}:{}", model, short)` 之类的硬拼接。
    // 不同 provider 命名空间不同时（有的 `<model_id>:<voice>`、有的直接 `<voice_id>`），
    // 调整 / 加音色都只需要改 [`SUPPORTED_VOICES`] 一处，不需要改拼接逻辑。
}

#[derive(Debug, Serialize)]
pub(crate) struct TtsRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) input: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) voice: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) response_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) speed: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) task_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_new_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) initial_codec_chunk_frames: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) non_streaming_mode: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stream_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) ref_audio: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) ref_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) x_vector_only_mode: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) sample_rate: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct SpeechAudioError {
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SpeechAudioEnvelope {
    #[serde(default)]
    audio: Option<String>,
    #[serde(default)]
    error: Option<SpeechAudioError>,
}

#[derive(Debug, Deserialize)]
struct VoiceListResponse {
    #[serde(default)]
    voices: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct LegacyTtsStreamChunk {
    #[serde(default)]
    data: Option<String>,
    #[serde(default)]
    finish_reason: Option<String>,
}

fn parse_speech_audio_event(
    event: &str,
    payload: &[u8],
    seq: &mut u32,
) -> Option<Result<TtsEvent, ClientError>> {
    let parsed: SpeechAudioEnvelope = match serde_json::from_slice(payload) {
        Ok(value) => value,
        Err(e) => {
            return Some(Err(ClientError::Decode(format!(
                "invalid TTS SSE payload: {e}"
            ))))
        }
    };
    match event {
        "speech.audio.delta" => {
            let bytes = match base64::engine::general_purpose::STANDARD
                .decode(parsed.audio.unwrap_or_default())
            {
                Ok(bytes) => bytes,
                Err(e) => {
                    return Some(Err(ClientError::Decode(format!(
                        "invalid TTS audio base64: {e}"
                    ))))
                }
            };
            if bytes.is_empty() {
                return None;
            }
            *seq += 1;
            Some(Ok(TtsEvent {
                seq: *seq,
                data: bytes,
                is_last: false,
            }))
        }
        "speech.audio.done" => Some(Ok(TtsEvent {
            seq: *seq + 1,
            data: Vec::new(),
            is_last: true,
        })),
        "speech.audio.error" => Some(Err(ClientError::Decode(
            parsed
                .error
                .and_then(|e| e.message)
                .unwrap_or_else(|| "TTS generation failed".into()),
        ))),
        _ if event.is_empty() => {
            let legacy: LegacyTtsStreamChunk = serde_json::from_slice(payload).ok()?;
            let audio = legacy.data.unwrap_or_default();
            if audio.is_empty() {
                return None;
            }
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(audio)
                .ok()?;
            if bytes.is_empty() {
                return None;
            }
            *seq += 1;
            Some(Ok(TtsEvent {
                seq: *seq,
                data: bytes,
                is_last: legacy.finish_reason.is_some(),
            }))
        }
        _ => None,
    }
}

#[cfg(test)]
fn parse_vllm_sse_stream(input: &[u8]) -> BoxStream<Result<TtsEvent, ClientError>> {
    let owned = input.to_vec();
    let stream = async_stream::stream! {
        let mut event = String::new();
        let mut seq = 0;
        for line in owned.split(|b| *b == b'\n') {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line.starts_with(b"event: ") { event = String::from_utf8_lossy(&line[7..]).into_owned(); }
            else if line.starts_with(b"data: ") {
                if let Some(result) = parse_speech_audio_event(&event, &line[6..], &mut seq) { yield result; }
            }
        }
    };
    Box::pin(stream)
}

#[async_trait]
impl TtsClient for HttpTtsClient {
    fn output_format(&self) -> (Option<u32>, u8) { (self.sample_rate, self.channels) }

    fn default_voice_short(&self) -> &str {
        &self.voice_short
    }

    async fn list_voices(&self) -> Result<Vec<String>, ClientError> {
        let url = format!("{}/audio/voices", self.base_url.trim_end_matches('/'));
        let mut req = self.client.get(&url);
        if let Some(key) = &self.api_key {
            let authorization = if key.starts_with("Bearer ") || key.starts_with("bearer ") {
                key.clone()
            } else {
                format!("Bearer {key}")
            };
            req = req.header("Authorization", authorization);
        }
        for (name, value) in &self.extra_headers {
            req = req.header(name, value);
        }

        let response = req.send().await.map_err(|error| {
            warn!(
                target: "voice_server.tts.err",
                url = %url,
                error = %error,
                "TTS 音色列表请求失败"
            );
            ClientError::Http(error.to_string())
        })?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            if let Some(error) = parse_openai_error(&body) {
                return Err(ClientError::Api {
                    status: status.as_u16(),
                    error,
                });
            }
            return Err(ClientError::Status(status.as_u16()));
        }

        let response: VoiceListResponse = response.json().await.map_err(|error| {
            ClientError::Decode(format!("invalid TTS voice list response: {error}"))
        })?;
        Ok(response.voices)
    }

    async fn synthesize(
        &self,
        session_id: &str,
        text: &str,
        sample_rate_override: Option<u32>,
        voice_override: Option<String>,
    ) -> Result<BoxStream<Result<TtsEvent, ClientError>>, ClientError> {
        // ===== 解析 effective sample_rate =====
        // 优先级：端侧 override > 配置 sample_rate（兜底）
        let effective_sample_rate = sample_rate_override.or(self.sample_rate);

        // 端侧 override 与 response_format 的兼容性校验（仅 override 时校验，
        // 配置 fallback 已在 build_tts_client 时校验过，这里不重复）。
        validate_sample_rate_override(sample_rate_override, &self.response_format)?;

        // Voice values are model-specific in vLLM-Omni (for example "vivian").
        let effective_voice_short = voice_override.as_deref().unwrap_or(&self.voice_short);
        let effective_voice = effective_voice_short;

        let url = if self.path.is_empty() {
            self.base_url.clone()
        } else {
            format!("{}{}", self.base_url, self.path)
        };

        let body = TtsRequest {
            input: Some(text),
            model: if self.model.is_empty() {
                None
            } else {
                Some(self.model.clone())
            },
            voice: if effective_voice.is_empty() {
                None
            } else {
                Some(effective_voice.to_string())
            },
            response_format: Some(if self.response_format.is_empty() {
                "wav".into()
            } else {
                self.response_format.clone()
            }),
            speed: Some(self.speed.unwrap_or(1.0)),
            task_type: Some(
                self.task_type
                    .clone()
                    .unwrap_or_else(|| "CustomVoice".into()),
            ),
            language: Some(self.language.clone().unwrap_or_else(|| "Auto".into())),
            instructions: self.instructions.clone().or_else(|| Some(String::new())),
            max_new_tokens: Some(self.max_new_tokens.unwrap_or(2048)),
            initial_codec_chunk_frames: self.initial_codec_chunk_frames,
            non_streaming_mode: self.non_streaming_mode,
            stream: if self.stream || matches!(self.stream_format.as_deref(), Some("sse" | "audio"))
            {
                Some(true)
            } else {
                None
            },
            stream_format: self.stream_format.clone(),
            ref_audio: self.ref_audio.clone(),
            ref_text: self.ref_text.clone(),
            x_vector_only_mode: self.x_vector_only_mode,
            sample_rate: effective_sample_rate,
        };
        let mut req = apply_trace_header(self
            .client
            .request(reqwest::Method::POST, &url)
            .header("x-session-id", session_id)
            .json(&body));
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
            voice = %effective_voice,
            voice_short = %effective_voice_short,
            voice_override = voice_override.as_deref(),
            voice_short_config = %self.voice_short,
            response_format = %self.response_format,
            stream = self.stream,
            sample_rate = effective_sample_rate,
            sample_rate_override,
            sample_rate_config = self.sample_rate,
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
                .map(|(k, v)| format!("{}: {}", k, v.to_str().unwrap_or("<binary>")))
                .collect::<Vec<_>>()
                .join(" | ");
            // 抓 body 一次：既要写日志（截断预览），又要尝试解析 OpenAI 信封
            let body = match resp.text().await {
                Ok(t) => t,
                Err(e) => {
                    warn!(
                        target: "voice_server.tts.err",
                        session_id,
                        url = %url,
                        status = status_u16,
                        error = %e,
                        "TTS 非 2xx body 读取失败"
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
            // 优先按 yapi.md OpenAI 信封解析；解析失败降级到裸 Status
            if let Some(api_err) = parse_openai_error(&body) {
                return Err(ClientError::Api {
                    status: status_u16,
                    error: api_err,
                });
            }
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

        if is_sse {
            // vLLM-Omni emits OpenAI speech.audio.* SSE events.
            let mut sse_buf: Vec<u8> = Vec::new();
            let mut byte_stream = Box::pin(resp.bytes_stream());
            let stream = async_stream::stream! {
                let mut seq: u32 = 0;
                let mut event_name = String::new();
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
                        if line.starts_with(b"event: ") {
                            event_name = String::from_utf8_lossy(&line[7..]).into_owned();
                        } else if line.starts_with(b"data: ") {
                            let payload = &line[6..];
                            if payload == b"[DONE]" || payload.is_empty() { continue; }
                            if let Some(result) = parse_speech_audio_event(&event_name, payload, &mut seq) {
                                let terminal = result.as_ref().map(|e| e.is_last).unwrap_or(true);
                                yield result;
                                if terminal { break; }
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
            info!(
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
    build_tts_client_with_metrics(cfg, provider, Arc::new(crate::metrics::NoopMetricsSink))
}

pub fn build_tts_client_with_metrics(
    cfg: &TtsConfig,
    provider: Option<&ProviderConfig>,
    metrics: Arc<dyn crate::metrics::VoiceMetricsSink>,
) -> anyhow::Result<Arc<dyn TtsClient>> {
    let (resolved, path) = cfg.resolved(provider);
    let timeout = resolved.timeout();
    if cfg.transport_kind() == "websocket" {
        let model_format = cfg.model_format();
        let endpoint = cfg.resolved_endpoint(provider);
        let ws_cfg = TtsWsConfig {
            endpoint,
            api_key: (!resolved.api_key.is_empty()).then_some(resolved.api_key),
            voice: (!cfg.voice.is_empty()).then_some(cfg.voice.clone()),
            task_type: cfg.task_type.clone(),
            language: cfg.language.clone(),
            instructions: cfg.instructions.clone(),
            response_format: if cfg.response_format.is_empty() {
                "pcm".into()
            } else {
                cfg.response_format.clone()
            },
            sample_rate: model_format.sample_rate,
            channels: model_format.channels,
            speed: cfg.speed,
            ref_audio: cfg.ref_audio.clone(),
            ref_text: cfg.ref_text.clone(),
            max_new_tokens: cfg.max_new_tokens,
            stream_audio: cfg.stream_format.as_deref() == Some("audio"),
            extra_headers: cfg
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            timeout,
            max_connections: cfg.max_connections,
        };
        info!(
            target: "voice_server.factory",
            kind = "websocket",
            endpoint = %ws_cfg.endpoint,
            model = %cfg.model,
            voice = %cfg.voice,
            response_format = %ws_cfg.response_format,
            stream_audio = ws_cfg.stream_audio,
            "构造 TTS WebSocket 客户端"
        );
        return Ok(Arc::new(TtsWsClient::new_with_metrics(ws_cfg, metrics)));
    }
    let base_url = resolved.api_base.clone();
    let api_key = resolved.api_key.clone();
    let headers = resolved.to_header_map();
    let model_format = cfg.model_format();

    // 校验 sample_rate 与 response_format 的兼容性（如果用户显式给了 sample_rate）
    if let Some(sr) = model_format.sample_rate {
        if cfg.response_format.is_empty() {
            tracing::warn!(
                target: "voice_server.factory",
                sample_rate = sr,
                "TTS sample_rate 已设置但 response_format 为空 —— 跳过兼容性校验，直接发给 provider"
            );
        } else {
            let supported = supported_sample_rates(&cfg.response_format);
            if !supported.is_empty() && !supported.contains(&sr) {
                let default_sr = default_sample_rate(&cfg.response_format);
                anyhow::bail!(
                    "TTS sample_rate {} Hz 与 response_format '{}' 不兼容；该格式支持的采样率为 {:?}{}",
                    sr,
                    cfg.response_format,
                    supported,
                    match default_sr {
                        Some(d) => format!("，默认 {} Hz", d),
                        None => String::new(),
                    }
                );
            }
        }
    }

    tracing::info!(
        target: "voice_server.factory",
        kind = "http",
        base_url = %base_url,
        path = %path,
        model = %cfg.model,
        voice_short = %cfg.voice,
        default_voice = %cfg.voice,
        response_format = %cfg.response_format,
        sample_rate = ?model_format.sample_rate,
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
        model_format.sample_rate,
        model_format.channels,
        cfg.speed,
        cfg.task_type.clone(),
        cfg.language.clone(),
        cfg.instructions.clone(),
        cfg.max_new_tokens,
        cfg.initial_codec_chunk_frames,
        cfg.non_streaming_mode,
        cfg.stream_format.clone(),
        cfg.ref_audio.clone(),
        cfg.ref_text.clone(),
        cfg.x_vector_only_mode,
        headers,
        timeout,
    )?))
}

/// 返回某种 `response_format` 支持的采样率集合（Hz）。
///
/// 未知 / 不识别的格式返回空切片 —— 调用方应视为「不校验」。
/// 大小写不敏感（先 `to_ascii_lowercase` 再匹配）。
pub fn supported_sample_rates(format: &str) -> &'static [u32] {
    match format.to_ascii_lowercase().as_str() {
        "opus" => &[48000],
        "wav" | "pcm" => &[8000, 16000, 24000, 32000, 44100],
        "mp3" => &[32000, 44100],
        _ => &[],
    }
}

/// 返回某种 `response_format` 的默认采样率（Hz）。未知格式返回 `None`。
pub fn default_sample_rate(format: &str) -> Option<u32> {
    match format.to_ascii_lowercase().as_str() {
        "opus" => Some(48000),
        "wav" | "pcm" | "mp3" => Some(44100),
        _ => None,
    }
}

/// TTS 音色元数据。
///
/// - `short`：外部 API（前端下拉框 / admin API / yaml 默认）用的 key。
/// - `wire_voice`：**原样**发给 TTS provider 的字符串（**已含** provider
///   期望的全部前缀/路径，例如 `"fnlp/MOSS-TTSD-v0.5:alex"`）—— 不要再做任何
///   `format!` / `+ ":" +` 之类的拼接，命名本身就是为了挡住这种"简化"。
///
/// 之前 HttpTtsClient 写死 `format!("{}:{}", model, short)` 拼 full —— 改成在
/// 条目里直接给 `wire_voice`，不同 provider 命名空间不同时（有的 `<model_id>:<voice>`、
/// 有的直接 `<voice_id>`）调整 / 加音色都只需要改这张表，不需要动拼接逻辑。
///
/// 后续可在此结构体上加 `gender` / `language` / 默认 `sample_rate` 等字段，
/// 调用方按需读取；不会破坏 `HashMap<&str, VoiceEntry>` 的 key 类型。
/// 新加字段时建议用 required（而不是 `Option<...>`），backfill 所有条目比
/// 半填充状态好维护。
#[derive(Debug, Clone, Copy)]
pub struct VoiceEntry {
    pub short: &'static str,
    pub wire_voice: &'static str,
}

/// 全部支持的 TTS 音色 → 元数据映射。
///
/// 用 `HashMap` 而非数组，便于：
///   - O(1) 短名校验 / 查 entry
///   - 后续给 `VoiceEntry` 加字段（gender / language / 默认 sample_rate 等），
///     调整 / 扩展一条即可，不需要同步改拼接 / 序列化逻辑
///
/// `LazyLock` 里一次性 build，启动后只读；外部可直接 `SUPPORTED_VOICES.get("alex")`。
///
/// 改这张表时，记得同步检查 `config.rs::TtsConfig.voice` 默认值仍是合法 short。
pub static SUPPORTED_VOICES: LazyLock<HashMap<&'static str, VoiceEntry>> = LazyLock::new(|| {
    HashMap::from([
        (
            "alex",
            VoiceEntry {
                short: "alex",
                wire_voice: "fnlp/MOSS-TTSD-v0.5:alex",
            },
        ),
        (
            "anna",
            VoiceEntry {
                short: "anna",
                wire_voice: "fnlp/MOSS-TTSD-v0.5:anna",
            },
        ),
        (
            "bella",
            VoiceEntry {
                short: "bella",
                wire_voice: "fnlp/MOSS-TTSD-v0.5:bella",
            },
        ),
        (
            "benjamin",
            VoiceEntry {
                short: "benjamin",
                wire_voice: "fnlp/MOSS-TTSD-v0.5:benjamin",
            },
        ),
        (
            "charles",
            VoiceEntry {
                short: "charles",
                wire_voice: "fnlp/MOSS-TTSD-v0.5:charles",
            },
        ),
        (
            "claire",
            VoiceEntry {
                short: "claire",
                wire_voice: "fnlp/MOSS-TTSD-v0.5:claire",
            },
        ),
        (
            "david",
            VoiceEntry {
                short: "david",
                wire_voice: "fnlp/MOSS-TTSD-v0.5:david",
            },
        ),
        (
            "diana",
            VoiceEntry {
                short: "diana",
                wire_voice: "fnlp/MOSS-TTSD-v0.5:diana",
            },
        ),
        (
            "aiden",
            VoiceEntry {
                short: "aiden",
                wire_voice: "aiden",
            },
        ),
        (
            "dylan",
            VoiceEntry {
                short: "dylan",
                wire_voice: "dylan",
            },
        ),
        (
            "eric",
            VoiceEntry {
                short: "eric",
                wire_voice: "eric",
            },
        ),
        (
            "ono_anna",
            VoiceEntry {
                short: "ono_anna",
                wire_voice: "ono_anna",
            },
        ),
        (
            "ryan",
            VoiceEntry {
                short: "ryan",
                wire_voice: "ryan",
            },
        ),
        (
            "serena",
            VoiceEntry {
                short: "serena",
                wire_voice: "serena",
            },
        ),
        (
            "sohee",
            VoiceEntry {
                short: "sohee",
                wire_voice: "sohee",
            },
        ),
        (
            "uncle_fu",
            VoiceEntry {
                short: "uncle_fu",
                wire_voice: "uncle_fu",
            },
        ),
        (
            "vivian",
            VoiceEntry {
                short: "vivian",
                wire_voice: "vivian",
            },
        ),
    ])
});

/// 校验短名是否在 [`SUPPORTED_VOICES`] 白名单里。大小写敏感 —— 短名都是小写。
pub fn is_supported_voice(short_name: &str) -> bool {
    SUPPORTED_VOICES.contains_key(short_name)
}

/// 查 short 对应的 entry；找不到返回 `None`。
pub fn lookup_voice(short_name: &str) -> Option<&'static VoiceEntry> {
    SUPPORTED_VOICES.get(short_name)
}

/// 给 `/admin/voices` 用：按 short 排序的短名列表（保证 API 输出稳定）。
pub fn supported_voice_shorts() -> Vec<&'static str> {
    let mut v: Vec<&'static str> = SUPPORTED_VOICES.keys().copied().collect();
    v.sort();
    v
}

/// 校验端侧 `sample_rate_override` 与 `response_format` 的兼容性。
///
/// - `None`：端侧没上报，跳过校验（HttpTtsClient 会自动走配置兜底）
/// - `Some(sr)`：sr 必须在该 response_format 的支持列表里；否则 `Err(Config)`
/// - `response_format` 为空（配置层未指定）：不校验，透传给 provider（与 build_tts_client 行为一致）
///
/// 抽出独立函数便于直接单测；synthesize 主路径只 `?` 一下。
pub fn validate_sample_rate_override(
    sample_rate_override: Option<u32>,
    response_format: &str,
) -> Result<(), ClientError> {
    if let Some(sr) = sample_rate_override {
        if !response_format.is_empty() {
            let supported = supported_sample_rates(response_format);
            if !supported.is_empty() && !supported.contains(&sr) {
                let default_sr = default_sample_rate(response_format);
                let detail = match default_sr {
                    Some(d) => format!("，默认 {} Hz", d),
                    None => String::new(),
                };
                return Err(ClientError::Config(format!(
                    "TTS sample_rate {} Hz 与 response_format '{}' 不兼容；该格式支持的采样率为 {:?}{}",
                    sr, response_format, supported, detail
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn incremental_tts_session_interface_defaults_to_none() {
        let mut cfg = TtsConfig::default();
        cfg.api_base = "http://127.0.0.1:0".into();
        cfg.model = "m".into();
        cfg.voice = "alex".into();

        let client = build_tts_client(&cfg, None).expect("HTTP TTS client should build");
        let session = client
            .open_input_session("test-session", None, None)
            .await
            .expect("HTTP TTS should use the default incremental-session behavior");
        assert!(session.is_none());
    }

    #[test]
    fn tts_transport_info_resolves_http_and_websocket_endpoints() {
        let mut cfg = TtsConfig::default();
        cfg.api_base = "http://127.0.0.1:8091/v1".into();

        assert_eq!(cfg.transport_kind(), "http");
        assert_eq!(cfg.resolved_endpoint(None), "http://127.0.0.1:8091/v1/audio/speech");

        cfg.transport = "websocket".into();
        assert_eq!(cfg.transport_kind(), "websocket");
        assert_eq!(
            cfg.resolved_endpoint(None),
            "ws://127.0.0.1:8091/v1/audio/speech/stream"
        );
    }

    #[test]
    fn supported_sample_rates_opus() {
        assert_eq!(supported_sample_rates("opus"), &[48000]);
        assert_eq!(supported_sample_rates("OPUS"), &[48000]);
    }

    #[test]
    fn supported_sample_rates_wav_pcm() {
        assert_eq!(
            supported_sample_rates("wav"),
            &[8000, 16000, 24000, 32000, 44100]
        );
        assert_eq!(supported_sample_rates("pcm"), supported_sample_rates("wav"));
        assert_eq!(supported_sample_rates("WAV"), supported_sample_rates("wav"));
    }

    #[test]
    fn supported_sample_rates_mp3() {
        assert_eq!(supported_sample_rates("mp3"), &[32000, 44100]);
    }

    #[test]
    fn supported_sample_rates_unknown_is_empty() {
        assert!(supported_sample_rates("flac").is_empty());
        assert!(supported_sample_rates("aac").is_empty());
        assert!(supported_sample_rates("").is_empty());
    }

    #[test]
    fn default_sample_rate_opus() {
        assert_eq!(default_sample_rate("opus"), Some(48000));
        assert_eq!(default_sample_rate("OPUS"), Some(48000));
    }

    #[test]
    fn default_sample_rate_wav_pcm_mp3() {
        assert_eq!(default_sample_rate("wav"), Some(44100));
        assert_eq!(default_sample_rate("pcm"), Some(44100));
        assert_eq!(default_sample_rate("mp3"), Some(44100));
    }

    #[test]
    fn default_sample_rate_unknown_is_none() {
        assert_eq!(default_sample_rate("flac"), None);
        assert_eq!(default_sample_rate(""), None);
    }

    #[test]
    fn build_tts_client_rejects_incompatible_sample_rate() {
        // opus 仅支持 48000；给 16000 应该 bail
        let mut cfg = TtsConfig::default();
        cfg.api_base = "http://127.0.0.1:0".to_string();
        cfg.model = "m".to_string();
        cfg.voice = "alex".to_string();
        cfg.response_format = "opus".to_string();
        cfg.sample_rate = Some(16000);

        match build_tts_client(&cfg, None) {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("16000") && msg.contains("opus"),
                    "expected error to mention both rate and format; got: {}",
                    msg
                );
            }
            Ok(_) => panic!("expected build_tts_client to fail for opus + 16000Hz"),
        }
    }

    #[test]
    fn build_tts_client_rejects_unsupported_sample_rate_for_mp3() {
        // mp3 支持 [32000, 44100]；给 24000 应该 bail
        let mut cfg = TtsConfig::default();
        cfg.api_base = "http://127.0.0.1:0".to_string();
        cfg.model = "m".to_string();
        cfg.voice = "alex".to_string();
        cfg.response_format = "mp3".to_string();
        cfg.sample_rate = Some(24000);

        match build_tts_client(&cfg, None) {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("24000") && msg.contains("mp3"),
                    "expected error to mention both rate and format; got: {}",
                    msg
                );
            }
            Ok(_) => panic!("expected build_tts_client to fail for mp3 + 24000Hz"),
        }
    }

    #[test]
    fn build_tts_client_accepts_supported_sample_rate() {
        // wav + 16000 —— 在支持范围内，应该成功构造
        let mut cfg = TtsConfig::default();
        cfg.api_base = "http://127.0.0.1:0".to_string();
        cfg.model = "m".to_string();
        cfg.voice = "alex".to_string();
        cfg.response_format = "wav".to_string();
        cfg.sample_rate = Some(16000);

        let _client = build_tts_client(&cfg, None).expect("should accept 16000 for wav");
    }

    #[test]
    fn build_tts_client_passes_through_when_response_format_empty() {
        // 用户显式给了 sample_rate 但没给 response_format —— 不校验，直接放行
        let mut cfg = TtsConfig::default();
        cfg.api_base = "http://127.0.0.1:0".to_string();
        cfg.model = "m".to_string();
        cfg.voice = "alex".to_string();
        // response_format 留空
        cfg.sample_rate = Some(12345);

        let _client = build_tts_client(&cfg, None)
            .expect("should pass through when response_format is empty");
    }

    #[test]
    fn build_tts_client_accepts_model_specific_voice() {
        // vLLM-Omni voice names are model-specific and may not be in the legacy map.
        let mut cfg = TtsConfig::default();
        cfg.api_base = "http://127.0.0.1:0".to_string();
        cfg.model = "m".to_string();
        cfg.voice = "snake_oil".to_string(); // 不在白名单
        cfg.response_format = "pcm".to_string();

        let client = build_tts_client(&cfg, None).expect("model-specific voice should be accepted");
        assert_eq!(client.default_voice_short(), "snake_oil");
    }

    // ===== validate_sample_rate_override（端侧 override 的请求期校验）=====

    #[test]
    fn validate_override_none_passes() {
        // None = 端侧没上报，直接放行（HttpTtsClient 会走配置兜底）
        assert!(validate_sample_rate_override(None, "wav").is_ok());
        assert!(validate_sample_rate_override(None, "").is_ok());
        assert!(validate_sample_rate_override(None, "flac").is_ok());
    }

    #[test]
    fn validate_override_compatible_rate_passes() {
        // wav/pcm 支持 [8000, 16000, 24000, 32000, 44100]
        for fmt in ["wav", "pcm", "WAV", "PCM"] {
            for sr in [8000u32, 16000, 24000, 32000, 44100] {
                assert!(
                    validate_sample_rate_override(Some(sr), fmt).is_ok(),
                    "{fmt} + {sr} should pass"
                );
            }
        }
        // opus 仅 48000
        assert!(validate_sample_rate_override(Some(48000), "opus").is_ok());
        // mp3 仅 [32000, 44100]
        for sr in [32000u32, 44100] {
            assert!(validate_sample_rate_override(Some(sr), "mp3").is_ok());
        }
    }

    #[test]
    fn validate_override_incompatible_rate_bails() {
        // opus + 16000 → 不兼容
        let err = validate_sample_rate_override(Some(16000), "opus").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("16000"), "msg should mention rate: {msg}");
        assert!(msg.contains("opus"), "msg should mention format: {msg}");
        // mp3 + 24000 → 不兼容
        let err = validate_sample_rate_override(Some(24000), "mp3").unwrap_err();
        assert!(err.to_string().contains("24000"));
        // wav + 48000 → 不在 wav 支持列表
        let err = validate_sample_rate_override(Some(48000), "wav").unwrap_err();
        assert!(err.to_string().contains("48000"));
    }

    #[test]
    fn validate_override_empty_response_format_passes_through() {
        // response_format 为空 → 不校验（与 build_tts_client 行为一致）
        assert!(validate_sample_rate_override(Some(12345), "").is_ok());
    }

    #[test]
    fn validate_override_unknown_response_format_passes_through() {
        // 未知格式（flac / aac）→ supported_sample_rates 返回空，保守放行
        assert!(validate_sample_rate_override(Some(12345), "flac").is_ok());
        assert!(validate_sample_rate_override(Some(12345), "aac").is_ok());
    }

    // ===== SUPPORTED_VOICES / is_supported_voice / lookup_voice =====

    #[test]
    fn supported_voices_matches_doc_list() {
        // 与用户提供的图片一致：alex / anna / bella / benjamin / charles / claire / david / diana
        let expected = vec![
            "aiden", "alex", "anna", "bella", "benjamin", "charles", "claire", "david", "diana",
            "dylan", "eric", "ono_anna", "ryan", "serena", "sohee", "uncle_fu", "vivian",
        ];
        let actual = supported_voice_shorts();
        assert_eq!(actual, expected);
    }

    #[test]
    fn lookup_voice_returns_wire_voice_for_known_shorts() {
        for (short, wire_voice) in [
            ("alex", "fnlp/MOSS-TTSD-v0.5:alex"),
            ("anna", "fnlp/MOSS-TTSD-v0.5:anna"),
            ("diana", "fnlp/MOSS-TTSD-v0.5:diana"),
        ] {
            let entry = lookup_voice(short).unwrap_or_else(|| panic!("{short} missing"));
            assert_eq!(entry.short, short);
            assert_eq!(entry.wire_voice, wire_voice);
        }
    }

    #[test]
    fn lookup_voice_unknown_returns_none() {
        assert!(lookup_voice("snake_oil").is_none());
        assert!(lookup_voice("nobody").is_none());
        assert!(lookup_voice("").is_none());
        assert!(lookup_voice("ALEX").is_none()); // 大小写敏感
        assert!(lookup_voice("fnlp/MOSS-TTSD-v0.5:alex").is_none()); // 全名不是 short
    }

    #[test]
    fn supported_voice_shorts_is_sorted() {
        // /admin/voices 输出必须稳定 —— 同一份 SUPPORTED_VOICES 跑两次顺序一致
        let first = supported_voice_shorts();
        let second = supported_voice_shorts();
        assert_eq!(first, second);
        // 且已排好序
        let mut sorted = first.clone();
        sorted.sort();
        assert_eq!(first, sorted);
    }

    #[test]
    fn is_supported_voice_accepts_whitelist_and_rejects_others() {
        for (short, _) in SUPPORTED_VOICES.iter() {
            assert!(is_supported_voice(short), "{short} should be supported");
        }
        // 一些典型拒绝 case
        assert!(!is_supported_voice(""));
        assert!(!is_supported_voice("ALEX")); // 大小写敏感 —— 短名都是小写
        assert!(!is_supported_voice("alex "));
        assert!(!is_supported_voice("snake_oil"));
        assert!(!is_supported_voice("FunAudioLLM/CosyVoice2-0.5B:alex")); // 全名不是短名
    }

    #[test]
    fn vllm_omni_request_serializes_extension_fields() {
        let req = TtsRequest {
            input: Some("hello"),
            model: Some("Qwen/Qwen3-TTS-12Hz-1.7B-CustomVoice".into()),
            voice: Some("vivian".into()),
            response_format: Some("pcm".into()),
            speed: Some(1.25),
            task_type: Some("CustomVoice".into()),
            language: Some("English".into()),
            instructions: Some("warm".into()),
            max_new_tokens: Some(128),
            initial_codec_chunk_frames: Some(1),
            non_streaming_mode: Some(false),
            stream: Some(true),
            stream_format: Some("sse".into()),
            ref_audio: Some("https://example.test/ref.wav".into()),
            ref_text: Some("reference".into()),
            x_vector_only_mode: Some(true),
            sample_rate: None,
        };
        let value = serde_json::to_value(req).expect("request should serialize");
        assert_eq!(value["voice"], "vivian");
        assert_eq!(value["task_type"], "CustomVoice");
        assert_eq!(value["stream_format"], "sse");
        assert_eq!(value["initial_codec_chunk_frames"], 1);
        assert_eq!(value["x_vector_only_mode"], true);
    }

    #[tokio::test]
    async fn parses_vllm_speech_audio_sse_events() {
        let input = concat!(
            "event: speech.audio.delta\n",
            "data: {\"type\":\"speech.audio.delta\",\"audio\":\"AQI=\",\"response_format\":\"pcm\"}\n\n",
            "event: speech.audio.done\n",
            "data: {\"type\":\"speech.audio.done\",\"usage\":{\"input_tokens\":1}}\n\n"
        );
        let mut stream = parse_vllm_sse_stream(input.as_bytes());
        let first = stream
            .next()
            .await
            .expect("delta event")
            .expect("valid delta");
        assert_eq!(first.data, vec![1, 2]);
        assert!(!first.is_last);
        let done = stream
            .next()
            .await
            .expect("done event")
            .expect("valid done");
        assert!(done.data.is_empty());
        assert!(done.is_last);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn parses_vllm_speech_audio_error_event() {
        let input = "event: speech.audio.error\ndata: {\"type\":\"speech.audio.error\",\"error\":{\"message\":\"boom\"}}\n\n";
        let mut stream = parse_vllm_sse_stream(input.as_bytes());
        let err = stream.next().await.expect("error event").unwrap_err();
        assert!(err.to_string().contains("boom"));
    }
}
