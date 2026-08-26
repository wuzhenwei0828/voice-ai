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
use tracing::{info, warn};

use crate::client::error::{parse_openai_error, ClientError};
use crate::config::{ProviderConfig, TtsConfig};
use crate::session::TtsEvent;

pub type BoxStream<T> = Pin<Box<dyn futures_util::Stream<Item = T> + Send>>;
pub type ArcTts = Arc<dyn TtsClient>;

#[async_trait]
pub trait TtsClient: Send + Sync {
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
    /// 配置默认音色的**短名**（如 `"alex"`）。HttpTtsClient 内部会拼成 `"{model}:{voice_short}"`。
    voice_short: String,
    response_format: String,
    stream: bool,
    /// 输出采样率（Hz）。None = 不在请求里发 `sample_rate`，由 provider 自行决定。
    sample_rate: Option<u32>,
    extra_headers: HeaderMap,
    timeout: Duration,
    client: reqwest::Client,
}

impl HttpTtsClient {
    /// 用配置里的默认音色短名构造 HttpTtsClient。
    ///
    /// 构造期就校验 `voice_short` 是否在 [`SUPPORTED_VOICES`] 白名单里 —— 启动失败时
    /// 就崩，不留到运行时才发现 yaml 配错。
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
        extra_headers: HeaderMap,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        if !is_supported_voice(&voice_short) {
            anyhow::bail!(
                "TTS 默认音色 '{}' 不在白名单 {:?} 中；请修改 yaml tts.voice",
                voice_short,
                SUPPORTED_VOICES
            );
        }
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()?;
        Ok(Self {
            base_url,
            path,
            api_key,
            model,
            voice_short,
            response_format,
            stream,
            sample_rate,
            extra_headers,
            timeout,
            client,
        })
    }

    /// 拼出完整的 voice 字符串（`"{model}:{short}"`）—— 内部 helper，单元测试与 synthesize 都用。
    fn full_voice(&self, short_name: &str) -> String {
        format!("{}:{}", self.model, short_name)
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
    fn default_voice_short(&self) -> &str {
        &self.voice_short
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

        // ===== 解析 effective voice（短名 → 拼 model 前缀成全名）=====
        // 优先级：端侧 override 短名 > 配置默认短名
        let effective_voice_short = voice_override.as_deref().unwrap_or(&self.voice_short);
        // 短名校验：端侧 override 必须命中白名单；配置 fallback 已在 HttpTtsClient::new 校验过
        if let Some(short) = voice_override.as_deref() {
            if !is_supported_voice(short) {
                return Err(ClientError::Config(format!(
                    "TTS voice '{}' 不在白名单 {:?} 中",
                    short, SUPPORTED_VOICES
                )));
            }
        }
        let effective_voice = self.full_voice(effective_voice_short);

        let url = if self.path.is_empty() {
            self.base_url.clone()
        } else {
            format!("{}{}", self.base_url, self.path)
        };

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
            #[serde(skip_serializing_if = "Option::is_none")]
            sample_rate: Option<u32>,
        }
        let body = Req {
            input: Some(text),
            model: if self.model.is_empty() { None } else { Some(self.model.clone()) },
            voice: if effective_voice.is_empty() { None } else { Some(effective_voice.clone()) },
            response_format: if self.response_format.is_empty() {
                None
            } else {
                Some(self.response_format.clone())
            },
            stream: if self.stream { Some(true) } else { None },
            sample_rate: effective_sample_rate,
        };
        let mut req = self.client
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
                .map(|(k, v)| {
                    format!(
                        "{}: {}",
                        k,
                        v.to_str().unwrap_or("<binary>")
                    )
                })
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
    let (resolved, path) = cfg.resolved(provider);
    let base_url = resolved.api_base.clone();
    let api_key = resolved.api_key.clone();
    let timeout = resolved.timeout();
    let headers = resolved.to_header_map();

    // 校验 sample_rate 与 response_format 的兼容性（如果用户显式给了 sample_rate）
    if let Some(sr) = cfg.sample_rate {
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
        full_voice_default = %format!("{}:{}", cfg.model, cfg.voice),
        response_format = %cfg.response_format,
        sample_rate = ?cfg.sample_rate,
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
        cfg.sample_rate,
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

/// 支持的 TTS 音色短名列表（**不**含模型前缀）。
///
/// 短名在请求时由端侧（前端 / admin API 调用方）传入；HttpTtsClient 会拼上
/// `model + ":" + 短名` 后再发给 TTS provider。
///
/// 改这张表时，记得同步检查 `config.rs::TtsConfig.voice` 默认值仍是合法短名。
pub const SUPPORTED_VOICES: &[&str] = &[
    "alex",
    "anna",
    "bella",
    "benjamin",
    "charles",
    "claire",
    "david",
    "diana",
];

/// 校验短名是否在 [`SUPPORTED_VOICES`] 白名单里。大小写敏感 —— 短名都是小写。
pub fn is_supported_voice(short_name: &str) -> bool {
    SUPPORTED_VOICES.contains(&short_name)
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
    fn build_tts_client_rejects_voice_not_in_whitelist() {
        // 默认音色短名不在 SUPPORTED_VOICES 白名单里 → 构造期 bail
        let mut cfg = TtsConfig::default();
        cfg.api_base = "http://127.0.0.1:0".to_string();
        cfg.model = "m".to_string();
        cfg.voice = "snake_oil".to_string(); // 不在白名单
        cfg.response_format = "pcm".to_string();

        match build_tts_client(&cfg, None) {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("snake_oil") && msg.contains("白名单"),
                    "expected error to mention the bad voice and the whitelist; got: {}",
                    msg
                );
            }
            Ok(_) => panic!("expected build_tts_client to reject non-whitelist voice"),
        }
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

    // ===== SUPPORTED_VOICES / is_supported_voice =====

    #[test]
    fn supported_voices_matches_doc_list() {
        // 与用户提供的图片一致：alex / anna / bella / benjamin / charles / claire / david / diana
        assert_eq!(
            SUPPORTED_VOICES,
            &[
                "alex",
                "anna",
                "bella",
                "benjamin",
                "charles",
                "claire",
                "david",
                "diana",
            ]
        );
    }

    #[test]
    fn is_supported_voice_accepts_whitelist_and_rejects_others() {
        for v in SUPPORTED_VOICES {
            assert!(is_supported_voice(v), "{v} should be supported");
        }
        // 一些典型拒绝 case
        assert!(!is_supported_voice(""));
        assert!(!is_supported_voice("ALEX")); // 大小写敏感 —— 短名都是小写
        assert!(!is_supported_voice("alex "));
        assert!(!is_supported_voice("snake_oil"));
        assert!(!is_supported_voice("FunAudioLLM/CosyVoice2-0.5B:alex")); // 全名不是短名
    }
}
