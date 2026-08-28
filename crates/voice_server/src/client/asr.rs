//! ASR 客户端：手搓 reqwest multipart（不走 async-openai）
//!
//! ## 为什么不用 async-openai
//! SDK 的 `CreateTranscriptionRequest` 只覆盖 OpenAI-Whisper 标准字段（`file`/`model`/`language`/
//! `response_format`/`prompt`/`temperature`），**没有** FunASR 私有扩展 `spk` / `tags`；
//! SDK 的 multipart 构造也是 internal 的，没法往里塞 FunASR 扩展字段。手搓以透传全部请求参数。
//!
//! ## Wire format
//! 请求：`POST {base_url}/audio/transcriptions`，`multipart/form-data`
//!   - file: 音频字节（filename + content-type）
//!   - model: 模型 ID
//!   - language / response_format / spk / tags: 可选
//! 响应（按 response_format 分支）：
//!   - `json`（默认）：`{"text": "..."}`
//!   - `text`：纯文本 body
//!   - `verbose_json`：`{"text", "language", "duration", "segments": [...]}`，spk=true 时 segments 带 speaker

use async_stream::try_stream;
use async_trait::async_trait;
use reqwest::header::HeaderMap;
use serde::Deserialize;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use crate::client::error::{parse_openai_error, ClientError};
use crate::config::{AsrConfig, ProviderConfig};
use crate::events::AsrEvent;

pub type BoxStream<T> = Pin<Box<dyn futures_util::Stream<Item = T> + Send>>;
pub type ArcAsr = Arc<dyn AsrClient>;

#[async_trait]
pub trait AsrClient: Send + Sync {
    /// 一次性喂入整段音频字节（mock 简化版），输出流式识别事件
    /// `filename` 用于 multipart 上传时告诉上游音频格式（如 "audio.wav" / "audio.pcm"）；
    /// 传 None 时默认 "audio.wav"。
    async fn recognize(
        &self,
        session_id: &str,
        filename: Option<&str>,
        audio: Vec<u8>,
    ) -> Result<BoxStream<Result<AsrEvent, ClientError>>, ClientError>;
}

/// 通用 OpenAI-兼容 ASR 客户端（手搓 reqwest，支持 FunASR 私有扩展）
pub struct HttpAsrClient {
    base_url: String,
    /// OpenAI-compat 路径。FunASR server 通常挂在 `{api_base}/audio/transcriptions`
    /// （config 里的 `api_base` 含 `/v1` 后缀时正好对齐）
    path: String,
    api_key: Option<String>,
    extra_headers: HeaderMap,
    timeout: Duration,
    client: reqwest::Client,
    model: String,
    language: Option<String>,
    response_format: Option<String>,
    spk: Option<bool>,
    tags: Option<bool>,
}

impl HttpAsrClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        base_url: String,
        path: String,
        api_key: Option<String>,
        extra_headers: HeaderMap,
        timeout: Duration,
        model: String,
        language: Option<String>,
        response_format: Option<String>,
        spk: Option<bool>,
        tags: Option<bool>,
    ) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder().timeout(timeout).build()?;
        Ok(Self {
            base_url,
            path,
            api_key,
            extra_headers,
            timeout,
            client,
            model,
            language,
            response_format,
            spk,
            tags,
        })
    }

    /// 推一个可选字段到 multipart —— None 时不输出，节省 payload 字节
    fn push_opt_text(form: reqwest::multipart::Form, name: &str, val: Option<&str>) -> reqwest::multipart::Form {
        match val {
            Some(v) if !v.is_empty() => form.text(name.to_string(), v.to_string()),
            _ => form,
        }
    }

    fn push_opt_bool(form: reqwest::multipart::Form, name: &str, val: Option<bool>) -> reqwest::multipart::Form {
        match val {
            Some(b) => form.text(name.to_string(), b.to_string()),
            None => form,
        }
    }
}

// ===== 响应解析 =====

#[derive(Deserialize)]
struct JsonResponse {
    #[serde(default)]
    text: String,
}

#[derive(Deserialize)]
struct VerboseJsonResponse {
    #[serde(default)]
    text: String,
    #[serde(default)]
    #[allow(dead_code)]
    language: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    duration: Option<f64>,
    #[serde(default)]
    #[allow(dead_code)]
    segments: Option<Vec<serde_json::Value>>,
}

/// 按 response_format 提取 `text` 字段；任何格式都收敛到 `AsrEvent { text, is_final: true }`
fn extract_text(body: &str, response_format: Option<&str>) -> Result<String, ClientError> {
    match response_format.unwrap_or("json") {
        "text" => Ok(body.trim().to_string()),
        "json" => {
            let r: JsonResponse = serde_json::from_str(body)
                .map_err(|e| ClientError::Decode(format!("decode ASR json response: {}", e)))?;
            Ok(r.text)
        }
        "verbose_json" => {
            let r: VerboseJsonResponse = serde_json::from_str(body)
                .map_err(|e| ClientError::Decode(format!("decode ASR verbose_json response: {}", e)))?;
            Ok(r.text)
        }
        other => Err(ClientError::Decode(format!(
            "unsupported response_format: {}",
            other
        ))),
    }
}

/// 按文件名后缀猜 mime —— 上游 funasr-server 用 ffmpeg 解码，content-type 只是 hint，
/// 主要靠 `filename` 字段决定。给一个合理猜测避免 multipart 默认的 application/octet-stream。
fn guess_audio_mime(filename: &str) -> &'static str {
    let lower = filename.to_ascii_lowercase();
    if lower.ends_with(".wav") {
        "audio/wav"
    } else if lower.ends_with(".mp3") {
        "audio/mpeg"
    } else if lower.ends_with(".flac") {
        "audio/flac"
    } else if lower.ends_with(".ogg") {
        "audio/ogg"
    } else if lower.ends_with(".m4a") {
        "audio/mp4"
    } else if lower.ends_with(".webm") {
        "audio/webm"
    } else {
        "application/octet-stream"
    }
}

#[async_trait]
impl AsrClient for HttpAsrClient {
    async fn recognize(
        &self,
        session_id: &str,
        filename: Option<&str>,
        audio: Vec<u8>,
    ) -> Result<BoxStream<Result<AsrEvent, ClientError>>, ClientError> {
        let url = if self.path.is_empty() {
            self.base_url.clone()
        } else {
            format!("{}{}", self.base_url, self.path)
        };
        let upload_name = filename.unwrap_or("audio.wav").to_string();

        // ===== multipart 组装 =====
        let file_part = reqwest::multipart::Part::bytes(audio.clone())
            .file_name(upload_name.clone())
            .mime_str(guess_audio_mime(&upload_name))
            .map_err(|e| ClientError::Http(format!("invalid mime: {}", e)))?;

        let mut form = reqwest::multipart::Form::new()
            .text("model", self.model.clone())
            .part("file", file_part);
        form = Self::push_opt_text(form, "language", self.language.as_deref());
        form = Self::push_opt_text(form, "response_format", self.response_format.as_deref());
        form = Self::push_opt_bool(form, "spk", self.spk);
        form = Self::push_opt_bool(form, "tags", self.tags);

        // ===== 请求构造（鉴权 + extra_headers，照搬 tts.rs 模式）=====
        let mut req = self.client.post(&url).multipart(form);
        if let Some(key) = &self.api_key {
            if key.starts_with("Bearer ") || key.starts_with("bearer ") {
                req = req.header("Authorization", key);
            } else {
                req = req.header("Authorization", format!("Bearer {}", key));
            }
        }
        for (name, value) in &self.extra_headers {
            req = req.header(name, value);
        }

        // ===== 请求日志 =====
        info!(
            target: "voice_server.asr",
            session_id,
            method = "POST",
            url = %url,
            bytes = audio.len(),
            model = %self.model,
            filename = %upload_name,
            language = self.language.as_deref().unwrap_or(""),
            response_format = self.response_format.as_deref().unwrap_or(""),
            spk = self.spk.unwrap_or(false),
            tags = self.tags.unwrap_or(false),
            api_key_present = self.api_key.is_some(),
            extra_headers_count = self.extra_headers.len(),
            timeout_ms = self.timeout.as_millis() as u64,
            "ASR 请求即将发送 (multipart, {} bytes)", audio.len()
        );

        // ===== 发送 =====
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                let status = e.status().map(|s| s.as_u16()).unwrap_or(0);
                warn!(
                    target: "voice_server.asr.err",
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
                    "ASR 请求发送失败（连接/传输层）"
                );
                return Err(ClientError::Http(e.to_string()));
            }
        };

        // ===== 非 2xx → 抓诊断信息（照搬 tts.rs:243-254）=====
        let status = resp.status();
        if !status.is_success() {
            let status_u16 = status.as_u16();
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
                        target: "voice_server.asr.err",
                        session_id,
                        url = %url,
                        status = status_u16,
                        error = %e,
                        "ASR 非 2xx body 读取失败"
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
                target: "voice_server.asr.err",
                session_id,
                url = %url,
                status = status_u16,
                content_type = %content_type,
                request_id = %request_id,
                headers = %headers_dump,
                body = %body_preview,
                "ASR 返回非 2xx"
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

        // ===== 2xx：按 response_format 解析 =====
        let response_format = self.response_format.clone();
        let raw = resp
            .text()
            .await
            .map_err(|e| ClientError::Http(e.to_string()))?;

        // 原始 body 日志（debug 排查用）
        info!(
            target: "voice_server.asr.resp",
            session_id,
            body_len = raw.chars().count(),
            body_preview = %raw.chars().take(512).collect::<String>(),
            "ASR 原始响应 body"
        );

        let text = extract_text(&raw, response_format.as_deref())?;
        info!(
            target: "voice_server.asr",
            session_id,
            text_len = text.chars().count(),
            text = %text,
            "ASR 识别完成"
        );

        // OpenAI-Whisper ASR 返回单 JSON，非流式
        // 包装成单元素 stream（session.rs 已有逻辑适配）
        let stream = try_stream! {
            yield AsrEvent {
                text,
                is_final: true,
                // 预留字段 —— verbose_json 的 language/duration/segments 目前未消费，
                // extract_text 也只返回 text（参见 asr.rs:137）。后续要启用时把
                // extract_text 换成返回完整结构体即可。
                language: None,
                duration: None,
                segments: None,
            };
        };
        Ok(Box::pin(stream))
    }
}

pub fn build_asr_client(
    cfg: &AsrConfig,
    provider: Option<&ProviderConfig>,
) -> anyhow::Result<Arc<dyn AsrClient>> {
    let resolved = cfg.resolved(provider);
    let base_url = resolved.api_base.clone();
    let api_key = if resolved.api_key.is_empty() {
        None
    } else {
        Some(resolved.api_key.clone())
    };
    let timeout = resolved.timeout();
    let headers = resolved.to_header_map();

    tracing::info!(
        target: "voice_server.factory",
        kind = "http",
        base_url = %base_url,
        path = "/audio/transcriptions",
        model = %cfg.model,
        language = cfg.language.as_deref().unwrap_or(""),
        response_format = cfg.response_format.as_deref().unwrap_or(""),
        spk = cfg.spk.unwrap_or(false),
        tags = cfg.tags.unwrap_or(false),
        "构造 HttpAsrClient (reqwest multipart)"
    );

    Ok(Arc::new(HttpAsrClient::new(
        base_url,
        "/audio/transcriptions".to_string(),
        api_key,
        headers,
        timeout,
        cfg.model.clone(),
        cfg.language.clone(),
        cfg.response_format.clone(),
        cfg.spk,
        cfg.tags,
    )?))
}

// 注：上一版 `asr.headers` 配置项 / OpenAIError 分类的注释已删除 —— 见当前 `extra_headers`
// 与 5 类 reqwest::Error 分类（is_timeout/is_connect/is_request/is_body/is_decode）。

/// 把裸 PCM（s16le，sample_rate Hz，channels 通道）包成 44 字节 RIFF/WAVE 头。
///
/// 上游 siliconflow / OpenAI 兼容 ASR 端点按 multipart 文件名后缀选解码器：
///   - filename=`audio.pcm` → 部分 provider 不认，仍然 500
///   - filename=`audio.wav` → 按 RIFF/WAVE 解析，要求完整 44 字节头 + fmt/data chunk
///
/// 浏览器 / WS pipeline 攒的是裸 PCM 字节，没有 RIFF 头，会被上游当损坏 WAV 拒掉。
/// 在 boundary 处包一层头是硅基流动 / OpenAI 官方 / Azure ASR 都吃的稳妥格式。
pub fn wrap_pcm_as_wav(pcm: &[u8], sample_rate: u32, channels: u16) -> Vec<u8> {
    let data_len = pcm.len() as u32;
    let byte_rate = sample_rate * channels as u32 * 2; // s16le = 2 bytes/sample
    let block_align = channels * 2;
    let mut out = Vec::with_capacity(44 + pcm.len());
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&(36 + data_len).to_le_bytes()); // RIFF chunk size
    out.extend_from_slice(b"WAVE");
    out.extend_from_slice(b"fmt ");
    out.extend_from_slice(&16u32.to_le_bytes()); // fmt chunk size = 16 (PCM)
    out.extend_from_slice(&1u16.to_le_bytes()); // format = 1 (PCM)
    out.extend_from_slice(&channels.to_le_bytes());
    out.extend_from_slice(&sample_rate.to_le_bytes());
    out.extend_from_slice(&byte_rate.to_le_bytes());
    out.extend_from_slice(&block_align.to_le_bytes());
    out.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_len.to_le_bytes());
    out.extend_from_slice(pcm);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wav_header_44_bytes() {
        let pcm = vec![0u8; 320]; // 20ms @ 16kHz mono s16le
        let wav = wrap_pcm_as_wav(&pcm, 16000, 1);
        assert_eq!(wav.len(), 44 + 320);
        assert_eq!(&wav[0..4], b"RIFF");
        assert_eq!(&wav[8..12], b"WAVE");
        assert_eq!(&wav[12..16], b"fmt ");
        assert_eq!(&wav[36..40], b"data");
        // fmt fields
        assert_eq!(u16::from_le_bytes([wav[20], wav[21]]), 1); // PCM
        assert_eq!(u16::from_le_bytes([wav[22], wav[23]]), 1); // channels
        assert_eq!(u32::from_le_bytes([wav[24], wav[25], wav[26], wav[27]]), 16000);
        assert_eq!(u16::from_le_bytes([wav[34], wav[35]]), 16); // bits
        // dataLen
        assert_eq!(u32::from_le_bytes([wav[40], wav[41], wav[42], wav[43]]), 320);
        // data follows
        assert_eq!(&wav[44..], &pcm[..]);
    }

    #[test]
    fn guess_audio_mime_known_extensions() {
        assert_eq!(guess_audio_mime("audio.wav"), "audio/wav");
        assert_eq!(guess_audio_mime("audio.mp3"), "audio/mpeg");
        assert_eq!(guess_audio_mime("audio.flac"), "audio/flac");
        assert_eq!(guess_audio_mime("audio.ogg"), "audio/ogg");
        assert_eq!(guess_audio_mime("audio.m4a"), "audio/mp4");
        assert_eq!(guess_audio_mime("audio.webm"), "audio/webm");
        // 大小写不敏感
        assert_eq!(guess_audio_mime("AUDIO.WAV"), "audio/wav");
        // 未知后缀兜底
        assert_eq!(guess_audio_mime("audio.bin"), "application/octet-stream");
        assert_eq!(guess_audio_mime("noext"), "application/octet-stream");
    }

    #[test]
    fn extract_text_json_format() {
        let body = r#"{"text": "你好"}"#;
        assert_eq!(extract_text(body, Some("json")).unwrap(), "你好");
    }

    #[test]
    fn extract_text_json_default_format() {
        // 缺省 response_format 视为 json
        let body = r#"{"text": "hello"}"#;
        assert_eq!(extract_text(body, None).unwrap(), "hello");
    }

    #[test]
    fn extract_text_text_format() {
        // text/plain 格式：body 即文本
        let body = "你好世界";
        assert_eq!(extract_text(body, Some("text")).unwrap(), "你好世界");
        // 带尾随空白也 trim
        let body = "  hello\n";
        assert_eq!(extract_text(body, Some("text")).unwrap(), "hello");
    }

    #[test]
    fn extract_text_verbose_json_format() {
        let body = r#"{
            "task": "transcribe",
            "language": "zh",
            "duration": 2.37,
            "text": "你好，世界。",
            "segments": [{"id": 0, "start": 0.0, "end": 2.37, "text": "你好，世界。", "words": []}]
        }"#;
        assert_eq!(extract_text(body, Some("verbose_json")).unwrap(), "你好，世界。");
    }

    #[test]
    fn extract_text_verbose_json_with_spk_segment() {
        // spk=true 时 segments 带 speaker 字段，必须能解析
        let body = r#"{
            "text": "spk0 says hi, spk1 says bye",
            "language": "en",
            "duration": 3.0,
            "segments": [
                {"id": 0, "start": 0.0, "end": 1.5, "text": "spk0 says hi", "speaker": "spk0", "words": []},
                {"id": 1, "start": 1.5, "end": 3.0, "text": "spk1 says bye", "speaker": "spk1", "words": []}
            ]
        }"#;
        assert_eq!(
            extract_text(body, Some("verbose_json")).unwrap(),
            "spk0 says hi, spk1 says bye"
        );
    }

    #[test]
    fn extract_text_unsupported_format_errors() {
        let body = "anything";
        assert!(extract_text(body, Some("xml")).is_err());
    }

    #[test]
    fn extract_text_invalid_json_errors() {
        let body = "{not json}";
        assert!(extract_text(body, Some("json")).is_err());
        assert!(extract_text(body, Some("verbose_json")).is_err());
    }

    #[test]
    fn push_opt_text_skips_none_and_empty() {
        let form = reqwest::multipart::Form::new().text("k", "v");
        // None → 不动 form
        let _ = HttpAsrClient::push_opt_text(form, "x", None);
        // 空字符串 → 也不动（避免空值噪音）
        let _ = HttpAsrClient::push_opt_text(
            reqwest::multipart::Form::new(),
            "x",
            Some(""),
        );
        // 这里只能验证不 panic；form 内部状态不可直接 inspect
    }

    #[test]
    fn push_opt_bool_skips_none() {
        let _ = HttpAsrClient::push_opt_bool(reqwest::multipart::Form::new(), "spk", None);
        let _ = HttpAsrClient::push_opt_bool(reqwest::multipart::Form::new(), "spk", Some(true));
    }
}
