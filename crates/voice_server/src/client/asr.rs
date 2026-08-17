//! ASR 客户端：基于 `async-openai::audio().transcribe()`
//!
//! Wire format（OpenAI-Whisper 兼容）：multipart/form-data
//!   - file: 音频字节
//!   - model: 模型 ID
//! response: `{"text": "..."}`

use async_openai::config::{Config, OpenAIConfig};
use async_openai::error::OpenAIError;
use async_openai::types::{
    AudioInput, CreateTranscriptionRequest, CreateTranscriptionResponseJson, InputSource,
};
use async_openai::Client;
use async_stream::try_stream;
use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;
use tracing::{info, warn};

use crate::client::error::ClientError;
use crate::config::{asr_openai, AsrConfig, ProviderConfig};
use crate::session::AsrEvent;

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

/// 通用 OpenAI-兼容 ASR 客户端
pub struct HttpAsrClient {
    openai: OpenAIConfig,
    client: Client<OpenAIConfig>,
    model: String,
    // 注意：async-openai 暂不暴露往请求里塞自定义 header 的口子
    // （OpenAIConfig::with_http_client 可以接自定义 reqwest，但用起来与 SDK 其他路径有冲突），
    // 所以 provider / asr.headers 配置项在 ASR / LLM 这里先不消费；TTS 走手搓 reqwest 所以能用。
}

impl HttpAsrClient {
    pub fn new(openai: OpenAIConfig, model: String) -> Self {
        let client = Client::with_config(openai.clone());
        Self { openai, client, model }
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
        // 请求侧：只打 debug（用户当前不需要看请求细节）
        info!(
            target: "voice_server.asr",
            session_id,
            api_base = %self.openai.api_base(),
            bytes = audio.len(),
            model = %self.model,
            filename = %filename.unwrap_or("audio.wav"),
            "ASR 请求 (async-openai transcribe, {} bytes)", audio.len()
        );

        // 用前端上传的原始文件名，siliconflow 等 provider 依此选解码器；
        // 兜底 "audio.wav" 保留旧行为（WS pipeline 是裸 PCM，会沿用兜底）。
        let upload_name = filename.unwrap_or("audio.wav");
        let audio_input = AudioInput {
            source: InputSource::VecU8 {
                filename: upload_name.to_string(),
                vec: audio,
            },
        };
        let req = CreateTranscriptionRequest {
            file: audio_input,
            model: self.model.clone(),
            ..Default::default()
        };

        let resp: CreateTranscriptionResponseJson = match self.client.audio().transcribe(req).await {
            Ok(r) => r,
            Err(e) => {
                let err_text = e.to_string();
                // 按 OpenAIError 变体分类：API 返回的 JSON 错误 / reqwest 传输错误 / 其它
                let (api_message, api_type, api_param, api_code, reqwest_status, is_timeout, is_connect) =
                    match &e {
                        OpenAIError::ApiError(api_err) => (
                            Some(api_err.message.clone()),
                            api_err.r#type.clone(),
                            api_err.param.clone(),
                            api_err.code.clone(),
                            None,
                            false,
                            false,
                        ),
                        OpenAIError::Reqwest(re) => {
                            let s = re.status().map(|s| s.as_u16());
                            (
                                None,
                                None,
                                None,
                                None,
                                s,
                                re.is_timeout(),
                                re.is_connect(),
                            )
                        }
                        _ => (None, None, None, None, None, false, false),
                    };
                warn!(
                    target: "voice_server.asr.err",
                    session_id,
                    api_base = %self.openai.api_base(),
                    model = %self.model,
                    error = %err_text,
                    error_debug = ?e,
                    api_message = api_message.as_deref().unwrap_or(""),
                    api_type = api_type.as_deref().unwrap_or(""),
                    api_param = api_param.as_deref().unwrap_or(""),
                    api_code = api_code.as_deref().unwrap_or(""),
                    reqwest_status = reqwest_status.unwrap_or(0),
                    is_timeout,
                    is_connect,
                    "ASR transcribe 失败"
                );
                return Err(ClientError::Http(err_text));
            }
        };

        // 响应侧：raw JSON + 解析后的 text
        match serde_json::to_string(&resp) {
            Ok(raw) => info!(
                target: "voice_server.asr.resp",
                session_id,
                raw = %raw,
                "ASR 原始响应"
            ),
            Err(e) => warn!(
                target: "voice_server.asr.resp",
                session_id,
                "ASR 响应序列化失败: {}",
                e
            ),
        }
        info!(
            target: "voice_server.asr",
            session_id,
            text_len = resp.text.chars().count(),
            text = %resp.text,
            "ASR 识别完成"
        );

        // OpenAI-Whisper ASR 返回单 JSON `{"text": "..."}`，非流式
        // 包装成单元素 stream（session.rs 已有逻辑适配）
        let stream = try_stream! {
            yield AsrEvent { text: resp.text, is_final: true };
        };
        Ok(Box::pin(stream))
    }
}

// 注：asr.headers 配置项当前不消费 —— 见 HttpAsrClient 字段上的注释。

pub fn build_asr_client(
    cfg: &AsrConfig,
    provider: Option<&ProviderConfig>,
) -> anyhow::Result<Arc<dyn AsrClient>> {
    let resolved = cfg.resolved(provider);
    let openai = asr_openai(cfg, provider);

    tracing::info!(
        target: "voice_server.factory",
        kind = "http",
        api_base = %resolved.api_base,
        model = %cfg.model,
        "构造 HttpAsrClient"
    );

    Ok(Arc::new(HttpAsrClient::new(openai, cfg.model.clone())))
}

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
}
