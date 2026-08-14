//! voice-server 配置（业务部分）
//!
//! `log` 段由 voice-config crate 提供 LogConfig
//! 这里只定义 voice-server 特有的部分：`server` / `asr` / `llm` / `tts`
//!
//! 完整示例：
//! ```yaml
//! log:
//!   level: info
//!   file: ./logs/voice-server.log   # 空 = stdout
//!   format: pretty                   # pretty | json
//!
//! server:
//!   port: 8080
//!   worker_num: 3
//!
//! asr:
//!   kind: http
//!   endpoint: "https://api.example.com/v1/audio/transcriptions"
//!   method: POST                    # GET / POST / PUT（默认 POST）
//!   model: whisper-1                # 模型名（部分服务会读 header X-Model）
//!   authorization: "Bearer sk-xxx"  # 完整 Authorization 值
//!   headers:
//!     X-Custom: foo                 # 自定义额外 header
//!   timeout_ms: 30000
//!
//! llm:
//!   kind: http
//!   endpoint: "https://api.openai.com/v1/chat/completions"
//!   method: POST
//!   model: gpt-4o-mini
//!   authorization: "Bearer sk-xxx"
//!   headers: {}
//!   timeout_ms: 30000
//!
//! tts:
//!   kind: http
//!   endpoint: "https://api.example.com/v1/audio/speech"
//!   method: POST
//!   model: tts-1
//!   authorization: "Bearer sk-xxx"
//!   headers: {}
//!   timeout_ms: 30000
//! ```

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// re-export 给 voice_server 上层用
pub use voice_config::LogConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoiceConfig {
    #[serde(default)]
    pub log: LogConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub asr: ClientConfig,
    #[serde(default)]
    pub llm: ClientConfig,
    #[serde(default)]
    pub tts: ClientConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_server_port")]
    pub port: u16,
    #[serde(default = "default_worker_num")]
    pub worker_num: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: default_server_port(),
            worker_num: default_worker_num(),
        }
    }
}

fn default_server_port() -> u16 {
    8080
}
fn default_worker_num() -> usize {
    3
}

/// 通用 HTTP 客户端配置（asr/llm/tts 复用）
///
/// 多数字段三段都用；`voice` / `response_format` 是 TTS 专用（OpenAI/CosyVoice 等）；
/// ASR/LLM 段设为空即可，不会发出。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    #[serde(default = "default_client_kind")]
    pub kind: String,
    #[serde(default)]
    pub endpoint: String,
    /// 请求路径（如 `/recognize` 或空）；空 = 直接用 endpoint 当完整 URL
    #[serde(default)]
    pub path: String,
    /// HTTP method：POST / GET / PUT（默认 POST）
    #[serde(default = "default_method")]
    pub method: String,
    /// 模型名（OpenAI Whisper、Qwen、CosyVoice 等）；会作为 header `X-Model` 发送 + body 字段
    #[serde(default)]
    pub model: String,
    /// 完整的 Authorization 值（如 `Bearer sk-xxx`）；非空时作为 `Authorization` header
    #[serde(default)]
    pub authorization: String,
    /// 其它自定义 header（用于厂商特有字段）
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    // ===== TTS 专用字段 =====
    /// TTS 发音人/音色（如 OpenAI 的 "alloy"/"nova"/"shimmer"、CosyVoice 的 "zhitian_emo"）
    #[serde(default)]
    pub voice: String,
    /// TTS 输出格式（如 "mp3"/"wav"/"pcm"/"opus"/"aac"/"flac"）；OpenAI 叫 response_format
    #[serde(default)]
    pub response_format: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            kind: default_client_kind(),
            endpoint: String::new(),
            path: String::new(),
            method: default_method(),
            model: String::new(),
            authorization: String::new(),
            headers: HashMap::new(),
            timeout_ms: default_timeout_ms(),
            voice: String::new(),
            response_format: String::new(),
        }
    }
}

fn default_client_kind() -> String {
    "http".into()
}
fn default_method() -> String {
    "POST".into()
}
fn default_timeout_ms() -> u64 {
    30_000
}

impl VoiceConfig {
    /// 用环境变量覆盖 config：
    ///   - VOICE_LOG_LEVEL / VOICE_LOG_FILE （log 段）
    ///   - VOICE_PORT                          （server 段）
    ///   - VOICE_<ASR|LLM|TTS>_URL             （endpoint）
    ///   - VOICE_<ASR|LLM|TTS>_AUTHORIZATION   （authorization）
    ///   - VOICE_<ASR|LLM|TTS>_MODEL           （model）
    pub fn apply_env_overrides(&mut self) {
        self.log.apply_env_overrides();
        if let Ok(v) = std::env::var("VOICE_PORT") {
            if let Ok(p) = v.parse() {
                self.server.port = p;
            }
        }
        apply_client_env(&mut self.asr, "ASR");
        apply_client_env(&mut self.llm, "LLM");
        apply_client_env(&mut self.tts, "TTS");
    }
}

fn apply_client_env(c: &mut ClientConfig, prefix: &str) {
    if let Ok(v) = std::env::var(format!("VOICE_{}_URL", prefix)) {
        c.endpoint = v;
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_AUTHORIZATION", prefix)) {
        c.authorization = v;
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_MODEL", prefix)) {
        c.model = v;
    }
}

impl Default for VoiceConfig {
    fn default() -> Self {
        Self {
            log: LogConfig::default(),
            server: ServerConfig::default(),
            asr: ClientConfig::default(),
            llm: ClientConfig::default(),
            tts: ClientConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_with_auth_headers() {
        let yaml = r#"
            asr:
              endpoint: "https://api.example.com/asr"
              method: POST
              model: whisper-1
              authorization: "Bearer sk-test"
              headers:
                X-Region: cn-beijing
            llm:
              model: gpt-4o
            tts:
              model: cosyvoice
        "#;
        let cfg: VoiceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.asr.method, "POST");
        assert_eq!(cfg.asr.model, "whisper-1");
        assert_eq!(cfg.asr.authorization, "Bearer sk-test");
        assert_eq!(cfg.asr.headers.get("X-Region").unwrap(), "cn-beijing");
        assert_eq!(cfg.llm.model, "gpt-4o");
        assert_eq!(cfg.tts.model, "cosyvoice");
    }

    #[test]
    fn defaults_when_empty() {
        let cfg = VoiceConfig::default();
        assert_eq!(cfg.log.level, "info");
        assert_eq!(cfg.server.port, 8080);
        assert_eq!(cfg.asr.kind, "http");
        assert_eq!(cfg.asr.method, "POST");
        assert_eq!(cfg.asr.timeout_ms, 30_000);
        assert!(cfg.asr.headers.is_empty());
    }
}