//! voice-server 配置（业务部分）
//!
//! `log` 段由本 crate 的 `logging` 模块提供 LogConfig
//! `server` 段：监听端口 / worker 数
//! `asr` / `llm` / `tts` 段：每个 section 直接对应一个 client，**复用 async-openai 的 OpenAIConfig**
//! 顶层 `provider` 段：可作为 asr/llm/tts 的连接级默认值（api_base / api_key / timeout / headers）
//!
//! 完整示例：
//! ```yaml
//! log:
//!   level: info
//!   file: ./logs/voice-server.log
//!
//! server:
//!   port: 8080
//!   worker_num: 3
//!
//! provider:
//!   api_base: "https://api.siliconflow.cn/v1"   # 所有 client 默认走这个 base
//!   api_key: "sk-eqrxgvn..."                    # 裸 token（OpenAIConfig 会自动加 Bearer）
//!   timeout_ms: 30000
//!   headers:                                    # 透传给所有 client
//!     X-Region: cn-beijing
//!
//! asr:
//!   model: "FunAudioLLM/SenseVoiceSmall"
//!
//! llm:
//!   model: "deepseek-ai/DeepSeek-V4-Flash"
//!
//! tts:
//!   model: "fnlp/MOSS-TTSD-v0.5"
//!   voice: "fnlp/MOSS-TTSD-v0.5:alex"
//!   response_format: "wav"
//!   stream: true
//! ```

use std::collections::HashMap;
use std::time::Duration;

use async_openai::config::OpenAIConfig;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};

// re-export 给 voice_server 上层用
pub use crate::logging::LogConfig;

/// Redis key 统一前缀。所有 redis 消费者（agent / 未来的 cache / rate-limit 等）
/// 在自己 namespace 后缀前先拼这个。代码里写死，需要换前缀时直接改这里。
pub const REDIS_KEY_PREFIX: &str = "voice:";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoiceConfig {
    #[serde(default)]
    pub log: LogConfig,
    #[serde(default)]
    pub server: ServerConfig,
    /// 可选：顶层 provider 默认配置（asr/llm/tts 会继承这些字段除非自己覆盖）
    #[serde(default)]
    pub provider: Option<ProviderConfig>,
    #[serde(default)]
    pub asr: AsrConfig,
    #[serde(default)]
    pub llm: LlmConfig,
    #[serde(default)]
    pub tts: TtsConfig,
    /// Redis 连接配置（横切：agent / 未来的 cache / rate-limit 等共用）
    #[serde(default)]
    pub redis: RedisConfig,
    /// LLM Agent 配置：短期记忆后端选择 + 窗口容量
    #[serde(default)]
    pub agent: AgentConfig,
}

/// 顶层 Redis 连接配置。所有需要 Redis 的功能（短期记忆、缓存、限流等）
/// 都从这一段拿连接信息和默认 TTL。key 前缀由代码里的 [`REDIS_KEY_PREFIX`] 常量控制。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    /// Redis URL（如 `redis://127.0.0.1:6379/`）。None = 未启用 Redis。
    #[serde(default)]
    pub url: Option<String>,
    /// 各功能未单独指定 TTL 时用这个，默认 3600 秒（1 小时）
    #[serde(default = "default_redis_ttl_secs")]
    pub default_ttl_secs: u64,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: None,
            default_ttl_secs: default_redis_ttl_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    /// 记忆后端：`in_memory`（默认，单进程）| `redis`（跨进程共享）
    #[serde(default = "default_memory_backend")]
    pub memory_backend: String,
    /// 滑动窗口容量（保留最近 N 条对话记录）
    #[serde(default = "default_memory_window")]
    pub memory_window: usize,
    /// 短期记忆 Redis TTL（秒）。None = 用 `redis.default_ttl_secs`。
    /// `memory_backend=in_memory` 时不生效。
    #[serde(default)]
    pub memory_ttl_secs: Option<u64>,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            memory_backend: default_memory_backend(),
            memory_window: default_memory_window(),
            memory_ttl_secs: None,
        }
    }
}

fn default_memory_backend() -> String {
    "in_memory".to_string()
}
fn default_memory_window() -> usize {
    20
}
fn default_redis_ttl_secs() -> u64 {
    3600
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

// ===== Provider / Client 通用配置 =====

/// 顶层 provider：作为 asr/llm/tts 的连接级默认
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    /// OpenAI-兼容 base URL，如 https://api.siliconflow.cn/v1
    /// 客户端会自动拼 OpenAI 标准路径（/audio/transcriptions、/chat/completions、/audio/speech）
    #[serde(default)]
    pub api_base: String,
    /// 裸 API token（如 "sk-xxx"），OpenAIConfig 会自动加 "Bearer "
    /// 也支持完整 "Bearer xxx" 形式（兼容旧 yaml）
    #[serde(default)]
    pub api_key: String,
    /// HTTP 超时（ms）
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// 透传 header（厂商特有字段）
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

impl ProviderConfig {
    /// 构造 async-openai 的 OpenAIConfig
    pub fn to_openai_config(&self) -> OpenAIConfig {
        let mut cfg = OpenAIConfig::new();
        if !self.api_base.is_empty() {
            cfg = cfg.with_api_base(&self.api_base);
        }
        if !self.api_key.is_empty() {
            // 兼容 "Bearer xxx" 形式 —— 剥前缀
            let token = self
                .api_key
                .strip_prefix("Bearer ")
                .or_else(|| self.api_key.strip_prefix("bearer "))
                .unwrap_or(&self.api_key);
            cfg = cfg.with_api_key(token);
        }
        cfg
    }

    pub fn to_header_map(&self) -> HeaderMap {
        let mut hm = HeaderMap::new();
        for (k, v) in &self.headers {
            if let (Ok(name), Ok(value)) = (
                HeaderName::from_bytes(k.as_bytes()),
                HeaderValue::from_str(v),
            ) {
                hm.insert(name, value);
            }
        }
        hm
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            api_base: String::new(),
            api_key: String::new(),
            timeout_ms: default_timeout_ms(),
            headers: HashMap::new(),
        }
    }
}

fn default_timeout_ms() -> u64 {
    30_000
}

// ===== ASR =====

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsrConfig {
    /// 覆盖 ProviderConfig.api_base
    #[serde(default)]
    pub api_base: String,
    /// 覆盖 ProviderConfig.api_key
    #[serde(default)]
    pub api_key: String,
    /// 覆盖 ProviderConfig.timeout_ms
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    /// 覆盖 ProviderConfig.headers
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// 必填：模型 ID
    pub model: String,
}

impl AsrConfig {
    pub fn resolved(&self, provider: Option<&ProviderConfig>) -> ProviderConfig {
        let p = provider.cloned().unwrap_or_default();
        ProviderConfig {
            api_base: if self.api_base.is_empty() { p.api_base } else { self.api_base.clone() },
            api_key: if self.api_key.is_empty() { p.api_key } else { self.api_key.clone() },
            timeout_ms: self.timeout_ms.unwrap_or(p.timeout_ms),
            headers: {
                let mut h = p.headers;
                for (k, v) in &self.headers {
                    h.insert(k.clone(), v.clone());
                }
                h
            },
        }
    }
}

impl Default for AsrConfig {
    fn default() -> Self {
        Self {
            api_base: String::new(),
            api_key: String::new(),
            timeout_ms: None,
            headers: HashMap::new(),
            model: String::new(),
        }
    }
}

// ===== LLM =====

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmConfig {
    #[serde(default)]
    pub api_base: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    pub model: String,
}

impl LlmConfig {
    pub fn resolved(&self, provider: Option<&ProviderConfig>) -> ProviderConfig {
        let p = provider.cloned().unwrap_or_default();
        ProviderConfig {
            api_base: if self.api_base.is_empty() { p.api_base } else { self.api_base.clone() },
            api_key: if self.api_key.is_empty() { p.api_key } else { self.api_key.clone() },
            timeout_ms: self.timeout_ms.unwrap_or(p.timeout_ms),
            headers: {
                let mut h = p.headers;
                for (k, v) in &self.headers {
                    h.insert(k.clone(), v.clone());
                }
                h
            },
        }
    }
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            api_base: String::new(),
            api_key: String::new(),
            timeout_ms: None,
            headers: HashMap::new(),
            model: String::new(),
        }
    }
}

// ===== TTS =====

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtsConfig {
    #[serde(default)]
    pub api_base: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    pub model: String,
    /// 必填：voice / 音色
    pub voice: String,
    /// 输出格式（mp3/wav/pcm/opus/aac/flac）
    #[serde(default)]
    pub response_format: String,
    /// 是否请求流式输出（部分 provider 即使收到也返回单段 blob，见 client/tts.rs）
    #[serde(default)]
    pub stream: bool,
}

impl TtsConfig {
    /// 返回 (ProviderConfig, path)，path 是 OpenAI 标准 TTS 路径
    pub fn resolved(&self, provider: Option<&ProviderConfig>) -> (ProviderConfig, String) {
        let p = provider.cloned().unwrap_or_default();
        (
            ProviderConfig {
                api_base: if self.api_base.is_empty() { p.api_base } else { self.api_base.clone() },
                api_key: if self.api_key.is_empty() { p.api_key } else { self.api_key.clone() },
                timeout_ms: self.timeout_ms.unwrap_or(p.timeout_ms),
                headers: {
                    let mut h = p.headers;
                    for (k, v) in &self.headers {
                        h.insert(k.clone(), v.clone());
                    }
                    h
                },
            },
            "/audio/speech".to_string(),
        )
    }
}

impl Default for TtsConfig {
    fn default() -> Self {
        Self {
            api_base: String::new(),
            api_key: String::new(),
            timeout_ms: None,
            headers: HashMap::new(),
            model: String::new(),
            voice: String::new(),
            response_format: String::new(),
            stream: false,
        }
    }
}

impl VoiceConfig {
    /// 用环境变量覆盖 config：
    ///   - VOICE_LOG_LEVEL / VOICE_LOG_FILE       （log 段）
    ///   - VOICE_PORT                              （server 段）
    ///   - VOICE_PROVIDER_API_BASE                 （provider 段）
    ///   - VOICE_PROVIDER_API_KEY                  （provider 段）
    ///   - VOICE_<ASR|LLM|TTS>_API_KEY             （per-section 覆盖）
    ///   - VOICE_<ASR|LLM|TTS>_MODEL               （per-section）
    ///   - VOICE_REDIS_URL                          （redis 顶层段，连接 URL）
    ///   - VOICE_REDIS_DEFAULT_TTL_SECS             （redis 顶层段，默认 TTL）
    ///   - VOICE_AGENT_MEMORY_BACKEND              （agent 段，in_memory | redis）
    ///   - VOICE_AGENT_MEMORY_WINDOW               （agent 段，滑动窗口容量）
    ///   - VOICE_AGENT_MEMORY_TTL_SECS             （agent 段，TTL 覆盖）
    pub fn apply_env_overrides(&mut self) {
        self.log.apply_env_overrides();
        if let Ok(v) = std::env::var("VOICE_PORT") {
            if let Ok(p) = v.parse() {
                self.server.port = p;
            }
        }
        if let Some(p) = self.provider.as_mut() {
            if let Ok(v) = std::env::var("VOICE_PROVIDER_API_BASE") {
                p.api_base = v;
            }
            if let Ok(v) = std::env::var("VOICE_PROVIDER_API_KEY") {
                p.api_key = v;
            }
        }
        apply_section_env(&mut self.asr, "ASR");
        apply_section_env_llm(&mut self.llm, "LLM");
        apply_section_env_tts(&mut self.tts, "TTS");
        apply_redis_env(&mut self.redis);
        apply_agent_env(&mut self.agent);
    }
}

fn apply_redis_env(c: &mut RedisConfig) {
    if let Ok(v) = std::env::var("VOICE_REDIS_URL") {
        c.url = Some(v);
    }
    if let Ok(v) = std::env::var("VOICE_REDIS_DEFAULT_TTL_SECS") {
        if let Ok(n) = v.parse() {
            c.default_ttl_secs = n;
        }
    }
}

fn apply_agent_env(c: &mut AgentConfig) {
    if let Ok(v) = std::env::var("VOICE_AGENT_MEMORY_BACKEND") {
        c.memory_backend = v;
    }
    if let Ok(v) = std::env::var("VOICE_AGENT_MEMORY_WINDOW") {
        if let Ok(n) = v.parse() {
            c.memory_window = n;
        }
    }
    if let Ok(v) = std::env::var("VOICE_AGENT_MEMORY_TTL_SECS") {
        if let Ok(n) = v.parse() {
            c.memory_ttl_secs = Some(n);
        }
    }
}

fn apply_section_env(c: &mut AsrConfig, prefix: &str) {
    if let Ok(v) = std::env::var(format!("VOICE_{}_API_KEY", prefix)) {
        c.api_key = v;
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_MODEL", prefix)) {
        c.model = v;
    }
}
fn apply_section_env_llm(c: &mut LlmConfig, prefix: &str) {
    if let Ok(v) = std::env::var(format!("VOICE_{}_API_KEY", prefix)) {
        c.api_key = v;
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_MODEL", prefix)) {
        c.model = v;
    }
}
fn apply_section_env_tts(c: &mut TtsConfig, prefix: &str) {
    if let Ok(v) = std::env::var(format!("VOICE_{}_API_KEY", prefix)) {
        c.api_key = v;
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_MODEL", prefix)) {
        c.model = v;
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_VOICE", prefix)) {
        c.voice = v;
    }
}

impl Default for VoiceConfig {
    fn default() -> Self {
        Self {
            log: LogConfig::default(),
            server: ServerConfig::default(),
            provider: None,
            asr: AsrConfig::default(),
            llm: LlmConfig::default(),
            tts: TtsConfig::default(),
            redis: RedisConfig::default(),
            agent: AgentConfig::default(),
        }
    }
}

// ===== 辅助：让 ASR/LLM client 拿到 OpenAIConfig =====

pub fn asr_openai(cfg: &AsrConfig, provider: Option<&ProviderConfig>) -> OpenAIConfig {
    cfg.resolved(provider).to_openai_config()
}
pub fn llm_openai(cfg: &LlmConfig, provider: Option<&ProviderConfig>) -> OpenAIConfig {
    cfg.resolved(provider).to_openai_config()
}
pub fn tts_parts(
    cfg: &TtsConfig,
    provider: Option<&ProviderConfig>,
) -> (OpenAIConfig, String) {
    // TTS 走手搓 reqwest，不直接用 OpenAIConfig（但我们仍然用其 api_base 字段语义）
    let (resolved, path) = cfg.resolved(provider);
    (resolved.to_openai_config(), path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_yaml() {
        let yaml = r#"
            provider:
              api_base: "https://api.siliconflow.cn/v1"
              api_key: "sk-test"
              headers:
                X-Region: cn-beijing
            asr:
              model: "FunAudioLLM/SenseVoiceSmall"
            llm:
              model: "deepseek-ai/DeepSeek-V4-Flash"
            tts:
              model: "fnlp/MOSS-TTSD-v0.5"
              voice: "fnlp/MOSS-TTSD-v0.5:alex"
              response_format: "wav"
              stream: true
        "#;
        let cfg: VoiceConfig = serde_yaml::from_str(yaml).unwrap();
        let p = cfg.provider.as_ref().unwrap();
        assert_eq!(p.api_base, "https://api.siliconflow.cn/v1");
        let resolved = cfg.asr.resolved(Some(p));
        assert_eq!(resolved.api_base, p.api_base);
        assert!(resolved.headers.contains_key("X-Region"));
    }
}
