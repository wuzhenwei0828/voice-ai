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
//!   model: "sensevoice"
//!
//! llm:
//!   model: "deepseek-ai/DeepSeek-V4-Flash"
//!
//! tts:
//!   model: "fnlp/MOSS-TTSD-v0.5"
//!   voice: "fnlp/MOSS-TTSD-v0.5:alex"
//!   response_format: "wav"
//!   stream: true
//!   # sample_rate: 16000          # Hz，None = 走 provider 默认
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
    /// 模型 ID。FunASR SenseVoice 服务仅支持 `sensevoice`；缺省使用该预设。
    #[serde(default = "default_asr_model")]
    pub model: String,
    /// 强制指定语种（auto / zh / en / yue / ja / ko / nospeech）。None = 由模型自动检测
    #[serde(default)]
    pub language: Option<String>,
    /// 响应格式（json | text | verbose_json）。None = 走上游默认（json）
    #[serde(default)]
    pub response_format: Option<String>,
    /// 是否启用说话人分离（首请求 +1~3s 懒加载 cam++ 模型）
    #[serde(default)]
    pub spk: Option<bool>,
    /// 是否在结果文本里保留 `<|zh|><|HAPPY|>` 等 SenseVoice 标签。
    #[serde(default)]
    pub tags: Option<bool>,
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
            model: default_asr_model(),
            language: None,
            response_format: None,
            spk: None,
            tags: None,
        }
    }
}

fn default_asr_model() -> String {
    "sensevoice".to_string()
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
    /// 必填：默认音色**短名**（如 `"alex"`）。
    ///
    /// 白名单见 `client::tts::SUPPORTED_VOICES`（HashMap 形式，构造 HttpTtsClient 时
    /// 校验，不在表里直接 bail）。
    ///
    /// 端侧（前端下拉框 / admin API）可以传另一个 short 覆盖。HttpTtsClient 在
    /// 请求时查 `SUPPORTED_VOICES` 取对应条目的 `VoiceEntry::wire_voice` 原样
    /// 发给 TTS provider —— **这里只填 short**；不要在 yaml 里手写 `<model>:<short>`。
    pub voice: String,
    /// 输出格式（mp3/wav/pcm/opus/aac/flac）
    #[serde(default)]
    pub response_format: String,
    /// 是否请求流式输出（部分 provider 即使收到也返回单段 blob，见 client/tts.rs）
    #[serde(default)]
    pub stream: bool,
    /// 输出采样率（Hz）。None = 走 provider 默认。
    ///
    /// 各 `response_format` 的支持范围 / 默认值（见 `client::tts::supported_sample_rates`）：
    ///   - `opus`             仅 48000
    ///   - `wav` / `pcm`     8000 / 16000 / 24000 / 32000 / 44100（默认 44100）
    ///   - `mp3`              32000 / 44100（默认 44100）
    #[serde(default)]
    pub sample_rate: Option<u32>,
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
            sample_rate: None,
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
    ///   - VOICE_TTS_VOICE                          （tts 段）
    ///   - VOICE_TTS_SAMPLE_RATE                    （tts 段，u32；非法值忽略）
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
    if let Ok(v) = std::env::var(format!("VOICE_{}_LANGUAGE", prefix)) {
        c.language = Some(v);
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_RESPONSE_FORMAT", prefix)) {
        c.response_format = Some(v);
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_SPK", prefix)) {
        if let Some(b) = parse_env_bool(&v) {
            c.spk = Some(b);
        }
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_TAGS", prefix)) {
        if let Some(b) = parse_env_bool(&v) {
            c.tags = Some(b);
        }
    }
}

/// 解析环境变量里的 bool —— 容错 `true`/`false`/`1`/`0`/`yes`/`no`/`on`/`off`（大小写不敏感）；
/// 无法识别返回 None（保留 yaml 里的值，不静默覆盖为 false）
fn parse_env_bool(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
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
    if let Ok(v) = std::env::var(format!("VOICE_{}_SAMPLE_RATE", prefix)) {
        if let Ok(n) = v.trim().parse() {
            c.sample_rate = Some(n);
        } else {
            tracing::warn!(
                target: "voice_server.config",
                env = format!("VOICE_{}_SAMPLE_RATE", prefix),
                raw = %v,
                "TTS sample_rate 环境变量无法解析为 u32，忽略"
            );
        }
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
// 注：ASR 已切到手搓 reqwest（multipart/form-data），不再需要 OpenAIConfig 桥接；
// 仅 LLM 仍走 async-openai SDK，保留 llm_openai / tts_parts。

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
              model: "sensevoice"
            llm:
              model: "deepseek-ai/DeepSeek-V4-Flash"
            tts:
              model: "fnlp/MOSS-TTSD-v0.5"
              voice: "fnlp/MOSS-TTSD-v0.5:alex"
              response_format: "wav"
              stream: true
              sample_rate: 16000
        "#;
        let cfg: VoiceConfig = serde_yaml::from_str(yaml).unwrap();
        let p = cfg.provider.as_ref().unwrap();
        assert_eq!(p.api_base, "https://api.siliconflow.cn/v1");
        let resolved = cfg.asr.resolved(Some(p));
        assert_eq!(resolved.api_base, p.api_base);
        assert!(resolved.headers.contains_key("X-Region"));
        assert_eq!(cfg.tts.sample_rate, Some(16000));
        assert!(cfg.tts.stream);
    }

    #[test]
    fn asr_model_defaults_to_sensevoice() {
        let yaml = r#"
            asr: {}
        "#;
        let cfg: VoiceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.asr.model, "sensevoice");
    }

    #[test]
    fn parse_tts_without_sample_rate_defaults_to_none() {
        // sample_rate 是可选字段 —— 缺省 = None
        let yaml = r#"
            tts:
              model: "m"
              voice: "v"
        "#;
        let cfg: VoiceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.tts.sample_rate, None);
    }

    #[test]
    fn apply_env_tts_sample_rate_override_and_invalid() {
        // 单线程顺序执行两个场景 —— VOICE_TTS_SAMPLE_RATE 是进程级环境变量，
        // 并发跑会和别的 env 测试相互覆盖。
        // 场景 1：合法 u32 覆盖 yaml 默认值（None → Some(24000)）
        std::env::set_var("VOICE_TTS_SAMPLE_RATE", "24000");
        let mut cfg = VoiceConfig::default();
        cfg.apply_env_overrides();
        assert_eq!(cfg.tts.sample_rate, Some(24000));
        std::env::remove_var("VOICE_TTS_SAMPLE_RATE");

        // 场景 2：非法字符串走 warn-忽略路径，sample_rate 保持 None
        std::env::set_var("VOICE_TTS_SAMPLE_RATE", "not-a-number");
        let mut cfg = VoiceConfig::default();
        cfg.apply_env_overrides();
        assert_eq!(cfg.tts.sample_rate, None);
        std::env::remove_var("VOICE_TTS_SAMPLE_RATE");
    }
}
