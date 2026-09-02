//! voice-server 配置（业务部分）
//!
//! `log` 段由本 crate 的 `logging` 模块提供 LogConfig
//! `server` 段：监听端口 / worker 数
//! `asr` / `llm` / `tts` 段：每个 section 直接对应一个 client
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
//!   api_key: "sk-eqrxgvn..."                    # 裸 token；各 client 映射为目标鉴权 header
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
//!   voice: "vivian"
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
    pub llm_strong: Option<LlmConfig>,
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
    /// 裸 API token（如 "sk-xxx"），由具体 client 映射为目标鉴权 header
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
            api_base: if self.api_base.is_empty() {
                p.api_base
            } else {
                self.api_base.clone()
            },
            api_key: if self.api_key.is_empty() {
                p.api_key
            } else {
                self.api_key.clone()
            },
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
    #[serde(default)]
    pub max_completion_tokens: Option<u32>,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub top_p: Option<f64>,
    #[serde(default)]
    pub top_k: Option<u32>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub include_usage: bool,
}

impl LlmConfig {
    pub fn resolved(&self, provider: Option<&ProviderConfig>) -> ProviderConfig {
        let p = provider.cloned().unwrap_or_default();
        ProviderConfig {
            api_base: if self.api_base.is_empty() {
                p.api_base
            } else {
                self.api_base.clone()
            },
            api_key: if self.api_key.is_empty() {
                p.api_key
            } else {
                self.api_key.clone()
            },
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
            max_completion_tokens: None,
            temperature: None,
            top_p: None,
            top_k: None,
            reasoning_effort: None,
            include_usage: false,
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
    #[serde(default)]
    pub model: String,
    /// 默认音色名称（模型相关，例如 vLLM-Omni Qwen3-TTS 的 `vivian`）。
    /// 端侧可以传另一个 voice 覆盖。
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
    /// 输出声道数，默认单声道。
    #[serde(default = "default_tts_channels")]
    pub channels: u8,
    #[serde(default)]
    /// 播放速度（0.25-4.0），None 使用服务端默认值 1.0。
    pub speed: Option<f32>,
    #[serde(default)]
    /// TTS 任务类型：CustomVoice / VoiceDesign / Base。
    pub task_type: Option<String>,
    #[serde(default)]
    /// 合成语言，例如 Chinese、English 或 Auto。
    pub language: Option<String>,
    #[serde(default)]
    /// 声音风格、情绪等自然语言指令。
    pub instructions: Option<String>,
    #[serde(default)]
    /// 最大生成 token 数，服务端默认 2048。
    pub max_new_tokens: Option<u32>,
    #[serde(default)]
    /// 首个 codec 音频块的帧数，用于 TTFA 调优。
    pub initial_codec_chunk_frames: Option<u32>,
    #[serde(default)]
    /// Qwen3-TTS prompt 构造模式覆盖值。
    pub non_streaming_mode: Option<bool>,
    #[serde(default)]
    /// 流式格式：sse 或 audio。
    pub stream_format: Option<String>,
    #[serde(default)]
    /// Base 任务参考音频 URL、Data URL 或 file URI。
    pub ref_audio: Option<String>,
    #[serde(default)]
    /// 参考音频文字转写。
    pub ref_text: Option<String>,
    #[serde(default)]
    /// 仅使用说话人 embedding，不进行 ICL。
    pub x_vector_only_mode: Option<bool>,
    #[serde(default)]
    /// TTS 传输方式：`http`（默认）或 `websocket`。
    pub transport: String,
    #[serde(default = "default_tts_max_connections")]
    /// WebSocket TTS 连接池最大并发连接数。
    pub max_connections: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct TtsModelFormat {
    #[serde(default)]
    pub sample_rate: Option<u32>,
    #[serde(default = "default_tts_channels")]
    pub channels: u8,
}

fn default_tts_channels() -> u8 { 1 }

fn default_tts_max_connections() -> usize { 4 }

/// 模型专属的音频输出格式。
///
/// 新增或调整模型时，只改这里的对应条目；不需要在 YAML 配置中重复填写。
const TTS_MODEL_FORMATS: &[(&str, TtsModelFormat)] = &[
    (
        "Qwen/Qwen3-TTS-12Hz-1.7B-CustomVoice",
        TtsModelFormat { sample_rate: Some(24000), channels: 1 },
    ),
];

impl TtsConfig {
    pub fn model_format(&self) -> TtsModelFormat {
        TTS_MODEL_FORMATS
            .iter()
            .find_map(|(model, format)| (*model == self.model).then_some(*format))
            .unwrap_or(TtsModelFormat { sample_rate: self.sample_rate, channels: self.channels })
    }
}

impl TtsConfig {
    /// 返回 (ProviderConfig, path)，path 是 OpenAI 标准 TTS 路径
    pub fn resolved(&self, provider: Option<&ProviderConfig>) -> (ProviderConfig, String) {
        let p = provider.cloned().unwrap_or_default();
        (
            ProviderConfig {
                api_base: if self.api_base.is_empty() {
                    p.api_base
                } else {
                    self.api_base.clone()
                },
                api_key: if self.api_key.is_empty() {
                    p.api_key
                } else {
                    self.api_key.clone()
                },
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

    /// 返回实际使用的 TTS 传输类型。
    pub fn transport_kind(&self) -> &'static str {
        if self.transport.eq_ignore_ascii_case("websocket") {
            "websocket"
        } else {
            "http"
        }
    }

    /// 返回按传输类型转换后的完整 TTS endpoint，供日志和客户端工厂共用。
    pub fn resolved_endpoint(&self, provider: Option<&ProviderConfig>) -> String {
        let (resolved, path) = self.resolved(provider);
        let base = resolved.api_base.trim_end_matches('/');
        if self.transport_kind() == "websocket" {
            let ws_base = if base.starts_with("ws://") || base.starts_with("wss://") {
                base.to_string()
            } else {
                base.replace("https://", "wss://")
                    .replace("http://", "ws://")
            };
            format!(
                "{}{}",
                ws_base,
                path.replace("/audio/speech", "/audio/speech/stream")
            )
        } else {
            format!("{}{}", base, path)
        }
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
            channels: default_tts_channels(),
            speed: None,
            task_type: None,
            language: None,
            instructions: None,
            max_new_tokens: None,
            initial_codec_chunk_frames: None,
            non_streaming_mode: None,
            stream_format: None,
            ref_audio: None,
            ref_text: None,
            x_vector_only_mode: None,
            transport: "http".into(),
            max_connections: default_tts_max_connections(),
        }
    }
}

impl VoiceConfig {
    /// 用环境变量覆盖 config：
    ///   - VOICE_LOG_LEVEL / VOICE_LOG_FILE       （log 段）
    ///   - HTTP_PORT                               （server 段）
    ///   - VOICE_PROVIDER_API_BASE                 （provider 段）
    ///   - VOICE_PROVIDER_API_KEY                  （provider 段）
    ///   - VOICE_<ASR|LLM|TTS>_API_KEY             （per-section 覆盖）
    ///   - VOICE_<ASR|LLM|TTS>_MODEL               （per-section）
    ///   - VOICE_LLM_{API_BASE,TIMEOUT_MS,HEADERS}  （LLM 连接覆盖；HEADERS 是 JSON object）
    ///   - VOICE_LLM_{MAX_COMPLETION_TOKENS,TEMPERATURE,TOP_P,TOP_K,REASONING_EFFORT,INCLUDE_USAGE}
    ///   - VOICE_LLM_STRONG_*                      （strong LLM 独立覆盖）
    ///   - VOICE_TTS_VOICE                          （tts 段）
    ///   - VOICE_TTS_SAMPLE_RATE                    （tts 段，u32；非法值忽略）
    ///   - VOICE_REDIS_URL                          （redis 顶层段，连接 URL）
    ///   - VOICE_REDIS_DEFAULT_TTL_SECS             （redis 顶层段，默认 TTL）
    ///   - VOICE_AGENT_MEMORY_BACKEND              （agent 段，in_memory | redis）
    ///   - VOICE_AGENT_MEMORY_WINDOW               （agent 段，滑动窗口容量）
    ///   - VOICE_AGENT_MEMORY_TTL_SECS             （agent 段，TTL 覆盖）
    pub fn apply_env_overrides(&mut self) {
        self.log.apply_env_overrides();
        if let Ok(v) = std::env::var("HTTP_PORT") {
            if let Ok(p) = v.parse() {
                self.server.port = p;
            }
        }
        let provider_api_base = std::env::var("VOICE_PROVIDER_API_BASE").ok();
        let provider_api_key = std::env::var("VOICE_PROVIDER_API_KEY").ok();
        if provider_api_base.is_some() || provider_api_key.is_some() {
            let p = self.provider.get_or_insert_with(ProviderConfig::default);
            if let Some(v) = provider_api_base {
                p.api_base = v;
            }
            if let Some(v) = provider_api_key {
                p.api_key = v;
            }
        }
        apply_section_env(&mut self.asr, "ASR");
        apply_section_env_llm(&mut self.llm, "LLM");
        if self.llm_strong.is_some() || has_llm_env("LLM_STRONG") {
            let strong = self.llm_strong.get_or_insert_with(|| self.llm.clone());
            apply_section_env_llm(strong, "LLM_STRONG");
        }
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
    if let Ok(v) = std::env::var(format!("VOICE_{}_API_BASE", prefix)) {
        c.api_base = v;
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_API_KEY", prefix)) {
        c.api_key = v;
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_TIMEOUT_MS", prefix)) {
        match v.trim().parse() {
            Ok(n) => c.timeout_ms = Some(n),
            Err(_) => tracing::warn!(
                target: "voice_server.config",
                env = format!("VOICE_{}_TIMEOUT_MS", prefix),
                raw = %v,
                "LLM timeout_ms 环境变量无法解析为 u64，忽略"
            ),
        }
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_HEADERS", prefix)) {
        match serde_json::from_str::<HashMap<String, String>>(&v) {
            Ok(headers) => c.headers = headers,
            Err(e) => tracing::warn!(
                target: "voice_server.config",
                env = format!("VOICE_{}_HEADERS", prefix),
                error = %e,
                "LLM headers 环境变量不是 JSON 字符串对象，忽略"
            ),
        }
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_MODEL", prefix)) {
        c.model = v;
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_MAX_COMPLETION_TOKENS", prefix)) {
        match v.trim().parse() {
            Ok(n) => c.max_completion_tokens = Some(n),
            Err(_) => tracing::warn!(
                target: "voice_server.config",
                env = format!("VOICE_{}_MAX_COMPLETION_TOKENS", prefix),
                raw = %v,
                "LLM max_completion_tokens 环境变量无法解析为 u32，忽略"
            ),
        }
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_TEMPERATURE", prefix)) {
        match v.trim().parse() {
            Ok(n) => c.temperature = Some(n),
            Err(_) => tracing::warn!(
                target: "voice_server.config",
                env = format!("VOICE_{}_TEMPERATURE", prefix),
                raw = %v,
                "LLM temperature 环境变量无法解析为浮点数，忽略"
            ),
        }
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_TOP_P", prefix)) {
        match v.trim().parse() {
            Ok(n) => c.top_p = Some(n),
            Err(_) => tracing::warn!(
                target: "voice_server.config",
                env = format!("VOICE_{}_TOP_P", prefix),
                raw = %v,
                "LLM top_p 环境变量无法解析为浮点数，忽略"
            ),
        }
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_TOP_K", prefix)) {
        match v.trim().parse() {
            Ok(n) => c.top_k = Some(n),
            Err(_) => tracing::warn!(
                target: "voice_server.config",
                env = format!("VOICE_{}_TOP_K", prefix),
                raw = %v,
                "LLM top_k 环境变量无法解析为 u32，忽略"
            ),
        }
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_REASONING_EFFORT", prefix)) {
        c.reasoning_effort = Some(v);
    }
    if let Ok(v) = std::env::var(format!("VOICE_{}_INCLUDE_USAGE", prefix)) {
        if let Some(include_usage) = parse_env_bool(&v) {
            c.include_usage = include_usage;
        }
    }
}

fn has_llm_env(prefix: &str) -> bool {
    [
        "API_BASE",
        "API_KEY",
        "TIMEOUT_MS",
        "HEADERS",
        "MODEL",
        "MAX_COMPLETION_TOKENS",
        "TEMPERATURE",
        "TOP_P",
        "TOP_K",
        "REASONING_EFFORT",
        "INCLUDE_USAGE",
    ]
    .iter()
    .any(|suffix| std::env::var_os(format!("VOICE_{prefix}_{suffix}")).is_some())
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
            llm_strong: None,
            tts: TtsConfig::default(),
            redis: RedisConfig::default(),
            agent: AgentConfig::default(),
        }
    }
}

// ===== OpenAIConfig 兼容桥接 =====
// 当前 client 已直接使用 reqwest；保留这些 public helper，避免破坏现有外部调用。

pub fn llm_openai(cfg: &LlmConfig, provider: Option<&ProviderConfig>) -> OpenAIConfig {
    cfg.resolved(provider).to_openai_config()
}
pub fn tts_parts(cfg: &TtsConfig, provider: Option<&ProviderConfig>) -> (OpenAIConfig, String) {
    // TTS 走手搓 reqwest，不直接用 OpenAIConfig（但我们仍然用其 api_base 字段语义）
    let (resolved, path) = cfg.resolved(provider);
    (resolved.to_openai_config(), path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static LLM_ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard(Vec<&'static str>);

    impl EnvGuard {
        fn new(keys: Vec<&'static str>) -> Self {
            for key in &keys {
                std::env::remove_var(key);
            }
            Self(keys)
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for key in &self.0 {
                std::env::remove_var(key);
            }
        }
    }

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
    fn parses_llm_generation_options() {
        let yaml = r#"
            llm:
              model: "test-model"
              max_completion_tokens: 512
              temperature: 0.4
              top_p: 0.9
              reasoning_effort: "low"
              include_usage: true
        "#;

        let cfg: VoiceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.llm.max_completion_tokens, Some(512));
        assert_eq!(cfg.llm.temperature, Some(0.4));
        assert_eq!(cfg.llm.top_p, Some(0.9));
        assert_eq!(cfg.llm.reasoning_effort.as_deref(), Some("low"));
        assert!(cfg.llm.include_usage);
    }

    #[test]
    fn parses_fast_and_strong_llm_config() {
        let yaml = r#"
            llm:
              model: "Qwen/Qwen3-8B"
              top_k: 20
            llm_strong:
              model: "Qwen/Qwen3-30B-A3B"
              top_k: 30
        "#;

        let cfg: VoiceConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(cfg.llm.model, "Qwen/Qwen3-8B");
        assert_eq!(cfg.llm.top_k, Some(20));
        let strong = cfg.llm_strong.as_ref().expect("strong config");
        assert_eq!(strong.model, "Qwen/Qwen3-30B-A3B");
        assert_eq!(strong.top_k, Some(30));
    }

    #[test]
    fn env_only_provider_settings_create_provider() {
        let _guard = EnvGuard::new(vec!["VOICE_PROVIDER_API_BASE", "VOICE_PROVIDER_API_KEY"]);
        std::env::set_var("VOICE_PROVIDER_API_BASE", "https://provider.test/v1");
        std::env::set_var("VOICE_PROVIDER_API_KEY", "provider-key");
        let mut cfg = VoiceConfig::default();

        cfg.apply_env_overrides();

        let provider = cfg.provider.expect("provider should be created from env");
        assert_eq!(provider.api_base, "https://provider.test/v1");
        assert_eq!(provider.api_key, "provider-key");
    }

    #[test]
    fn llm_environment_overrides_all_supported_options() {
        let _lock = LLM_ENV_LOCK.lock().unwrap();
        let keys = vec![
            "VOICE_LLM_API_BASE",
            "VOICE_LLM_API_KEY",
            "VOICE_LLM_TIMEOUT_MS",
            "VOICE_LLM_HEADERS",
            "VOICE_LLM_MODEL",
            "VOICE_LLM_MAX_COMPLETION_TOKENS",
            "VOICE_LLM_TEMPERATURE",
            "VOICE_LLM_TOP_P",
            "VOICE_LLM_TOP_K",
            "VOICE_LLM_REASONING_EFFORT",
            "VOICE_LLM_INCLUDE_USAGE",
        ];
        let _guard = EnvGuard::new(keys);
        std::env::set_var("VOICE_LLM_API_BASE", "https://llm.test/v1");
        std::env::set_var("VOICE_LLM_API_KEY", "llm-key");
        std::env::set_var("VOICE_LLM_TIMEOUT_MS", "45000");
        std::env::set_var("VOICE_LLM_HEADERS", r#"{"X-Tenant":"tenant-a"}"#);
        std::env::set_var("VOICE_LLM_MODEL", "test-model");
        std::env::set_var("VOICE_LLM_MAX_COMPLETION_TOKENS", "384");
        std::env::set_var("VOICE_LLM_TEMPERATURE", "0.3");
        std::env::set_var("VOICE_LLM_TOP_P", "0.8");
        std::env::set_var("VOICE_LLM_TOP_K", "20");
        std::env::set_var("VOICE_LLM_REASONING_EFFORT", "medium");
        std::env::set_var("VOICE_LLM_INCLUDE_USAGE", "true");
        let mut cfg = VoiceConfig::default();

        cfg.apply_env_overrides();

        assert_eq!(cfg.llm.api_base, "https://llm.test/v1");
        assert_eq!(cfg.llm.api_key, "llm-key");
        assert_eq!(cfg.llm.timeout_ms, Some(45000));
        assert_eq!(
            cfg.llm.headers.get("X-Tenant").map(String::as_str),
            Some("tenant-a")
        );
        assert_eq!(cfg.llm.model, "test-model");
        assert_eq!(cfg.llm.max_completion_tokens, Some(384));
        assert_eq!(cfg.llm.temperature, Some(0.3));
        assert_eq!(cfg.llm.top_p, Some(0.8));
        assert_eq!(cfg.llm.top_k, Some(20));
        assert_eq!(cfg.llm.reasoning_effort.as_deref(), Some("medium"));
        assert!(cfg.llm.include_usage);
    }

    #[test]
    fn strong_llm_environment_overrides_do_not_change_fast_config() {
        let _lock = LLM_ENV_LOCK.lock().unwrap();
        let keys = vec![
            "VOICE_LLM_STRONG_MODEL",
            "VOICE_LLM_STRONG_TIMEOUT_MS",
            "VOICE_LLM_STRONG_TOP_K",
        ];
        let _guard = EnvGuard::new(keys);
        std::env::set_var("VOICE_LLM_STRONG_MODEL", "strong-env-model");
        std::env::set_var("VOICE_LLM_STRONG_TIMEOUT_MS", "90000");
        std::env::set_var("VOICE_LLM_STRONG_TOP_K", "40");
        let mut cfg = VoiceConfig {
            llm: LlmConfig {
                model: "fast-model".into(),
                top_k: Some(20),
                ..LlmConfig::default()
            },
            ..VoiceConfig::default()
        };

        cfg.apply_env_overrides();

        assert_eq!(cfg.llm.model, "fast-model");
        assert_eq!(cfg.llm.top_k, Some(20));
        let strong = cfg.llm_strong.as_ref().expect("env creates strong config");
        assert_eq!(strong.model, "strong-env-model");
        assert_eq!(strong.timeout_ms, Some(90000));
        assert_eq!(strong.top_k, Some(40));
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
    fn tts_model_format_is_hardcoded_for_qwen3_tts() {
        let yaml = r#"
            tts:
              model: "Qwen/Qwen3-TTS-12Hz-1.7B-CustomVoice"
              voice: "Vivian"
              sample_rate: 16000
              channels: 1
        "#;
        let cfg: VoiceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.tts.model_format(), TtsModelFormat { sample_rate: Some(24000), channels: 1 });
    }

    #[test]
    fn tts_model_format_falls_back_to_section_defaults_for_unknown_model() {
        let yaml = r#"
            tts:
              model: "example/tts"
              voice: "v"
              sample_rate: 16000
              channels: 2
        "#;
        let cfg: VoiceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.tts.model_format(), TtsModelFormat { sample_rate: Some(16000), channels: 2 });
    }

    #[test]
    fn parse_vllm_omni_tts_options() {
        let yaml = r#"
            tts:
              model: "Qwen/Qwen3-TTS-12Hz-1.7B-CustomVoice"
              voice: "vivian"
              speed: 1.25
              task_type: "CustomVoice"
              language: "English"
              instructions: "warm narrator"
              max_new_tokens: 128
              initial_codec_chunk_frames: 1
              non_streaming_mode: false
              stream_format: "sse"
              ref_audio: "https://example.test/ref.wav"
              ref_text: "reference"
              x_vector_only_mode: true
              transport: "websocket"
        "#;
        let cfg: VoiceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.tts.voice, "vivian");
        assert_eq!(cfg.tts.speed, Some(1.25));
        assert_eq!(cfg.tts.max_new_tokens, Some(128));
        assert_eq!(cfg.tts.stream_format.as_deref(), Some("sse"));
        assert_eq!(cfg.tts.x_vector_only_mode, Some(true));
        assert_eq!(cfg.tts.transport, "websocket");
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

    #[test]
    fn apply_env_uses_http_port_and_ignores_voice_port() {
        std::env::set_var("VOICE_PORT", "9124");
        let mut cfg = VoiceConfig::default();
        cfg.apply_env_overrides();
        assert_eq!(cfg.server.port, 8080);

        std::env::set_var("HTTP_PORT", "9123");
        cfg.apply_env_overrides();
        assert_eq!(cfg.server.port, 9123);

        std::env::remove_var("HTTP_PORT");
        std::env::remove_var("VOICE_PORT");
    }
}
