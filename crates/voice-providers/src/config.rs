//! BailianConfig：从 serde_yaml::Mapping 解析的最小配置
//!
//! voice-providers **不**依赖 voice-server 的 VoiceConfig（依赖方向：voice-server → voice-providers）。
//! PR1 在 voice-server 侧拿到 `cfg.bailian: Option<serde_yaml::Mapping>` 后，把整段 Mapping
//! 序列化 / 透传给本模块的 `BailianConfig::from_mapping`。

use std::time::Duration;

use serde::{Deserialize, Serialize};

// ===== 顶层 =====

#[derive(Debug, Clone)]
pub struct BailianConfig {
    pub ws_endpoint: String,
    pub api_key: String,
    pub pool: PoolConfig,
    pub asr: AsrCfg,
    pub tts: TtsCfg,
    pub llm: LlmCfg,
}

// ===== 子段 =====

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub max_connections: usize,
    pub acquire_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub connect_timeout_ms: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 16,
            acquire_timeout_ms: 5_000,
            idle_timeout_ms: 60_000,
            connect_timeout_ms: 10_000,
        }
    }
}

impl PoolConfig {
    pub fn acquire_timeout(&self) -> Duration {
        Duration::from_millis(self.acquire_timeout_ms)
    }
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_millis(self.idle_timeout_ms)
    }
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsrCfg {
    #[serde(default = "default_asr_model")]
    pub model: String,
    #[serde(default = "default_sample_rate")]
    pub sample_rate: u32,
    #[serde(default = "default_channels")]
    pub channels: u16,
    #[serde(default = "default_true")]
    pub enable_intermediate_result: bool,
    #[serde(default = "default_true")]
    pub enable_punctuation: bool,
    /// 业务空间 ID（用于 `{WorkspaceId}.cn-beijing.maas.aliyuncs.com` 风格的 WSS endpoint）
    #[serde(default)]
    pub workspace_id: Option<String>,
    /// WSS endpoint 完整 URL（覆盖默认构造）
    /// - 北京：`wss://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/api-ws/v1/inference`
    /// - 新加坡：`wss://{WorkspaceId}.ap-southeast-1.maas.aliyuncs.com/api-ws/v1/inference`
    /// - Realtime：`wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=qwen3-asr-flash-realtime`
    #[serde(default)]
    pub endpoint: Option<String>,
}

impl Default for AsrCfg {
    fn default() -> Self {
        Self {
            model: default_asr_model(),
            sample_rate: default_sample_rate(),
            channels: default_channels(),
            enable_intermediate_result: true,
            enable_punctuation: true,
            workspace_id: None,
            endpoint: None,
        }
    }
}

fn default_asr_model() -> String { "paraformer-realtime-v2".to_string() }
fn default_sample_rate() -> u32 { 16_000 }
fn default_channels() -> u16 { 1 }
fn default_true() -> bool { true }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtsCfg {
    #[serde(default = "default_tts_model")]
    pub model: String,
    #[serde(default = "default_tts_voice")]
    pub voice: String,
    #[serde(default = "default_sample_rate_u32")]
    pub sample_rate: u32,
    #[serde(default = "default_response_format")]
    pub response_format: String,
    #[serde(default = "default_true")]
    pub stream: bool,
    /// 业务空间 ID（用于 `{WorkspaceId}.cn-beijing.maas.aliyuncs.com` 风格的 WSS endpoint）
    #[serde(default)]
    pub workspace_id: Option<String>,
    /// WSS endpoint 完整 URL（覆盖默认构造）
    /// - 北京：`wss://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/api-ws/v1/inference`
    /// - 新加坡：`wss://{WorkspaceId}.ap-southeast-1.maas.aliyuncs.com/api-ws/v1/inference`
    /// - Realtime：`wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=qwen3-tts-flash-realtime`
    #[serde(default)]
    pub endpoint: Option<String>,
}

impl Default for TtsCfg {
    fn default() -> Self {
        Self {
            model: default_tts_model(),
            voice: default_tts_voice(),
            sample_rate: default_sample_rate_u32(),
            response_format: default_response_format(),
            stream: true,
            workspace_id: None,
            endpoint: None,
        }
    }
}

fn default_tts_model() -> String { "cosyvoice-v2".to_string() }
fn default_tts_voice() -> String { "longxiaochun".to_string() }
fn default_sample_rate_u32() -> u32 { 16_000 }
fn default_response_format() -> String { "pcm".to_string() }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmCfg {
    pub model: String,
}

impl Default for LlmCfg {
    fn default() -> Self {
        // 默认 qwen-max（百炼主力）
        Self { model: "qwen-max".to_string() }
    }
}

// ===== Raw mapping 解析 =====

/// 接收一个 `serde_yaml::Mapping`（来自 `VoiceConfig.bailian`），解析出 `BailianConfig`。
/// mapping 为 None 时返回默认配置。
pub fn from_mapping(
    mapping: Option<&serde_yaml::Mapping>,
    ws_endpoint: &str,
    api_key: &str,
) -> anyhow::Result<BailianConfig> {
    let pool = match mapping.and_then(|m| m.get("pool")) {
        Some(v) => serde_yaml::from_value::<PoolYaml>(v.clone())
            .unwrap_or_default()
            .into(),
        None => PoolConfig::default(),
    };
    let asr = match mapping.and_then(|m| m.get("asr")) {
        Some(v) => serde_yaml::from_value::<AsrCfg>(v.clone()).unwrap_or_default(),
        None => AsrCfg::default(),
    };
    let tts = match mapping.and_then(|m| m.get("tts")) {
        Some(v) => serde_yaml::from_value::<TtsCfg>(v.clone()).unwrap_or_default(),
        None => TtsCfg::default(),
    };
    let llm = match mapping.and_then(|m| m.get("llm")) {
        Some(v) => serde_yaml::from_value::<LlmCfg>(v.clone()).unwrap_or_default(),
        None => LlmCfg::default(),
    };
    Ok(BailianConfig {
        ws_endpoint: ws_endpoint.to_string(),
        api_key: api_key.to_string(),
        pool,
        asr,
        tts,
        llm,
    })
}

#[derive(Debug, Default, Deserialize)]
struct PoolYaml {
    #[serde(default = "default_max_conn")]
    max_connections: usize,
    #[serde(default = "default_acquire_ms")]
    acquire_timeout_ms: u64,
    #[serde(default = "default_idle_ms")]
    idle_timeout_ms: u64,
    #[serde(default = "default_connect_ms")]
    connect_timeout_ms: u64,
}

impl From<PoolYaml> for PoolConfig {
    fn from(y: PoolYaml) -> Self {
        Self {
            max_connections: y.max_connections,
            acquire_timeout_ms: y.acquire_timeout_ms,
            idle_timeout_ms: y.idle_timeout_ms,
            connect_timeout_ms: y.connect_timeout_ms,
        }
    }
}

fn default_max_conn() -> usize { 16 }
fn default_acquire_ms() -> u64 { 5_000 }
fn default_idle_ms() -> u64 { 60_000 }
fn default_connect_ms() -> u64 { 10_000 }

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value;

    #[test]
    fn from_mapping_returns_defaults_when_empty() {
        let cfg = from_mapping(None, "wss://test", "sk-test").unwrap();
        assert_eq!(cfg.ws_endpoint, "wss://test");
        assert_eq!(cfg.asr.model, "paraformer-realtime-v2");
        assert_eq!(cfg.tts.model, "cosyvoice-v2");
        assert_eq!(cfg.llm.model, "qwen-max");
    }

    #[test]
    fn from_mapping_parses_subkeys() {
        let yaml = r#"
            pool:
              max_connections: 8
            asr:
              model: "paraformer-realtime-v2"
              sample_rate: 16000
              channels: 1
            tts:
              model: "cosyvoice-v2"
              voice: "longxiaochun"
            llm:
              model: "qwen-plus"
        "#;
        let v: Value = serde_yaml::from_str(yaml).unwrap();
        let m = match v {
            Value::Mapping(m) => m,
            _ => unreachable!(),
        };
        let cfg = from_mapping(Some(&m), "wss://x", "k").unwrap();
        assert_eq!(cfg.pool.max_connections, 8);
        assert_eq!(cfg.tts.voice, "longxiaochun");
        assert_eq!(cfg.llm.model, "qwen-plus");
    }
}