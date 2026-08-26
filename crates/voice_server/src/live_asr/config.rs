//! live-asr 配置：YAML `asr_live` 段 + 环境变量覆盖

use std::collections::HashMap;
use std::time::Duration;

use tracing::warn;

use crate::client::funasr::{FunasrConfig, FunasrMode};

/// live-asr 配置（YAML `asr_live` 段 + 环境变量覆盖）
///
/// 字段都是 `Option`，缺失时走默认；只有 endpoint 可空（用 FunASR 默认本地端口）。
///
/// **sample_rate / channels 不在配置里** —— 浏览器 AudioContext 实际采样率由前端
/// SessionStart 传上来（asr_realtime.js:517）；服务端只在前端 0 值时硬编码 fallback
/// 16000Hz / 1ch。配置层不再保留这两个字段，避免让运维误以为可以"调采样率"。
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
pub struct AsrLiveCfg {
    /// WSS 端点。缺省 `ws://127.0.0.1:10095/`（FunASR runtime 默认端口）。
    pub endpoint: Option<String>,
    /// 推理模式：offline / online / 2pass（默认 2pass）
    pub mode: Option<String>,
    /// 服务端日志关联用 wav_name。None 时按 `session_id` 派生（每浏览器会话唯一）。
    pub wav_name: Option<String>,
    /// 音频格式（pcm / wav / mpj / ...），按 spec 透传。默认 pcm
    pub wav_format: Option<String>,
    /// 流式 latency 配置 `[左看, 当前, 右看]`（1 chunk = 60ms）。默认 `[5, 10, 5]`。
    /// None = 不传（服务端默认）。长度必须 = 3。
    pub chunk_size: Option<Vec<u32>>,
    /// 热词 JSON 字符串，例：`{"阿里巴巴":20,"通义实验室":30}`。None = 不传
    pub hotwords: Option<String>,
    /// ITN（数字、日期等转写）。默认 true
    pub itn: Option<bool>,
    /// SenseVoiceSmall 模型语种。None = 服务端 auto
    pub svs_lang: Option<String>,
    /// SenseVoiceSmall 是否开启标点 / ITN。默认 true
    pub svs_itn: Option<bool>,
    /// 附加 header（一般不用；本地 FunASR 无鉴权）
    pub headers: HashMap<String, String>,
    /// ASR 建连 + 整轮收尾超时（默认 30s）
    pub timeout_secs: Option<u64>,
    /// **应用层 keepalive ping 间隔**（秒）。后台 task 每 N 秒向上游 FunASR WSS 发 Ping，
    /// 防止 FunASR 服务端（Python `websockets` 库）的 idle timeout 把连接误杀 —— 用户
    /// 长停顿不说话的常见故障来源。默认 20s。`0` = 禁用。
    pub keepalive_secs: Option<u64>,
    /// **上游 close 宽限期**（秒）。浏览器点「结束」后等服务端收完剩余 transcript 的最
    /// 长等待；超时则强制 close 上游 WSS（不等 FunASR 自然 Close）。默认 3s。
    pub close_grace_secs: Option<u64>,
}

impl AsrLiveCfg {
    pub fn endpoint(&self) -> &str {
        self.endpoint
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("ws://127.0.0.1:10095/")
    }

    pub fn mode(&self) -> FunasrMode {
        match self
            .mode
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("2pass")
        {
            "offline" => FunasrMode::Offline,
            "online" => FunasrMode::Online,
            // 默认 + 任何 unknown 值都退回 2pass（推荐路径）
            _ => FunasrMode::TwoPass,
        }
    }

    pub fn wav_format(&self) -> &str {
        self.wav_format
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("pcm")
    }

    pub fn itn(&self) -> bool {
        self.itn.unwrap_or(true)
    }

    pub fn svs_itn(&self) -> bool {
        self.svs_itn.unwrap_or(true)
    }

    pub fn chunk_size(&self) -> Option<Vec<u32>> {
        self.chunk_size.clone().filter(|v| v.len() == 3)
    }

    /// 转成 `FunasrConfig`（**按当前浏览器会话**派生）。
    ///
    /// per-session 字段：
    /// - `wav_name` —— 由调用方按 session_id 注入（每会话一个，保证服务端日志可关联）。
    ///   None 或空 → 回落 `cfg.wav_name`，再回落 `"default"`。
    /// - `sample_rate` / `channels` —— **优先用前端 SessionStart 传值**（浏览器
    ///   AudioContext 实际采样率）。`0` 表示"前端没传"，硬编码 fallback 16000 / 1。
    ///
    /// 本地部署无鉴权，所以只需 endpoint 非空（默认 endpoint 即可满足）。
    pub fn into_client_config(
        &self,
        wav_name: Option<&str>,
        sample_rate: u32,
        channels: u16,
    ) -> Option<FunasrConfig> {
        let endpoint = self.endpoint();
        if endpoint.trim().is_empty() {
            return None;
        }
        // wav_name：调用方显式传 > 配置 > "default"
        let wav_name = wav_name
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                self.wav_name
                    .clone()
                    .filter(|s| !s.is_empty())
            })
            .unwrap_or_else(|| "default".to_string());
        // sample_rate / channels：前端值（> 0）> 硬编码 fallback
        const FALLBACK_SAMPLE_RATE: u32 = 16000;
        const FALLBACK_CHANNELS: u16 = 1;
        let sample_rate = if sample_rate > 0 { sample_rate } else { FALLBACK_SAMPLE_RATE };
        let channels = if channels > 0 { channels } else { FALLBACK_CHANNELS };
        Some(FunasrConfig {
            endpoint: endpoint.to_string(),
            mode: self.mode(),
            wav_name,
            wav_format: self.wav_format().to_string(),
            sample_rate,
            channels,
            // AsrLiveCfg.chunk_size = None → 走 FunasrConfig 默认 Some([5,10,5])
            chunk_size: self.chunk_size().or_else(|| Some(vec![5, 10, 5])),
            hotwords: self.hotwords.clone().filter(|s| !s.is_empty()),
            itn: self.itn(),
            svs_lang: self.svs_lang.clone().filter(|s| !s.is_empty()),
            svs_itn: self.svs_itn(),
            extra_headers: self.headers.clone(),
            timeout: Duration::from_secs(self.timeout_secs.unwrap_or(30)),
            keepalive_interval: Duration::from_secs(self.keepalive_secs.unwrap_or(20)),
        })
    }

    /// close 宽限期：浏览器点「结束」后等服务端收完剩余 transcript 的最长等待。超时则强制 close 上游 WSS。
    pub fn close_grace(&self) -> Duration {
        Duration::from_secs(self.close_grace_secs.unwrap_or(3))
    }
}

fn apply_env_overrides(cfg: &mut AsrLiveCfg) {
    fn take(var: &str) -> Option<String> {
        std::env::var(var).ok().filter(|s| !s.trim().is_empty())
    }
    // 字符串字段统一走 tuple 数组
    for (var, slot) in [
        ("VOICE_ASR_LIVE_ENDPOINT", &mut cfg.endpoint),
        ("VOICE_ASR_LIVE_MODE", &mut cfg.mode),
        ("VOICE_ASR_LIVE_WAV_NAME", &mut cfg.wav_name),
        ("VOICE_ASR_LIVE_WAV_FORMAT", &mut cfg.wav_format),
        ("VOICE_ASR_LIVE_HOTWORDS", &mut cfg.hotwords),
        ("VOICE_ASR_LIVE_SVS_LANG", &mut cfg.svs_lang),
    ] {
        if let Some(v) = take(var) {
            *slot = Some(v);
        }
    }
    // 注：VOICE_ASR_LIVE_SAMPLE_RATE / _CHANNELS 已移除 —— sample_rate / channels 由浏览器
    // SessionStart 传入，服务端硬编码 fallback 16000 / 1（不暴露给配置层）。
    // keepalive_secs / close_grace_secs 走 Option<u64>，由 `voice_asr_live_keepalive_secs` /
    // `voice_asr_live_close_grace_secs` 解析；非 u64 解析失败时静默忽略（不破坏启动）。
    for (var, slot) in [
        ("VOICE_ASR_LIVE_TIMEOUT_SECS", &mut cfg.timeout_secs),
        ("VOICE_ASR_LIVE_KEEPALIVE_SECS", &mut cfg.keepalive_secs),
        ("VOICE_ASR_LIVE_CLOSE_GRACE_SECS", &mut cfg.close_grace_secs),
    ] {
        if let Some(v) = take(var) {
            if let Ok(n) = v.trim().parse::<u64>() {
                *slot = Some(n);
            } else {
                warn!(
                    target: "voice_server.live_asr",
                    var,
                    value = %v,
                    "环境变量解析失败（非 u64），忽略"
                );
            }
        }
    }
}

fn read_yaml_section() -> AsrLiveCfg {
    let default_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("config")
        .join("config.yaml");
    let path = crate::resolve_config_path("voice_server", None, Some(&default_path));
    if !path.exists() {
        return AsrLiveCfg::default();
    }
    let Ok(text) = std::fs::read_to_string(&path) else {
        return AsrLiveCfg::default();
    };
    let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(&text) else {
        warn!(target: "voice_server.live_asr", path = %path.display(), "配置文件解析失败，asr_live 段按缺省处理");
        return AsrLiveCfg::default();
    };
    match value.get("asr_live") {
        Some(section) => serde_yaml::from_value::<AsrLiveCfg>(section.clone()).unwrap_or_default(),
        None => AsrLiveCfg::default(),
    }
}

pub fn load_cfg() -> AsrLiveCfg {
    let mut cfg = read_yaml_section();
    apply_env_overrides(&mut cfg);
    cfg
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== AsrLiveCfg default / helpers =====

    #[test]
    fn cfg_defaults_local_two_pass_16k() {
        let cfg = AsrLiveCfg::default();
        assert_eq!(cfg.endpoint(), "ws://127.0.0.1:10095/");
        assert_eq!(cfg.mode(), FunasrMode::TwoPass);
        assert_eq!(cfg.wav_format(), "pcm");
        assert!(cfg.itn());
        assert!(cfg.svs_itn());
        // chunk_size 默认 None（AsrLiveCfg）→ 走 FunasrConfig 的默认 Some(vec![5,10,5])
        assert!(cfg.chunk_size().is_none());
    }

    #[test]
    fn cfg_mode_parses_doc_values() {
        let cfg = AsrLiveCfg {
            mode: Some("offline".into()),
            ..Default::default()
        };
        assert_eq!(cfg.mode(), FunasrMode::Offline);
        let cfg = AsrLiveCfg {
            mode: Some("online".into()),
            ..Default::default()
        };
        assert_eq!(cfg.mode(), FunasrMode::Online);
        let cfg = AsrLiveCfg {
            mode: Some("2pass".into()),
            ..Default::default()
        };
        assert_eq!(cfg.mode(), FunasrMode::TwoPass);
        // 未知值退回 2pass（安全默认）
        let cfg = AsrLiveCfg {
            mode: Some("bogus".into()),
            ..Default::default()
        };
        assert_eq!(cfg.mode(), FunasrMode::TwoPass);
    }

    #[test]
    fn cfg_endpoint_falls_back_to_local_default() {
        let cfg = AsrLiveCfg::default();
        assert_eq!(cfg.endpoint(), "ws://127.0.0.1:10095/");
        let cfg = AsrLiveCfg {
            endpoint: Some("ws://my-funasr.local:9999/".into()),
            ..Default::default()
        };
        assert_eq!(cfg.endpoint(), "ws://my-funasr.local:9999/");
    }

    #[test]
    fn cfg_chunk_size_requires_len_3() {
        let cfg = AsrLiveCfg {
            chunk_size: Some(vec![5, 10, 5]),
            ..Default::default()
        };
        assert_eq!(cfg.chunk_size(), Some(vec![5, 10, 5]));
        // 长度不为 3 → 当作 None（让 FunasrConfig 用默认）
        let cfg = AsrLiveCfg {
            chunk_size: Some(vec![5, 10]),
            ..Default::default()
        };
        assert!(cfg.chunk_size().is_none());
        let cfg = AsrLiveCfg {
            chunk_size: Some(vec![5, 10, 5, 6]),
            ..Default::default()
        };
        assert!(cfg.chunk_size().is_none());
    }

    #[test]
    fn cfg_into_client_config_default_local() {
        let cfg = AsrLiveCfg::default();
        // 前端传 0 → 用 cfg 的 fallback（16000/1）
        let c = cfg.into_client_config(None, 0, 0).unwrap();
        assert_eq!(c.endpoint, "ws://127.0.0.1:10095/");
        assert_eq!(c.mode, FunasrMode::TwoPass);
        assert_eq!(c.sample_rate, 16000);
        assert_eq!(c.channels, 1);
        assert_eq!(c.wav_format, "pcm");
        assert!(c.itn);
        assert!(c.svs_itn);
        // AsrLiveCfg.chunk_size 默认 None → FunasrConfig 用 Some(vec![5,10,5])
        assert_eq!(c.chunk_size, Some(vec![5, 10, 5]));
        assert_eq!(c.wav_name, "default");
    }

    /// 前端 SessionStart 传的 sample_rate / channels **覆盖**配置 fallback
    #[test]
    fn cfg_into_client_config_uses_browser_sample_rate() {
        let cfg = AsrLiveCfg {
            // 配置 fallback 是 16k/1
            ..Default::default()
        };
        // 前端传 8kHz + 2 声道 → 必须透传
        let c = cfg.into_client_config(Some("browser-1"), 8000, 2).unwrap();
        assert_eq!(c.sample_rate, 8000);
        assert_eq!(c.channels, 2);
        assert_eq!(c.wav_name, "browser-1");
    }

    /// 前端 0 → 回落硬编码 fallback（避免把 0 传到 FunASR 导致识别失败）
    #[test]
    fn cfg_into_client_config_falls_back_when_browser_zero() {
        let cfg = AsrLiveCfg::default();
        // 前端没传（0）→ 用硬编码 16k/1
        let c = cfg.into_client_config(Some("x"), 0, 0).unwrap();
        assert_eq!(c.sample_rate, 16000);
        assert_eq!(c.channels, 1);
    }

    /// wav_name：调用方显式 > 配置 > "default"
    #[test]
    fn cfg_into_client_config_wav_name_resolution() {
        // 调用方显式给的 wav_name 优先
        let cfg = AsrLiveCfg {
            wav_name: Some("from-config".into()),
            ..Default::default()
        };
        let c = cfg.into_client_config(Some("from-browser"), 0, 0).unwrap();
        assert_eq!(c.wav_name, "from-browser");
        // 调用方 None → 回落 cfg.wav_name
        let c = cfg.into_client_config(None, 0, 0).unwrap();
        assert_eq!(c.wav_name, "from-config");
        // 调用方空字符串 → 同样回落
        let c = cfg.into_client_config(Some("   "), 0, 0).unwrap();
        assert_eq!(c.wav_name, "from-config");
        // 全 None → "default"
        let cfg = AsrLiveCfg::default();
        let c = cfg.into_client_config(None, 0, 0).unwrap();
        assert_eq!(c.wav_name, "default");
    }

    #[test]
    fn cfg_into_client_config_with_hotwords_and_chunk() {
        let cfg = AsrLiveCfg {
            endpoint: Some("ws://funasr.local:10095/".into()),
            mode: Some("online".into()),
            wav_name: Some("live-x".into()),
            wav_format: Some("pcm".into()),
            chunk_size: Some(vec![3, 6, 3]),
            hotwords: Some(r#"{"阿里巴巴":20}"#.into()),
            svs_lang: Some("zh".into()),
            itn: Some(false),
            svs_itn: Some(false),
            ..Default::default()
        };
        // 前端传 8000Hz / 1ch → 透传
        let c = cfg.into_client_config(None, 8000, 1).unwrap();
        assert_eq!(c.endpoint, "ws://funasr.local:10095/");
        assert_eq!(c.mode, FunasrMode::Online);
        assert_eq!(c.wav_name, "live-x");
        assert_eq!(c.sample_rate, 8000);
        assert_eq!(c.channels, 1);
        assert_eq!(c.wav_format, "pcm");
        assert_eq!(c.chunk_size, Some(vec![3, 6, 3]));
        assert_eq!(c.hotwords.as_deref(), Some(r#"{"阿里巴巴":20}"#));
        assert_eq!(c.svs_lang.as_deref(), Some("zh"));
        assert!(!c.itn);
        assert!(!c.svs_itn);
    }

    #[test]
    fn cfg_into_client_config_uses_default_when_endpoint_blank() {
        // endpoint 给空字符串或纯空白 → endpoint() 会回落到本地默认，into_client_config 必须成功
        let cfg = AsrLiveCfg {
            endpoint: Some("   ".into()),
            ..Default::default()
        };
        let c = cfg.into_client_config(None, 0, 0).expect("blank endpoint 应回落到本地默认");
        assert_eq!(c.endpoint, "ws://127.0.0.1:10095/");
    }

    // ===== YAML 段解析 =====

    #[test]
    fn yaml_section_parse_full() {
        let yaml = r#"
endpoint: "ws://funasr.local:10095/"
mode: "2pass"
wav_name: "browser-session"
chunk_size: [5, 10, 5]
hotwords: '{"阿里巴巴":20}'
itn: true
svs_lang: "zh"
svs_itn: true
"#;
        let v: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        let cfg: AsrLiveCfg = serde_yaml::from_value(v).unwrap();
        assert_eq!(cfg.endpoint(), "ws://funasr.local:10095/");
        assert_eq!(cfg.mode(), FunasrMode::TwoPass);
        assert_eq!(cfg.wav_name.as_deref(), Some("browser-session"));
        assert_eq!(cfg.chunk_size(), Some(vec![5, 10, 5]));
        assert_eq!(cfg.hotwords.as_deref(), Some(r#"{"阿里巴巴":20}"#));
        assert_eq!(cfg.svs_lang.as_deref(), Some("zh"));
        assert!(cfg.itn());
        assert!(cfg.svs_itn());
    }

    #[test]
    fn yaml_section_defaults() {
        let cfg: AsrLiveCfg = serde_yaml::from_value(serde_yaml::Value::Null).unwrap();
        // 默认值：本地 FunASR
        assert_eq!(cfg.endpoint(), "ws://127.0.0.1:10095/");
        assert_eq!(cfg.mode(), FunasrMode::TwoPass);
    }
}