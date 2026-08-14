//! voice-config：所有 binary 共享的配置原语
//!
//! 提供：
//!   - `LogConfig` 日志配置结构 + `init_logging()` 初始化函数
//!   - `load_toml::<T>()` 通用 TOML 加载器
//!
//! 各 binary 自己定义自己的 TOML schema，调用本 crate 的工具加载/初始化。

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::warn;
use tracing_subscriber::{fmt, EnvFilter};

// ===== LogConfig（所有 binary 都会用到）=====

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    /// trace | debug | info | warn | error
    #[serde(default = "default_log_level")]
    pub level: String,
    /// 日志文件路径；空字符串 = stdout，特殊值 "stderr" = stderr，否则按天滚动写文件
    #[serde(default)]
    pub file: String,
    /// pretty | json
    #[serde(default = "default_log_format")]
    pub format: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            file: String::new(),
            format: default_log_format(),
        }
    }
}

fn default_log_level() -> String {
    "info".into()
}
fn default_log_format() -> String {
    "pretty".into()
}

impl LogConfig {
    /// 环境变量 `VOICE_LOG_LEVEL` / `VOICE_LOG_FILE` 覆盖现有值
    pub fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("VOICE_LOG_LEVEL") {
            self.level = v;
        }
        if let Ok(v) = std::env::var("VOICE_LOG_FILE") {
            self.file = v;
        }
    }

    /// 把 `file` 字段的相对路径以 `base_dir` 为基准解析为绝对路径。
    /// 如果 `file` 已是绝对路径、或为空、或为 "stdout"/"stderr"，不动。
    pub fn resolve_relative_paths(&mut self, base_dir: &Path) {
        if self.file.is_empty() || self.file == "stdout" || self.file == "stderr" {
            return;
        }
        let p = Path::new(&self.file);
        if p.is_absolute() {
            return;
        }
        let abs = base_dir.join(p);
        self.file = abs.to_string_lossy().into_owned();
    }
}

/// 根据 LogConfig 初始化全局 tracing subscriber。
/// 重复调用静默成功（try_init）。
///
/// 因 `tracing_subscriber::Layer` 是 generic over Subscriber，
/// 这里把 stdout/file 两种情况拆成独立分支各自构建 Layer，
/// 然后挂到 `tracing_subscriber::registry()` 上 try_init。
pub fn init_logging(cfg: &LogConfig) -> anyhow::Result<()> {
    let env_filter = EnvFilter::try_new(&cfg.level).unwrap_or_else(|_| EnvFilter::new("info"));

    let use_file = !cfg.file.is_empty() && cfg.file != "stdout" && cfg.file != "stderr";

    let json_mode = cfg.format == "json";

    // 注意：不能用 tracing_subscriber::registry() —— 它会启用 tracing-log feature，
    // 然后在初始化时调 log::set_logger 占住全局 log facade，
    // 导致后续 webhttp::start 里的 env_logger::init_from_env() panic。
    // 用 fmt() 直接构建 subscriber，绕过 tracing-log。

    if use_file {
        // cfg.file 是文件路径（如 ./logs/voice-server.log）；父目录不存在则创建
        let log_path = Path::new(&cfg.file);
        if let Some(parent) = log_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    anyhow::anyhow!("create log dir {}: {}", parent.display(), e)
                })?;
            }
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .map_err(|e| anyhow::anyhow!("open log file {}: {}", log_path.display(), e))?;
        let result = if json_mode {
            tracing::subscriber::set_global_default(
                fmt::Subscriber::builder()
                    .with_env_filter(env_filter)
                    .with_writer(file)
                    .json()
                    .finish(),
            )
        } else {
            tracing::subscriber::set_global_default(
                fmt::Subscriber::builder()
                    .with_env_filter(env_filter)
                    .with_writer(file)
                    .finish(),
            )
        };
        result.ok();
    } else {
        let result = if json_mode {
            tracing::subscriber::set_global_default(
                fmt::Subscriber::builder()
                    .with_env_filter(env_filter)
                    .json()
                    .finish(),
            )
        } else {
            tracing::subscriber::set_global_default(
                fmt::Subscriber::builder()
                    .with_env_filter(env_filter)
                    .finish(),
            )
        };
        result.ok();
    }
    Ok(())
}

// ===== YAML 加载 =====

/// 从 YAML 文件加载配置为类型 T；文件不存在则用 Default
pub fn load_yaml<T: serde::de::DeserializeOwned + Default>(path: &Path) -> anyhow::Result<T> {
    if !path.exists() {
        warn!(
            target: "voice_config",
            path = %path.display(),
            "配置文件不存在，使用内置默认"
        );
        return Ok(T::default());
    }
    let text = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("read config {}: {}", path.display(), e))?;
    let cfg: T = serde_yaml::from_str(&text)
        .map_err(|e| anyhow::anyhow!("parse config {}: {}", path.display(), e))?;
    tracing::info!(
        target: "voice_config",
        path = %path.display(),
        "已加载配置文件"
    );
    Ok(cfg)
}

/// 给定 binary 名，返回默认配置文件路径 ./voice-<bin_name>.yaml
pub fn default_config_path(bin_name: &str) -> PathBuf {
    PathBuf::from(format!("voice-{}.yaml", bin_name))
}

/// 智能解析 web 静态目录路径（在标准位置搜索）
///
/// 优先级：
///   1. 显式传入 `explicit`（即使不存在也用它）
///   2. 环境变量 `VOICE_WEB_STATIC_DIR`
///   3. 搜索常见路径：
///        - `./static`
///        - `./web`
///        - `./crates/voice-server/static`     (workspace 根目录启动)
///        - `./target/debug/web`              (cargo run 后的输出位置)
///        - `../static`
///        - `../web`
///        - `../../crates/voice-server/static`
///   4. 兜底：`./static`
pub fn resolve_web_static_dir(explicit: Option<&str>) -> Option<PathBuf> {
    if let Some(p) = explicit {
        let path = PathBuf::from(p);
        return Some(path);
    }
    if let Ok(v) = std::env::var("VOICE_WEB_STATIC_DIR") {
        return Some(PathBuf::from(v));
    }
    let candidates: &[&str] = &[
        "./static",
        "./web",
        "./crates/voice-server/static",
        "./target/debug/web",
        "./crates/voice-client/static",
        "../static",
        "../web",
        "../../crates/voice-server/static",
    ];
    for c in candidates {
        let p = PathBuf::from(c);
        if p.exists() && p.is_dir() {
            return Some(p);
        }
    }
    Some(PathBuf::from("./static"))
}

/// 智能解析配置文件路径。
///
/// 优先级（高到低）：
///   1. CLI 显式传入的 `explicit`（即使文件不存在也使用它，load_yaml 会告警）
///   2. 环境变量 `VOICE_CONFIG`
///   3. 按顺序搜索以下路径，**第一个存在**的胜出：
///        - `./voice-<bin>.yaml`
///        - `./voice-<bin>.yml`
///        - `./configs/voice-<bin>.yaml`
///        - `./config/voice-<bin>.yaml`
///        - `../configs/voice-<bin>.yaml`     (从 voice-app/configs/ 运行)
///        - `../voice-<bin>.yaml`              (从 voice-app/crates/<x>/ 运行)
///        - `$HOME/.config/voice-app/voice-<bin>.yaml`
///   4. 全部找不到 → 返回 `./voice-<bin>.yaml`（不存在的占位，让 load_yaml 用默认）
///
/// 返回的 PathBuf 始终是 Some，调用方决定是否警告"没找到"。
pub fn resolve_config_path(bin_name: &str, explicit: Option<&Path>) -> PathBuf {
    // 1. CLI 显式传入
    if let Some(p) = explicit {
        return p.to_path_buf();
    }

    // 2. 环境变量
    if let Ok(v) = std::env::var("VOICE_CONFIG") {
        let p = PathBuf::from(v);
        if p.exists() {
            return p;
        }
    }

    // 3. 搜索常见路径
    let candidates = candidate_paths(bin_name);
    for c in &candidates {
        if c.exists() {
            return c.clone();
        }
    }

    // 4. 兜底：返回当前默认（load_yaml 检测不到就 Default::default()）
    default_config_path(bin_name)
}

/// 列出所有候选路径（不实际搜索，仅打印用）
pub fn candidate_paths(bin_name: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let yaml = format!("voice-{}.yaml", bin_name);
    let yml = format!("voice-{}.yml", bin_name);

    // 当前目录（含 .yaml 和 .yml 两种后缀）
    out.push(PathBuf::from(&yaml));
    out.push(PathBuf::from(&yml));

    // 当前目录的 configs/ 或 config/ 子目录
    for sub in &["configs", "config"] {
        out.push(PathBuf::from(sub).join(&yaml));
    }

    // 向上一层、向上两层的 configs/（覆盖从 crates/<x>/ 或 target/debug/ 启动的场景）
    for up in &["..", "../..", "../../.."] {
        out.push(PathBuf::from(up).join("configs").join(&yaml));
        out.push(PathBuf::from(up).join(&yaml));
    }

    // 用户级配置
    if let Ok(home) = std::env::var("HOME") {
        out.push(
            PathBuf::from(home)
                .join(".config")
                .join("voice-app")
                .join(&yaml),
        );
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_config_defaults() {
        let l = LogConfig::default();
        assert_eq!(l.level, "info");
        assert_eq!(l.file, "");
        assert_eq!(l.format, "pretty");
    }

    #[test]
    fn log_config_parse() {
        let yaml = r#"
            level: debug
            file: /tmp/x.log
            format: json
        "#;
        let l: LogConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(l.level, "debug");
        assert_eq!(l.file, "/tmp/x.log");
        assert_eq!(l.format, "json");
    }

    #[test]
    fn default_config_path_for_bin() {
        assert_eq!(
            default_config_path("voice_server"),
            PathBuf::from("voice-voice_server.yaml")
        );
    }

    #[test]
    fn log_file_relative_path_resolution() {
        let mut l = LogConfig {
            level: "info".into(),
            file: "../logs/x.log".into(),
            format: "pretty".into(),
        };
        l.resolve_relative_paths(Path::new("/tmp/configs"));
        assert_eq!(l.file, "/tmp/logs/x.log");

        // 绝对路径不变
        let mut l = LogConfig {
            level: "info".into(),
            file: "/var/log/x.log".into(),
            format: "pretty".into(),
        };
        l.resolve_relative_paths(Path::new("/tmp/configs"));
        assert_eq!(l.file, "/var/log/x.log");

        // 空字符串不变
        let mut l = LogConfig {
            level: "info".into(),
            file: String::new(),
            format: "pretty".into(),
        };
        l.resolve_relative_paths(Path::new("/tmp/configs"));
        assert_eq!(l.file, "");

        // "stdout" 不变
        let mut l = LogConfig {
            level: "info".into(),
            file: "stdout".into(),
            format: "pretty".into(),
        };
        l.resolve_relative_paths(Path::new("/tmp/configs"));
        assert_eq!(l.file, "stdout");
    }
}