//! voice-server 的日志与配置加载原语
//!
//! 早期在独立 crate `voice-config` 里；现已合并进 voice-server 本体，
//! 通过 `voice_server::logging` 访问。提供：
//!   - `LogConfig` 日志配置结构 + `init_logging()` 初始化函数
//!   - `load_yaml::<T>()` 通用 YAML 加载器
//!   - `resolve_config_path` / `resolve_web_static_dir` / `default_config_path` / `candidate_paths`

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use time::format_description::BorrowedFormatItem;
use time::macros::format_description;
use tracing::warn;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::fmt::writer::MakeWriter;
use tracing_subscriber::{fmt, EnvFilter};

// ===== 自定义时间格式：HH:MM:SS.mmm 本地时间 =====
//
// 避开 ISO 8601 的 UTC 时区/纳秒噪音
struct ShortLocalTime;

const SHORT_TIME_FMT: &[BorrowedFormatItem] =
    format_description!("[hour]:[minute]:[second].[subsecond digits:3]");

impl FormatTime for ShortLocalTime {
    fn format_time(&self, w: &mut fmt::format::Writer<'_>) -> std::fmt::Result {
        let now = time::OffsetDateTime::now_local()
            .unwrap_or_else(|_| time::OffsetDateTime::now_utc());
        write!(
            w,
            "{}",
            now.format(&SHORT_TIME_FMT)
                .unwrap_or_else(|_| "--:--:--.---".to_string())
        )
    }
}

// ===== LogConfig =====

/// 日志输出格式
/// - `text`（默认）：人类可读，紧凑 + 短时间戳 HH:MM:SS.mmm + 终端带 ANSI 颜色 / 文件无 ANSI
/// - `json`：每条一行 JSON（log aggregator / 告警系统用）
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Text,
    Json,
}

impl std::fmt::Display for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogFormat::Text => f.write_str("text"),
            LogFormat::Json => f.write_str("json"),
        }
    }
}

impl Default for LogFormat {
    fn default() -> Self {
        LogFormat::Text
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    /// trace | debug | info | warn | error
    #[serde(default = "default_log_level")]
    pub level: String,
    /// 日志文件路径；空字符串 = stdout，特殊值 "stderr" = stderr，否则按天滚动写文件
    #[serde(default)]
    pub file: String,
    #[serde(default)]
    pub format: LogFormat,
    /// 当 `file` 配置为具体路径时，是否同时把日志写到 stdout（默认 true）
    #[serde(default = "default_also_stdout")]
    pub also_stdout: bool,
    /// 每条日志是否带上源码文件 + 行号（默认 true；生产环境可关掉省点开销）
    #[serde(default = "default_with_location")]
    pub with_location: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            file: String::new(),
            format: default_log_format(),
            also_stdout: default_also_stdout(),
            with_location: default_with_location(),
        }
    }
}

fn default_log_level() -> String {
    "info".into()
}
fn default_log_format() -> LogFormat {
    LogFormat::Text
}
fn default_also_stdout() -> bool {
    true
}
fn default_with_location() -> bool {
    true
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

    /// 把 `file` 字段的相对路径以 `base_dir` 为基准解析，并把 `..` `.` 规范化掉（不依赖文件系统）。
    /// 如果 `file` 已是绝对路径、或为空、或为 "stdout"/"stderr"，不动。
    /// 注意：始终保留相对路径形式，不锚到当前工作目录。
    pub fn resolve_relative_paths(&mut self, base_dir: &Path) {
        if self.file.is_empty() || self.file == "stdout" || self.file == "stderr" {
            return;
        }
        let p = Path::new(&self.file);
        if p.is_absolute() {
            return;
        }
        let abs = base_dir.join(p);
        self.file = normalize_path(&abs).to_string_lossy().into_owned();
    }
}

/// 路径规范化：处理 `..` `.` 段，不访问文件系统（不要求路径存在）
fn normalize_path(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in p.components() {
        match comp {
            std::path::Component::Prefix(p) => out.push(p.as_os_str()),
            std::path::Component::RootDir => out.push(comp.as_os_str()),
            std::path::Component::CurDir => {} // 跳过 "."
            std::path::Component::ParentDir => {
                // ".."：弹掉上一段（除非已是根）
                if !matches!(
                    out.components().last(),
                    Some(std::path::Component::Prefix(_) | std::path::Component::RootDir)
                ) {
                    out.pop();
                }
            }
            std::path::Component::Normal(name) => out.push(name),
        }
    }
    out
}

// ===== Tee writer：同一条日志同时写文件和控制台 =====
//
// 文件不能带 ANSI 转义（否则 [2m/[32m 等控制符会写进文件），
// 所以 tee 模式下统一关掉 ANSI —— 控制台输出也是纯文本。
// 如需在终端看颜色，请把 `log.file` 留空（纯 stdout 模式）。
#[derive(Clone)]
struct TeeWriter {
    file: Arc<Mutex<std::fs::File>>,
}

impl std::io::Write for TeeWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut out = std::io::stdout().lock();
        out.write_all(buf)?;
        out.flush()?;
        self.file.lock().unwrap().write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stdout().flush()?;
        self.file.lock().unwrap().flush()
    }
}

impl<'a> MakeWriter<'a> for TeeWriter {
    type Writer = TeeWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
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

        // 文件输出统一关掉 ANSI（避免 [2m/[32m 等控制符写进文件；
        // 即便 cfg.also_stdout = true，tee 模式下控制台也只能拿到纯文本）
        let result = match cfg.format {
            LogFormat::Json if cfg.also_stdout => tracing::subscriber::set_global_default(
                fmt::Subscriber::builder()
                    .with_env_filter(env_filter)
                    .with_writer(TeeWriter { file: Arc::new(Mutex::new(file)) })
                    .with_ansi(false)
                    .with_timer(ShortLocalTime)
                    .with_file(cfg.with_location)
                    .with_line_number(cfg.with_location)
                    .json()
                    .finish(),
            ),
            LogFormat::Json => tracing::subscriber::set_global_default(
                fmt::Subscriber::builder()
                    .with_env_filter(env_filter)
                    .with_writer(file)
                    .with_ansi(false)
                    .with_timer(ShortLocalTime)
                    .with_file(cfg.with_location)
                    .with_line_number(cfg.with_location)
                    .json()
                    .finish(),
            ),
            LogFormat::Text if cfg.also_stdout => tracing::subscriber::set_global_default(
                fmt::Subscriber::builder()
                    .with_env_filter(env_filter)
                    .with_writer(TeeWriter { file: Arc::new(Mutex::new(file)) })
                    .with_ansi(false)
                    .with_timer(ShortLocalTime)
                    .with_target(true)
                    .with_file(cfg.with_location)
                    .with_line_number(cfg.with_location)
                    .compact()
                    .finish(),
            ),
            LogFormat::Text => tracing::subscriber::set_global_default(
                fmt::Subscriber::builder()
                    .with_env_filter(env_filter)
                    .with_writer(file)
                    .with_ansi(false)
                    .with_timer(ShortLocalTime)
                    .with_target(true)
                    .with_file(cfg.with_location)
                    .with_line_number(cfg.with_location)
                    .compact()
                    .finish(),
            ),
        };
        result.ok();
    } else {
        // stdout：保留 ANSI 颜色（终端可见）
        let result = match cfg.format {
            LogFormat::Json => tracing::subscriber::set_global_default(
                fmt::Subscriber::builder()
                    .with_env_filter(env_filter)
                    .with_writer(std::io::stdout)
                    .with_ansi(true)
                    .with_timer(ShortLocalTime)
                    .with_file(cfg.with_location)
                    .with_line_number(cfg.with_location)
                    .json()
                    .finish(),
            ),
            LogFormat::Text => tracing::subscriber::set_global_default(
                fmt::Subscriber::builder()
                    .with_env_filter(env_filter)
                    .with_writer(std::io::stdout)
                    .with_ansi(true)
                    .with_timer(ShortLocalTime)
                    .with_target(true)
                    .with_file(cfg.with_location)
                    .with_line_number(cfg.with_location)
                    .compact()
                    .finish(),
            ),
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
///   3. `default` 参数（caller 传的 in-crate 路径，如 `<crate>/src/config/config.yaml`）
///   4. 按顺序搜索以下路径，**第一个存在**的胜出：
///        - `./voice-<bin>.yaml`
///        - `./voice-<bin>.yml`
///        - `./configs/voice-<bin>.yaml`
///        - `./config/voice-<bin>.yaml`
///        - `../configs/voice-<bin>.yaml`     (从 voice-app/configs/ 运行)
///        - `../voice-<bin>.yaml`              (从 voice-app/crates/<x>/ 运行)
///        - `$HOME/.config/voice-app/voice-<bin>.yaml`
///   5. 全部找不到 → 返回 `default`（若提供）或 `./voice-<bin>.yaml`（不存在的占位，让 load_yaml 用默认）
///
/// 返回的 PathBuf 始终是 Some，调用方决定是否警告"没找到"。
pub fn resolve_config_path(
    bin_name: &str,
    explicit: Option<&Path>,
    default: Option<&Path>,
) -> PathBuf {
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

    // 3. 调用方提供的 in-crate 默认路径
    if let Some(d) = default {
        if d.exists() {
            return d.to_path_buf();
        }
    }

    // 4. 搜索常见路径
    let candidates = candidate_paths(bin_name);
    for c in &candidates {
        if c.exists() {
            return c.clone();
        }
    }

    // 5. 兜底：返回 default 或旧默认（load_yaml 检测不到就 Default::default()）
    default
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| default_config_path(bin_name))
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
        assert_eq!(l.format, LogFormat::Text);
        assert!(l.also_stdout);
        assert!(l.with_location);
    }

    #[test]
    fn log_config_parse() {
        let yaml = r#"
            level: debug
            file: /tmp/x.log
            format: json
            also_stdout: false
            with_location: false
        "#;
        let l: LogConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(l.level, "debug");
        assert_eq!(l.file, "/tmp/x.log");
        assert_eq!(l.format, LogFormat::Json);
        assert!(!l.also_stdout);
        assert!(!l.with_location);
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
            format: LogFormat::Text,
            also_stdout: true,
            with_location: true,
        };
        l.resolve_relative_paths(Path::new("/tmp/configs"));
        assert_eq!(l.file, "/tmp/logs/x.log");

        // 绝对路径不变
        let mut l = LogConfig {
            level: "info".into(),
            file: "/var/log/x.log".into(),
            format: LogFormat::Text,
            also_stdout: true,
            with_location: true,
        };
        l.resolve_relative_paths(Path::new("/tmp/configs"));
        assert_eq!(l.file, "/var/log/x.log");

        // 空字符串不变
        let mut l = LogConfig {
            level: "info".into(),
            file: String::new(),
            format: LogFormat::Text,
            also_stdout: true,
            with_location: true,
        };
        l.resolve_relative_paths(Path::new("/tmp/configs"));
        assert_eq!(l.file, "");

        // "stdout" 不变
        let mut l = LogConfig {
            level: "info".into(),
            file: "stdout".into(),
            format: LogFormat::Text,
            also_stdout: true,
            with_location: true,
        };
        l.resolve_relative_paths(Path::new("/tmp/configs"));
        assert_eq!(l.file, "stdout");
    }
}
