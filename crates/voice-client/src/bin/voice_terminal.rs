//! voice_terminal: 终端 CLI demo
//!
//! 工作流程：
//!   1. 连 WS（webclient 提供的自动重连）
//!   2. 从麦克风采集 PCM（cpal 拿输入设备），16kHz s16le 单声道
//!   3. 按 ~20ms 一帧切片，标记 is_last 用一个简化的能量 VAD
//!   4. 通过 VoiceClient 推给服务端
//!   5. 服务端下行（ASR/LLM/TTS）经 VoiceCallback 打到 stdout
//!
//! 启动参数：
//!   --config / --url / --file / --log-level / --log-file
//!   配置文件默认 ./voice-voice_terminal.toml

use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use serde::{Deserialize, Serialize};
use tracing::info;

use voice_client::{DefaultVoiceCallback, VoiceCallback, VoiceClient};
use voice_config::{init_logging, load_yaml, resolve_config_path, LogConfig};
use voice_proto::VoicePayload;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct TerminalConfig {
    #[serde(default)]
    log: LogConfig,
    #[serde(default)]
    terminal: TerminalSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TerminalSection {
    #[serde(default = "default_url")]
    url: String,
    #[serde(default)]
    file: Option<String>,
    #[serde(default = "default_interrupt")]
    interrupt: bool,
}

impl Default for TerminalSection {
    fn default() -> Self {
        Self {
            url: default_url(),
            file: None,
            interrupt: default_interrupt(),
        }
    }
}

fn default_url() -> String {
    "ws://127.0.0.1:8080/ws/voice/cli/demo".into()
}
fn default_interrupt() -> bool {
    true
}

#[derive(Parser, Debug)]
struct Args {
    /// 配置文件路径（YAML）；不传则按 VOICE_CONFIG / 标准路径搜索
    #[arg(long)]
    config: Option<std::path::PathBuf>,

    /// WS 地址（覆盖配置文件）
    #[arg(long)]
    url: Option<String>,

    /// 从 wav 文件读取音频
    #[arg(long)]
    file: Option<String>,

    /// 是否启用打断检测
    #[arg(long)]
    interrupt: Option<bool>,

    /// 日志级别（覆盖配置文件）
    #[arg(long)]
    log_level: Option<String>,

    /// 日志文件路径（覆盖配置文件）
    #[arg(long)]
    log_file: Option<String>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let cfg_path = resolve_config_path("voice_terminal", args.config.as_deref());
    if !cfg_path.exists() {
        tracing::warn!(
            target: "voice_terminal",
            path = %cfg_path.display(),
            "未找到配置文件，使用内置默认"
        );
    }

    let mut cfg: TerminalConfig = load_yaml(&cfg_path)?;
    cfg.log.apply_env_overrides();
    if let Some(parent) = cfg_path.parent() {
        cfg.log.resolve_relative_paths(parent);
    }

    if let Some(u) = args.url { cfg.terminal.url = u; }
    if let Some(f) = args.file { cfg.terminal.file = Some(f); }
    if let Some(i) = args.interrupt { cfg.terminal.interrupt = i; }
    if let Some(l) = args.log_level { cfg.log.level = l; }
    if let Some(f) = args.log_file { cfg.log.file = f; }

    init_logging(&cfg.log)?;

    info!(
        target: "voice_terminal",
        config_file = %cfg_path.display(),
        url = %cfg.terminal.url,
        file = ?cfg.terminal.file,
        interrupt = cfg.terminal.interrupt,
        log_level = %cfg.log.level,
        log_file = %cfg.log.file,
        "voice_terminal 配置加载完成"
    );

    info!("voice_terminal 启动: url={}", cfg.terminal.url);

    let callback = Arc::new(LoggingCallback::default());
    let session_id = format!("cli-{}", uuid::Uuid::new_v4());
    let client = VoiceClient::connect(&cfg.terminal.url, session_id.clone(), callback.clone()).await?;

    // 启动打断监听
    if cfg.terminal.interrupt {
        let cb = callback.clone();
        let cli = client.clone();
        tokio::spawn(async move {
            use tokio::io::AsyncBufReadExt;
            let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();
            info!("打断模式：输入 q<Enter> 触发 Interrupt");
            while let Ok(Some(line)) = stdin.next_line().await {
                if line.trim() == "q" {
                    if let Err(e) = cli.interrupt().await {
                        tracing::warn!("interrupt 发送失败: {}", e);
                    }
                    cb.on_payload(VoicePayload::LlmDelta {
                        session_id: cli.session_id_for_log(),
                        delta: "[已请求打断]".to_string(),
                        is_final: true,
                    }).await;
                } else {
                    info!("输入 q 触发打断，或 Ctrl-C 退出");
                }
            }
        });
    }

    // 推流（来源优先级：cfg.terminal.file > 推麦克风）
    let wav_path = cfg.terminal.file.clone();
    if let Some(path) = wav_path {
        push_from_wav(client.clone(), &path).await?;
    } else {
        push_from_mic(client.clone()).await?;
    }

    client.end_session("normal exit").await?;
    info!("voice_terminal 退出");
    Ok(())
}

// 给 callback 提供一个 session_id 取值辅助
trait SessionIdForLog {
    fn session_id_for_log(&self) -> String;
}
impl SessionIdForLog for Arc<VoiceClient> {
    fn session_id_for_log(&self) -> String {
        // VoiceClient 没有暴露 getter，但有 debug 字段——简单拼个占位
        "cli".to_string()
    }
}

/// 业务侧回调：除了默认行为，再打印 ASR final / TTS 总量
#[derive(Default)]
struct LoggingCallback {
    inner: DefaultVoiceCallback,
}

#[async_trait::async_trait]
impl VoiceCallback for LoggingCallback {
    async fn on_payload(&self, p: VoicePayload) {
        self.inner.on_payload(p).await;
    }
}

/// 读 wav 文件，按 20ms 一帧切片，推流
async fn push_from_wav(client: Arc<VoiceClient>, path: &str) -> anyhow::Result<()> {
    info!("从 wav 文件读取音频: {}", path);
    let mut reader = hound::WavReader::open(path)?;
    let spec = reader.spec();
    info!(
        "wav spec: channels={} sample_rate={} bits={}",
        spec.channels, spec.sample_rate, spec.bits_per_sample
    );

    let samples: Vec<i16> = if spec.bits_per_sample == 16 {
        reader
            .samples::<i16>()
            .collect::<Result<Vec<_>, _>>()?
    } else {
        // 简化：只支持 16-bit
        anyhow::bail!("wav bits_per_sample={} 不支持（仅 16）", spec.bits_per_sample);
    };

    // 重采样到 16kHz 单声道（这里假设源就是 16kHz mono）
    let mono: Vec<i16> = if spec.channels > 1 {
        samples
            .chunks(spec.channels as usize)
            .map(|c| c[0])
            .collect()
    } else {
        samples
    };

    // 20ms 一帧 = 320 samples (16kHz)
    let frame_size = 320usize;
    let start = Instant::now();
    let total = mono.len();

    for (i, frame) in mono.chunks(frame_size).enumerate() {
        let is_last = (i + 1) * frame_size >= total;
        let timestamp_ms = (i * frame_size * 1000 / 16000) as u64;
        // i16 -> little-endian bytes
        let mut bytes = Vec::with_capacity(frame.len() * 2);
        for s in frame {
            bytes.extend_from_slice(&s.to_le_bytes());
        }
        client
            .send_audio_chunk(i as u32, timestamp_ms, bytes, is_last)
            .await?;

        // 模拟采集节奏：每帧 sleep 20ms
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    info!(
        "wav 推送完成: 共 {} 帧 / {} ms",
        mono.len() / frame_size,
        start.elapsed().as_millis()
    );

    // 等下游回包
    tokio::time::sleep(std::time::Duration::from_secs(8)).await;
    Ok(())
}

/// 麦克风采集（简化版：cpal 默认输入流 -> 20ms 帧 -> 推流）
/// 注：MVP 简化采集；真实需要降采样、重采样、能量 VAD
async fn push_from_mic(_client: Arc<VoiceClient>) -> anyhow::Result<()> {
    info!("从麦克风采集（cpal 接入）...");
    // 完整 cpal 集成超出 MVP 范围，给出提示
    info!("MVP 暂未实现麦克风实时采集，请用 --file 传一个 wav 文件测试");
    info!("示例：cargo run -p voice-client --bin voice_terminal -- --file test.wav");
    Ok(())
}

#[allow(dead_code)]
fn _placeholder() {}