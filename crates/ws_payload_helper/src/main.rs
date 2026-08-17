//! ws-payload-helper: 把 VoicePayload 编码成 msgpack 字节，输出 hex
//!
//! 用法：
//!   ws-payload-helper session_start --session-id web_admin
//!   ws-payload-helper audio_chunk --session-id web_admin --seq 1 --is-last true --data @test.raw
//!   ws-payload-helper interrupt --session-id web_admin
//!   ws-payload-helper session_end --session-id web_admin --reason "done"
//!   ws-payload-helper all    一次打印所有常用 payload 的 hex
//!
//! 用 @filename 从文件读 bytes（如音频）

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use voice_proto::VoicePayload;

#[derive(Parser, Debug)]
#[command(name = "ws-payload-helper", about = "VoicePayload → msgpack hex")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// 编码 SessionStart
    SessionStart {
        #[arg(long, default_value = "postman-test")]
        session_id: String,
        #[arg(long, default_value_t = 16000)]
        sample_rate: u32,
        #[arg(long, default_value_t = 1)]
        channels: u8,
        #[arg(long, default_value = "pcm_s16le")]
        codec: String,
        #[arg(long, default_value = "zh-CN")]
        language: String,
    },
    /// 编码 AudioChunk
    AudioChunk {
        #[arg(long, default_value = "postman-test")]
        session_id: String,
        #[arg(long, default_value_t = 1)]
        seq: u32,
        #[arg(long, default_value_t = 0)]
        timestamp_ms: u64,
        /// 音频字节（hex 字符串 或 @filename.bin）
        #[arg(long)]
        data: String,
        #[arg(long, default_value_t = false)]
        is_last: bool,
    },
    /// 编码 Interrupt
    Interrupt {
        #[arg(long, default_value = "postman-test")]
        session_id: String,
    },
    /// 编码 SessionEnd
    SessionEnd {
        #[arg(long, default_value = "postman-test")]
        session_id: String,
        #[arg(long, default_value = "manual end")]
        reason: String,
    },
    /// 编码 Error
    Error {
        #[arg(long, default_value_t = 1001)]
        code: u32,
        #[arg(long)]
        message: String,
    },
    /// 一次打印所有常用 payload 的 hex（session_start + audio_chunk + interrupt + session_end）
    All {
        #[arg(long, default_value = "postman-test")]
        session_id: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let payload = match cli.cmd {
        Cmd::SessionStart {
            session_id,
            sample_rate,
            channels,
            codec,
            language,
        } => VoicePayload::SessionStart {
            session_id,
            sample_rate,
            channels,
            codec,
            language,
        },
        Cmd::AudioChunk {
            session_id,
            seq,
            timestamp_ms,
            data,
            is_last,
        } => {
            let bytes = parse_data_arg(&data)?;
            VoicePayload::AudioChunk {
                session_id,
                seq,
                timestamp_ms,
                data: bytes,
                is_last,
            }
        }
        Cmd::Interrupt { session_id } => VoicePayload::Interrupt { session_id },
        Cmd::SessionEnd { session_id, reason } => {
            VoicePayload::SessionEnd { session_id, reason }
        }
        Cmd::Error { code, message } => VoicePayload::Error { code, message },
        Cmd::All { session_id } => return print_all(&session_id),
    };

    let bytes = webproto::Indication::<VoicePayload>::encode(payload)?;
    let hex = hex::encode(&bytes);
    println!("hex ({} bytes):", bytes.len());
    println!("{}", hex);
    println!();
    println!("base64 ({} bytes):", bytes.len());
    use base64::Engine;
    println!("{}", base64::engine::general_purpose::STANDARD.encode(&bytes));
    Ok(())
}

fn parse_data_arg(arg: &str) -> anyhow::Result<Vec<u8>> {
    if let Some(path) = arg.strip_prefix('@') {
        std::fs::read(PathBuf::from(path))
            .map_err(|e| anyhow::anyhow!("read {}: {}", path, e))
    } else {
        // hex 字符串
        hex::decode(arg).map_err(|e| anyhow::anyhow!("hex decode {}: {}", arg, e))
    }
}

fn print_all(session_id: &str) -> anyhow::Result<()> {
    use base64::Engine;
    println!("=== SessionStart ===");
    let p = VoicePayload::SessionStart {
        session_id: session_id.to_string(),
        sample_rate: 16000,
        channels: 1,
        codec: "pcm_s16le".to_string(),
        language: "zh-CN".to_string(),
    };
    let b = webproto::Indication::<VoicePayload>::encode(p)?;
    println!("hex ({} bytes): {}", b.len(), hex::encode(&b));
    println!("base64: {}\n", base64::engine::general_purpose::STANDARD.encode(&b));

    println!("=== AudioChunk (空 data, is_last=true) ===");
    let p = VoicePayload::AudioChunk {
        session_id: session_id.to_string(),
        seq: 1,
        timestamp_ms: 0,
        data: vec![],
        is_last: true,
    };
    let b = webproto::Indication::<VoicePayload>::encode(p)?;
    println!("hex ({} bytes): {}", b.len(), hex::encode(&b));
    println!("base64: {}\n", base64::engine::general_purpose::STANDARD.encode(&b));

    println!("=== Interrupt ===");
    let p = VoicePayload::Interrupt {
        session_id: session_id.to_string(),
    };
    let b = webproto::Indication::<VoicePayload>::encode(p)?;
    println!("hex ({} bytes): {}", b.len(), hex::encode(&b));
    println!("base64: {}\n", base64::engine::general_purpose::STANDARD.encode(&b));

    println!("=== SessionEnd ===");
    let p = VoicePayload::SessionEnd {
        session_id: session_id.to_string(),
        reason: "manual end".to_string(),
    };
    let b = webproto::Indication::<VoicePayload>::encode(p)?;
    println!("hex ({} bytes): {}", b.len(), hex::encode(&b));
    println!("base64: {}\n", base64::engine::general_purpose::STANDARD.encode(&b));

    Ok(())
}