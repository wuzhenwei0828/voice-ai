//! VoiceClient: Rust 终端 SDK
//!
//! 在 webclient::WsClient 之上封装一层：
//!   - 自动 SessionStart / SessionEnd
//!   - 提供 send_audio_chunk / interrupt API
//!   - 通过 VoiceCallback trait 把下行 payload 派发给业务
//!
//! CLI demo 见 bin/voice_terminal.rs

pub mod callback;
pub mod vad;

use std::sync::Arc;

use async_trait::async_trait;
use tracing::{debug, info, warn};
use voice_proto::{encode_indication, VoicePayload};
use webclient::{ws::wsclient::WsClient, ClientCallback, DataEntity};

pub use callback::{DefaultVoiceCallback, VoiceCallback};
pub use vad::{EnergyVad, EnergyVadConfig, VadEvent, VoiceActivityDetector};

/// VoiceClient：包装 webclient::WsClient
#[derive(Clone)]
pub struct VoiceClient {
    ws: Arc<WsClient>,
    session_id: String,
}

impl VoiceClient {
    pub async fn connect(
        ws_url: &str,
        session_id: String,
        callback: Arc<dyn VoiceCallback>,
    ) -> anyhow::Result<Arc<Self>> {
        info!(target: "voice_client", "连接 {}", ws_url);
        let voice_cb_holder = Arc::new(VoiceCallbackHolder {
            inner: callback.clone(),
        });

        let mut ws = WsClient::new(
            ws_url,
            None,                                 // token
            voice_cb_holder.clone() as Arc<dyn ClientCallback>,
            true,                                 // reconnect
            Some(1),                              // reconn_time
        );
        ws.start();

        let me = Arc::new(Self {
            ws: Arc::new(ws),
            session_id,
        });

        // 等 WS ready（轮询 conn_status）
        for _ in 0..50 {
            if *me.ws.conn_status.lock().unwrap() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        info!(target: "voice_client", session_id = %me.session_id, "WS ready");

        // 发 SessionStart
        me.start_session().await?;
        Ok(me)
    }

    pub async fn start_session(&self) -> anyhow::Result<()> {
        let sample_rate = 16000u32;
        let channels = 1u8;
        let codec = "pcm_s16le";
        let language = "zh-CN";
        let p = VoicePayload::SessionStart {
            session_id: self.session_id.clone(),
            sample_rate,
            channels,
            codec: codec.into(),
            language: language.into(),
        };
        let bytes = encode_indication(&p)?;
        self.ws
            .send_binary(bytes)
            .map_err(|e| anyhow::anyhow!("send_binary failed: {:?}", e))?;
        // 与服务端"收到 SessionStart"日志逐字段对齐，便于两端对比
        info!(target: "voice_client", session_id = %self.session_id, sample_rate, channels, codec, language, "发送 SessionStart");
        Ok(())
    }

    pub async fn send_audio_chunk(
        &self,
        seq: u32,
        timestamp_ms: u64,
        data: Vec<u8>,
        is_last: bool,
    ) -> anyhow::Result<()> {
        let bytes_len = data.len();
        let p = VoicePayload::AudioChunk {
            session_id: self.session_id.clone(),
            seq,
            timestamp_ms,
            data,
            is_last,
        };
        let bytes = encode_indication(&p)?;
        self.ws
            .send_binary(bytes)
            .map_err(|e| anyhow::anyhow!("send_binary failed: {:?}", e))?;
        // 与服务端"收到 AudioChunk"日志逐字段对齐，便于两端对比
        info!(target: "voice_client", session_id = %self.session_id, seq, bytes = bytes_len, timestamp_ms, is_last, "发送 AudioChunk");
        Ok(())
    }

    pub async fn interrupt(&self) -> anyhow::Result<()> {
        let p = VoicePayload::Interrupt {
            session_id: self.session_id.clone(),
        };
        let bytes = encode_indication(&p)?;
        self.ws
            .send_binary(bytes)
            .map_err(|e| anyhow::anyhow!("send_binary failed: {:?}", e))?;
        warn!(target: "voice_client", session_id = %self.session_id, "发送 Interrupt");
        Ok(())
    }

    pub async fn end_session(&self, reason: &str) -> anyhow::Result<()> {
        let p = VoicePayload::SessionEnd {
            session_id: self.session_id.clone(),
            reason: reason.to_string(),
        };
        let bytes = encode_indication(&p)?;
        self.ws
            .send_binary(bytes)
            .map_err(|e| anyhow::anyhow!("send_binary failed: {:?}", e))?;
        info!(target: "voice_client", session_id = %self.session_id, reason, "发送 SessionEnd");
        Ok(())
    }
}

/// 把 webclient 的回调转成 VoicePayload 派发
struct VoiceCallbackHolder {
    inner: Arc<dyn VoiceCallback>,
}

#[async_trait]
impl ClientCallback for VoiceCallbackHolder {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn process(
        &self,
        data: DataEntity,
        _callback: Arc<dyn ClientCallback>,
        _client: &WsClient,
    ) {
        match data {
            DataEntity::Binary { data } => {
                let (_kind, p) = match voice_proto::decode_payload(&data) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(target: "voice_client", "解码下行 payload 失败: {}", e);
                        return;
                    }
                };
                debug!(target: "voice_client", "收到下行 payload: {:?}", p);
                self.inner.on_payload(p).await;
            }
            DataEntity::Error { err } => {
                warn!(target: "voice_client", "连接错误: {:?}", err);
            }
            _ => {}
        }
    }
}