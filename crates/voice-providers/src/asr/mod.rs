//! ASR 适配层：把百炼 GAX WS 桥接到 voice-server 的 AsrClient trait
//!
//! 关键抽象：
//!   - `AsrEvent` / `LlmEvent` / `TtsEvent` / `ClientError` / `BoxStream` / `ArcAsr` 等类型：
//!     与 voice-server 中对应字段同构（text/is_final / delta/is_final / seq/data/is_last）。
//!     voice-providers **不**依赖 voice-server —— 类型并行定义，让 PR1 在 voice-server 侧
//!     用 `impl AsrClient for ...` 把 voice-providers 流包成 voice-server 期望的 trait 对象。
//!   - `AsrModelAdapter` trait：不同 ASR 模型（paraformer-realtime-v2 / sensevoice-v1）
//!     实现不同 `parse_event` 和 `open_request` 字段语义
//!   - `select_asr_adapter(model_name)` 按模型名挑 adapter
//!
//! ## 切片发送策略
//!
//! 默认按 100ms 切片（16kHz s16le mono = 3200 字节）。短于此的请求会被合并成长度 ≥ 3200
//! 的单帧或几帧（最后一帧可能不足）。本 PR 骨架阶段：dialer 由 build_all 时注入。

pub mod paraformer;
pub mod qwen;
pub mod qwen3_realtime;
pub mod session;

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_stream::stream;
use async_trait::async_trait;
use futures_util::Stream;
use tracing::{debug, info, warn};

use crate::codec::GaxFrame;
use crate::ws_pool::{LaneKind, PoolError, WsPool};

// ===== 公共类型（与 voice-server 同构） =====

#[derive(Debug, Clone)]
pub struct AsrEvent {
    pub text: String,
    pub is_final: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("http error: {0}")]
    Http(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("service returned status {0}")]
    Status(u16),
    #[error("pool error: {0}")]
    Pool(String),
    #[error("ws error: {0}")]
    Ws(String),
}

impl From<PoolError> for ClientError {
    fn from(e: PoolError) -> Self {
        ClientError::Pool(e.to_string())
    }
}

pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;
pub type ArcAsr = Arc<dyn AsrClient>;

// ===== Adapter trait =====

pub trait AsrModelAdapter: Send + Sync {
    fn model_name(&self) -> &'static str;

    /// 构造 AsrOpenRequest protobuf frame
    fn open_request(&self, session_id: &str, sr: u32, ch: u16) -> GaxFrame;

    /// 构造音频分片 frame（cmd + AsrAudioChunk payload）
    fn audio_frame(&self, pcm: &[u8]) -> GaxFrame;

    /// 构造 stop frame
    fn stop_frame(&self, session_id: &str) -> GaxFrame;

    /// 解析服务端 transcript frame → AsrEvent
    /// 返回 Ok(Some(event)) 表示有 ASR 事件；Ok(None) 表示非 transcript 帧（可忽略）；
    /// 返回 Err 表示真错误（解码失败）。
    fn parse_event(&self, payload: &[u8]) -> Result<Option<AsrEvent>, ClientError>;
}

/// 按模型名挑 adapter。
///
/// - `paraformer-realtime-v2`（GAX 二进制占位实现，PR 5 之前用）→ `paraformer::ParaformerRealtime`
/// - Qwen-Audio-3.0 / Fun-ASR / Qwen-Paraformer-realtime 走 JSON 文本 + 裸 PCM 协议
///   → `qwen::QwenAsrAdapter`（实际调试中校准的协议版本）
pub fn select_asr_adapter(model: &str) -> anyhow::Result<Box<dyn AsrModelAdapter>> {
    match model {
        // 旧 GAX 占位实现，保留兼容
        "paraformer-realtime-v2" => Ok(Box::new(paraformer::ParaformerRealtime::new())),
        // Qwen3-ASR-Flash-Realtime（OpenAI-Realtime 风格协议，JSON+base64）
        "qwen3-asr-flash-realtime"
        | "qwen3-asr-flash-realtime-2026-02-10"
        | "qwen3-asr-flash-realtime-2025-10-27" => {
            Ok(Box::new(qwen3_realtime::Qwen3RealtimeAdapter::for_model(model)))
        }
        // Qwen-Audio 3.0 主力
        "qwen-audio-3.0-asr-flash-streaming" | "qwen-audio-3.0" => {
            Ok(Box::new(qwen::QwenAsrAdapter::for_model(model)))
        }
        // Fun-ASR-Realtime 系列
        "fun-asr-realtime"
        | "fun-asr-realtime-2025-11-07"
        | "fun-asr-realtime-2026-02-28"
        | "fun-asr-realtime-2025-09-15" => Ok(Box::new(qwen::QwenAsrAdapter::for_model(model))),
        // Fun-ASR 8kHz 电话场景
        "fun-asr-flash-8k-realtime" | "fun-asr-flash-8k-realtime-2026-01-28" => {
            Ok(Box::new(qwen::QwenAsrAdapter::for_model(model)))
        }
        // Qwen-Paraformer 16kHz（Qwen 公共协议）
        "paraformer-realtime" | "paraformer-realtime-v1" => {
            Ok(Box::new(qwen::QwenAsrAdapter::for_model(model)))
        }
        // Qwen-Paraformer 8kHz（带情感识别）
        "paraformer-realtime-8k-v2" | "paraformer-realtime-8k-v1" => {
            Ok(Box::new(qwen::QwenAsrAdapter::for_model(model)))
        }
        other => anyhow::bail!("不支持的百炼 ASR 模型: {other}"),
    }
}

// ===== AsrClient trait =====

#[async_trait]
pub trait AsrClient: Send + Sync {
    async fn recognize(
        &self,
        session_id: &str,
        filename: Option<&str>,
        audio: Vec<u8>,
    ) -> Result<BoxStream<Result<AsrEvent, ClientError>>, ClientError>;
}

// ===== StreamingAsrClient =====

/// PCM s16le 单声道 16kHz 下 100ms = 3200 字节
pub const CHUNK_BYTES: usize = 3_200;
/// 单次 ASR 会话总超时（含建连 + 数据发送 + 接收 final）：30s 兜底
pub const ASR_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);

/// 一个 session 内的 ASR 客户端。AsrClient::recognize 每次调用都新建一个会话。
pub struct StreamingAsrClient {
    pool: Arc<WsPool>,
    adapter: Box<dyn AsrModelAdapter>,
    /// 拨号器：在 build_all 时注入
    dialer: crate::ws_pool::Dialer,
    /// 采样率 / 声道
    sample_rate: u32,
    channels: u16,
}

impl StreamingAsrClient {
    pub fn new(
        pool: Arc<WsPool>,
        adapter: Box<dyn AsrModelAdapter>,
        dialer: crate::ws_pool::Dialer,
        sample_rate: u32,
        channels: u16,
    ) -> Self {
        Self {
            pool,
            adapter,
            dialer,
            sample_rate,
            channels,
        }
    }
}

#[async_trait]
impl AsrClient for StreamingAsrClient {
    async fn recognize(
        &self,
        session_id: &str,
        _filename: Option<&str>,
        audio: Vec<u8>,
    ) -> Result<BoxStream<Result<AsrEvent, ClientError>>, ClientError> {
        let adapter_name = self.adapter.model_name();
        info!(
            target: "voice_providers.asr",
            session_id,
            adapter = adapter_name,
            bytes = audio.len(),
            "ASR 开始（GAX WS 骨架）"
        );

        let pool = self.pool.clone();
        let dialer = self.dialer.clone();
        let adapter_name = adapter_name.to_string();
        let sr = self.sample_rate;
        let ch = self.channels;
        let session_id = session_id.to_string();

        // 骨架实现：每次 recognize 启动一个后台任务跑 GAX WS pipeline，
        // 通过 channel 把 AsrEvent 推给返回的 stream。
        // 真实路径在 PR 5 注入 dialer 后接通。
        let stream = stream! {
            // 切片 + 拨号 + 发送 + 解析 transcript
            let chunks: Vec<Vec<u8>> = audio
                .chunks(CHUNK_BYTES)
                .map(|c| c.to_vec())
                .collect();
            debug!(
                target: "voice_providers.asr",
                session_id,
                chunk_count = chunks.len(),
                chunk_bytes = CHUNK_BYTES,
                "音频切片完成"
            );

            // 拨号（真实实现）
            let mut conn = match pool.acquire_or_dial(LaneKind::Asr, dialer).await {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        target: "voice_providers.asr",
                        session_id,
                        error = %e,
                        "ASR 拨号失败（dialer 占位）"
                    );
                    yield Err(ClientError::Pool(e.to_string()));
                    return;
                }
            };

            // 发送 open 请求
            let open = adapter_open_for(&adapter_name, &session_id, sr, ch);
            if let Err(e) = conn.send(open).await {
                yield Err(ClientError::Ws(e.to_string()));
                conn.release(false);
                return;
            }

            // 发送所有 audio chunks
            for chunk in &chunks {
                let frame = adapter_audio_for(&adapter_name, chunk);
                if let Err(e) = conn.send(frame).await {
                    yield Err(ClientError::Ws(e.to_string()));
                    conn.release(false);
                    return;
                }
            }

            // 发送 stop
            let stop = adapter_stop_for(&adapter_name, &session_id);
            if let Err(e) = conn.send(stop).await {
                warn!(
                    target: "voice_providers.asr",
                    session_id,
                    error = %e,
                    "ASR stop 发送失败"
                );
            }

            // 读 transcript 直到 is_final=true 或连接断开
            loop {
                match conn.recv().await {
                    Ok(frame) => {
                        match adapter_parse_for(&adapter_name, &frame.payload) {
                            Ok(Some(evt)) => {
                                let is_final = evt.is_final;
                                yield Ok(evt);
                                if is_final { break; }
                            }
                            Ok(None) => continue,
                            Err(e) => {
                                yield Err(e);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        if !conn.is_healthy() {
                            yield Err(ClientError::Ws(e.to_string()));
                        }
                        break;
                    }
                }
            }

            conn.release(true);
        };
        Ok(Box::pin(stream))
    }
}

// ===== adapter 调度辅助 =====
//
// 骨架阶段简化：用 model_name 字符串动态 dispatch，避免把 adapter 实例 move 进 async stream；
// 真实生产里应直接持有 `Box<dyn AsrModelAdapter>` clone。

fn adapter_open_for(model: &str, session_id: &str, sr: u32, ch: u16) -> GaxFrame {
    let adapter = select_asr_adapter(model).expect("已校验");
    adapter.open_request(session_id, sr, ch)
}

fn adapter_audio_for(model: &str, pcm: &[u8]) -> GaxFrame {
    let adapter = select_asr_adapter(model).expect("已校验");
    adapter.audio_frame(pcm)
}

fn adapter_stop_for(model: &str, session_id: &str) -> GaxFrame {
    let adapter = select_asr_adapter(model).expect("已校验");
    adapter.stop_frame(session_id)
}

fn adapter_parse_for(model: &str, payload: &[u8]) -> Result<Option<AsrEvent>, ClientError> {
    let adapter = select_asr_adapter(model).expect("已校验");
    adapter.parse_event(payload)
}

// ===== factory =====

pub fn build_asr_client(
    pool: Arc<WsPool>,
    model: &str,
    sample_rate: u32,
    channels: u16,
    dialer: crate::ws_pool::Dialer,
) -> anyhow::Result<ArcAsr> {
    let adapter = select_asr_adapter(model)?;
    Ok(Arc::new(StreamingAsrClient::new(
        pool, adapter, dialer, sample_rate, channels,
    )))
}

/// 占位 dialer：返回 Handshake 错误。生产代码在 build_all 时替换为真实 dialer。
pub fn make_dummy_dialer() -> crate::ws_pool::Dialer {
    use futures_util::future::FutureExt;
    Arc::new(move |_kind| {
        async move { Err(PoolError::Handshake("voice-providers: dialer 尚未注入（PR 5 占位）".into())) }
            .boxed()
    })
}

// ===== 增量式 streaming session（start / send_audio / finish）=====

pub use session::{start_session as start_streaming_session, StreamingAsrSession};