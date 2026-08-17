//! TTS 适配层：把百炼 GAX WS / 公共 DashScope WS 协议桥接到 voice-server 的 TtsClient trait
//!
//! 与 asr/mod.rs 同构：定义自己的 TtsClient / TtsEvent 类型，让 PR1 在 voice-server 侧
//! 包装成 voice-server 期望的 trait 对象。
//!
//! ## 协议分发
//!
//! voice-providers 支持两套 TTS 协议：
//!
//! | 模型                          | 协议               | adapter                       |
//! |-------------------------------|--------------------|-------------------------------|
//! | cosyvoice-v2 (legacy GAX)     | GAX cmd+protobuf   | `cosyvoice::CosyVoiceV2`      |
//! | qwen-audio-3.0-tts-flash/plus | JSON run-task ...  | `qwen_audio::QwenAudioTts`    |
//! | cosyvoice-v2/v3-flash/v3-plus | JSON run-task ...  | `qwen_audio::QwenAudioTts`    |
//! | qwen3-tts-flash-realtime      | Realtime session   | `qwen_realtime::QwenRealtimeTts` |
//!
//! 协议由 adapter 的 `protocol()` 给出；`StreamingTtsClient::synthesize` 据此走不同 pipeline。

pub mod cosyvoice;
pub mod qwen_audio;
pub mod qwen_realtime;

use std::pin::Pin;
use std::sync::Arc;

use async_stream::stream;
use async_trait::async_trait;
use futures_util::Stream;
use tracing::{debug, info, warn};

use crate::asr::{BoxStream, ClientError};
use crate::codec::GaxFrame;
use crate::ws_pool::{LaneKind, PooledConn, WsMessage, WsPool};

use base64::Engine as _;

pub use cosyvoice::CosyVoiceV2;
pub use qwen_audio::QwenAudioTts;
pub use qwen_realtime::QwenRealtimeTts;

// ===== 公共类型 =====

#[derive(Debug, Clone)]
pub struct TtsEvent {
    pub seq: u32,
    pub data: Vec<u8>,
    pub is_last: bool,
}

pub type ArcTts = Arc<dyn TtsClient>;

// ===== 协议枚举 =====

/// TTS adapter 协议族。`StreamingTtsClient` 据此选择 pipeline。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TtsProtocol {
    /// 老的 GAX binary 协议（cmd byte + protobuf payload），仅 cosyvoice-v2 legacy 走这里。
    Gax,
    /// 公共 DashScope WS 协议：JSON run-task/continue-task/finish-task 控制面 + binary 音频。
    /// 用于 qwen-audio-tts 系列、cosyvoice-v2/v3/v3.5 系列。
    JsonDuplex,
    /// Qwen-TTS Realtime API：session.update/input_text_buffer.* + response.audio.delta (base64)。
    /// 用于 qwen3-tts-flash-realtime / qwen3-tts-instruct-flash-realtime。
    RealtimeSession,
}

// ===== Adapter trait =====

pub trait TtsModelAdapter: Send + Sync {
    fn model_name(&self) -> &'static str;
    fn voice(&self) -> &str;
    fn protocol(&self) -> TtsProtocol;

    // ===== GAX 协议钩子（仅 protocol=Gax 的 adapter 实现） =====
    fn open_request(&self, sr: u32, format: &str, stream: bool) -> GaxFrame {
        let _ = (sr, format, stream);
        GaxFrame::new(0, Vec::new())
    }
    fn text_frame(&self, text: &str) -> GaxFrame {
        let _ = text;
        GaxFrame::new(0, Vec::new())
    }
    fn stop_frame(&self) -> GaxFrame {
        GaxFrame::new(0, Vec::new())
    }
    /// 解析 GAX 音频分片 → 原始 PCM bytes
    fn parse_audio(&self, payload: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        let _ = payload;
        Ok(None)
    }

    // ===== JSON duplex 协议钩子（仅 protocol=JsonDuplex 的 adapter 实现） =====
    fn run_task_text(&self, _task_id: &str, _sample_rate: u32, _format: &str) -> Result<String, ClientError> {
        Err(ClientError::Decode("adapter does not implement JsonDuplex protocol".into()))
    }
    fn continue_task_text(&self, _task_id: &str, _text: &str) -> Result<String, ClientError> {
        Err(ClientError::Decode("adapter does not implement JsonDuplex protocol".into()))
    }
    fn finish_task_text(&self, _task_id: &str) -> Result<String, ClientError> {
        Err(ClientError::Decode("adapter does not implement JsonDuplex protocol".into()))
    }
    fn parse_server_event(&self, _text: &str) -> Result<ServerEventHint, ClientError> {
        Err(ClientError::Decode("adapter does not implement JsonDuplex protocol".into()))
    }

    // ===== Realtime 协议钩子（仅 protocol=RealtimeSession 的 adapter 实现） =====
    fn session_update_text(&self) -> Result<String, ClientError> {
        Err(ClientError::Decode("adapter does not implement RealtimeSession protocol".into()))
    }
    fn append_text_text(&self, _text: &str) -> Result<String, ClientError> {
        Err(ClientError::Decode("adapter does not implement RealtimeSession protocol".into()))
    }
    fn commit_text(&self) -> Result<String, ClientError> {
        Err(ClientError::Decode("adapter does not implement RealtimeSession protocol".into()))
    }
    fn session_finish_text(&self) -> Result<String, ClientError> {
        Err(ClientError::Decode("adapter does not implement RealtimeSession protocol".into()))
    }
    fn parse_realtime_event(&self, _text: &str) -> Result<RealtimeEventHint, ClientError> {
        Err(ClientError::Decode("adapter does not implement RealtimeSession protocol".into()))
    }
}

/// server event 提示：JSON duplex 协议的上层只需关心几个事件类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerEventHint {
    /// task-started：可发 continue-task
    TaskStarted,
    /// result-generated: type=sentence-begin
    SentenceBegin { index: u32, original_text: Option<String> },
    /// result-generated: type=sentence-synthesis（紧跟一个 binary 音频帧）
    SentenceSynthesis { index: u32 },
    /// result-generated: type=sentence-end（audio 已结束，可发 finish-task）
    SentenceEnd { index: u32, characters: u32 },
    /// task-finished：所有音频已收完
    TaskFinished { request_uuid: Option<String>, characters: u32 },
    /// task-failed：任务失败，错误信息
    TaskFailed { code: Option<String>, message: Option<String> },
    /// 其它未识别事件（忽略）
    Other,
}

/// Realtime event 提示
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RealtimeEventHint {
    SessionCreated { session_id: String },
    SessionUpdated,
    ResponseCreated { response_id: String },
    ResponseDone,
    SessionFinished,
    AudioDelta { sample_b64: String },
    Error { message: String },
    Other,
}

/// 按模型 + 音色挑 adapter
pub fn select_tts_adapter(model: &str, voice: &str) -> anyhow::Result<Box<dyn TtsModelAdapter>> {
    match model {
        // legacy GAX（cosyvoice-v2 在 GAX 协议下保留向后兼容入口）
        "cosyvoice-gax" | "cosyvoice-v2-gax" => {
            Ok(Box::new(CosyVoiceV2::new(voice.to_string())))
        }
        // 公共 DashScope JSON duplex（qwen-audio-tts / cosyvoice 系列都走这个）
        m if m.starts_with("qwen-audio") || m.starts_with("cosyvoice") => {
            Ok(Box::new(QwenAudioTts::new(model.to_string(), voice.to_string())))
        }
        // Qwen-TTS Realtime（qwen3-tts-flash-realtime / qwen3-tts-instruct-flash-realtime）
        m if m.starts_with("qwen3-tts") || m.starts_with("qwen-tts") => {
            Ok(Box::new(QwenRealtimeTts::new(model.to_string(), voice.to_string())))
        }
        other => anyhow::bail!("不支持的百炼 TTS 模型: {other}"),
    }
}

// ===== TtsClient trait =====

#[async_trait]
pub trait TtsClient: Send + Sync {
    async fn synthesize(
        &self,
        session_id: &str,
        text: &str,
    ) -> Result<BoxStream<Result<TtsEvent, ClientError>>, ClientError>;
}

// ===== StreamingTtsClient =====

pub struct StreamingTtsClient {
    pool: Arc<WsPool>,
    adapter: Box<dyn TtsModelAdapter>,
    /// 调用方传入的 model 名（如 `cosyvoice-v2-gax`），用于在 pipeline 内重新 select adapter
    /// （不能用 adapter.model_name()，因为 adapter 可能返回标准化的 `&'static str`，丢失路由信息）
    input_model: String,
    dialer: crate::ws_pool::Dialer,
    sample_rate: u32,
    response_format: String,
    stream: bool,
}

impl StreamingTtsClient {
    pub fn new(
        pool: Arc<WsPool>,
        adapter: Box<dyn TtsModelAdapter>,
        input_model: String,
        dialer: crate::ws_pool::Dialer,
        sample_rate: u32,
        response_format: String,
        stream: bool,
    ) -> Self {
        Self {
            pool,
            adapter,
            input_model,
            dialer,
            sample_rate,
            response_format,
            stream,
        }
    }
}

#[async_trait]
impl TtsClient for StreamingTtsClient {
    async fn synthesize(
        &self,
        session_id: &str,
        text: &str,
    ) -> Result<BoxStream<Result<TtsEvent, ClientError>>, ClientError> {
        let adapter_name = self.adapter.model_name().to_string();
        let voice = self.adapter.voice().to_string();
        let sr = self.sample_rate;
        let format = self.response_format.clone();
        let stream = self.stream;
        let protocol = self.adapter.protocol();
        let input_model = self.input_model.clone();
        info!(
            target: "voice_providers.tts",
            session_id,
            adapter = %adapter_name,
            input_model = %input_model,
            voice = %voice,
            ?protocol,
            sr,
            format = %format,
            stream,
            text_chars = text.chars().count(),
            "TTS 开始"
        );

        let pool = self.pool.clone();
        let dialer = self.dialer.clone();
        let session_id = session_id.to_string();
        let text = text.to_string();

        let s = stream! {
            // 拨号
            let mut conn = match pool.acquire_or_dial(LaneKind::Tts, dialer).await {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        target: "voice_providers.tts",
                        session_id,
                        error = %e,
                        "TTS 拨号失败"
                    );
                    yield Err(ClientError::Pool(e.to_string()));
                    return;
                }
            };

            // 按协议分发：pipeline 函数内部按需重新构造 adapter
            // （adapter 构造便宜，避免把 Box<dyn> 跨 await 点传递）
            let result: Result<Vec<TtsEvent>, ClientError> = match protocol {
                TtsProtocol::Gax => {
                    gax_pipeline(&mut conn, &input_model, &voice, sr, &format, stream, &text, &session_id).await
                }
                TtsProtocol::JsonDuplex => {
                    json_duplex_pipeline(&mut conn, &input_model, &voice, &text, &session_id).await
                }
                TtsProtocol::RealtimeSession => {
                    realtime_pipeline(&mut conn, &input_model, &voice, sr, &format, &text, &session_id).await
                }
            };

            // 用完归还连接
            let healthy = conn.is_healthy();
            conn.release(healthy);

            match result {
                Ok(events) => {
                    for ev in events {
                        yield Ok(ev);
                    }
                }
                Err(e) => {
                    yield Err(e);
                }
            }
        };
        Ok(Box::pin(s) as Pin<Box<dyn Stream<Item = Result<TtsEvent, ClientError>> + Send>>)
    }
}

// ===== GAX pipeline（cosyvoice-v2 legacy GAX） =====

async fn gax_pipeline(
    conn: &mut PooledConn,
    adapter_name: &str,
    voice: &str,
    sr: u32,
    format: &str,
    stream: bool,
    text: &str,
    session_id: &str,
) -> Result<Vec<TtsEvent>, ClientError> {
    let adapter = select_tts_adapter(adapter_name, voice).map_err(|e| ClientError::Decode(e.to_string()))?;

    let open = adapter.open_request(sr, format, stream);
    // sanity check：CosyVoiceV2 等 GAX adapter 必须产生 cmd != 0 的有效帧
    if open.cmd == 0 && open.payload.is_empty() {
        return Err(ClientError::Decode(format!(
            "GAX adapter missing (model={}, voice={})",
            adapter_name, voice
        )));
    }
    conn.send(open).await.map_err(|e| ClientError::Ws(e.to_string()))?;

    let tf = adapter.text_frame(text);
    conn.send(tf).await.map_err(|e| ClientError::Ws(e.to_string()))?;

    let sf = adapter.stop_frame();
    if let Err(e) = conn.send(sf).await {
        warn!(
            target: "voice_providers.tts",
            session_id,
            error = %e,
            "TTS stop 发送失败"
        );
    }

    let mut events = Vec::new();
    let mut seq: u32 = 0;
    loop {
        let frame = match conn.recv().await {
            Ok(f) => f,
            Err(e) => {
                return Err(ClientError::Ws(e.to_string()));
            }
        };
        use crate::codec::{RESP_AUDIO_TTS, RESP_DONE_TTS, RESP_ERR_TTS};
        match frame.cmd {
            RESP_AUDIO_TTS => {
                match adapter.parse_audio(&frame.payload) {
                    Ok(Some(bytes)) => {
                        seq += 1;
                        debug!(
                            target: "voice_providers.tts",
                            session_id,
                            seq,
                            bytes = bytes.len(),
                            "TTS 收到音频"
                        );
                        events.push(TtsEvent { seq, data: bytes, is_last: false });
                    }
                    Ok(None) => continue,
                    Err(e) => return Err(e),
                }
            }
            RESP_DONE_TTS => {
                seq += 1;
                events.push(TtsEvent { seq, data: Vec::new(), is_last: true });
                return Ok(events);
            }
            RESP_ERR_TTS => {
                return Err(ClientError::Decode(String::from_utf8_lossy(&frame.payload).into_owned()));
            }
            _ => continue,
        }
    }
}

// ===== JSON duplex pipeline（qwen-audio-tts / cosyvoice-v2/v3） =====

async fn json_duplex_pipeline(
    conn: &mut PooledConn,
    adapter_name: &str,
    voice: &str,
    text: &str,
    session_id: &str,
) -> Result<Vec<TtsEvent>, ClientError> {
    let adapter = select_tts_adapter(adapter_name, voice)
        .map_err(|e| ClientError::Decode(e.to_string()))?;
    let task_id = uuid::Uuid::new_v4().to_string();
    debug!(
        target: "voice_providers.tts",
        session_id,
        task_id = %task_id,
        "qwen audio tts: 生成 task_id"
    );

    // 1) run-task
    let run_text = adapter.run_task_text(&task_id, 0, "")?;
    conn.send_text(&run_text).await.map_err(|e| ClientError::Ws(e.to_string()))?;

    // 2) 等 task-started，再发 continue-task + finish-task
    let mut started = false;
    let mut text_sent = false;
    let mut finished_signal = false;
    let mut last_event: Option<ServerEventHint> = None;
    let mut events = Vec::new();
    let mut seq: u32 = 0;

    while !finished_signal {
        match conn.recv_message().await {
            Err(e) => return Err(ClientError::Ws(e.to_string())),
            Ok(WsMessage::Close) => return Err(ClientError::Ws("closed by peer".into())),
            Ok(WsMessage::Text(s)) => {
                let hint = adapter.parse_server_event(&s)?;
                debug!(
                    target: "voice_providers.tts",
                    session_id,
                    ?hint,
                    "server event"
                );
                match &hint {
                    ServerEventHint::TaskStarted => {
                        started = true;
                        if !text_sent {
                            let cont = adapter.continue_task_text(&task_id, text)?;
                            conn.send_text(&cont).await.map_err(|e| ClientError::Ws(e.to_string()))?;
                            let fin = adapter.finish_task_text(&task_id)?;
                            conn.send_text(&fin).await.map_err(|e| ClientError::Ws(e.to_string()))?;
                            text_sent = true;
                        }
                    }
                    ServerEventHint::TaskFinished { characters, .. } => {
                        debug!(
                            target: "voice_providers.tts",
                            session_id,
                            characters,
                            "qwen audio tts: task-finished"
                        );
                        finished_signal = true;
                    }
                    ServerEventHint::TaskFailed { code, message } => {
                        return Err(ClientError::Decode(format!(
                            "task-failed code={:?} msg={:?}",
                            code, message
                        )));
                    }
                    _ => {}
                }
                last_event = Some(hint);
            }
            Ok(WsMessage::Binary(audio)) => {
                if !started {
                    debug!(
                        target: "voice_providers.tts",
                        session_id,
                        "收到 binary 音频但 task 尚未 started，忽略"
                    );
                    continue;
                }
                seq += 1;
                debug!(
                    target: "voice_providers.tts",
                    session_id,
                    seq,
                    bytes = audio.len(),
                    "TTS 收到音频帧"
                );
                events.push(TtsEvent {
                    seq,
                    data: audio,
                    is_last: false,
                });
            }
        }
    }

    if !matches!(last_event, Some(ServerEventHint::TaskFinished { .. })) {
        return Err(ClientError::Decode("task 未正常结束".into()));
    }

    seq += 1;
    events.push(TtsEvent {
        seq,
        data: Vec::new(),
        is_last: true,
    });

    Ok(events)
}

// ===== Realtime pipeline（qwen3-tts-flash-realtime） =====

async fn realtime_pipeline(
    conn: &mut PooledConn,
    adapter_name: &str,
    voice: &str,
    _sample_rate: u32,
    _format: &str,
    text: &str,
    session_id: &str,
) -> Result<Vec<TtsEvent>, ClientError> {
    let adapter = select_tts_adapter(adapter_name, voice)
        .map_err(|e| ClientError::Decode(e.to_string()))?;

    // 1) session.update
    let session_text = adapter.session_update_text()?;
    conn.send_text(&session_text).await.map_err(|e| ClientError::Ws(e.to_string()))?;

    // 2) 追加文本
    let append_text = adapter.append_text_text(text)?;
    conn.send_text(&append_text).await.map_err(|e| ClientError::Ws(e.to_string()))?;

    // 3) 结束会话（默认 server_commit 模式）
    let finish_text = adapter.session_finish_text()?;
    conn.send_text(&finish_text).await.map_err(|e| ClientError::Ws(e.to_string()))?;

    let mut events = Vec::new();
    let mut seq: u32 = 0;
    let mut session_finished = false;
    let b64 = base64::engine::general_purpose::STANDARD;

    while !session_finished {
        match conn.recv_message().await {
            Err(e) => return Err(ClientError::Ws(e.to_string())),
            Ok(WsMessage::Close) => return Err(ClientError::Ws("closed by peer".into())),
            Ok(WsMessage::Text(s)) => {
                let hint = adapter.parse_realtime_event(&s)?;
                debug!(
                    target: "voice_providers.tts",
                    session_id,
                    ?hint,
                    "realtime event"
                );
                match &hint {
                    RealtimeEventHint::AudioDelta { sample_b64 } => {
                        match b64.decode(sample_b64.as_bytes()) {
                            Ok(bytes) => {
                                seq += 1;
                                events.push(TtsEvent {
                                    seq,
                                    data: bytes,
                                    is_last: false,
                                });
                            }
                            Err(e) => {
                                warn!(
                                    target: "voice_providers.tts",
                                    session_id,
                                    error = %e,
                                    "realtime audio delta base64 解码失败"
                                );
                            }
                        }
                    }
                    RealtimeEventHint::SessionFinished => {
                        session_finished = true;
                    }
                    RealtimeEventHint::Error { message } => {
                        return Err(ClientError::Decode(format!("realtime error: {}", message)));
                    }
                    _ => {}
                }
            }
            Ok(WsMessage::Binary(_)) => {
                debug!(
                    target: "voice_providers.tts",
                    session_id,
                    "realtime 收到 binary 帧（忽略）"
                );
            }
        }
    }

    seq += 1;
    events.push(TtsEvent {
        seq,
        data: Vec::new(),
        is_last: true,
    });

    Ok(events)
}

// ===== factory =====

pub fn build_tts_client(
    pool: Arc<WsPool>,
    model: &str,
    voice: &str,
    sample_rate: u32,
    response_format: String,
    stream: bool,
    dialer: crate::ws_pool::Dialer,
) -> anyhow::Result<ArcTts> {
    let adapter = select_tts_adapter(model, voice)?;
    Ok(Arc::new(StreamingTtsClient::new(
        pool, adapter, model.to_string(), dialer, sample_rate, response_format, stream,
    )))
}
