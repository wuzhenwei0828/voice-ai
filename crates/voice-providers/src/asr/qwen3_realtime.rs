//! Qwen3-ASR-Flash-Realtime（`qwen3-asr-flash-realtime`）适配层
//! —— DashScope Realtime API（OpenAI-Realtime 风格，全 JSON 文本帧）
//!
//! 协议概览（参考 `crates/voice-providers/docs/qwen-asr-docs/real-asr-code.md` L528-860、L2182-2369）：
//!
//! | 方向       | 内容                                             | 对应方法 / 通道            |
//! | ---------- | ------------------------------------------------ | -------------------------- |
//! | C → S      | `session.update`（modalities/format/sample_rate/turn_detection） | `open_request` → Text 帧 |
//! | C → S      | `input_audio_buffer.append`（**base64** PCM）    | `audio_frame` → Text 帧    |
//! | C → S      | `session.finish`                                 | `stop_frame` → Text 帧     |
//! | S → C      | `conversation.item.input_audio_transcription.text`（partial：`text`+`stash`） | `parse_realtime_event` |
//! | S → C      | `conversation.item.input_audio_transcription.completed`（final：`transcript`） | 同上 |
//! | S → C      | `input_audio_buffer.speech_started` / `speech_stopped`（服务端 VAD） | 同上 |
//! | S → C      | `session.finished`（会话终态）                   | 同上 → `RealtimeEvent::Finished` |
//! | S → C      | `error`                                          | 同上 → `Err(ClientError)`  |
//!
//! 与 `qwen.rs`（DashScope 公共协议）的关键差异：
//! 1. 端点是业务空间专属域名 `wss://{WorkspaceId}.{region}.maas.aliyuncs.com/api-ws/v1/realtime?model=...`
//!    （**不是** `dashscope.aliyuncs.com`——那是 TTS realtime 规则；端点解析见本文件
//!    `resolve_realtime_endpoint` 与 voice-server 侧 `asr_stream` 配置）
//! 2. 握手需额外请求头 `OpenAI-Beta: realtime=v1`（见 `make_realtime_dialer`）
//! 3. 音频走 **JSON 文本帧 + base64**（公共协议是裸 PCM 二进制帧）
//! 4. 断句由服务端 VAD（`turn_detection.server_vad`）决定；终态是 `session.finished`
//!    （不是每句的 `sentence_end`）——因此本模块自带 `start_realtime_session`
//!    会话循环，不复用 `session.rs`（那个在首个 is_final 即断流，多句听写会丢句）

use base64::Engine;
use serde::Deserialize;
use serde_json::json;

use crate::asr::{AsrEvent, AsrModelAdapter, ClientError};
use crate::codec::{GaxFrame, REQ_AUDIO_ASR, REQ_OPEN_ASR, REQ_STOP_ASR};

/// 对外 canonical 模型名（`AsrModelAdapter::model_name` 返回值）
pub const CANONICAL_MODEL: &str = "qwen3-asr-flash-realtime";

/// `turn_detection.silence_duration_ms`：VAD 断句静音阈值。
/// docs 推荐对话/聊天等需快速断句的场景设 400（服务端默认 800）。
pub const DEFAULT_SILENCE_MS: u32 = 400;

/// `turn_detection.threshold`：VAD 灵敏度（服务端默认 0.2；raw 示例取 0.0 最灵敏）。
pub const DEFAULT_VAD_THRESHOLD: f32 = 0.0;

// ===== 服务端事件 JSON 形状 =====

/// Realtime 服务端事件（按 `type` 分发的扁平事件；字段全部防御式可选）
#[derive(Debug, Deserialize)]
struct RtServerEvent {
    #[serde(rename = "type")]
    kind: String,
    /// partial 事件的已确认文本部分
    #[serde(default)]
    text: String,
    /// partial 事件的暂存（未确认）文本部分，展示时与 text 拼接
    #[serde(default)]
    stash: String,
    /// completed 事件的最终转写
    #[serde(default)]
    transcript: String,
    /// error 事件的错误详情（对象 `{code,message}` 或退化字符串）
    #[serde(default)]
    error: Option<serde_json::Value>,
}

/// 把 `error` 字段格式化成可读消息（防御式：对象 / 字符串 / 缺失）
fn format_realtime_error(err: &Option<serde_json::Value>) -> String {
    match err {
        None => "realtime error (no detail)".to_string(),
        Some(serde_json::Value::String(s)) => format!("realtime error: {}", s),
        Some(v) => {
            let msg = v.get("message").and_then(|x| x.as_str()).unwrap_or("");
            let code = v.get("code").and_then(|x| x.as_str()).unwrap_or("");
            if msg.is_empty() && code.is_empty() {
                format!("realtime error: {}", v)
            } else {
                format!("realtime error: code={} message={}", code, msg)
            }
        }
    }
}

/// 客户端事件 id（docs raw 示例带 event_id；仅用于服务端去重/追踪，随机即可）
fn new_event_id() -> String {
    format!("ev-{}", uuid::Uuid::new_v4().simple())
}

// ===== Adapter =====

pub struct Qwen3RealtimeAdapter {
    /// 实际发给服务端的 model 查询参数 / 会话模型
    model: &'static str,
    /// 对外名称（快照版统一归一到稳定版）
    canonical: &'static str,
}

impl Qwen3RealtimeAdapter {
    /// 按 model 名挑预设。
    ///
    /// - `qwen3-asr-flash-realtime`（稳定版，当前等同 2025-10-27）
    /// - `qwen3-asr-flash-realtime-2026-02-10` / `qwen3-asr-flash-realtime-2025-10-27`（快照版）
    pub fn for_model(model: &str) -> Self {
        match model {
            "qwen3-asr-flash-realtime-2026-02-10" => Self {
                model: "qwen3-asr-flash-realtime-2026-02-10",
                canonical: CANONICAL_MODEL,
            },
            "qwen3-asr-flash-realtime-2025-10-27" => Self {
                model: "qwen3-asr-flash-realtime-2025-10-27",
                canonical: CANONICAL_MODEL,
            },
            _ => Self {
                model: CANONICAL_MODEL,
                canonical: CANONICAL_MODEL,
            },
        }
    }

    /// wire 模型名（快照版保留快照串，稳定版即 canonical）
    pub fn wire_model(&self) -> &'static str {
        self.model
    }

    /// 富解析：Realtime 服务端 JSON 事件 → `RealtimeEvent`。
    ///
    /// - `Ok(Some(evt))`：有语义事件（partial / final / VAD / finished）
    /// - `Ok(None)`：控制事件或未知事件（session.created / updated / committed 等），忽略
    /// - `Err`：`error` 事件
    ///
    /// trait 方法 `parse_event` 委托本方法并降级映射成 `AsrEvent`。
    pub fn parse_realtime_event(&self, payload: &[u8]) -> Result<Option<RealtimeEvent>, ClientError> {
        let evt: RtServerEvent = match serde_json::from_slice(payload) {
            Ok(e) => e,
            // 非 JSON / 非 JSON 对象：视为无关帧忽略（与 qwen.rs 的防御策略一致）
            Err(_) => return Ok(None),
        };
        match evt.kind.as_str() {
            "conversation.item.input_audio_transcription.text" => {
                Ok(Some(RealtimeEvent::Partial {
                    // docs 示例：展示 = text + stash（stash 是未确认尾段）
                    text: format!("{}{}", evt.text, evt.stash),
                }))
            }
            "conversation.item.input_audio_transcription.completed" => {
                Ok(Some(RealtimeEvent::Final { text: evt.transcript }))
            }
            "input_audio_buffer.speech_started" => Ok(Some(RealtimeEvent::SpeechStarted)),
            "input_audio_buffer.speech_stopped" => Ok(Some(RealtimeEvent::SpeechStopped)),
            "session.finished" => Ok(Some(RealtimeEvent::Finished)),
            "error" => Err(ClientError::Decode(format_realtime_error(&evt.error))),
            // session.created / session.updated / input_audio_buffer.committed / 未知类型：忽略
            _ => Ok(None),
        }
    }
}

/// Realtime 协议富事件（比 `AsrEvent` 多 VAD 边沿与终态语义）
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RealtimeEvent {
    /// 增量识别（`conversation.item.input_audio_transcription.text`，`text` + `stash` 拼接）
    Partial { text: String },
    /// 句终识别（`conversation.item.input_audio_transcription.completed` 的 `transcript`）
    Final { text: String },
    /// 服务端 VAD 检测到语音起点
    SpeechStarted,
    /// 服务端 VAD 检测到语音终点（静音达到 silence_duration_ms）
    SpeechStopped,
    /// 会话终态（`session.finished`），事件流到此结束
    Finished,
}

// ===== 增量式 Realtime 会话（start / send_audio / finish） =====
//
// 与 `session.rs::start_session`（公共协议）的关键差异：
//   - 事件流不因首个 final（VAD 断句）结束——多句连续听写时每句一个 Final，
//     真正的终态是 `session.finished`（调 finish() 后服务端回）
//   - 收消息走 `recv_message()`（Realtime 全 JSON 文本帧；recv_frame 的 GAX
//     解码路径对纯文本协议不适用）
//   - 调用方 drop `RealtimeAsrSession` 即 abandon：cmd 通道关闭 → 后台任务
//     退出 → 连接按 RAII 归还池（不发 session.finish，与 session.rs 行为一致）

use std::sync::Arc;

use async_stream::stream;
use tokio::sync::mpsc;

use crate::asr::BoxStream;
use crate::ws_pool::{Dialer, LaneKind, WsMessage, WsPool};

/// 客户端 → 后台任务的命令
#[derive(Debug)]
enum RealtimeCmd {
    Audio(Vec<u8>),
    Finish,
}

/// Realtime 流式会话句柄。`Clone` 便宜，多个组件可共享。
#[derive(Clone)]
pub struct RealtimeAsrSession {
    cmd_tx: mpsc::UnboundedSender<RealtimeCmd>,
}

impl RealtimeAsrSession {
    /// 推一段 PCM 字节（s16le 16k mono）。无长度限制，内部按 CHUNK_BYTES(3200) 切片。
    pub fn send_audio(&self, pcm: &[u8]) -> Result<(), ClientError> {
        self.cmd_tx
            .send(RealtimeCmd::Audio(pcm.to_vec()))
            .map_err(|_| ClientError::Ws("realtime session already closed".into()))
    }

    /// 发 `session.finish`。事件流继续产出剩余结果，直到 `Finished`（或连接关闭）。
    pub fn finish(&self) -> Result<(), ClientError> {
        self.cmd_tx
            .send(RealtimeCmd::Finish)
            .map_err(|_| ClientError::Ws("realtime session already closed".into()))
    }
}

/// 启动一个 Realtime 流式 ASR 会话，返回 (session 句柄, 事件流)。
///
/// 流程：
/// 1. acquire WSS 连接（dialer 需带 OpenAI-Beta 头，用 [`make_realtime_dialer`]）
/// 2. 发 `session.update`（VAD 模式配置）
/// 3. spawn 后台任务 select! 循环：
///    - cmd 通道的 Audio → 按 3200B 切片成 `input_audio_buffer.append` 帧转发
///    - cmd 通道的 Finish → 发 `session.finish`，继续收事件
///    - `recv_message()` 的 Text 事件 → 解析后推 event_tx；`session.finished` 终止
/// 4. 终止时连接按 clean/errored 决定归还或关闭
pub async fn start_realtime_session(
    pool: Arc<WsPool>,
    adapter: Qwen3RealtimeAdapter,
    dialer: Dialer,
    sample_rate: u32,
    channels: u16,
    session_id: String,
) -> Result<
    (
        RealtimeAsrSession,
        BoxStream<Result<RealtimeEvent, ClientError>>,
    ),
    ClientError,
> {
    // 1. acquire 连接
    let mut conn = pool
        .acquire_or_dial(LaneKind::Asr, dialer)
        .await
        .map_err(|e| ClientError::Pool(e.to_string()))?;

    // 2. 发 session.update
    let open = adapter.open_request(&session_id, sample_rate, channels);
    if let Err(e) = conn.send(open).await {
        conn.release(false);
        return Err(ClientError::Ws(e.to_string()));
    }

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<RealtimeCmd>();
    let (event_tx, event_rx) = mpsc::unbounded_channel::<Result<RealtimeEvent, ClientError>>();

    let model = adapter.wire_model();
    tracing::info!(
        target: "voice_providers.asr.realtime",
        session_id = %session_id,
        model = model,
        sample_rate,
        "Realtime ASR 会话启动"
    );

    // 3. 后台任务：命令转发 + 事件解析
    let sid_bg = session_id;
    tokio::spawn(async move {
        let mut cmd_rx = cmd_rx;
        let mut conn = conn;
        let adapter = adapter;
        let mut errored = false;
        let mut clean_end = false;

        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(RealtimeCmd::Audio(pcm)) => {
                            // 内部按 100ms（3200B）切片；超长段拆多帧
                            for chunk in pcm.chunks(crate::asr::CHUNK_BYTES) {
                                if let Err(e) = conn.send(adapter.audio_frame(chunk)).await {
                                    let _ = event_tx.send(Err(ClientError::Ws(e.to_string())));
                                    errored = true;
                                    break;
                                }
                            }
                            if errored { break; }
                        }
                        Some(RealtimeCmd::Finish) => {
                            if let Err(e) = conn.send(adapter.stop_frame(&sid_bg)).await {
                                // 连接已坏，drain 也拿不到剩余事件，直接终止
                                let _ = event_tx.send(Err(ClientError::Ws(e.to_string())));
                                errored = true;
                                break;
                            }
                            // 不退出循环：等服务端回 session.finished
                        }
                        None => {
                            // 调用方 drop 了句柄（abandon）：干净退出
                            clean_end = true;
                            break;
                        }
                    }
                }
                msg = conn.recv_message() => {
                    match msg {
                        Ok(WsMessage::Text(text)) => {
                            match adapter.parse_realtime_event(text.as_bytes()) {
                                Ok(Some(evt)) => {
                                    let terminal = matches!(evt, RealtimeEvent::Finished);
                                    let _ = event_tx.send(Ok(evt));
                                    if terminal {
                                        clean_end = true;
                                        break;
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    let _ = event_tx.send(Err(e));
                                    errored = true;
                                    break;
                                }
                            }
                        }
                        Ok(WsMessage::Binary(_)) => {
                            // Realtime 协议服务端不下发 binary 帧；防御性忽略
                        }
                        Ok(WsMessage::Close) | Err(_) => {
                            let _ = event_tx.send(Err(ClientError::Ws(
                                "realtime connection closed by peer".into(),
                            )));
                            errored = true;
                            break;
                        }
                    }
                }
            }
        }

        let healthy = clean_end && !errored;
        conn.release(healthy);
        tracing::info!(
            target: "voice_providers.asr.realtime",
            session_id = %sid_bg,
            healthy,
            "Realtime ASR 会话结束"
        );
        // event_tx drop 后 event_rx.next() 返回 None，事件流自然结束
    });

    let session = RealtimeAsrSession { cmd_tx };
    let stream = stream! {
        let mut rx = event_rx;
        while let Some(item) = rx.recv().await {
            yield item;
        }
    };
    Ok((session, Box::pin(stream)))
}

// ===== 端点解析（Realtime 专属域名规则） =====

/// Realtime 协议端点解析。
///
/// 优先级（ASR realtime 用业务空间专属域名，与 TTS 的 dashscope.aliyuncs.com 不同）：
/// 1. `endpoint` 显式配置 → 原样使用（缺 `?model=` 自动追加）
/// 2. `workspace_id` + `region` → `wss://{ws}.{region}.maas.aliyuncs.com/api-ws/v1/realtime?model={model}`
/// 3. 两者皆缺 → Err（提示配置）
pub fn resolve_realtime_endpoint(
    model: &str,
    workspace_id: Option<&str>,
    region: Option<&str>,
    endpoint: Option<&str>,
) -> Result<String, ClientError> {
    // 1. 显式 endpoint 原样使用（缺 model 查询参数时自动追加）
    if let Some(ep) = endpoint.map(str::trim).filter(|s| !s.is_empty()) {
        if ep.contains("model=") {
            return Ok(ep.to_string());
        }
        let sep = if ep.contains('?') { '&' } else { '?' };
        return Ok(format!("{}{}model={}", ep, sep, model));
    }
    // 2. 业务空间专属域名（ASR realtime 的文档形态；region 默认华北2-北京）
    if let Some(ws) = workspace_id.map(str::trim).filter(|s| !s.is_empty()) {
        let region = region
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("cn-beijing");
        return Ok(format!(
            "wss://{}.{}.maas.aliyuncs.com/api-ws/v1/realtime?model={}",
            ws, region, model
        ));
    }
    Err(ClientError::Decode(
        "asr_stream 配置缺失：endpoint 或 workspace_id 至少需要一个 \
         (ASR realtime 端点形如 wss://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/api-ws/v1/realtime)"
            .into(),
    ))
}

/// 构造 Realtime 握手 HTTP request（含 Authorization + OpenAI-Beta 头）。
/// 抽成独立纯函数便于单测：不真正建连。
fn realtime_handshake_request(
    url: &str,
    api_key: &str,
) -> Result<async_tungstenite::tungstenite::http::Request<()>, String> {
    use async_tungstenite::tungstenite::client::IntoClientRequest;
    use async_tungstenite::tungstenite::http::HeaderValue;

    let mut req = url
        .into_client_request()
        .map_err(|e| format!("into_client_request({url}): {e}"))?;

    // Authorization: Bearer <key>（兼容 "Bearer xxx" 形式的 key；空 key 不发）
    if !api_key.is_empty() {
        let key = api_key
            .strip_prefix("Bearer ")
            .or_else(|| api_key.strip_prefix("bearer "))
            .unwrap_or(api_key);
        let v = HeaderValue::from_str(&format!("Bearer {}", key))
            .map_err(|e| format!("invalid Authorization header: {e}"))?;
        req.headers_mut().insert("Authorization", v);
    }

    // Realtime API 必需（docs raw 示例请求头之一，公共 /inference 协议没有）
    req.headers_mut()
        .insert("OpenAI-Beta", HeaderValue::from_static("realtime=v1"));
    Ok(req)
}

/// Realtime 拨号器：`Authorization: Bearer <key>` + `OpenAI-Beta: realtime=v1`。
///
/// 与 `provider::make_realtime_dialer` 的差异：多带 `OpenAI-Beta` 头（Realtime API 必需）。
pub fn make_realtime_dialer(ws_endpoint: String, api_key: String) -> crate::ws_pool::Dialer {
    use futures_util::future::FutureExt;
    use std::sync::Arc;

    Arc::new(move |_kind: crate::ws_pool::LaneKind| {
        let url = ws_endpoint.clone();
        let key = api_key.clone();
        async move {
            let req = realtime_handshake_request(&url, &key)
                .map_err(crate::ws_pool::PoolError::Handshake)?;
            let (stream, _resp) = async_tungstenite::tokio::connect_async(req)
                .await
                .map_err(|e| {
                    crate::ws_pool::PoolError::Handshake(format!("realtime connect_async: {e}"))
                })?;
            tracing::info!(
                target: "voice_providers.asr.realtime",
                url = %url,
                "Realtime WSS 拨号成功"
            );
            Ok(Box::new(crate::ws_pool::TungsteniteWs::new(stream))
                as Box<dyn crate::ws_pool::WebSocketLike>)
        }
        .boxed()
    })
}

// ===== Adapter trait 实现 =====

impl AsrModelAdapter for Qwen3RealtimeAdapter {
    fn model_name(&self) -> &'static str {
        self.canonical
    }

    fn open_request(&self, _session_id: &str, sr: u32, _ch: u16) -> GaxFrame {
        // session.update：modalities=text + pcm/16k + server_vad（断句静音 400ms，docs 推荐对话场景值）
        let value = json!({
            "event_id": new_event_id(),
            "type": "session.update",
            "session": {
                "modalities": ["text"],
                "input_audio_format": "pcm",
                "sample_rate": sr,
                "turn_detection": {
                    "type": "server_vad",
                    "threshold": DEFAULT_VAD_THRESHOLD,
                    "silence_duration_ms": DEFAULT_SILENCE_MS,
                },
            },
        });
        let payload =
            serde_json::to_vec(&value).expect("encode session.update JSON");
        GaxFrame::text(REQ_OPEN_ASR, payload)
    }

    fn audio_frame(&self, pcm: &[u8]) -> GaxFrame {
        // Realtime 协议音频走 JSON 文本帧 + base64（与公共协议的裸 PCM 二进制帧不同）
        let b64 = base64::engine::general_purpose::STANDARD.encode(pcm);
        let value = json!({
            "event_id": new_event_id(),
            "type": "input_audio_buffer.append",
            "audio": b64,
        });
        let payload =
            serde_json::to_vec(&value).expect("encode input_audio_buffer.append JSON");
        GaxFrame::text(REQ_AUDIO_ASR, payload)
    }

    fn stop_frame(&self, _session_id: &str) -> GaxFrame {
        let value = json!({
            "event_id": new_event_id(),
            "type": "session.finish",
        });
        let payload =
            serde_json::to_vec(&value).expect("encode session.finish JSON");
        GaxFrame::text(REQ_STOP_ASR, payload)
    }

    fn parse_event(&self, payload: &[u8]) -> Result<Option<AsrEvent>, ClientError> {
        // 委托富解析后降级映射：VAD / Finished 在批量 AsrEvent 语义里没有对应物 → None
        match self.parse_realtime_event(payload)? {
            Some(RealtimeEvent::Partial { text }) => Ok(Some(AsrEvent { text, is_final: false })),
            Some(RealtimeEvent::Final { text }) => Ok(Some(AsrEvent { text, is_final: true })),
            Some(_) => Ok(None),
            None => Ok(None),
        }
    }
}

// ===== 单元测试 =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::WireFormat;

    // ===== A1: for_model + open_request =====

    #[test]
    fn for_model_stable_and_snapshots() {
        assert_eq!(
            Qwen3RealtimeAdapter::for_model("qwen3-asr-flash-realtime").model_name(),
            CANONICAL_MODEL
        );
        assert_eq!(
            Qwen3RealtimeAdapter::for_model("qwen3-asr-flash-realtime-2026-02-10").model_name(),
            CANONICAL_MODEL
        );
        // 快照版 wire model 保留快照串
        assert_eq!(
            Qwen3RealtimeAdapter::for_model("qwen3-asr-flash-realtime-2026-02-10").wire_model(),
            "qwen3-asr-flash-realtime-2026-02-10"
        );
        // 未知串兜底到稳定版
        assert_eq!(Qwen3RealtimeAdapter::for_model("whatever").wire_model(), CANONICAL_MODEL);
    }

    #[test]
    fn open_request_is_session_update_json() {
        let adapter = Qwen3RealtimeAdapter::for_model(CANONICAL_MODEL);
        let frame = adapter.open_request("task-abc", 16000, 1);

        assert_eq!(frame.cmd, REQ_OPEN_ASR);
        assert_eq!(frame.wire, WireFormat::Text);

        let v: serde_json::Value = serde_json::from_slice(&frame.payload).unwrap();
        assert_eq!(v["type"], "session.update");
        assert_eq!(v["session"]["modalities"][0], "text");
        assert_eq!(v["session"]["input_audio_format"], "pcm");
        assert_eq!(v["session"]["sample_rate"], 16000);
        assert_eq!(v["session"]["turn_detection"]["type"], "server_vad");
        assert_eq!(v["session"]["turn_detection"]["silence_duration_ms"], 400);
        assert_eq!(v["session"]["turn_detection"]["threshold"], 0.0);
    }

    // ===== A2: audio_frame / stop_frame =====

    #[test]
    fn audio_frame_is_base64_append() {
        let adapter = Qwen3RealtimeAdapter::for_model(CANONICAL_MODEL);
        let pcm: Vec<u8> = (0..64u8).collect();
        let frame = adapter.audio_frame(&pcm);

        assert_eq!(frame.cmd, REQ_AUDIO_ASR);
        assert_eq!(frame.wire, WireFormat::Text);

        let v: serde_json::Value = serde_json::from_slice(&frame.payload).unwrap();
        assert_eq!(v["type"], "input_audio_buffer.append");
        let b64 = v["audio"].as_str().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap();
        assert_eq!(decoded, pcm);
    }

    #[test]
    fn stop_frame_is_session_finish() {
        let adapter = Qwen3RealtimeAdapter::for_model(CANONICAL_MODEL);
        let frame = adapter.stop_frame("task-abc");

        assert_eq!(frame.cmd, REQ_STOP_ASR);
        assert_eq!(frame.wire, WireFormat::Text);

        let v: serde_json::Value = serde_json::from_slice(&frame.payload).unwrap();
        assert_eq!(v["type"], "session.finish");
    }

    // ===== A3: parse_event（trait 降级映射） =====

    #[test]
    fn parse_event_partial_concatenates_text_and_stash() {
        let adapter = Qwen3RealtimeAdapter::for_model(CANONICAL_MODEL);
        let evt = json!({
            "type": "conversation.item.input_audio_transcription.text",
            "text": "你好",
            "stash": "世界"
        });
        let out = adapter
            .parse_event(serde_json::to_vec(&evt).unwrap().as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(out.text, "你好世界");
        assert!(!out.is_final);
    }

    #[test]
    fn parse_event_final_uses_transcript() {
        let adapter = Qwen3RealtimeAdapter::for_model(CANONICAL_MODEL);
        let evt = json!({
            "type": "conversation.item.input_audio_transcription.completed",
            "transcript": "你好世界。"
        });
        let out = adapter
            .parse_event(serde_json::to_vec(&evt).unwrap().as_slice())
            .unwrap()
            .unwrap();
        assert_eq!(out.text, "你好世界。");
        assert!(out.is_final);
    }

    #[test]
    fn parse_event_control_and_vad_events_map_to_none() {
        let adapter = Qwen3RealtimeAdapter::for_model(CANONICAL_MODEL);
        for ty in [
            "session.created",
            "session.updated",
            "input_audio_buffer.speech_started",
            "input_audio_buffer.speech_stopped",
            "input_audio_buffer.committed",
        ] {
            let evt = json!({ "type": ty });
            let out = adapter
                .parse_event(serde_json::to_vec(&evt).unwrap().as_slice())
                .unwrap();
            assert!(out.is_none(), "{ty} 应映射为 None");
        }
    }

    #[test]
    fn parse_event_finished_maps_to_none_for_batch_path() {
        // 批量 AsrEvent 语义没有"会话结束"，Finished 在 trait 路径降级为 None；
        // 富语义由 parse_realtime_event 承载（见 realtime_parse 测试组）
        let adapter = Qwen3RealtimeAdapter::for_model(CANONICAL_MODEL);
        let evt = json!({ "type": "session.finished" });
        let out = adapter
            .parse_event(serde_json::to_vec(&evt).unwrap().as_slice())
            .unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn parse_event_error_returns_err() {
        let adapter = Qwen3RealtimeAdapter::for_model(CANONICAL_MODEL);
        // error 字段为对象（OpenAI-Realtime 风格）
        let evt = json!({
            "type": "error",
            "error": { "code": "invalid_api_key", "message": "Invalid API key" }
        });
        let r = adapter.parse_event(serde_json::to_vec(&evt).unwrap().as_slice());
        assert!(r.is_err());
        let msg = r.unwrap_err().to_string();
        assert!(msg.contains("invalid_api_key"), "msg={msg}");
        assert!(msg.contains("Invalid API key"), "msg={msg}");
    }

    #[test]
    fn parse_event_error_string_form() {
        // 防御：error 字段也可能退化成字符串
        let adapter = Qwen3RealtimeAdapter::for_model(CANONICAL_MODEL);
        let evt = json!({ "type": "error", "error": "boom" });
        let r = adapter.parse_event(serde_json::to_vec(&evt).unwrap().as_slice());
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("boom"));
    }

    #[test]
    fn parse_event_invalid_json_returns_none() {
        let adapter = Qwen3RealtimeAdapter::for_model(CANONICAL_MODEL);
        let r = adapter.parse_event(b"not json at all");
        assert!(r.is_ok());
        assert!(r.unwrap().is_none());
    }

    #[test]
    fn parse_event_unknown_type_returns_none() {
        let adapter = Qwen3RealtimeAdapter::for_model(CANONICAL_MODEL);
        let evt = json!({ "type": "rate_limits.updated" });
        let r = adapter.parse_event(serde_json::to_vec(&evt).unwrap().as_slice());
        assert!(r.is_ok());
        assert!(r.unwrap().is_none());
    }

    // ===== A6: parse_realtime_event 富解析 =====

    #[test]
    fn realtime_parse_vad_and_finished() {
        let adapter = Qwen3RealtimeAdapter::for_model(CANONICAL_MODEL);
        let mk = |ty: &str| serde_json::to_vec(&json!({ "type": ty })).unwrap();
        assert_eq!(
            adapter.parse_realtime_event(&mk("input_audio_buffer.speech_started")).unwrap(),
            Some(RealtimeEvent::SpeechStarted)
        );
        assert_eq!(
            adapter.parse_realtime_event(&mk("input_audio_buffer.speech_stopped")).unwrap(),
            Some(RealtimeEvent::SpeechStopped)
        );
        assert_eq!(
            adapter.parse_realtime_event(&mk("session.finished")).unwrap(),
            Some(RealtimeEvent::Finished)
        );
        assert_eq!(
            adapter.parse_realtime_event(&mk("session.created")).unwrap(),
            None
        );
    }

    #[test]
    fn realtime_parse_partial_final_shapes() {
        let adapter = Qwen3RealtimeAdapter::for_model(CANONICAL_MODEL);
        let p = serde_json::to_vec(&json!({
            "type": "conversation.item.input_audio_transcription.text",
            "text": "你", "stash": "好"
        }))
        .unwrap();
        assert_eq!(
            adapter.parse_realtime_event(&p).unwrap(),
            Some(RealtimeEvent::Partial { text: "你好".into() })
        );
        let f = serde_json::to_vec(&json!({
            "type": "conversation.item.input_audio_transcription.completed",
            "transcript": "完成。"
        }))
        .unwrap();
        assert_eq!(
            adapter.parse_realtime_event(&f).unwrap(),
            Some(RealtimeEvent::Final { text: "完成。".into() })
        );
    }

    // ===== A5: 端点解析 + 握手请求头 =====

    #[test]
    fn endpoint_explicit_with_model_param() {
        let url = resolve_realtime_endpoint(
            CANONICAL_MODEL,
            None,
            None,
            Some("wss://custom.example.com/api-ws/v1/realtime?model=qwen3-asr-flash-realtime"),
        )
        .unwrap();
        assert_eq!(
            url,
            "wss://custom.example.com/api-ws/v1/realtime?model=qwen3-asr-flash-realtime"
        );
    }

    #[test]
    fn endpoint_explicit_without_model_param_appends_it() {
        let url = resolve_realtime_endpoint(
            CANONICAL_MODEL,
            None,
            None,
            Some("wss://custom.example.com/api-ws/v1/realtime"),
        )
        .unwrap();
        assert_eq!(
            url,
            "wss://custom.example.com/api-ws/v1/realtime?model=qwen3-asr-flash-realtime"
        );
    }

    #[test]
    fn endpoint_from_workspace_id_default_region() {
        let url = resolve_realtime_endpoint(CANONICAL_MODEL, Some("llm-abc123"), None, None).unwrap();
        assert_eq!(
            url,
            "wss://llm-abc123.cn-beijing.maas.aliyuncs.com/api-ws/v1/realtime?model=qwen3-asr-flash-realtime"
        );
    }

    #[test]
    fn endpoint_from_workspace_id_sg_region() {
        let url = resolve_realtime_endpoint(
            CANONICAL_MODEL,
            Some("llm-abc123"),
            Some("ap-southeast-1"),
            None,
        )
        .unwrap();
        assert_eq!(
            url,
            "wss://llm-abc123.ap-southeast-1.maas.aliyuncs.com/api-ws/v1/realtime?model=qwen3-asr-flash-realtime"
        );
    }

    #[test]
    fn endpoint_missing_workspace_and_endpoint_is_err() {
        let r = resolve_realtime_endpoint(CANONICAL_MODEL, None, None, None);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("workspace"));
    }

    #[test]
    fn handshake_request_carries_auth_and_beta_headers() {
        let req = realtime_handshake_request(
            "wss://llm-abc123.cn-beijing.maas.aliyuncs.com/api-ws/v1/realtime?model=qwen3-asr-flash-realtime",
            "sk-test-123",
        )
        .unwrap();
        assert_eq!(
            req.headers().get("Authorization").unwrap(),
            "Bearer sk-test-123"
        );
        assert_eq!(req.headers().get("OpenAI-Beta").unwrap(), "realtime=v1");
    }

    #[test]
    fn handshake_request_bearer_prefix_is_normalized() {
        // "Bearer xxx" 形式的 key 兼容（与 provider.rs make_real_dialer 行为一致）
        let req = realtime_handshake_request("wss://x.example.com/rt", "Bearer sk-abc").unwrap();
        assert_eq!(req.headers().get("Authorization").unwrap(), "Bearer sk-abc");
    }

    #[test]
    fn handshake_request_empty_key_omits_auth() {
        let req = realtime_handshake_request("wss://x.example.com/rt", "").unwrap();
        assert!(req.headers().get("Authorization").is_none());
        assert_eq!(req.headers().get("OpenAI-Beta").unwrap(), "realtime=v1");
    }
}
