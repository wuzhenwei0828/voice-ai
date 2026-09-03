//! VoiceSession: 每条 WS 连接对应一个会话
//!
//! 状态机：Idle / Listening / Processing / Speaking
//! 关键节点（转换、推流、错误）都打 `tracing::*!` 日志，便于观测。
//!
//! 下行通过 webhttp::websocket::OutMessage 推给客户端。
//!
//! pipeline 复用 [`crate::pipeline::llm_tts_items`]（LLM → 切句 → TTS → 句间 crossfade），
//! 本模块只负责：上行消息分发、ASR 阶段、事件 → VoicePayload 映射、取消。
//!
//! ## 模块拆分
//! - [`state`]    状态机枚举（`SessionState` / `TriggerReason`）
//! - [`audio`]    音频缓冲（`AudioAccumulator` + 触发阈值常量）
//! - 文本切句统一使用 [`crate::pipeline::sentence::next_sentence_end`]；ASR 标签解析见
//!   [`crate::utils::postprocess_utils::parse_asr_text`]
//! - [`pipeline`] 完整 pipeline 编排（`run_pipeline` / `send_down` / `CurrentCancelGuard`）
//!
//! ## Pipeline 并发策略：**打断旧的**（auto-interrupt on new utterance），但只在新句真有内容时
//!
//! 同一会话里若上一条 pipeline 还没跑完，新一条 `AudioChunk{is_last:true}` 触发时：
//!
//! 1. `trigger_pipeline()` **不立即 cancel 旧 pipeline** —— spawn 一个新 pipeline，
//!    各自跑 ASR；
//! 2. 若新 pipeline 的 ASR 拿到**非空文本**，才把旧 LLM/TTS pipeline 取消（注册
//!    `current_real_cancel` 时 cancel 上一个 real token），然后跑 LLM/TTS；
//! 3. 若新 ASR 拿到**空文本**（噪声 / VAD 误切 / 上游返回 ""），直接 return：
//!    - 不取消旧 pipeline → 正在跑的 LLM/TTS 不被误杀
//!    - 不注册为 current real → 不污染后续 cancel 链
//!    - 不走 LLM/TTS 链路 → 省 token、省上游调用
//!
//! 「全局 cancel」是另一条线：`SessionEnd` / `Drop` 通过 `global_cancel.cancel()` 终止
//! 所有在跑的 pipeline（包括正在 ASR 的）；`Interrupt` 则 cancel 并立刻重建
//! `global_cancel` + 清空 `current_real_cancel`，让下一句能从干净状态起。
//!
//! 等价于"`Interrupt` 由**新语音真的有效**自动触发"。空文本不算有效 —— 排队策略需要
//! 单独设计调度器，超出本模块职责。
//!
//! ## 句尾判定：客户端权威 + 服务端两条兜底
//!
//! 正常路径：客户端 VAD 决定句尾，把最后一帧 `AudioChunk.is_last` 置 true。
//! 服务端不做 VAD，但**不盲信客户端**，每收到一帧就检查两条兜底：
//!
//! 1. **单句时长上限**（`MAX_UTTERANCE_MS`）：客户端崩了 / 忘了发 is_last 时，
//!    音频不会无限攒着不处理；
//! 2. **缓冲字节上限**（`MAX_AUDIO_BYTES`）：防内存无界增长。
//!
//! 任一超限即强制触发 pipeline（`TriggerReason::DurationCap` / `BufferCap`），
//! 语义等同于替客户端补发了一次 is_last。
//!
//! 反向也有一道闸：累积音频不足 `MIN_UTTERANCE_BYTES`（≈200ms）的 is_last 直接
//! 丢弃不触发 pipeline，防客户端 VAD 误切/抽风时往 ASR 灌碎片音频。
//!
//! ## 会话生命周期与取消保证
//!
//! - `SessionEnd` 把会话标记为 `closed`：此后上行消息一律忽略、不再触发 pipeline，
//!   会话实体等 `WsDisconnect`（`service.rs` 从 DashMap 移除）回收。
//! - `VoiceSession` 在 `Drop` 里 cancel pipeline token：断连移除 session 时，
//!   已 spawn 的 pipeline 任务也会在下一个取消边界退出，不白跑 LLM/TTS。
//! - 取消是协作式的：pipeline 在 `tokio::select!` 边界（**含 ASR 建连阶段**）
//!   观察 token 提前退出；`on_payload` 本身是同步非阻塞的，Interrupt/SessionEnd
//!   随下一条 WS 消息顺序到达即生效，不存在抢占问题。

pub mod audio;
pub mod pipeline;
pub mod state;

use std::time::{Duration, Instant};
use std::{collections::HashMap, sync::Arc};

use actix::prelude::Recipient;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn, Instrument};
use voice_proto::{ClientMetricKind, VoicePayload};
use webhttp::websocket::OutMessage;

use crate::agent::LlmAgent;
use crate::client::{AsrClient, TtsClient};
use crate::metrics::{NoopMetricsSink, VoiceMetricsSink};
use crate::trace_context::scope;

pub use state::SessionState;

use audio::{AudioAccumulator, MAX_AUDIO_BYTES, MAX_UTTERANCE_MS, MIN_UTTERANCE_BYTES};
use pipeline::{run_pipeline, send_down};
use state::TriggerReason;

pub struct VoiceSession {
    pub session_id: String,
    /// 仅用于日志/观测的"用户视角"状态机。注意：spawn pipeline 后立即转回 Listening，
    /// 不反映 pipeline 内部 ASR/LLM/TTS 子阶段；并发安全靠 CancellationToken + current_real_cancel，
    /// 不是靠这个字段。
    pub state: SessionState,
    /// SessionEnd 之后不再接受任何上行消息，也不触发新 pipeline
    closed: bool,
    /// 全局 cancel：每个 pipeline 通过 `child_token()` 派生自己的 token；
    /// SessionEnd / Drop 时 cancel 所有 child（终止在跑的 pipeline，包括 ASR 中）；
    /// Interrupt 时 cancel + 重建，让下一句从干净状态起
    global_cancel: CancellationToken,
    /// 最近一个进入 LLM/TTS 阶段的 pipeline 的 cancel token（auto-interrupt 用）；
    /// 空文本 pipeline 不注册，不影响正在跑的 LLM/TTS
    current_real_cancel: Arc<Mutex<Option<CancellationToken>>>,
    /// 单会话内的 pipeline 请求序号；0 保留给没有请求序列的兼容事件。
    next_request_id: u64,
    client_metrics_reported: HashMap<(String, ClientMetricKind), Instant>,
    trace_id: String,
    audio_buf: AudioAccumulator,
    metrics: Arc<dyn VoiceMetricsSink>,
    input_started_at: Option<Instant>,
    /// 最近一次通过最短时长校验的语音请求，供显式 Retry 重放。
    last_request_audio: Option<Vec<u8>>,
    /// 当前音频轮次对应的客户端 message_id。
    current_message_id: Option<String>,
    /// 最近一次有效语音请求的 message_id，供 Retry 复用。
    last_request_message_id: Option<String>,
    /// 从 SessionStart 记下，用于包 WAV 头给 ASR（siliconflow 等 provider 按文件后缀选解码器）
    sample_rate: u32,
    channels: u16,
    /// 端侧（前端 voice 下拉）选中的 TTS 音色**短名**（如 `"alex"`）。
    /// - `Some(short)` —— 传给 HttpTtsClient 当 override，HttpTtsClient 拼 model 前缀后发给 provider
    /// - `None` / 空串 —— 端侧没传，让 HttpTtsClient 走配置 `tts.voice` 兜底
    client_voice: Option<String>,
    asr: Arc<dyn AsrClient>,
    /// LLM 调用走 agent（带短期记忆）
    llm: Arc<LlmAgent>,
    tts: Arc<dyn TtsClient>,
    /// 下行发送目标：actix 的 Recipient，跨任务 clone
    down_addr: Recipient<OutMessage>,
}

impl VoiceSession {
    const CLIENT_METRIC_TTL: Duration = Duration::from_secs(30);
    const MAX_CLIENT_METRIC_REPORTS: usize = 4096;

    fn remember_client_metric_once(
        &mut self,
        message_id: String,
        metric: ClientMetricKind,
        now: Instant,
    ) -> bool {
        self.client_metrics_reported.retain(|_, reported_at| {
            now.saturating_duration_since(*reported_at) <= Self::CLIENT_METRIC_TTL
        });
        if self
            .client_metrics_reported
            .contains_key(&(message_id.clone(), metric))
        {
            return false;
        }
        if self.client_metrics_reported.len() >= Self::MAX_CLIENT_METRIC_REPORTS {
            if let Some(oldest) = self
                .client_metrics_reported
                .iter()
                .min_by_key(|(_, reported_at)| *reported_at)
                .map(|(key, _)| key.clone())
            {
                self.client_metrics_reported.remove(&oldest);
            }
        }
        self.client_metrics_reported
            .insert((message_id, metric), now);
        true
    }

    pub fn new(
        session_id: String,
        asr: Arc<dyn AsrClient>,
        llm: Arc<LlmAgent>,
        tts: Arc<dyn TtsClient>,
        down_addr: Recipient<OutMessage>,
    ) -> Self {
        Self::new_with_metrics(
            session_id,
            asr,
            llm,
            tts,
            down_addr,
            Arc::new(NoopMetricsSink),
        )
    }

    pub fn new_with_metrics(
        session_id: String,
        asr: Arc<dyn AsrClient>,
        llm: Arc<LlmAgent>,
        tts: Arc<dyn TtsClient>,
        down_addr: Recipient<OutMessage>,
        metrics: Arc<dyn VoiceMetricsSink>,
    ) -> Self {
        info!(target: "voice_server.session", session_id = %session_id, "VoiceSession 创建");
        Self {
            session_id,
            state: SessionState::Idle,
            closed: false,
            global_cancel: CancellationToken::new(),
            current_real_cancel: Arc::new(Mutex::new(None)),
            next_request_id: 0,
            client_metrics_reported: HashMap::new(),
            trace_id: crate::trace_context::new_trace_id(),
            audio_buf: AudioAccumulator::new(),
            metrics,
            input_started_at: None,
            last_request_audio: None,
            current_message_id: None,
            last_request_message_id: None,
            sample_rate: 0,
            channels: 0,
            client_voice: None,
            asr,
            llm,
            tts,
            down_addr,
        }
    }

    fn transition(&mut self, next: SessionState) {
        if self.state != next {
            info!(
                target: "voice_server.session",
                session_id = %self.session_id,
                from = ?self.state,
                to = ?next,
                "状态转换"
            );
            self.state = next;
        }
    }

    /// 处理上行 payload（同步；可触发异步 pipeline 任务）
    ///
    /// 返回 `Some(JoinHandle)` 表示本调用 spawn 了一个 pipeline 任务，
    /// 调用方（service.rs）应将其推入 service 级 JoinSet 以追踪 panic / 优雅退出。
    /// `None` 表示本调用没 spawn（例如音频过短、空文本、非 AudioChunk）。
    pub fn on_payload(&mut self, p: VoicePayload) -> Option<JoinHandle<()>> {
        self.on_payload_with_trace_id(p, crate::trace_context::new_trace_id())
    }

    pub fn on_payload_with_trace_id(
        &mut self,
        p: VoicePayload,
        trace_id: String,
    ) -> Option<JoinHandle<()>> {
        self.trace_id = trace_id;
        if self.closed {
            debug!(
                target: "voice_server.session",
                session_id = %self.session_id,
                "会话已 SessionEnd，忽略上行 payload: {:?}", p
            );
            return None;
        }
        match p {
            VoicePayload::SessionStart {
                sample_rate,
                channels,
                codec,
                language,
                voice,
                ..
            } => {
                // 端侧上报的 voice：None / 空串都视为"没上报"，存 None 走配置兜底
                let voice_short = voice.filter(|s| !s.trim().is_empty());
                info!(
                    target: "voice_server.session",
                    session_id = %self.session_id,
                    sample_rate,
                    channels,
                    codec = %codec,
                    language = %language,
                    client_voice = ?voice_short,
                    "收到 SessionStart"
                );
                // 记下格式参数，供后续 pipeline 包 WAV 头喂给 ASR
                self.sample_rate = sample_rate;
                self.channels = channels as u16;
                self.client_voice = voice_short;
                self.transition(SessionState::Listening);
                None
            }
            VoicePayload::AudioChunk {
                seq,
                timestamp_ms,
                data,
                is_last,
                ..
            } => {
                let audio_received_at = Instant::now();
                let bytes = data.len();
                debug!(
                    target: "voice_server.session",
                    session_id = %self.session_id,
                    seq,
                    bytes,
                    timestamp_ms,
                    is_last,
                    "收到 AudioChunk"
                );
                if self.state == SessionState::Idle {
                    self.transition(SessionState::Listening);
                }
                if self.audio_buf.len() == 0 {
                    self.input_started_at = Some(Instant::now());
                    self.current_message_id = Some(self.trace_id.clone());
                }
                self.audio_buf.push(data);
                if is_last {
                    self.trigger_pipeline(TriggerReason::ClientIsLast, audio_received_at)
                } else if self.audio_buf.elapsed_ms() >= MAX_UTTERANCE_MS {
                    warn!(
                        target: "voice_server.session",
                        session_id = %self.session_id,
                        elapsed_ms = self.audio_buf.elapsed_ms() as u64,
                        limit_ms = MAX_UTTERANCE_MS as u64,
                        "单句超长，服务端强制触发"
                    );
                    self.trigger_pipeline(TriggerReason::DurationCap, audio_received_at)
                } else if self.audio_buf.len() >= MAX_AUDIO_BYTES {
                    warn!(
                        target: "voice_server.session",
                        session_id = %self.session_id,
                        buffered = self.audio_buf.len(),
                        limit = MAX_AUDIO_BYTES,
                        "缓冲超限，服务端强制触发"
                    );
                    self.trigger_pipeline(TriggerReason::BufferCap, audio_received_at)
                } else {
                    None
                }
            }
            VoicePayload::Interrupt { .. } => {
                warn!(
                    target: "voice_server.session",
                    session_id = %self.session_id,
                    "收到 Interrupt，取消当前任务"
                );
                // 全局 cancel 终止所有在跑 pipeline，再重建让下一句从干净状态起
                self.global_cancel.cancel();
                self.global_cancel = CancellationToken::new();
                *self.current_real_cancel.lock() = None;
                self.transition(SessionState::Listening);
                send_down(
                    &self.down_addr,
                    VoicePayload::LlmDelta {
                        session_id: self.session_id.clone(),
                        delta: "[已打断]".to_string(),
                        is_final: true,
                        message_id: self.last_request_message_id.clone().unwrap_or_default(),
                    },
                );
                None
            }
            VoicePayload::Retry { .. } => {
                self.metrics.request_retried();
                let Some(audio) = self.last_request_audio.clone() else {
                    warn!(
                        target: "voice_server.session",
                        session_id = %self.session_id,
                        "收到 Retry，但没有可重放的有效请求"
                    );
                    return None;
                };
                info!(
                    target: "voice_server.session",
                    session_id = %self.session_id,
                    bytes = audio.len(),
                    "收到 Retry，重放最近一次有效请求"
                );
                self.global_cancel.cancel();
                self.global_cancel = CancellationToken::new();
                *self.current_real_cancel.lock() = None;
                let _ = self.audio_buf.drain();
                self.input_started_at = Some(Instant::now());
                self.audio_buf.push(audio);
                self.current_message_id = self.last_request_message_id.clone();
                self.trigger_pipeline(TriggerReason::Retry, Instant::now())
            }
            VoicePayload::PlaybackStarted {
                message_id,
                request_id,
                delay_ms,
                ..
            } => {
                const MAX_PLAYBACK_DELAY_MS: u64 = 30_000;
                let metric_message_id = if message_id.is_empty() {
                    if request_id == 0 {
                        String::new()
                    } else {
                        format!("legacy-request-{request_id}")
                    }
                } else {
                    message_id
                };
                if metric_message_id.is_empty() || delay_ms > MAX_PLAYBACK_DELAY_MS {
                    warn!(
                        target: "voice_server.session",
                        session_id = %self.session_id,
                        message_id = %metric_message_id,
                        delay_ms,
                        "忽略无效的客户端播放时延"
                    );
                    return None;
                }
                if self.remember_client_metric_once(
                    metric_message_id,
                    ClientMetricKind::FirstAudioReceivedToPlayback,
                    Instant::now(),
                ) {
                    self.metrics
                        .observe_client_first_audio_received_to_playback(Duration::from_millis(
                            delay_ms,
                        ));
                }
                None
            }
            VoicePayload::ClientMetricReport {
                message_id,
                metric,
                duration_ms,
                ..
            } => {
                const MAX_CLIENT_DURATION_MS: f64 = 30_000.0;
                if message_id.is_empty()
                    || !duration_ms.is_finite()
                    || !(0.0..=MAX_CLIENT_DURATION_MS).contains(&duration_ms)
                {
                    warn!(
                        target: "voice_server.session",
                        session_id = %self.session_id,
                        message_id = %message_id,
                        metric = ?metric,
                        duration_ms,
                        "忽略无效的客户端时延指标"
                    );
                    return None;
                }
                if self.remember_client_metric_once(message_id, metric, Instant::now()) {
                    let duration = Duration::from_secs_f64(duration_ms / 1_000.0);
                    match metric {
                        ClientMetricKind::FirstAudioReceivedToPlayback => self
                            .metrics
                            .observe_client_first_audio_received_to_playback(duration),
                        ClientMetricKind::InputEndToFinalAudioSent => self
                            .metrics
                            .observe_client_input_end_to_final_audio_sent(duration),
                    }
                }
                None
            }
            VoicePayload::SessionEnd { reason, .. } => {
                info!(
                    target: "voice_server.session",
                    session_id = %self.session_id,
                    reason = %reason,
                    "收到 SessionEnd，关闭会话"
                );
                self.global_cancel.cancel();
                self.closed = true;
                None
            }
            other => {
                debug!(
                    target: "voice_server.session",
                    session_id = %self.session_id,
                    "忽略非上行 payload: {:?}", other
                );
                None
            }
        }
    }

    fn trigger_pipeline(
        &mut self,
        reason: TriggerReason,
        input_ended_at: Instant,
    ) -> Option<JoinHandle<()>> {
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        let request_id = self.next_request_id;

        // 先把"耗时"取出来再 drain（drain 后 audio_buf 计时会被下次 push 重置）
        let bytes_before = self.audio_buf.len();
        let elapsed_before = self.audio_buf.elapsed_ms();
        let input_started_at = self.input_started_at.take().unwrap_or_else(Instant::now);
        let audio = self.audio_buf.drain();
        let message_id = self
            .current_message_id
            .take()
            .or_else(|| self.last_request_message_id.clone())
            .unwrap_or_else(crate::trace_context::new_trace_id);
        if audio.len() < MIN_UTTERANCE_BYTES {
            warn!(
                target: "voice_server.session",
                session_id = %self.session_id,
                bytes = audio.len(),
                bytes_before,
                elapsed_ms = elapsed_before as u64,
                min = MIN_UTTERANCE_BYTES,
                "音频过短（噪声/半句），丢弃不触发 pipeline"
            );
            return None;
        }
        self.last_request_audio = Some(audio.clone());
        self.last_request_message_id = Some(message_id.clone());
        info!(
            target: "voice_server.session",
            session_id = %self.session_id,
            trigger = ?reason,
            bytes = audio.len(),
            elapsed_ms = elapsed_before as u64,
            "句尾触发 pipeline"
        );

        // ===== 不再无条件 cancel 旧 pipeline =====
        // 旧实现：is_last 一到就 cancel 上一个，问题是 VAD 误切 / 上游返回空文本时
        // 也会把正在跑的 LLM/TTS 误杀。新实现：spawn 新 pipeline 各自跑 ASR，
        // ASR 拿到非空文本后才 cancel 旧 LLM/TTS（见 run_pipeline）。
        // 每个 pipeline 用 global_cancel.child_token() 派生自己的 token，
        // SessionEnd / Drop 时 global_cancel.cancel() 仍能终止所有在跑 pipeline。

        self.transition(SessionState::Processing);

        let cancel = self.global_cancel.child_token();
        let current_real_cancel = self.current_real_cancel.clone();
        let session_id = self.session_id.clone();
        let sample_rate = self.sample_rate;
        let channels = self.channels;
        // 端侧上报的 TTS 音色短名（None = 走配置兜底）
        let client_voice = self.client_voice.clone();
        let asr = self.asr.clone();
        let llm = self.llm.clone();
        let tts = self.tts.clone();
        let down_addr = self.down_addr.clone();
        let metrics = self.metrics.clone();

        let trace_id = self.trace_id.clone();
        let span = tracing::info_span!(
            target: "voice_server.ws",
            "WS pipeline",
            session_id = %session_id,
            request_id,
            trace_id = %trace_id,
        );
        let handle = tokio::spawn(
            scope(trace_id, async move {
                run_pipeline(
                    session_id,
                    request_id,
                    message_id,
                    audio,
                    sample_rate,
                    channels,
                    client_voice,
                    asr,
                    llm,
                    tts,
                    down_addr,
                    cancel,
                    current_real_cancel,
                    metrics,
                    input_started_at,
                    input_ended_at,
                )
                .await;
            })
            .instrument(span),
        );

        self.transition(SessionState::Listening);

        Some(handle)
    }
}

impl Drop for VoiceSession {
    fn drop(&mut self) {
        // session 析构（WsDisconnect 从 DashMap 移除）时兜底 cancel：
        // 已 spawn 的 pipeline 任务持有 Arc client + token，不 cancel 会一路跑完
        // LLM/TTS，白烧调用。global_cancel.cancel() 会传播到所有 child_token。
        self.global_cancel.cancel();
    }
}

// 编译期断言：VoiceSession 必须满足 DashMap 的 Send + Sync 要求（m2）。
// 不依赖任何外部 crate —— 直接用 where 子句即可，重构时若不小心破坏 Send/Sync，
// 编译器会在这里报错。
#[allow(dead_code)]
fn _assert_voice_session_send_sync()
where
    VoiceSession: Send + Sync,
{
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use actix::{Actor, Context, Handler};
    use async_trait::async_trait;
    use futures_util::{stream, Stream};
    use tokio::sync::mpsc;
    use voice_proto::{decode_payload, AgentPhase, ClientMetricKind};

    use crate::client::error::ClientError;
    use crate::client::llm::{BoxStream as LlmStream, ChatMessage};
    use crate::client::{LlmClient, TtsClient};
    use crate::events::{AsrEvent, LlmEvent, TtsEvent};
    use crate::metrics::VoiceMetrics;

    use super::*;

    struct CountingFailingAsr {
        calls: Arc<AtomicUsize>,
        wav_inputs: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    #[async_trait]
    impl AsrClient for CountingFailingAsr {
        async fn recognize(
            &self,
            _session_id: &str,
            _filename: Option<&str>,
            audio: Vec<u8>,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<AsrEvent, ClientError>> + Send>>, ClientError>
        {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.wav_inputs.lock().push(audio);
            Err(ClientError::Http("temporary ASR failure".to_string()))
        }
    }

    struct UnusedLlm;

    #[async_trait]
    impl LlmClient for UnusedLlm {
        async fn chat(
            &self,
            _session_id: &str,
            _prompt: &str,
            _emotion_hint: Option<&str>,
        ) -> Result<LlmStream<Result<LlmEvent, ClientError>>, ClientError> {
            Ok(Box::pin(stream::empty()))
        }

        async fn chat_with_messages(
            &self,
            _session_id: &str,
            _messages: &[ChatMessage],
            _emotion_hint: Option<&str>,
        ) -> Result<LlmStream<Result<LlmEvent, ClientError>>, ClientError> {
            Ok(Box::pin(stream::empty()))
        }
    }

    struct UnusedTts;

    #[async_trait]
    impl TtsClient for UnusedTts {
        async fn synthesize(
            &self,
            _session_id: &str,
            _text: &str,
            _voice_override: Option<String>,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<TtsEvent, ClientError>> + Send>>, ClientError>
        {
            Ok(Box::pin(stream::empty()))
        }

        fn default_voice_short(&self) -> &str {
            "unused"
        }
    }

    struct CaptureActor {
        tx: mpsc::UnboundedSender<VoicePayload>,
    }

    impl Actor for CaptureActor {
        type Context = Context<Self>;
    }

    impl Handler<OutMessage> for CaptureActor {
        type Result = ();

        fn handle(&mut self, msg: OutMessage, _ctx: &mut Self::Context) {
            let (_, payload) = decode_payload(&msg.data).expect("downlink payload should decode");
            self.tx.send(payload).expect("test receiver should be open");
        }
    }

    #[actix::test]
    async fn client_metric_reports_are_bounded_and_deduplicated_per_kind() {
        let metrics = Arc::new(VoiceMetrics::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let down_addr = CaptureActor { tx }.start().recipient();
        let mut session = VoiceSession::new_with_metrics(
            "session-1".to_string(),
            Arc::new(CountingFailingAsr {
                calls: Arc::new(AtomicUsize::new(0)),
                wav_inputs: Arc::new(Mutex::new(Vec::new())),
            }),
            Arc::new(LlmAgent::new(Arc::new(UnusedLlm))),
            Arc::new(UnusedTts),
            down_addr,
            metrics.clone(),
        );

        for (metric, duration_ms) in [
            (ClientMetricKind::FirstAudioReceivedToPlayback, 25.0),
            (ClientMetricKind::FirstAudioReceivedToPlayback, 30.0),
            (ClientMetricKind::InputEndToFinalAudioSent, 2.5),
            (ClientMetricKind::InputEndToFinalAudioSent, f64::NAN),
            (ClientMetricKind::InputEndToFinalAudioSent, 30_001.0),
        ] {
            session.on_payload(VoicePayload::ClientMetricReport {
                session_id: "session-1".to_string(),
                message_id: "message-1".to_string(),
                metric,
                duration_ms,
            });
        }

        let output = metrics.render();
        assert!(output.contains("voice_client_first_audio_received_to_playback_seconds_count 1"));
        assert!(output.contains("voice_client_input_end_to_final_audio_sent_seconds_count 1"));
    }

    #[actix::test]
    async fn retry_replays_the_last_valid_request_with_a_new_request_id() {
        let calls = Arc::new(AtomicUsize::new(0));
        let wav_inputs = Arc::new(Mutex::new(Vec::new()));
        let asr = Arc::new(CountingFailingAsr {
            calls: calls.clone(),
            wav_inputs: wav_inputs.clone(),
        });
        let llm = Arc::new(LlmAgent::new(Arc::new(UnusedLlm)));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let down_addr = CaptureActor { tx }.start().recipient();
        let mut session = VoiceSession::new(
            "session-1".to_string(),
            asr,
            llm,
            Arc::new(UnusedTts),
            down_addr,
        );
        session.on_payload(VoicePayload::SessionStart {
            session_id: "session-1".to_string(),
            sample_rate: 16_000,
            channels: 1,
            codec: "pcm_s16le".to_string(),
            language: "zh-CN".to_string(),
            voice: None,
        });

        let first = session
            .on_payload(VoicePayload::AudioChunk {
                session_id: "session-1".to_string(),
                seq: 1,
                timestamp_ms: 0,
                data: vec![0; MIN_UTTERANCE_BYTES],
                is_last: true,
            })
            .expect("the first valid request should spawn a pipeline");
        first.await.expect("the first pipeline should finish");

        let retry = session
            .on_payload(VoicePayload::Retry {
                session_id: "session-1".to_string(),
            })
            .expect("retry should spawn a pipeline for the saved request");
        retry.await.expect("the retry pipeline should finish");

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let inputs = wav_inputs.lock();
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0], inputs[1]);
        drop(inputs);

        let mut message_ids = Vec::new();
        while message_ids.len() < 2 {
            let payload = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
                .await
                .expect("retry statuses should arrive")
                .expect("capture actor should remain available");
            if let VoicePayload::AgentStatus {
                phase: AgentPhase::Transcribing,
                message_id,
                ..
            } = payload
            {
                message_ids.push(message_id);
            }
        }
        assert_eq!(message_ids.len(), 2);
        assert!(!message_ids[0].is_empty());
        assert_eq!(message_ids[0], message_ids[1]);
    }
}
