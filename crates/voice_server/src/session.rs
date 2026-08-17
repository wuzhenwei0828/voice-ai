//! VoiceSession: 每条 WS 连接对应一个会话
//!
//! 状态机：Idle / Listening / Processing / Speaking
//! 关键节点（转换、推流、错误）都打 `tracing::*!` 日志，便于观测。
//!
//! 下行通过 webhttp::websocket::OutMessage 推给客户端。
//!
//! pipeline 复用 admin_api::llm_tts_items（LLM → 切句 → TTS → 句间 crossfade），
//! 本文件只负责：上行消息分发、ASR 阶段、事件 → VoicePayload 映射、取消。
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
//! 单独设计调度器，超出本文件职责。
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

use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

use actix::prelude::Recipient;
use futures_util::StreamExt;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use voice_proto::VoicePayload;
use webhttp::websocket::OutMessage;

use crate::client::{asr::wrap_pcm_as_wav, AsrClient, LlmClient, TtsClient};
use crate::admin_api::{llm_tts_items, LlmTtsItem};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Idle,
    Listening,
    Processing,
    Speaking,
}

#[derive(Debug, Clone)]
pub struct AsrEvent {
    pub text: String,
    pub is_final: bool,
}

#[derive(Debug, Clone)]
pub struct LlmEvent {
    pub delta: String,
    pub is_final: bool,
}

#[derive(Debug, Clone)]
pub struct TtsEvent {
    pub seq: u32,
    pub data: Vec<u8>,
    pub is_last: bool,
}

/// 单句最长时长：超时未收到 is_last 也强制触发 pipeline。
/// 按墙钟算（不依赖采样率/声道假设）。
const MAX_UTTERANCE_MS: u128 = 30_000;
/// 单句最大缓冲字节（≈ 64s @ 16kHz s16le mono）：防客户端不发 is_last 导致内存无界增长。
const MAX_AUDIO_BYTES: usize = 2 * 1024 * 1024;
/// 单句最小字节（≈ 200ms @ 16kHz s16le mono）：低于此长度的 is_last 视为噪声/
/// 半句（VAD 误切、客户端抽风），直接丢弃不触发 pipeline，避免往 ASR 灌碎片音频。
const MIN_UTTERANCE_BYTES: usize = 6_400;

/// 本次 pipeline 由谁触发（仅用于日志观测）
#[derive(Debug, Clone, Copy)]
enum TriggerReason {
    /// 客户端 VAD 判定句尾（正常路径）
    ClientIsLast,
    /// 服务端兜底：单句超过 MAX_UTTERANCE_MS
    DurationCap,
    /// 服务端兜底：缓冲字节超过 MAX_AUDIO_BYTES
    BufferCap,
}

struct AudioAccumulator {
    chunks: Vec<Vec<u8>>,
    total_bytes: usize,
    started_at: Instant,
}

impl AudioAccumulator {
    fn new() -> Self {
        Self {
            chunks: Vec::new(),
            total_bytes: 0,
            started_at: Instant::now(),
        }
    }
    fn push(&mut self, chunk: Vec<u8>) {
        if self.total_bytes == 0 {
            // 新一句首帧：重新计时（覆盖 session 刚创建、上一句刚 drain 两种情况）
            self.started_at = Instant::now();
        }
        self.total_bytes += chunk.len();
        self.chunks.push(chunk);
    }
    fn len(&self) -> usize {
        self.total_bytes
    }
    fn elapsed_ms(&self) -> u128 {
        self.started_at.elapsed().as_millis()
    }
    fn drain(&mut self) -> Vec<u8> {
        let mut all = Vec::with_capacity(self.total_bytes);
        for c in self.chunks.drain(..) {
            all.extend_from_slice(&c);
        }
        self.total_bytes = 0;
        all
    }
}

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
    audio_buf: AudioAccumulator,
    /// 从 SessionStart 记下，用于包 WAV 头给 ASR（siliconflow 等 provider 按文件后缀选解码器）
    sample_rate: u32,
    channels: u16,
    asr: Arc<dyn AsrClient>,
    llm: Arc<dyn LlmClient>,
    tts: Arc<dyn TtsClient>,
    /// 下行发送目标：actix 的 Recipient，跨任务 clone
    down_addr: Recipient<OutMessage>,
}

/// pipeline 退出时兜底清掉自己在 `current_real_cancel` 中的注册（M4 part B）。
///
/// 只有当"自己仍然是 current"时才清，避免覆盖更新的 pipeline（auto-interrupt 链）的注册。
/// 这样无论 pipeline 是正常完成、被 cancel、还是 panic 早 return，current_real_cancel
/// 都不会留下 stale token 误 cancel 后续 pipeline。
struct CurrentCancelGuard {
    current: Arc<Mutex<Option<CancellationToken>>>,
    cancel: CancellationToken,
}

impl Drop for CurrentCancelGuard {
    fn drop(&mut self) {
        let mut g = self.current.lock();
        if g.as_ref().map(|c| c == &self.cancel).unwrap_or(false) {
            *g = None;
        }
    }
}

impl VoiceSession {
    pub fn new(
        session_id: String,
        asr: Arc<dyn AsrClient>,
        llm: Arc<dyn LlmClient>,
        tts: Arc<dyn TtsClient>,
        down_addr: Recipient<OutMessage>,
    ) -> Self {
        info!(target: "voice_server.session", session_id = %session_id, "VoiceSession 创建");
        Self {
            session_id,
            state: SessionState::Idle,
            closed: false,
            global_cancel: CancellationToken::new(),
            current_real_cancel: Arc::new(Mutex::new(None)),
            audio_buf: AudioAccumulator::new(),
            sample_rate: 0,
            channels: 0,
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
                ..
            } => {
                info!(
                    target: "voice_server.session",
                    session_id = %self.session_id,
                    sample_rate,
                    channels,
                    codec = %codec,
                    language = %language,
                    "收到 SessionStart"
                );
                // 记下格式参数，供后续 pipeline 包 WAV 头喂给 ASR
                self.sample_rate = sample_rate;
                self.channels = channels as u16;
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
                let bytes = data.len();
                info!(
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
                self.audio_buf.push(data);
                if is_last {
                    self.trigger_pipeline(TriggerReason::ClientIsLast)
                } else if self.audio_buf.elapsed_ms() >= MAX_UTTERANCE_MS {
                    warn!(
                        target: "voice_server.session",
                        session_id = %self.session_id,
                        elapsed_ms = self.audio_buf.elapsed_ms() as u64,
                        limit_ms = MAX_UTTERANCE_MS as u64,
                        "单句超长，服务端强制触发"
                    );
                    self.trigger_pipeline(TriggerReason::DurationCap)
                } else if self.audio_buf.len() >= MAX_AUDIO_BYTES {
                    warn!(
                        target: "voice_server.session",
                        session_id = %self.session_id,
                        buffered = self.audio_buf.len(),
                        limit = MAX_AUDIO_BYTES,
                        "缓冲超限，服务端强制触发"
                    );
                    self.trigger_pipeline(TriggerReason::BufferCap)
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
                send_down(&self.down_addr, VoicePayload::LlmDelta {
                    session_id: self.session_id.clone(),
                    delta: "[已打断]".to_string(),
                    is_final: true,
                });
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

    fn trigger_pipeline(&mut self, reason: TriggerReason) -> Option<JoinHandle<()>> {
        // 先把"耗时"取出来再 drain（drain 后 audio_buf 计时会被下次 push 重置）
        let bytes_before = self.audio_buf.len();
        let elapsed_before = self.audio_buf.elapsed_ms();
        let audio = self.audio_buf.drain();
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
        let asr = self.asr.clone();
        let llm = self.llm.clone();
        let tts = self.tts.clone();
        let down_addr = self.down_addr.clone();

        let handle = tokio::spawn(async move {
            run_pipeline(session_id, audio, sample_rate, channels, asr, llm, tts, down_addr, cancel, current_real_cancel).await;
        });

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

/// 完整 pipeline：ASR → LLM → 切句 → TTS（复用 admin_api::llm_tts_items）。
///
/// 与 /admin/asr_llm_tts 结构对称，差异仅在 wire 侧：
///   - 下行推 VoicePayload（msgpack 信封）而非 NDJSON
///   - 全程受 CancellationToken 约束（用户打断）
///   - Tts chunk 直接带原始 PCM 字节（不做 base64）
async fn run_pipeline(
    session_id: String,
    audio: Vec<u8>,
    sample_rate: u32,
    channels: u16,
    asr: Arc<dyn AsrClient>,
    llm: Arc<dyn LlmClient>,
    tts: Arc<dyn TtsClient>,
    down_addr: Recipient<OutMessage>,
    cancel: CancellationToken,
    current_real_cancel: Arc<Mutex<Option<CancellationToken>>>,
) {
    info!(target: "voice_server.session", session_id = %session_id, "pipeline 开始");

    // ===== 阶段 1: ASR —— 转发识别事件，收最终文本作为 LLM prompt =====
    // 浏览器 / WS pipeline 攒的是裸 PCM 字节；上游 siliconflow / OpenAI 兼容 ASR
    // 按 multipart 文件后缀选解码器，包成 RIFF/WAVE（44 字节头 + data chunk）才能解析
    let wav = wrap_pcm_as_wav(&audio, sample_rate, channels);
    debug!(
        target: "voice_server.session",
        session_id = %session_id,
        pcm_bytes = audio.len(),
        wav_bytes = wav.len(),
        sample_rate, channels,
        "包 WAV 头喂 ASR"
    );
    // recognize 建连（HTTP 请求）本身也进 select：否则建连期间无法被打断，
    // 只能等 HTTP 超时，Interrupt 形同虚设
    // 同时加 tokio::time::timeout：上游挂死/极慢响应 → future 被 drop，不会永久持有
    // Arc client + Mutex + HTTP 连接资源（M4 part A）
    let asr_timeout = std::time::Duration::from_secs(30);
    let mut asr_stream = match tokio::select! {
        _ = cancel.cancelled() => {
            warn!(target: "voice_server.session", session_id = %session_id, "ASR 建连阶段被取消");
            return;
        }
        r = tokio::time::timeout(asr_timeout, asr.recognize(&session_id, None, wav)) => {
            match r {
                Ok(inner) => inner,
                Err(_elapsed) => {
                    error!(
                        target: "voice_server.session",
                        session_id = %session_id,
                        timeout_secs = asr_timeout.as_secs(),
                        "ASR recognize 超时"
                    );
                    send_down(&down_addr, VoicePayload::Error {
                        code: 1001,
                        message: format!("asr timeout after {}s", asr_timeout.as_secs()),
                    });
                    return;
                }
            }
        }
    } {
        Ok(s) => s,
        Err(e) => {
            error!(target: "voice_server.session", session_id = %session_id, "ASR 调用失败: {}", e);
            send_down(&down_addr, VoicePayload::Error {
                code: 1001,
                message: format!("asr error: {}", e),
            });
            return;
        }
    };

    // 缓冲 ASR 事件，循环结束后根据最终文本决定 flush 还是全部丢弃：
    // 空文本时一条 AsrPartial 都不推到前端，避免前端拿到一条空识别记录
    let mut prompt = String::new();
    let mut asr_events: Vec<AsrEvent> = Vec::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                warn!(target: "voice_server.session", session_id = %session_id, "ASR 阶段被取消");
                return;
            }
            evt = asr_stream.next() => {
                match evt {
                    Some(Ok(e)) => {
                        // 取最新非空识别文本；非流式 ASR 客户端只发一个 is_final=true 的完整结果
                        if !e.text.is_empty() {
                            prompt = e.text.clone();
                        }
                        asr_events.push(e);
                        if let Some(last) = asr_events.last() {
                            if last.is_final { break; }
                        }
                    }
                    Some(Err(e)) => {
                        error!(target: "voice_server.session", session_id = %session_id, "ASR 流错误: {}", e);
                        send_down(&down_addr, VoicePayload::Error {
                            code: 1001,
                            message: format!("asr stream: {}", e),
                        });
                        return;
                    }
                    None => {
                        warn!(target: "voice_server.session", session_id = %session_id, "ASR 流提前结束");
                        break;
                    }
                }
            }
        }
    }

    let prompt = prompt.trim().to_string();
    if prompt.is_empty() {
        // 空文本：不推任何 AsrPartial / AsrFinal、不注册为 current real、
        // 不打断任何正在跑的 pipeline、跳过整个 LLM/TTS 链路
        // （VAD 误切、上游返回 ""、纯噪声都属于这一类；省一次 LLM 调用 + 一次 TTS 调用）
        warn!(
            target: "voice_server.session",
            session_id = %session_id,
            buffered_events = asr_events.len(),
            "ASR 最终文本为空，跳过 LLM/TTS（不推任何 ASR 事件、不打断其他 pipeline）"
        );
        return;
    }
    info!(target: "voice_server.session", session_id = %session_id, asr_text = %prompt, "ASR final 完成，进入 LLM");

    // 非空：把缓冲的 ASR 事件按顺序推给前端
    for e in asr_events {
        send_down(&down_addr, VoicePayload::AsrPartial {
            session_id: session_id.clone(),
            text: e.text,
            is_final: e.is_final,
        });
    }

    // ===== 注册为 current real 并 cancel 上一个 real pipeline（auto-interrupt on new utterance）=====
    // 空文本的 pipeline 已经早 return，不会污染这条路径。
    // 只 cancel 上一个「已经进入 LLM/TTS」的 pipeline（prev_token），不动当前 pipeline 的 cancel。
    // CurrentCancelGuard 在 pipeline 退出时清掉自己的 token，避免 stale token 误 cancel 后续
    // （M4 part B：drop guard）
    let _guard = CurrentCancelGuard {
        current: current_real_cancel.clone(),
        cancel: cancel.clone(),
    };
    let prev = current_real_cancel.lock().replace(cancel.clone());
    if let Some(prev) = prev {
        info!(
            target: "voice_server.session",
            session_id = %session_id,
            "ASR 拿到文本，打断上一个 LLM/TTS pipeline"
        );
        prev.cancel();
    }

    // ===== 阶段 2+3: LLM → 切句 → TTS（共享管线，含句间 crossfade / 全局 seq / 结束标记）=====
    let mut items = Box::pin(llm_tts_items(prompt, session_id.clone(), llm, tts));
    while let Some(item) = {
        tokio::select! {
            _ = cancel.cancelled() => {
                warn!(target: "voice_server.session", session_id = %session_id, "pipeline 被取消");
                return;
            }
            evt = items.next() => evt,
        }
    } {
        match item {
            LlmTtsItem::Llm { delta, is_final } => {
                if !delta.is_empty() || is_final {
                    send_down(&down_addr, VoicePayload::LlmDelta {
                        session_id: session_id.clone(),
                        delta,
                        is_final,
                    });
                }
            }
            LlmTtsItem::Tts { seq, audio, is_last } => {
                send_down(&down_addr, VoicePayload::TtsAudio {
                    session_id: session_id.clone(),
                    seq,
                    // admin_api 侧是 base64 字符串（NDJSON 需要），WS 侧还原为原始字节
                    data: base64::Engine::decode(
                        &base64::engine::general_purpose::STANDARD,
                        &audio,
                    )
                    .unwrap_or_default(),
                    is_last,
                });
            }
            LlmTtsItem::Failed { error, code } => {
                error!(target: "voice_server.session", session_id = %session_id, code, %error, "pipeline 失败");
                send_down(&down_addr, VoicePayload::Error {
                    code: code as u32,
                    message: error,
                });
                return;
            }
        }
    }
    info!(target: "voice_server.session", session_id = %session_id, "pipeline 全部完成");
}

fn send_down(addr: &Recipient<OutMessage>, p: VoicePayload) {
    match voice_proto::encode_indication(&p) {
        Ok(bytes) => {
            // 注：Recipient::do_send 返回 ()，无法获知失败 —— 用 try_send 接住 SendError
            // （SendError 的 Display 只输出变体名 "receiver is full" / "receiver is gone"），
            // 弱网下能定位"客户端没收到 TTS"这类问题
            if let Err(e) = addr.try_send(OutMessage { data: bytes }) {
                debug!(
                    target: "voice_server.session",
                    session_id = ?p.session_id(),
                    "下行 try_send 失败（客户端可能已断开）: {}", e
                );
            }
        }
        Err(e) => warn!(
            target: "voice_server.session",
            session_id = ?p.session_id(),
            "下行编码失败: {}", e
        ),
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

/// 在 `buf` 中找下一个"句末标点"，返回该标点之后第一个字节的位置（byte index）
/// pub 供 `admin_api` 切句逻辑复用
pub fn next_sentence_end(buf: &str) -> Option<usize> {
    for (i, c) in buf.char_indices() {
        if matches!(c, '。' | '！' | '?' | '.' | '!') {
            return Some(i + c.len_utf8());
        }
    }
    None
}
