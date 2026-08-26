//! 状态机：用户视角的 SessionState + 内部用的触发原因枚举

/// 用户视角的会话状态（仅用于日志/观测）。注意：spawn pipeline 后立即转回 Listening，
/// 不反映 pipeline 内部 ASR/LLM/TTS 子阶段；并发安全靠 CancellationToken + current_real_cancel，
/// 不是靠这个字段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Idle,
    Listening,
    Processing,
    Speaking,
}

/// 本次 pipeline 由谁触发（仅用于日志观测）
#[derive(Debug, Clone, Copy)]
pub(crate) enum TriggerReason {
    /// 客户端 VAD 判定句尾（正常路径）
    ClientIsLast,
    /// 服务端兜底：单句超过 MAX_UTTERANCE_MS
    DurationCap,
    /// 服务端兜底：缓冲字节超过 MAX_AUDIO_BYTES
    BufferCap,
}