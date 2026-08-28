//! 音频缓冲：攒 AudioChunk 到触发阈值，包成完整 PCM 喂给 ASR

use std::time::Instant;

/// 单句最长时长：超时未收到 is_last 也强制触发 pipeline。
/// 按墙钟算（不依赖采样率/声道假设）。
pub(super) const MAX_UTTERANCE_MS: u128 = 30_000;
/// 单句最大缓冲字节（≈ 64s @ 16kHz s16le mono）：防客户端不发 is_last 导致内存无界增长。
pub(super) const MAX_AUDIO_BYTES: usize = 2 * 1024 * 1024;
/// 单句最小字节（≈ 200ms @ 16kHz s16le mono）：低于此长度的 is_last 视为噪声/
/// 半句（VAD 误切、客户端抽风），直接丢弃不触发 pipeline，避免往 ASR 灌碎片音频。
pub(super) const MIN_UTTERANCE_BYTES: usize = 6_400;

/// 累积 AudioChunk 到一帧完整 utterance；触发时 drain 出原始 PCM 字节。
pub(super) struct AudioAccumulator {
    chunks: Vec<Vec<u8>>,
    total_bytes: usize,
    started_at: Instant,
}

impl AudioAccumulator {
    pub(super) fn new() -> Self {
        Self {
            chunks: Vec::new(),
            total_bytes: 0,
            started_at: Instant::now(),
        }
    }

    pub(super) fn push(&mut self, chunk: Vec<u8>) {
        if self.total_bytes == 0 {
            // 新一句首帧：重新计时（覆盖 session 刚创建、上一句刚 drain 两种情况）
            self.started_at = Instant::now();
        }
        self.total_bytes += chunk.len();
        self.chunks.push(chunk);
    }

    pub(super) fn len(&self) -> usize {
        self.total_bytes
    }

    pub(super) fn elapsed_ms(&self) -> u128 {
        self.started_at.elapsed().as_millis()
    }

    pub(super) fn drain(&mut self) -> Vec<u8> {
        let mut all = Vec::with_capacity(self.total_bytes);
        for c in self.chunks.drain(..) {
            all.extend_from_slice(&c);
        }
        self.total_bytes = 0;
        all
    }
}
