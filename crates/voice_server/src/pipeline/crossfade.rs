//! 句间 crossfade —— 任何"文本→TTS 切句拼接"流水线都用
//!
//! 每句 TTS 是独立合成，波形在拼接处瞬跳会产生"咔哒"声；把上一句末尾
//! `FADE_BYTES` 与下一句开头 `FADE_BYTES` 线性混合可消除。
//!
//! 按句使用：`begin_sentence` → `feed*` → `end_sentence`。

/// 句间拼接淡化区长度：10ms @ 16kHz s16le mono = 160 采样 = 320 字节。
const FADE_BYTES: usize = 320;

/// 句间 crossfade 状态机。
#[derive(Default)]
pub struct SentenceCrossfader {
    /// 上一句结尾扣下的、尚未下发的字节（≤ FADE_BYTES）
    tail: Vec<u8>,
    /// 当前句开头的缓冲（攒够 FADE_BYTES 后与 tail 混合）
    head: Vec<u8>,
    /// 当前句的滚动保留区（始终是当前句最近 ≤ FADE_BYTES 未发字节）
    hold: Vec<u8>,
    /// 当前句开头是否已完成混合（完成后再喂的数据走滚动保留）
    head_done: bool,
}

impl SentenceCrossfader {
    pub fn begin_sentence(&mut self) {
        self.head.clear();
        self.hold.clear();
        // 上一句没有遗留 tail（如第一句）时，本句开头无需混合
        self.head_done = self.tail.is_empty();
    }

    /// 喂入当前句一段 PCM，返回可立即下发的字节
    pub fn feed(&mut self, mut bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        if !self.head_done {
            let need = FADE_BYTES - self.head.len();
            let take = need.min(bytes.len());
            self.head.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.head.len() >= FADE_BYTES {
                out.extend_from_slice(&crossfade(&self.tail, &self.head[..FADE_BYTES]));
                out.extend_from_slice(&self.head[FADE_BYTES..]);
                self.tail.clear();
                self.head.clear();
                self.head_done = true;
            }
        }
        if self.head_done && !bytes.is_empty() {
            // 滚动扣留句尾 FADE_BYTES，其余下发
            self.hold.extend_from_slice(bytes);
            if self.hold.len() > FADE_BYTES {
                let emit = self.hold.len() - FADE_BYTES;
                out.extend_from_slice(&self.hold[..emit]);
                self.hold.drain(..emit);
            }
        }
        out
    }

    /// 当前句结束：句尾保留区转存为 tail，留给下一句混合
    pub fn end_sentence(&mut self) -> Vec<u8> {
        if self.head_done {
            self.tail = std::mem::take(&mut self.hold);
            Vec::new()
        } else {
            // 整句比一个淡化区还短：按实际长度混合后全部下发
            let n = (self.tail.len().min(self.head.len())) & !1; // 对齐到采样边界
            let mut out = crossfade(&self.tail[..n], &self.head[..n]);
            out.extend_from_slice(&self.head[n..]);
            self.tail.clear();
            self.head.clear();
            self.head_done = true;
            out
        }
    }

    /// 整条流结束：最后一句扣留的句尾不再需要留给别人，原样下发
    pub fn finish(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        out.append(&mut self.tail);
        out.append(&mut self.hold);
        out.append(&mut self.head);
        out
    }
}

/// 等长两段 s16le PCM 线性混合：a 淡出、b 淡入。长度需为偶数（采样对齐）。
fn crossfade(a: &[u8], b: &[u8]) -> Vec<u8> {
    debug_assert_eq!(a.len(), b.len());
    debug_assert_eq!(a.len() % 2, 0);
    let n = a.len() / 2;
    let mut out = Vec::with_capacity(a.len());
    for i in 0..n {
        let t = (i + 1) as f32 / (n + 1) as f32;
        let sa = i16::from_le_bytes([a[2 * i], a[2 * i + 1]]) as f32;
        let sb = i16::from_le_bytes([b[2 * i], b[2 * i + 1]]) as f32;
        out.extend_from_slice(&((sa * (1.0 - t) + sb * t) as i16).to_le_bytes());
    }
    out
}
