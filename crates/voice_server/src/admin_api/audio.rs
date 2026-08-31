//! 音频预处理：strip WAV 头 / 包成 16kHz mono s16le WAV
//!
//! `/admin/asr` 与 `/admin/asr_llm_tts` 端点的入参兼容两种格式：
//!   - 上传的就是合法 WAV（带 RIFF/WAVE 头）—— 拆 data chunk 取 PCM
//!   - 裸 PCM 字节流（前端约定 16kHz mono s16le）—— 直接当 PCM 处理
//!
//! 两种都统一包成完整 WAV（带 RIFF 头）喂给上游 ASR —— siliconflow 等
//! provider 按 multipart 文件后缀选解码器，没头的裸 PCM 经常被识别错。

use crate::client::asr::wrap_pcm_as_wav;

/// 在 WAV 里找 data chunk 的偏移和大小（兼容非标准 fmt 长度）。
fn find_wav_data(bytes: &[u8]) -> Option<(usize, usize)> {
    if bytes.len() < 12 || &bytes[0..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
        return None;
    }
    let mut pos = 12usize;
    while pos + 8 <= bytes.len() {
        let id = &bytes[pos..pos + 4];
        let size = u32::from_le_bytes([
            bytes[pos + 4],
            bytes[pos + 5],
            bytes[pos + 6],
            bytes[pos + 7],
        ]) as usize;
        if id == b"data" {
            let avail = bytes.len() - (pos + 8);
            return Some((pos + 8, size.min(avail)));
        }
        let advance = 8 + size + (size & 1);
        if advance == 0 {
            break;
        }
        pos += advance;
    }
    None
}

/// 把上传音频统一转成"16kHz mono s16le WAV 字节流"给 ASR。
/// - 如果上传的就是 WAV：找 data chunk 取出 PCM（要求原文件已是 16kHz mono s16le）
/// - 否则：按裸 PCM 处理（前端约定 16kHz mono s16le）
/// 返回 None 表示输入不是合法 WAV / 长度不够；前端在 UI 上把这种情况当错误显示。
pub(super) fn prepare_audio_for_asr(bytes: Vec<u8>) -> Option<Vec<u8>> {
    let pcm: &[u8] = if let Some((off, size)) = find_wav_data(&bytes) {
        &bytes[off..off + size]
    } else {
        &bytes[..]
    };
    if pcm.is_empty() {
        return None;
    }
    Some(wrap_pcm_as_wav(pcm, 16000, 1))
}
