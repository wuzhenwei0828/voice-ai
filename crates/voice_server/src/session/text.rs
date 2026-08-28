//! 文本处理：切句
//!
//! ASR 标签解析（emotion / event）已迁移到 [`crate::utils::postprocess_utils::parse_asr_text`]。

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_sentence_end_chinese_period() {
        // 返回的是标点**之后**的 byte index：标点 '。' 本身 3-byte，所以是 6 + 3 = 9
        assert_eq!(next_sentence_end("你好。"), Some(9));
        assert_eq!(next_sentence_end("你好。世界"), Some(9));
    }

    #[test]
    fn next_sentence_end_no_punct() {
        assert_eq!(next_sentence_end("你好"), None);
    }
}