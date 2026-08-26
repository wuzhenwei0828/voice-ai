//! 句末判定 —— 任何 pipeline（LLM→TTS / image→TTS / text→TTS）切句都用

/// 在 `buf` 中找下一个"句末标点"，返回该标点之后第一个字节的位置（byte index）
///
/// 判定字符：全角 `。` / `！` / `？` + 半角 `.` / `!` / `?`
/// （中文文本习惯用全角标点，必须支持，否则 LLM 输出会累积不切句）
pub fn next_sentence_end(buf: &str) -> Option<usize> {
    for (i, c) in buf.char_indices() {
        if matches!(c, '。' | '！' | '？' | '.' | '!' | '?') {
            return Some(i + c.len_utf8());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_chinese_period() {
        // "你好世界。" = 4 个汉字 + 1 个全角句号 = 12 + 3 = 15 字节
        let s = "你好世界。今天天气不错";
        assert_eq!(next_sentence_end(s), Some(15));
    }

    #[test]
    fn finds_exclamation() {
        // "快跑！" = 2 个汉字 + 1 个全角感叹号 = 6 + 3 = 9 字节
        let s = "快跑！";
        assert_eq!(next_sentence_end(s), Some(9));
    }

    #[test]
    fn finds_english_period() {
        let s = "Hello world.";
        assert_eq!(next_sentence_end(s), Some(12));
    }

    #[test]
    fn finds_question_mark() {
        // "为什么？" = 3 个汉字 + 1 个全角问号 = 9 + 3 = 12 字节
        let s = "为什么？";
        assert_eq!(next_sentence_end(s), Some(12));
    }

    #[test]
    fn returns_none_when_no_sentence_end() {
        let s = "你好世界今天天气不错";
        assert_eq!(next_sentence_end(s), None);
    }

    #[test]
    fn returns_first_match() {
        // 第一个句末标点之后的内容里还有句号，也只返回第一个
        // "第一句。" = 3 个汉字 + 1 个全角句号 = 9 + 3 = 12 字节
        let s = "第一句。第二句。";
        assert_eq!(next_sentence_end(s), Some(12));
    }
}