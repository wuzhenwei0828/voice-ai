const MIN_TTS_CHARS: usize = 5;

/// Convert an LLM response fragment into text suitable for direct speech.
///
/// This intentionally operates on complete sentence/paragraph fragments rather
/// than individual streaming deltas, so Markdown markers split across deltas
/// can still be handled together by the caller.
pub fn to_tts_text(input: &str) -> String {
    let normalized = normalize_line_break_tokens(input);
    let mut output = Vec::new();
    let mut in_code_block = false;

    for line in normalized.lines() {
        let trimmed = line.trim();
        if is_code_fence(trimmed) {
            in_code_block = !in_code_block;
            continue;
        }
        if in_code_block || trimmed.is_empty() {
            continue;
        }

        let line = strip_block_marker(trimmed);
        let line = strip_emoji(&strip_inline_markdown(line.trim()));
        if !line.is_empty() {
            output.push(line);
        }
    }

    output.join(" ")
}

/// Holds short cleaned sentences until a TTS request has enough speech content.
#[derive(Debug, Default)]
pub struct TtsSentenceBuffer {
    pending: String,
}

impl TtsSentenceBuffer {
    /// Add one cleaned sentence. Returns a merged request once the threshold is met.
    pub fn push(&mut self, sentence: &str) -> Option<String> {
        let sentence = sentence.trim();
        if sentence.is_empty() {
            return None;
        }
        self.pending.push_str(sentence);
        if effective_char_count(&self.pending) >= MIN_TTS_CHARS {
            Some(std::mem::take(&mut self.pending))
        } else {
            None
        }
    }

    /// Flush residual text at the end of a stream, even when it is shorter than the threshold.
    pub fn flush(&mut self) -> Option<String> {
        let text = std::mem::take(&mut self.pending);
        (!text.is_empty()).then_some(text)
    }
}

fn normalize_line_break_tokens(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if (ch == '\\' || ch == '/') && chars.peek() == Some(&'n') {
            chars.next();
            output.push('\n');
        } else {
            output.push(ch);
        }
    }
    output.replace("\r\n", "\n").replace('\r', "\n")
}

fn is_code_fence(line: &str) -> bool {
    line.starts_with("```") || line.starts_with("~~~")
}

fn strip_block_marker(line: &str) -> &str {
    let heading_len = line.chars().take_while(|ch| *ch == '#').count();
    if (1..=6).contains(&heading_len)
        && line
            .chars()
            .nth(heading_len)
            .is_some_and(char::is_whitespace)
    {
        return line[heading_len..].trim_start();
    }

    let list = line.strip_prefix(['-', '*', '+']);
    if let Some(rest) = list.filter(|rest| rest.starts_with(char::is_whitespace)) {
        return rest.trim_start();
    }

    let digits = line.chars().take_while(|ch| ch.is_ascii_digit()).count();
    if digits > 0 {
        let rest = &line[digits..];
        if let Some(rest) = rest.strip_prefix(['.', ')']) {
            if rest.starts_with(char::is_whitespace) {
                return rest.trim_start();
            }
        }
    }

    line
}

fn strip_inline_markdown(input: &str) -> String {
    let chars: Vec<char> = input.chars().collect();
    let mut output = String::with_capacity(input.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '[' || (chars[i] == '!' && chars.get(i + 1) == Some(&'[')) {
            let open = if chars[i] == '[' { i } else { i + 1 };
            if let Some(close) = find_char(&chars, open + 1, ']') {
                if chars.get(close + 1) == Some(&'(') {
                    if let Some(end) = find_char(&chars, close + 2, ')') {
                        output.extend(&chars[open + 1..close]);
                        i = end + 1;
                        continue;
                    }
                }
            }
        }

        if chars[i] == '<' {
            if let Some(end) = find_char(&chars, i + 1, '>') {
                i = end + 1;
                continue;
            }
        }

        if chars[i] == '\\' && chars.get(i + 1).is_some_and(|ch| ch.is_ascii_punctuation()) {
            output.push(chars[i + 1]);
            i += 2;
            continue;
        }

        if matches!(chars[i], '`' | '*' | '_' | '~') {
            i += 1;
            continue;
        }

        output.push(chars[i]);
        i += 1;
    }
    output
}

fn strip_emoji(input: &str) -> String {
    input.chars().filter(|ch| !is_emoji_char(*ch)).collect()
}

fn is_emoji_char(ch: char) -> bool {
    matches!(
        ch,
        '\u{00A9}'
            | '\u{00AE}'
            | '\u{203C}'
            | '\u{2049}'
            | '\u{2122}'
            | '\u{2139}'
            | '\u{3030}'
            | '\u{303D}'
            | '\u{3297}'
            | '\u{3299}'
            | '\u{200D}'
            | '\u{20E3}'
            | '\u{FE0E}'..='\u{FE0F}'
            | '\u{1F000}'..='\u{1FAFF}'
            | '\u{2300}'..='\u{23FF}'
            | '\u{2600}'..='\u{27BF}'
    )
}

fn find_char(chars: &[char], start: usize, target: char) -> Option<usize> {
    chars
        .iter()
        .enumerate()
        .skip(start)
        .find_map(|(index, ch)| (*ch == target).then_some(index))
}

fn effective_char_count(text: &str) -> usize {
    text.chars().filter(|ch| ch.is_alphanumeric()).count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removes_markdown_syntax_and_newline_tokens() {
        assert_eq!(
            to_tts_text("## 标题\\n\\n**好的**，请查看[帮助文档](https://example.com)。"),
            "标题 好的，请查看帮助文档。"
        );
    }

    #[test]
    fn removes_emojis_from_tts_text() {
        assert_eq!(to_tts_text("我很开心😊，真的👍。"), "我很开心，真的。");
    }

    #[test]
    fn removes_emoji_variation_and_zwj_components() {
        assert_eq!(to_tts_text("状态：👨‍💻，完成✅。"), "状态：，完成。");
    }

    #[test]
    fn buffers_short_sentences_until_threshold() {
        let mut buffer = TtsSentenceBuffer::default();

        assert_eq!(buffer.push("好的。"), None);
        assert_eq!(
            buffer.push("我马上处理。"),
            Some("好的。我马上处理。".to_string())
        );
        assert_eq!(buffer.flush(), None);
    }

    #[test]
    fn flushes_short_residue_at_end_of_stream() {
        let mut buffer = TtsSentenceBuffer::default();
        assert_eq!(buffer.push("嗯。"), None);
        assert_eq!(buffer.flush(), Some("嗯。".to_string()));
    }
}
