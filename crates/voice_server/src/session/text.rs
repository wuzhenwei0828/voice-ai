//! 文本处理：切句 + ASR 标签解析 + 情绪标签映射

/// ASR 情绪解析结果
#[derive(Debug, Clone, Default)]
pub struct AsrParseResult {
    /// 去除 `<|...|>` 标签后的纯文本
    pub text: String,
    /// 第二个 `<|...|>` 标签里的情绪字符串（大小写保留），无情绪时为 `None`
    pub emotion: Option<String>,
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

/// 从 ASR 文本里剥离 `<|...|>` 风格的特殊 token，并返回第二个 token 的内容作为情绪。
///
/// Qwen 等 ASR 可能在识别结果末尾附加诸如 `<|zh|><|SAD|><|Speech|><|woitn|>` 的 token：
///   - 第 1 个 token 是语言
///   - 第 2 个 token 是说话人情绪（如 SAD / HAPPY / NEUTRAL），若为 `unk`/`UNK`
///     表示 ASR 没有识别出情绪，按"无情绪"处理
///   - 后续 token 是占位 / 控制标记
///
/// 本函数把这些 token 从文本里剥掉（**只剥 token 本身，token 之间的空白保留**），
/// 并把第 2 个 token 的内容作为情绪返回，供 LLM 在系统提示里参考。
/// 情绪不一定准确，调用方应在提示里说明这一点。
///
/// 例子：
///   `<|zh|><|SAD|><|Speech|><|woitn|>今天好累。`
///   → `text = "今天好累。"`，`emotion = Some("SAD")`
///
///   `<|zh|><|UNK|><|Speech|><|woitn|>你好`
///   → `text = "你好"`，`emotion = None`（ASR 没能识别出情绪）
///
/// 异常输入：
///   - 没有 `<|...|>` 标签：返回原文、`emotion = None`
///   - 只有 1 个或 0 个完整标签：`emotion = None`，标签全部剥掉
///   - 未配对的 `<|`（没有 `|>`）：当作普通字符保留
pub fn parse_asr_emotion_tags(text: &str) -> AsrParseResult {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    let mut tag_count = 0usize;
    let mut emotion: Option<String> = None;

    while let Some(open_pos) = rest.find("<|") {
        // <| 之前的原文照搬
        out.push_str(&rest[..open_pos]);
        let after_open = &rest[open_pos + 2..];
        if let Some(close_pos) = after_open.find("|>") {
            let content = &after_open[..close_pos];
            tag_count += 1;
            if tag_count == 2 {
                // 只在命中 7 类已知情绪时 set emotion；其他一律 None
                // （unk / EMO_UNKNOWN / Speech / woitn / 自定义事件标签等都不透传给 LLM）
                if let Some(e) = Emotion::from_tag(content) {
                    emotion = Some(e.label_zh().to_string());
                }
            }
            rest = &after_open[close_pos + 2..];
        } else {
            // 未配对：把 <| 当成普通字符保留，从 < 处继续向后扫
            out.push('<');
            out.push('|');
            rest = &rest[open_pos + 2..];
        }
    }
    out.push_str(rest);
    AsrParseResult {
        text: out.trim().to_string(),
        emotion,
    }
}

/// SenseVoice 7 类情绪标签 → 中文标签。
///
/// 已知标签（大小写不敏感）映射到中文，透传给 LLM 时便于 LLM 理解与生成匹配情绪的回复。
/// 不在表里的标签（`unk` / `EMO_UNKNOWN` / `Speech` / `woitn` / 自定义事件等）一律不当作情绪，
/// 也不会透传给 LLM —— 由 [`parse_asr_emotion_tags`] 在第二个标签位上严格过滤。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Emotion {
    Happy,     // HAPPY     → 开心
    Sad,       // SAD       → 悲伤
    Angry,     // ANGRY     → 愤怒
    Neutral,   // NEUTRAL   → 中性
    Fearful,   // FEARFUL   → 恐惧
    Disgusted, // DISGUSTED → 厌恶
    Surprised, // SURPRISED → 惊讶
}

impl Emotion {
    /// 中文标签（传给 LLM 时使用）
    pub fn label_zh(self) -> &'static str {
        match self {
            Emotion::Happy => "开心",
            Emotion::Sad => "悲伤",
            Emotion::Angry => "愤怒",
            Emotion::Neutral => "中性",
            Emotion::Fearful => "恐惧",
            Emotion::Disgusted => "厌恶",
            Emotion::Surprised => "惊讶",
        }
    }

    /// 从 SenseVoice 标签解析（大小写不敏感）。命中 7 类之一返回对应变体，
    /// 否则返回 `None`（包括 `unk` / `EMO_UNKNOWN` 等"未识别"情况，调用方自行决定 None 语义）。
    pub fn from_tag(tag: &str) -> Option<Self> {
        // to_ascii_lowercase 分配；；7 个 case 不值得引入 once_cell 缓存
        match tag.to_ascii_uppercase().as_str() {
            "HAPPY" => Some(Emotion::Happy),
            "SAD" => Some(Emotion::Sad),
            "ANGRY" => Some(Emotion::Angry),
            "NEUTRAL" => Some(Emotion::Neutral),
            "FEARFUL" => Some(Emotion::Fearful),
            "DISGUSTED" => Some(Emotion::Disgusted),
            "SURPRISED" => Some(Emotion::Surprised),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_normal_tags_strips_and_extracts_emotion() {
        // 用户给的典型样例：第二个标签是 SAD → 映射成中文"悲伤"
        let r = parse_asr_emotion_tags("<|zh|><|SAD|><|Speech|><|woitn|>今天好累。");
        assert_eq!(r.text, "今天好累。");
        assert_eq!(r.emotion.as_deref(), Some("悲伤"));
    }

    #[test]
    fn parse_keeps_internal_whitespace() {
        // 标签之间有空格 —— 原始 ASR 输出里偶有；token 应被剥掉，空白保留
        let r = parse_asr_emotion_tags("你好世界 <|zh|><|HAPPY|><|Speech|>!");
        assert_eq!(r.text, "你好世界 !");
        assert_eq!(r.emotion.as_deref(), Some("开心"));
    }

    #[test]
    fn parse_no_tags_returns_original() {
        let r = parse_asr_emotion_tags("纯文本没有标签。");
        assert_eq!(r.text, "纯文本没有标签。");
        assert!(r.emotion.is_none());
    }

    #[test]
    fn parse_single_tag_has_no_emotion() {
        // 只有 1 个完整标签 → 没有第 2 个，不算情绪
        let r = parse_asr_emotion_tags("说话内容<|zh|>尾巴");
        assert_eq!(r.text, "说话内容尾巴");
        assert!(r.emotion.is_none());
    }

    #[test]
    fn parse_unclosed_tag_kept_as_literal() {
        // 尾部一个未配对的 <| —— 不应误删；当前实现是把孤立的 <| 视作普通字符继续向后扫
        let r = parse_asr_emotion_tags("a<|b ");
        assert_eq!(r.text, "a<|b");
        assert!(r.emotion.is_none());
    }

    #[test]
    fn parse_only_tags_yields_empty_text() {
        // 全部都是标签，剥完是空串（与 pipeline 的"空文本跳过 LLM"语义一致）
        let r = parse_asr_emotion_tags("<|zh|><|NEUTRAL|><|Speech|>");
        assert_eq!(r.text, "");
        assert_eq!(r.emotion.as_deref(), Some("中性"));
    }

    #[test]
    fn parse_unk_emotion_is_treated_as_none() {
        // ASR 没能识别出情绪时输出 <|unk|> / <|EMO_UNKNOWN|> 等 —— 不应透传给 LLM
        for tag in [
            "unk",
            "UNK",
            "Unk",
            "uNk",
            "EMO_UNKNOWN",
            "emo_unknown",
            "Emo_Unknown",
        ] {
            let r = parse_asr_emotion_tags(&format!("你好世界<|zh|><|{tag}|><|Speech|>"));
            assert_eq!(r.text, "你好世界", "tag={tag}");
            assert!(r.emotion.is_none(), "tag={tag} should be None");
        }
    }

    #[test]
    fn emotion_enum_maps_all_seven_tags_to_chinese() {
        // 7 类已知标签全部映射正确，大小写不敏感
        let cases: &[(&str, Emotion, &str)] = &[
            ("HAPPY", Emotion::Happy, "开心"),
            ("SAD", Emotion::Sad, "悲伤"),
            ("ANGRY", Emotion::Angry, "愤怒"),
            ("NEUTRAL", Emotion::Neutral, "中性"),
            ("FEARFUL", Emotion::Fearful, "恐惧"),
            ("DISGUSTED", Emotion::Disgusted, "厌恶"),
            ("SURPRISED", Emotion::Surprised, "惊讶"),
        ];
        for (tag, expected_variant, expected_zh) in cases {
            assert_eq!(Emotion::from_tag(tag), Some(*expected_variant), "tag={tag}");
            assert_eq!(Emotion::from_tag(&tag.to_ascii_lowercase()), Some(*expected_variant), "tag lower={tag}");
            assert_eq!(Emotion::from_tag(tag).unwrap().label_zh(), *expected_zh);
        }
        // 不在 7 类里的 → None
        assert_eq!(Emotion::from_tag("Speech"), None);
        assert_eq!(Emotion::from_tag("woitn"), None);
        assert_eq!(Emotion::from_tag(""), None);
    }

    #[test]
    fn parse_known_emotion_returns_chinese_label() {
        // 7 类已知情绪在 parse 里都映射成中文
        let cases: &[(&str, &str)] = &[
            ("HAPPY", "开心"),
            ("happy", "开心"),
            ("SAD", "悲伤"),
            ("ANGRY", "愤怒"),
            ("NEUTRAL", "中性"),
            ("FEARFUL", "恐惧"),
            ("DISGUSTED", "厌恶"),
            ("SURPRISED", "惊讶"),
        ];
        for (tag, zh) in cases {
            let r = parse_asr_emotion_tags(&format!("<|zh|><|{tag}|><|Speech|>今天天气不错"));
            assert_eq!(r.emotion.as_deref(), Some(*zh), "tag={tag}");
        }
    }

    #[test]
    fn parse_unknown_tag_does_not_set_emotion() {
        // 不在 7 类已知情绪里的标签（包括 unk / EMO_UNKNOWN / Speech / woitn / 自定义事件等）
        // 一律不进 emotion —— 只有 7 类已知情绪才会被透传给 LLM
        let r = parse_asr_emotion_tags("<|zh|><|Speech|><|NEUTRAL|>text");
        assert!(r.emotion.is_none(), "Speech 不是情绪标签，应为 None");

        let r = parse_asr_emotion_tags("<|zh|><|woitn|><|NEUTRAL|>text");
        assert!(r.emotion.is_none(), "woitn 不是情绪标签，应为 None");

        let r = parse_asr_emotion_tags("<|zh|><|foo|><|bar|>text");
        assert!(r.emotion.is_none(), "自定义标签 foo/bar 不算情绪");
    }
}