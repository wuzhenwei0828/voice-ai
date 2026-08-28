//! FunASR 后处理工具（SenseVoice 等模型的输出清洗）
//!
//! Port of `funasr_torch/utils/postprocess_utils.py`
//!
//! 入口：
//!   - [`rich_transcription_postprocess`]：清洗带 emotion / event / lang 标签的原始输出
//!   - [`sentence_postprocess`]：处理 word-level 输出（可选带时间戳）
//!
//! 其它辅助：`is_chinese` / `is_all_chinese` / `is_all_alpha` / `abbr_dispose` / `format_str_v2`

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

// =============================================================================
// 字符判断
// =============================================================================

/// 单个 char 是否为 CJK 汉字或 ASCII 数字（与 Python
/// `"一" <= ch <= "鿿" or "0" <= ch <= "9"` 等价）。
#[inline]
pub fn is_chinese(ch: char) -> bool {
    matches!(ch, '\u{4E00}'..='\u{9FFF}') || ch.is_ascii_digit()
}

/// 剥掉每个 token 中的空白 / `<s>` / `</s>`（与 Python `replace(" ", "")` + 两次 `replace` 等价）。
fn clean_token(w: &str) -> String {
    w.replace(' ', "").replace("</s>", "").replace("<s>", "")
}

/// tokens 中是否全部为汉字或数字。
///
/// 空输入返回 `false`（与 Python `len(word_lists) == 0` 时一致）。
pub fn is_all_chinese(words: &[&str]) -> bool {
    if words.is_empty() {
        return false;
    }
    words.iter().all(|w| {
        let cleaned = clean_token(w);
        !cleaned.is_empty() && cleaned.chars().all(is_chinese)
    })
}

/// tokens 中是否全部为非汉字的字母字符或 `'`。
///
/// 空输入返回 `false`。
pub fn is_all_alpha(words: &[&str]) -> bool {
    if words.is_empty() {
        return false;
    }
    words.iter().all(|w| {
        let cleaned = clean_token(w);
        !cleaned.is_empty()
            && cleaned.chars().all(|c| {
                // 等价 Python：`c.isalpha() and not isChinese(c)` 或 `c == "'"`
                (c.is_alphabetic() && !is_chinese(c)) || c == '\''
            })
    })
}

// =============================================================================
// parse_asr_text：把 ASR 原始带 `<|...|>` 标签的文本解析成结构化结果
// =============================================================================

/// ASR 原始带 `<|...|>` 标签文本的结构化解析结果
///
/// 从 [`rich_transcription_postprocess`] 的输出中提取 emoji 情绪/事件，
/// 并返回去除这些 emoji 后的正文。调用方负责先执行 rich 后处理。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AsrParsed {
    /// rich 后处理结果中去除事件/情感 emoji 后的正文
    pub text: String,
    /// 主导 emotion（中文标签，如 Some("开心")）；未识别到时 `None`
    pub emotion: Option<String>,
    /// 去重后的事件列表（按首次出现顺序）；未识别到时为空列表
    pub event: Vec<String>,
}

/// 输入应是 rich 后处理结果，而不是带 `<|...|>` 标签的 ASR 原文。
pub fn parse_asr_text(input: &str) -> AsrParsed {
    let mut emotion_stats: HashMap<&str, (usize, usize)> = HashMap::new();
    let mut event = Vec::new();
    let mut seen_events = HashSet::new();
    let mut position = 0usize;

    let mut text = String::with_capacity(input.len());
    for ch in input.chars() {
        let mut matched = false;
        for (emoji, label) in EMOJI_EMOTION_LABELS {
            if ch.to_string() == *emoji {
                let entry = emotion_stats.entry(label).or_insert((0, 0));
                entry.0 += 1;
                entry.1 = position;
                position += 1;
                matched = true;
                break;
            }
        }
        if matched {
            continue;
        }
        for (emoji, label) in EMOJI_EVENT_LABELS {
            if ch.to_string() == *emoji {
                if seen_events.insert(*label) {
                    event.push((*label).to_string());
                }
                position += 1;
                matched = true;
                break;
            }
        }
        if matched {
            continue;
        }
        text.push(ch);
    }

    let emotion = emotion_stats
        .into_iter()
        .max_by_key(|(_, (count, last_position))| (*count, *last_position))
        .map(|(label, _)| label.to_string());

    AsrParsed {
        text: text.trim().to_string(),
        emotion,
        event,
    }
}

/// 把 ASR 解析出的情绪与事件整理为一条传给 LLM 的参考提示。
/// 两者都缺失时返回 `None`；空事件不会污染提示内容。
pub fn format_asr_hint(emotion: Option<&str>, events: &[String]) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(emotion) = emotion.filter(|value| !value.is_empty()) {
        parts.push(format!("情绪：{emotion}"));
    }
    let events: Vec<&str> = events
        .iter()
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .collect();
    if !events.is_empty() {
        parts.push(format!("事件：{}", events.join("、")));
    }
    (!parts.is_empty()).then(|| parts.join("；"))
}

const EMOJI_EMOTION_LABELS: &[(&str, &str)] = &[
    ("😊", "开心"),
    ("😔", "悲伤"),
    ("😡", "愤怒"),
    ("😰", "恐惧"),
    ("🤢", "厌恶"),
    ("😮", "惊讶"),
];

const EMOJI_EVENT_LABELS: &[(&str, &str)] = &[
    ("❓", ""),
    ("🎼", "背景音乐"),
    ("👏", "掌声"),
    ("😀", "笑声"),
    ("😭", "哭声"),
    ("🤧", "喷嚏"),
    ("😷", "咳嗽"),
];

// =============================================================================
// abbr_dispose：检测缩写区间（用空格分隔的连续单字母 token），把区间内的 token
// 大写化，并丢掉区间内的 space token。区间外的 token 原样保留（包括区间右侧的
// 空格）。
//
// 注：本函数**不做 join**。join 由 [`sentence_postprocess`] 拿到
// `real_word_lists` 后自行处理。
//   `["H", " ", "I"]`         → `["H", "I"]`
//   `["H", " ", "I", " ", "foo"]` → `["H", "I", " ", "foo"]`
// =============================================================================

/// `abbr_dispose` 的返回结果
#[derive(Debug, Clone, Default)]
pub struct AbbrDisposeOutput {
    /// 处理后的 word 列表
    pub words: Vec<String>,
    /// 当输入提供了 time_stamp 时存在；元素是 `[begin, end]`（按 token 对齐）
    pub time_stamps: Option<Vec<[i64; 2]>>,
}

/// 单 ASCII 字母判定（仿 Python `len(s) == 1 and s.encode("utf-8").isalpha()`，
/// 排除了 CJK 等多字节字符）。
fn is_single_ascii_alpha(s: &str) -> bool {
    // ASCII 字母是单字节 UTF-8；这样写也天然排除 CJK / 希腊字母等非 ASCII 字母
    s.len() == 1 && s.as_bytes()[0].is_ascii_alphabetic()
}

pub fn abbr_dispose(words: &[&str], time_stamp: Option<&[[i64; 2]]>) -> AbbrDisposeOutput {
    let n = words.len();
    let mut word_lists: Vec<String> = Vec::new();
    let mut abbr_begin: Vec<usize> = Vec::new();
    let mut abbr_end: Vec<usize> = Vec::new();
    let mut last_num: i64 = -1;

    // ---- Pass 1：定位缩写区间（"H I T" 这类）----
    let mut num = 0usize;
    while num < n {
        if (num as i64) <= last_num {
            num += 1;
            continue;
        }
        if is_single_ascii_alpha(words[num])
            && num + 1 < n
            && words[num + 1] == " "
            && num + 2 < n
            && is_single_ascii_alpha(words[num + 2])
        {
            // 发现一个缩写起点
            abbr_begin.push(num);
            num += 2;
            abbr_end.push(num);
            // 往后探测缩写的末尾（继续 " alpha" 模式）
            loop {
                num += 1;
                if num < n && words[num] == " " {
                    num += 1;
                    if num < n && is_single_ascii_alpha(words[num]) {
                        abbr_end.pop();
                        abbr_end.push(num);
                        last_num = num as i64;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        } else {
            num += 1;
        }
    }

    // ---- ts_nums：把每个 word 位置映射到 time_stamp 的下标（仅在需要时间戳时构建）----
    let ts_nums: Vec<usize> = if time_stamp.is_some() {
        let mut v = Vec::with_capacity(n);
        let mut ts_index = 0usize;
        for i in 0..n {
            v.push(ts_index);
            if words[i] != " " {
                ts_index += 1;
            }
        }
        v
    } else {
        Vec::new()
    };

    // ---- Pass 2：构造输出 ----
    let mut ts_lists: Vec<[i64; 2]> = Vec::new();
    let mut begin_ts: i64 = 0;
    last_num = -1;
    let mut num = 0usize;
    while num < n {
        if (num as i64) <= last_num {
            num += 1;
            continue;
        }
        if abbr_begin.contains(&num) {
            if let Some(ts) = time_stamp {
                begin_ts = ts[ts_nums[num]][0];
            }
            word_lists.push(words[num].to_uppercase());
            num += 1;
            while num < n {
                if abbr_end.contains(&num) {
                    word_lists.push(words[num].to_uppercase());
                    last_num = num as i64;
                    break;
                } else if is_single_ascii_alpha(words[num]) {
                    word_lists.push(words[num].to_uppercase());
                }
                num += 1;
            }
            if let Some(ts) = time_stamp {
                let end_ts = ts[ts_nums[num]][1];
                ts_lists.push([begin_ts, end_ts]);
            }
        } else {
            word_lists.push(words[num].to_string());
            if let Some(ts) = time_stamp {
                if words[num] != " " {
                    let b = ts[ts_nums[num]][0];
                    let e = ts[ts_nums[num]][1];
                    ts_lists.push([b, e]);
                }
            }
        }
        num += 1;
    }

    AbbrDisposeOutput {
        words: word_lists,
        time_stamps: time_stamp.map(|_| ts_lists),
    }
}

// =============================================================================
// sentence_postprocess
// =============================================================================

/// `sentence_postprocess` 的返回结果
#[derive(Debug, Clone, Default)]
pub struct SentenceOutput {
    /// 最终拼好的句子
    pub sentence: String,
    /// 去掉空格后的 word 列表
    pub real_word_lists: Vec<String>,
    /// 仅当输入提供了 time_stamp 时存在
    pub time_stamps: Option<Vec<[i64; 2]>>,
}

pub fn sentence_postprocess(
    words: &[&str],
    time_stamp: Option<&[[i64; 2]]>,
) -> SentenceOutput {
    // ---- 1. 清洗 words：丢 `<s>` / `</s>` / `<unk>` ----
    let middle_lists: Vec<&str> = words
        .iter()
        .copied()
        .filter(|w| !matches!(*w, "<s>" | "</s>" | "<unk>"))
        .collect();

    let mut word_lists: Vec<String> = Vec::new();
    let mut ts_lists: Vec<[i64; 2]> = Vec::new();
    let mut word_item = String::new();

    if is_all_chinese(&middle_lists) {
        // ---- 分支 A：全汉字 ----
        for ch in &middle_lists {
            word_lists.push(ch.replace(' ', ""));
        }
        if let Some(ts) = time_stamp {
            ts_lists = ts.to_vec();
        }
    } else if is_all_alpha(&middle_lists) {
        // ---- 分支 B：全 alpha（含 `@@` 拼接的 BPE-style 输出）----
        let mut ts_flag = true;
        let mut begin_ts: i64 = 0;
        for (i, ch) in middle_lists.iter().enumerate() {
            if ts_flag {
                if let Some(ts) = time_stamp {
                    begin_ts = ts[i][0];
                }
            }
            if ch.contains("@@") {
                word_item.push_str(&ch.replace("@@", ""));
                ts_flag = false;
                if let Some(ts) = time_stamp {
                    let _ = ts[i][1]; // 仿 Python：end = time_stamp[i][1]
                }
            } else {
                word_item.push_str(ch);
                word_lists.push(word_item.clone());
                word_lists.push(" ".to_string());
                word_item.clear();
                ts_flag = true;
                if let Some(ts) = time_stamp {
                    let end_ts = ts[i][1];
                    ts_lists.push([begin_ts, end_ts]);
                    begin_ts = end_ts; // 与 Python 一致（虽然是死代码，保留以忠实于原版）
                }
            }
        }
    } else {
        // ---- 分支 C：混合（中英混杂）----
        let mut alpha_blank = false;
        let mut ts_flag = true;
        let mut begin_ts: i64 = 0;
        for (i, ch) in middle_lists.iter().enumerate() {
            if ts_flag {
                if let Some(ts) = time_stamp {
                    begin_ts = ts[i][0];
                }
            }
            if is_all_chinese(&[ch]) {
                if alpha_blank {
                    word_lists.pop();
                }
                word_lists.push(ch.to_string());
                alpha_blank = false;
                ts_flag = true;
                if let Some(ts) = time_stamp {
                    let end_ts = ts[i][1];
                    ts_lists.push([begin_ts, end_ts]);
                    begin_ts = end_ts;
                }
            } else if ch.contains("@@") {
                word_item.clear();
                word_item.push_str(&ch.replace("@@", ""));
                alpha_blank = false;
                ts_flag = false;
                if let Some(ts) = time_stamp {
                    let _ = ts[i][1];
                }
            } else if is_all_alpha(&[ch]) {
                word_item.clear();
                word_item.push_str(ch);
                word_lists.push(word_item.clone());
                word_lists.push(" ".to_string());
                word_item.clear();
                alpha_blank = true;
                ts_flag = true;
                if let Some(ts) = time_stamp {
                    let end_ts = ts[i][1];
                    ts_lists.push([begin_ts, end_ts]);
                    begin_ts = end_ts;
                }
            } else {
                // 与 Python 一致：未知 token 直接 panic
                panic!("invalid character: {ch}");
            }
        }
    }

    // ---- 2. 跑 abbr_dispose，把 "H I" 这类合并 ----
    let words_ref: Vec<&str> = word_lists.iter().map(String::as_str).collect();
    let ts_ref: Option<Vec<[i64; 2]>> = if time_stamp.is_some() {
        Some(ts_lists.clone())
    } else {
        None
    };
    let abbr = abbr_dispose(&words_ref, ts_ref.as_deref());

    // ---- 3. 去掉空格，拼最终句子 ----
    let real_word_lists: Vec<String> = abbr
        .words
        .iter()
        .filter(|w| w.as_str() != " ")
        .cloned()
        .collect();

    let sentence = if time_stamp.is_some() {
        real_word_lists.join(" ")
    } else {
        abbr.words.join("")
    }
    .trim()
    .to_string();

    SentenceOutput {
        sentence,
        real_word_lists,
        time_stamps: abbr.time_stamps,
    }
}

// =============================================================================
// 标签字典（顺序对替换结果有影响：原版 Python dict 的插入顺序）
// =============================================================================

/// (key, value) 元组切片 —— 保留插入顺序，提供 O(n) 顺序遍历 + 顺序敏感替换。
type TaggedEntries = &'static [(&'static str, &'static str)];

fn lookup(entries: TaggedEntries, key: &str) -> Option<&'static str> {
    entries.iter().find(|(k, _)| *k == key).map(|(_, v)| *v)
}

/// emotion 标签 → emoji
const EMO_DICT: TaggedEntries = &[
    ("<|HAPPY|>", "😊"),
    ("<|SAD|>", "😔"),
    ("<|ANGRY|>", "😡"),
    ("<|NEUTRAL|>", ""),
    ("<|FEARFUL|>", "😰"),
    ("<|DISGUSTED|>", "🤢"),
    ("<|SURPRISED|>", "😮"),
];

/// event 标签 → emoji（前置）
const EVENT_DICT: TaggedEntries = &[
    ("<|BGM|>", "🎼"),
    ("<|Speech|>", ""),
    ("<|Applause|>", "👏"),
    ("<|Laughter|>", "😀"),
    ("<|Cry|>", "😭"),
    ("<|Sneeze|>", "🤧"),
    ("<|Breath|>", ""),
    ("<|Cough|>", "🤧"),
];

/// 语言标签 → 统一占位符
const LANG_DICT: TaggedEntries = &[
    ("<|zh|>", "<|lang|>"),
    ("<|en|>", "<|lang|>"),
    ("<|yue|>", "<|lang|>"),
    ("<|ja|>", "<|lang|>"),
    ("<|ko|>", "<|lang|>"),
    ("<|nospeech|>", "<|lang|>"),
];

/// 全部标签字典（rich_transcription_postprocess 用）
const EMOJI_DICT: TaggedEntries = &[
    ("<|nospeech|><|Event_UNK|>", "❓"),
    ("<|zh|>", ""),
    ("<|en|>", ""),
    ("<|yue|>", ""),
    ("<|ja|>", ""),
    ("<|ko|>", ""),
    ("<|nospeech|>", ""),
    ("<|HAPPY|>", "😊"),
    ("<|SAD|>", "😔"),
    ("<|ANGRY|>", "😡"),
    ("<|NEUTRAL|>", ""),
    ("<|BGM|>", "🎼"),
    ("<|Speech|>", ""),
    ("<|Applause|>", "👏"),
    ("<|Laughter|>", "😀"),
    ("<|FEARFUL|>", "😰"),
    ("<|DISGUSTED|>", "🤢"),
    ("<|SURPRISED|>", "😮"),
    ("<|Cry|>", "😭"),
    ("<|EMO_UNKNOWN|>", ""),
    ("<|Sneeze|>", "🤧"),
    ("<|Breath|>", ""),
    ("<|Cough|>", "😷"),
    ("<|Sing|>", ""),
    ("<|Speech_Noise|>", ""),
    ("<|withitn|>", ""),
    ("<|woitn|>", ""),
    ("<|GBG|>", ""),
    ("<|Event_UNK|>", ""),
];

const EMO_SET: &[char] = &['😊', '😔', '😡', '😰', '🤢', '😮'];
const EVENT_SET: &[char] = &['🎼', '👏', '😀', '😭', '🤧', '😷'];

/// 拿到合并后的 (emo_set ∪ event_set)，懒加载到 HashSet 一次
fn emo_event_set() -> &'static std::collections::HashSet<char> {
    static SET: OnceLock<std::collections::HashSet<char>> = OnceLock::new();
    SET.get_or_init(|| EMO_SET.iter().chain(EVENT_SET.iter()).copied().collect())
}

/// 把单个语言片段清洗一遍：剥标签、加 event 头 emoji、加 emo 尾 emoji。
fn format_str_v2(s: &str) -> String {
    let mut sptk_dict = std::collections::HashMap::new();
    let mut s = s.to_string();
    // 顺序遍历 emoji_dict 计数并剥离
    for (sptk, _) in EMOJI_DICT {
        let count = s.matches(sptk).count();
        sptk_dict.insert(*sptk, count);
        if count > 0 {
            s = s.replace(sptk, "");
        }
    }
    // 选数量最多的 emotion
    let mut emo = "<|NEUTRAL|>";
    for (e, _) in EMO_DICT {
        let cnt_e = sptk_dict.get(e).copied().unwrap_or(0);
        let cnt_emo = sptk_dict.get(emo).copied().unwrap_or(0);
        if cnt_e > cnt_emo {
            emo = e;
        }
    }
    // event 拼到头部
    for (e, v) in EVENT_DICT {
        if sptk_dict.get(e).copied().unwrap_or(0) > 0 {
            s = format!("{v}{s}");
        }
    }
    // emo 拼到尾部
    let emo_val = lookup(EMO_DICT, emo).unwrap_or("");
    s.push_str(emo_val);

    // 把 emoji 两侧的空格挤掉
    for emoji in emo_event_set() {
        s = s.replace(&format!(" {emoji}"), &emoji.to_string());
        s = s.replace(&format!("{emoji} "), &emoji.to_string());
    }
    s.trim().to_string()
}

/// 去掉字符串的第一个字符（按 Unicode codepoint 切；event emoji 一般是 4-byte UTF-8 单 codepoint）。
fn strip_first_char(s: &str) -> String {
    let mut chars = s.chars();
    chars.next();
    chars.collect()
}

/// 顶层入口：清洗 SenseVoice 等模型带 emotion/event/lang 标签的原始输出。
pub fn rich_transcription_postprocess(s: &str) -> String {
    fn get_emo(s: &str) -> Option<char> {
        s.chars().last().filter(|c| EMO_SET.contains(c))
    }

    fn get_event(s: &str) -> Option<char> {
        s.chars().next().filter(|c| EVENT_SET.contains(c))
    }

    let mut s = s.replace("<|nospeech|><|Event_UNK|>", "❓");
    for (lang, _) in LANG_DICT {
        s = s.replace(*lang, "<|lang|>");
    }

    let s_list: Vec<String> = s
        .split("<|lang|>")
        .map(|si| format_str_v2(si).trim().to_string())
        .collect();

    if s_list.is_empty() {
        return String::new();
    }

    let mut new_s = format!(" {}", s_list[0]);
    let mut cur_ent_event = get_event(&new_s);

    for si_original in s_list.iter().skip(1) {
        if si_original.is_empty() {
            continue;
        }
        // 同 event 重复：去掉 si 的首字符（event emoji）。Python 原版是在 s_list 上原地改。
        let si_event = get_event(si_original);
        let si: String = if si_event == cur_ent_event && si_event.is_some() {
            strip_first_char(si_original)
        } else {
            si_original.clone()
        };
        cur_ent_event = get_event(&si);
        // 合并重复 emo
        let si_emo = get_emo(&si);
        let new_s_emo = get_emo(&new_s);
        if si_emo.is_some() && si_emo == new_s_emo {
            new_s.pop();
        }
        // 与 Python 一致：`s_list[i].strip().lstrip()` —— trim 掉两端空白
        new_s.push_str(si.trim());
    }

    new_s.replace("The.", " ").trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_chinese_basic() {
        assert!(is_chinese('中'));
        assert!(is_chinese('一'));
        assert!(is_chinese('5'));
        assert!(!is_chinese('a'));
        assert!(!is_chinese('@'));
    }

    #[test]
    fn is_all_chinese_basic() {
        assert!(is_all_chinese(&["中", "文"]));
        assert!(is_all_chinese(&["中1文"]));
        assert!(!is_all_chinese(&["中", "a"]));
        assert!(!is_all_chinese(&["<s>", "中"])); // "<s>" cleaned to "" → fails
        assert!(!is_all_chinese(&[]));
    }

    #[test]
    fn is_all_alpha_basic() {
        assert!(is_all_alpha(&["a", "b", "c"]));
        assert!(is_all_alpha(&["don't"]));
        assert!(!is_all_alpha(&["a", "中"]));
        assert!(!is_all_alpha(&[]));
    }

    #[test]
    fn abbr_dispose_basic() {
        // "H I" → ["H", "I"]（去掉中间的 space token；本函数不做 join）
        let r = abbr_dispose(&["H", " ", "I"], None);
        assert_eq!(r.words, vec!["H".to_string(), "I".to_string()]);
        assert!(r.time_stamps.is_none());

        // "H I T" → ["H", "I", "T"]
        let r = abbr_dispose(&["H", " ", "I", " ", "T"], None);
        assert_eq!(r.words, vec!["H".to_string(), "I".to_string(), "T".to_string()]);

        // "H I foo" → ["H", "I", " ", "foo"]（"foo" 不在 abbr 内，原样保留；abbr
        // 与非 abbr 之间的 space 也保留）
        let r = abbr_dispose(&["H", " ", "I", " ", "foo"], None);
        assert_eq!(
            r.words,
            vec![
                "H".to_string(),
                "I".to_string(),
                " ".to_string(),
                "foo".to_string()
            ]
        );

        // 缩写带时间戳：
//   - begin 取第一个 alpha 的 ts.begin（ts_nums[0]=0 → ts[0][0]=0）
//   - end   取最后一个 alpha 的 ts.end（ts_nums[2]=1，因为 space 共享下一个
//           alpha 的 ts_index，所以用 ts[1][1]=200，不是 ts[2][1]=400）
        let ts = [[0, 100], [100, 200], [300, 400]];
        let r = abbr_dispose(&["H", " ", "I"], Some(&ts));
        assert_eq!(r.words, vec!["H".to_string(), "I".to_string()]);
        assert_eq!(r.time_stamps, Some(vec![[0, 200]]));
    }

    #[test]
    fn sentence_postprocess_all_chinese() {
        let out = sentence_postprocess(&["你", "好", "<unk>"], None);
        assert_eq!(out.sentence, "你好");
        assert_eq!(out.real_word_lists, vec!["你".to_string(), "好".to_string()]);
    }

    #[test]
    fn rich_transcription_strips_lang_and_emotion() {
        // 只有 HAPPY（没有 NEUTRAL），HAPPY 胜出 → 保留 😊
        let s = "<|zh|><|HAPPY|>你好";
        let out = rich_transcription_postprocess(s);
        assert!(!out.contains("<|"), "out={out:?}");
        assert!(!out.contains("zh"), "out={out:?}");
        assert!(out.contains("你好"), "out={out:?}");
        assert!(out.contains('😊'), "out={out:?}");

        // HAPPY + NEUTRAL 各 1：按 Python 规则 NEUTRAL 胜出（迭代到 HAPPY 时 1 > 1 不成立）
        let s = "<|zh|><|HAPPY|>你好<|NEUTRAL|>";
        let out = rich_transcription_postprocess(s);
        assert!(!out.contains("<|"), "out={out:?}");
        assert!(!out.contains('😊'), "out={out:?}");

        // 双语切换
        let s = "<|zh|><|HAPPY|>你好<|en|>hello<|NEUTRAL|>";
        let out = rich_transcription_postprocess(s);
        assert!(!out.contains("<|"), "out={out:?}");
        assert!(out.contains("你好"), "out={out:?}");
        assert!(out.contains("hello"), "out={out:?}");
        assert!(out.contains('😊'), "out={out:?}");
    }

    #[test]
    fn parse_asr_text_rich_emotion() {
        let r = parse_asr_text("今天好累😊");
        assert_eq!(r.text, "今天好累");
        assert_eq!(r.emotion.as_deref(), Some("开心"));
        assert!(r.event.is_empty());
    }

    #[test]
    fn parse_asr_text_rich_emotion_at_end() {
        let r = parse_asr_text("今天好累😔");
        assert_eq!(r.text, "今天好累");
        assert_eq!(r.emotion.as_deref(), Some("悲伤"));
    }

    #[test]
    fn parse_asr_text_rich_emotion_and_event() {
        let r = parse_asr_text("今🎼天😊好累");
        assert_eq!(r.text, "今天好累");
        assert_eq!(r.emotion.as_deref(), Some("开心"));
        assert_eq!(r.event, vec!["背景音乐"]);
    }

    #[test]
    fn parse_asr_text_event_only() {
        let r = parse_asr_text("🎼music");
        assert_eq!(r.text, "music");
        assert!(r.emotion.is_none());
        assert_eq!(r.event, vec!["背景音乐"]);
    }

    #[test]
    fn parse_asr_text_unknown_tags_follow_rich_postprocess() {
        // parse_asr_text 不再负责处理原始 ASR 标签。
        for tag in ["unk", "UNK", "EMO_UNKNOWN", "Speech"] {
            let r = parse_asr_text(&format!("text<|{tag}|>"));
            assert_eq!(r.text, format!("text<|{tag}|>"), "tag={tag}");
            assert!(r.emotion.is_none(), "tag={tag}");
        }
    }

    #[test]
    fn parse_asr_text_case_sensitive_like_rich_postprocess() {
        let r = parse_asr_text("<|happy|>今天");
        assert_eq!(r.text, "<|happy|>今天");
        assert!(r.emotion.is_none());
    }

    #[test]
    fn parse_asr_text_no_tags() {
        let r = parse_asr_text("纯文本没有标签");
        assert_eq!(r.text, "纯文本没有标签");
        assert!(r.emotion.is_none());
        assert!(r.event.is_empty());
    }

    #[test]
    fn parse_asr_text_unclosed_tag_follows_rich_postprocess() {
        let r = parse_asr_text("a<|b");
        assert_eq!(r.text, "a<|b");
        assert!(r.emotion.is_none());
    }

    #[test]
    fn parse_asr_text_only_tags_yields_empty() {
        let r = parse_asr_text("");
        assert_eq!(r.text, "");
        assert!(r.emotion.is_none());
        assert!(r.event.is_empty());
    }

    #[test]
    fn parse_asr_text_emotion_tie_uses_last_occurrence() {
        // 同 utterance 出 2 个 emotion 且次数相同：最后出现者胜出
        let r = parse_asr_text("😊text😔");
        assert_eq!(r.emotion.as_deref(), Some("悲伤"));
    }

    #[test]
    fn parse_asr_text_parses_rich_output_emojis() {
        let r = parse_asr_text("😊🎼你好");
        assert_eq!(r.text, "你好");
        assert_eq!(r.emotion.as_deref(), Some("开心"));
        assert_eq!(r.event, vec!["背景音乐"]);
    }

    #[test]
    fn parse_asr_text_removes_all_rich_output_emojis() {
        let r = parse_asr_text("😊🎼你好👏呀");
        assert_eq!(r.text, "你好呀");
        assert_eq!(r.emotion.as_deref(), Some("开心"));
        assert_eq!(r.event, vec!["背景音乐", "掌声"]);
    }

    #[test]
    fn parse_asr_text_maps_unknown_event_marker_to_empty_event() {
        let r = parse_asr_text("❓");
        assert_eq!(r.text, "");
        assert_eq!(r.event, vec![""]);
        assert!(r.emotion.is_none());
    }

    #[test]
    fn parse_asr_text_emotion_majority_wins() {
        let r = parse_asr_text("😊a😔b😊");
        assert_eq!(r.emotion.as_deref(), Some("开心"));
    }

    #[test]
    fn parse_asr_text_events_are_deduplicated_in_first_seen_order() {
        let r = parse_asr_text("🎼a👏b🎼c👏");
        assert_eq!(r.event, vec!["背景音乐", "掌声"]);
    }

    #[test]
    fn format_asr_hint_includes_emotion_and_events() {
        let hint = format_asr_hint(Some("开心"), &["背景音乐".into(), "掌声".into()]);
        assert_eq!(hint.as_deref(), Some("情绪：开心；事件：背景音乐、掌声"));
    }
}
