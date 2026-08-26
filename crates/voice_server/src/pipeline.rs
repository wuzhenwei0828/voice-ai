//! 共享 LLM→TTS 流水线
//!
//! 把 LLM 流式输出 → 切句 → TTS 流式合成 → 句间 crossfade → 统一 seq 编号 → emit
//! 这一整条链路抽到这里，被 `admin_api.rs` 的 `/admin/llm_tts` / `/admin/asr_llm_tts`
//! 端点和 `session.rs` 的 WS pipeline 共用。
//!
//! 之前这两条调用链走的是 `admin_api::llm_tts_items` —— `session.rs` 反向依赖
//! `admin_api.rs` 形成反向边。本模块翻转方向：`admin_api.rs` 和 `session.rs` 都
//! 依赖 `crate::pipeline::llm_tts_items`，方向变成正。
//!
//! 同时承载 [`SentenceCrossfader`] + [`crossfade`] 这两个工具：`llm_tts_items` 用，
//! `admin_api::build_tts_sentence_stream`（仅 TTS、无 LLM 的版本）也用。放在一起避免
//! `pipeline → admin_api` 反向引用。

use async_stream::stream;
use futures_util::{Stream, StreamExt};
use tracing::{debug, info, warn};

use crate::client::{ArcLlm, ArcTts};
use crate::session::next_sentence_end;

/// `llm_tts_items()` 的输出事件，由各消费方映射成自己的 wire 格式
/// （NDJSON 行 / WS VoicePayload）。`Failed` 是终端事件（产出后流即结束）。
/// `pub`：session.rs 的 WS pipeline 也要消费。
pub enum LlmTtsItem {
    /// LLM 文本 delta（/admin/llm_tts 不透出，/admin/asr_llm_tts 与 WS 侧透出）
    Llm { delta: String, is_final: bool },
    /// TTS 音频 chunk（audio 为 base64）；最后一条 audio 为空、is_last=true，是结束标记
    Tts { seq: u32, audio: String, is_last: bool },
    /// 管线失败
    Failed { error: String, code: u16 },
}

// ====== 句间 crossfade 工具 ======

/// 把上一句末尾 FADE_BYTES 与下一句开头 FADE_BYTES 线性混合可消除。
const FADE_BYTES: usize = 320;

/// 句间 crossfade 状态机。按句使用：begin_sentence → feed* → end_sentence。
/// 当前句结尾的 FADE_BYTES 先扣着不发（tail），等下一句开头到了做混合后再发。
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

// ====== LLM→TTS 流水线 ======

/// 把 llm + sentence-split + tts 三个阶段串成一条事件流，
/// 供 /admin/llm_tts、/admin/asr_llm_tts 与 session.rs 的 WS pipeline 共用
/// （后两者额外透出 Llm 文本事件）。
/// 接受 Arc 而非 &Arc 是因为 actix-web 的 streaming() 要求 `Stream + 'static`，Arc 便宜 clone。
///
/// `emotion_hint`：从 ASR 文本里解析出来的说话人情绪（参见 `session::parse_asr_emotion_tags`），
/// 作为 system message 的参考传给 LLM。`None` 表示无情绪（如 `/admin/llm_tts` 直接调用的场景）。
///
/// `sample_rate_override`：端侧 SessionStart 上报的 TTS 输出采样率。
///   - `Some(n)` —— 覆盖 `TtsConfig.sample_rate`
///   - `None` —— 走配置兜底
///   - /admin/llm_tts 与 /admin/asr_llm_tts 没有 SessionStart，传 `None`。
///
/// `voice_override`：端侧（前端的 voice 下拉）选中的音色短名（如 `"alex"`）。
///   - `Some("alex")` —— 覆盖 `TtsConfig.voice`
///   - `None` —— 走配置兜底
///   - /admin/llm_tts 与 /admin/asr_llm_tts 没有下拉时传 `None`。
pub fn llm_tts_items(
    prompt: String,
    emotion_hint: Option<String>,
    sid: String,
    llm: ArcLlm,
    tts: ArcTts,
    sample_rate_override: Option<u32>,
    voice_override: Option<String>,
) -> impl Stream<Item = LlmTtsItem> + 'static {
    stream! {
        // 阶段 1: 拉 LLM 流
        let mut llm_stream = match llm.chat(&sid, &prompt, emotion_hint.as_deref()).await {
            Ok(s) => s,
            Err(e) => {
                warn!(target: "voice_server.pipeline", stage = "llm_call", session_id = %sid, "LLM 调用失败: {}", e);
                yield LlmTtsItem::Failed { error: format!("llm error: {}", e), code: 1002 };
                return;
            }
        };

        let mut sentence_buf = String::new();
        let mut global_seq: u32 = 0;
        let mut fader = SentenceCrossfader::default();

        // 把一段 PCM 包成 Tts 事件（空则跳过）
        macro_rules! emit_pcm {
            ($pcm:expr) => {{
                let pcm: Vec<u8> = $pcm;
                if !pcm.is_empty() {
                    global_seq += 1;
                    yield LlmTtsItem::Tts {
                        seq: global_seq,
                        audio: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &pcm),
                        is_last: false,
                    };
                }
            }};
        }

        // 阶段 2: 流式消费 LLM delta，切句后调 TTS
        while let Some(item) = llm_stream.next().await {
            let evt = match item {
                Ok(e) => e,
                Err(e) => {
                    warn!(target: "voice_server.pipeline", stage = "llm_stream", session_id = %sid, "LLM 流错误: {}", e);
                    yield LlmTtsItem::Failed { error: format!("llm stream: {}", e), code: 1003 };
                    return;
                }
            };
            sentence_buf.push_str(&evt.delta);
            yield LlmTtsItem::Llm { delta: evt.delta.clone(), is_final: evt.is_final };

            // 切出所有完整句
            while let Some(end) = next_sentence_end(&sentence_buf) {
                let sent: String = sentence_buf[..end].to_string();
                sentence_buf = sentence_buf[end..].to_string();
                info!(target: "voice_server.pipeline", stage = "tts_call", session_id = %sid, sentence = %sent, "切出句子送 TTS");

                let mut tts_stream = match tts.synthesize(&sid, &sent, sample_rate_override, voice_override.clone()).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(target: "voice_server.pipeline", stage = "tts_call", session_id = %sid, "TTS 调用失败: {}", e);
                        yield LlmTtsItem::Failed { error: format!("tts error: {}", e), code: 1004 };
                        return;
                    }
                };

                // 阶段 3: 转发 TTS chunk（过 crossfade，重编号 seq）
                fader.begin_sentence();
                while let Some(tts_item) = tts_stream.next().await {
                    let t = match tts_item {
                        Ok(t) => t,
                        Err(e) => {
                            warn!(target: "voice_server.pipeline", stage = "tts_stream", session_id = %sid, "TTS 流错误: {}", e);
                            yield LlmTtsItem::Failed { error: format!("tts stream: {}", e), code: 1005 };
                            return;
                        }
                    };
                    emit_pcm!(fader.feed(&t.data));
                    if t.is_last {
                        // 单句 TTS 已经结束，准备下一句
                        break;
                    }
                }
                emit_pcm!(fader.end_sentence());
            }

            if evt.is_final {
                break;
            }
        }

        // 收尾：剩余 sentence_buf
        let tail = sentence_buf.trim().to_string();
        if !tail.is_empty() {
            info!(target: "voice_server.pipeline", stage = "tts_tail", session_id = %sid, sentence = %tail, "LLM 末尾残余句子送 TTS");
            if let Ok(mut tts_stream) = tts.synthesize(&sid, &tail, sample_rate_override, voice_override.clone()).await {
                fader.begin_sentence();
                while let Some(tts_item) = tts_stream.next().await {
                    if let Ok(t) = tts_item {
                        emit_pcm!(fader.feed(&t.data));
                        if t.is_last { break; }
                    } else {
                        break;
                    }
                }
                emit_pcm!(fader.end_sentence());
            }
        }
        // 最后一句扣留的句尾不再需要淡化，原样下发
        emit_pcm!(fader.finish());

        // 结束标记：上面各 chunk 下发时无法预知自己是不是最后一条，
        // 统一在流末尾补一条 audio 为空的 {is_last:true}，前端据此判定结束
        global_seq += 1;
        yield LlmTtsItem::Tts { seq: global_seq, audio: String::new(), is_last: true };

        debug!(target: "voice_server.pipeline", session_id = %sid, "LLM→TTS 管线全部完成");
    }
}
