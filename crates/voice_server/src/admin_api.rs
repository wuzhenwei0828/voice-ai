//! 单能力验证 HTTP 接口
//!
//! 5 个独立 REST endpoint，方便 curl / Postman / 前端 tab 单点验证：
//!   POST /admin/asr         raw PCM body → NDJSON {text, is_final}
//!   POST /admin/llm         JSON {prompt}  → NDJSON {delta, is_final}
//!   POST /admin/tts         JSON {text}    → NDJSON {seq, audio(base64), is_last}
//!   POST /admin/llm_tts     JSON {text}    → NDJSON {seq, audio(base64), is_last}（复用切句逻辑）
//!   POST /admin/asr_llm_tts raw PCM body   → NDJSON 分阶段事件（前端按 stage 分发）：
//!       {"stage":"asr","text":..,"is_final":..}
//!       {"stage":"llm","delta":..,"is_final":..}
//!       {"stage":"tts","seq":..,"audio":base64,"is_last":..}（末尾补 audio 空、is_last:true 结束标记）
//!
//! 设计要点：
//!   - 复用现有 AsrClient / LlmClient / TtsClient trait，不重新实现
//!   - 切句复用 session::next_sentence_end（已从 fn 提升为 pub）
//!   - LLM→TTS 管线抽成 llm_tts_items()，/admin/llm_tts、/admin/asr_llm_tts 与
//!     session.rs 的 WS pipeline 共用（句间 crossfade / 全局 seq / 结束标记）
//!   - 流中途出错：插一行 {error, code} 然后断流（HTTP 200 已发，不能改 status）
//!     错误码约定：1001 asr / 1002 llm 调用 / 1003 llm 流 / 1004 tts 调用 / 1005 tts 流

use actix_web::web::{Bytes, Data, Json};
use actix_web::HttpResponse;
use async_stream::{stream, try_stream};
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

use crate::client::{asr::wrap_pcm_as_wav, ArcAsr, ArcLlm, ArcTts, ClientError};
use crate::session::{next_sentence_end, AsrEvent, LlmEvent};

// ====== 请求 / 响应结构 ======

#[derive(Debug, Deserialize)]
pub struct LlmReq {
    pub prompt: String,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TtsReq {
    pub text: String,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AsrLine {
    pub text: String,
    pub is_final: bool,
}

#[derive(Debug, Serialize)]
pub struct LlmLine {
    pub delta: String,
    pub is_final: bool,
}

#[derive(Debug, Serialize)]
pub struct TtsLine {
    pub seq: u32,
    /// base64 编码后的音频字节
    pub audio: String,
    pub is_last: bool,
}

#[derive(Debug, Serialize)]
pub struct ErrorLine {
    pub error: String,
    pub code: u16,
}

// ====== /admin/asr_llm_tts 分阶段事件行 ======
// 用扁平的 stage 字段（而非 serde tag），前端按 evt.stage 分发即可

#[derive(Debug, Serialize)]
struct StageAsrLine {
    stage: &'static str,
    text: String,
    is_final: bool,
}

#[derive(Debug, Serialize)]
struct StageLlmLine {
    stage: &'static str,
    delta: String,
    is_final: bool,
}

#[derive(Debug, Serialize)]
struct StageTtsLine {
    stage: &'static str,
    seq: u32,
    audio: String,
    is_last: bool,
}

// ====== LLM→TTS 共用管线事件 ======

/// `llm_tts_items()` 的输出事件，由各消费方映射成自己的 wire 格式
/// （NDJSON 行 / WS VoicePayload）。`Failed` 是终端事件（产出后流即结束）。
/// pub：session.rs 的 WS pipeline 也要消费。
pub enum LlmTtsItem {
    /// LLM 文本 delta（/admin/llm_tts 不透出，/admin/asr_llm_tts 与 WS 侧透出）
    Llm { delta: String, is_final: bool },
    /// TTS 音频 chunk（audio 为 base64）；最后一条 audio 为空、is_last=true，是结束标记
    Tts { seq: u32, audio: String, is_last: bool },
    /// 管线失败
    Failed { error: String, code: u16 },
}

// ====== 音频预处理：strip WAV 头 / 包成 16kHz mono s16le WAV ======

/// 在 WAV 里找 data chunk 的偏移和大小（兼容非标准 fmt 长度）。
fn find_wav_data(bytes: &[u8]) -> Option<(usize, usize)> {
    if bytes.len() < 12 || &bytes[0..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
        return None;
    }
    let mut pos = 12usize;
    while pos + 8 <= bytes.len() {
        let id = &bytes[pos..pos + 4];
        let size = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().ok()?) as usize;
        if id == b"data" {
            let avail = bytes.len() - (pos + 8);
            return Some((pos + 8, size.min(avail)));
        }
        let advance = 8 + size + (size & 1);
        if advance == 0 { break; }
        pos += advance;
    }
    None
}

/// 把上传音频统一转成"16kHz mono s16le WAV 字节流"给 ASR。
/// - 如果上传的就是 WAV：找 data chunk 取出 PCM（要求原文件已是 16kHz mono s16le）
/// - 否则：按裸 PCM 处理（前端约定 16kHz mono s16le）
/// 返回 None 表示输入不是合法 WAV / 长度不够；前端在 UI 上把这种情况当错误显示。
fn prepare_audio_for_asr(bytes: Vec<u8>) -> Option<Vec<u8>> {
    let pcm: &[u8] = if let Some((off, size)) = find_wav_data(&bytes) {
        &bytes[off..off + size]
    } else {
        &bytes[..]
    };
    if pcm.is_empty() { return None; }
    Some(wrap_pcm_as_wav(pcm, 16000, 1))
}

// ====== NDJSON 流序列化 helper ======

/// 把 `Stream<Item = Result<T, ClientError>>` 序列化成 NDJSON 字节流。
/// 出错时插入 `{error, code}` 一行 + break 流。
fn ndjson_stream<T, E, S>(inner: S) -> impl Stream<Item = Result<Bytes, actix_web::Error>>
where
    T: Serialize,
    E: Display,
    S: Stream<Item = Result<T, E>> + 'static,
{
    try_stream! {
        tokio::pin!(inner);
        while let Some(item) = inner.next().await {
            match item {
                Ok(v) => {
                    let mut line = serde_json::to_vec(&v)
                        .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
                    line.push(b'\n');
                    yield Bytes::from(line);
                }
                Err(e) => {
                    let err_line = ErrorLine { error: e.to_string(), code: 500 };
                    let mut line = serde_json::to_vec(&err_line)
                        .map_err(|e2| actix_web::error::ErrorInternalServerError(e2.to_string()))?;
                    line.push(b'\n');
                    yield Bytes::from(line);
                    break;
                }
            }
        }
    }
}

// ====== 句间 crossfade ======

/// 句间拼接淡化区长度：10ms @ 16kHz s16le mono = 160 采样 = 320 字节。
/// 每句 TTS 是独立合成，波形在拼接处瞬跳会产生"咔哒"声；
/// 把上一句末尾 FADE_BYTES 与下一句开头 FADE_BYTES 线性混合可消除。
const FADE_BYTES: usize = 320;

/// 句间 crossfade 状态机。按句使用：begin_sentence → feed* → end_sentence。
/// 当前句结尾的 FADE_BYTES 先扣着不发（tail），等下一句开头到了做混合后再发。
#[derive(Default)]
struct SentenceCrossfader {
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
    fn begin_sentence(&mut self) {
        self.head.clear();
        self.hold.clear();
        // 上一句没有遗留 tail（如第一句）时，本句开头无需混合
        self.head_done = self.tail.is_empty();
    }

    /// 喂入当前句的一段 PCM，返回可立即下发的字节
    fn feed(&mut self, mut bytes: &[u8]) -> Vec<u8> {
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
    fn end_sentence(&mut self) -> Vec<u8> {
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
    fn finish(&mut self) -> Vec<u8> {
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

/// 把一段 PCM 包成 NDJSON 行（不附加 is_last）。空 PCM 调用方应跳过。
fn serialize_pcm_line(seq: u32, pcm: &[u8]) -> Result<Bytes, actix_web::Error> {
    let line = TtsLine {
        seq,
        audio: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, pcm),
        is_last: false,
    };
    let mut json = serde_json::to_vec(&line)
        .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
    json.push(b'\n');
    Ok(Bytes::from(json))
}

/// 序列化任意一行 NDJSON（含结尾换行）
fn ndjson_line<T: Serialize>(v: &T) -> Result<Bytes, actix_web::Error> {
    let mut json = serde_json::to_vec(v)
        .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
    json.push(b'\n');
    Ok(Bytes::from(json))
}

// ====== Session id 生成 ======

fn gen_sid(prefix: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("test-{}-{}", prefix, ts)
}

// ====== Handlers ======

/// POST /admin/asr
/// body: 整段音频字节（前端直接转发，服务端统一包成 16kHz mono s16le WAV 喂给 ASR）
///   - 输入是 WAV：自动 strip header，取 data chunk 的 PCM 包回标准 WAV 头
///   - 输入是裸 PCM（约定 s16le 16kHz mono）：直接包 WAV 头
/// response: NDJSON `{"text":"...","is_final":false|true}`
pub async fn asr(
    body: Bytes,
    asr: Data<ArcAsr>,
) -> Result<HttpResponse, actix_web::Error> {
    let sid = gen_sid("asr");
    let bytes_len = body.len();
    info!(target: "voice_server.admin_api", endpoint = "asr", session_id = %sid, bytes = bytes_len, "/admin/asr 收到请求");

    let prepared = match prepare_audio_for_asr(body.to_vec()) {
        Some(b) => b,
        None => {
            return Ok(HttpResponse::BadRequest()
                .content_type("text/plain; charset=utf-8")
                .body("audio empty or not a valid wav/pcm"));
        }
    };

    let stream = asr
        .recognize(&sid, None, prepared)
        .await
        .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;

    // AsrEvent -> AsrLine
    let line_stream = stream.map(|res| res.map(|e: AsrEvent| AsrLine {
        text: e.text,
        is_final: e.is_final,
    }));

    Ok(HttpResponse::Ok()
        .content_type("application/x-ndjson")
        .streaming(ndjson_stream::<AsrLine, ClientError, _>(line_stream)))
}

/// POST /admin/llm
/// body: `{"prompt":"...","session_id":"可选"}`
/// response: NDJSON `{"delta":"...","is_final":false|true}`
pub async fn llm(
    req: Json<LlmReq>,
    llm: Data<ArcLlm>,
) -> Result<HttpResponse, actix_web::Error> {
    let sid = req.session_id.clone().unwrap_or_else(|| gen_sid("llm"));
    info!(target: "voice_server.admin_api", endpoint = "llm", session_id = %sid, prompt_len = req.prompt.chars().count(), "/admin/llm 收到请求");

    let stream = llm
        .chat(&sid, &req.prompt)
        .await
        .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;

    let line_stream = stream.map(|res| res.map(|e: LlmEvent| LlmLine {
        delta: e.delta,
        is_final: e.is_final,
    }));

    Ok(HttpResponse::Ok()
        .content_type("application/x-ndjson")
        .streaming(ndjson_stream::<LlmLine, ClientError, _>(line_stream)))
}

/// POST /admin/tts
/// body: `{"text":"...","session_id":"可选"}`
/// 内部：用 next_sentence_end 按句子切分，逐句调 tts.synthesize，
///       句间过 crossfade 消除波形拼接尖刺。
/// response: NDJSON `{"seq":N,"audio":"<base64>","is_last":false|true}`
///           最后补一条 `{"seq":N+1,"audio":"","is_last":true}` 作为结束标记
pub async fn tts(
    req: Json<TtsReq>,
    tts: Data<ArcTts>,
) -> Result<HttpResponse, actix_web::Error> {
    let sid = req.session_id.clone().unwrap_or_else(|| gen_sid("tts"));
    info!(target: "voice_server.admin_api", endpoint = "tts", session_id = %sid, text_len = req.text.chars().count(), "/admin/tts 收到请求");

    let text = req.text.clone();
    let sid_inner = sid.clone();

    Ok(HttpResponse::Ok()
        .content_type("application/x-ndjson")
        .streaming(build_tts_sentence_stream(text, sid_inner, tts.get_ref().clone())))
}

/// 把一段长文本按 next_sentence_end 切句，逐句调 TTS，过 SentenceCrossfader 后下发。
/// 与 build_llm_tts_stream 结构对称，只是没有 LLM 阶段。
fn build_tts_sentence_stream(
    text: String,
    sid: String,
    tts: ArcTts,
) -> impl Stream<Item = Result<Bytes, actix_web::Error>> + 'static {
    try_stream! {
        let mut buf = text;
        let mut global_seq: u32 = 0;
        let mut fader = SentenceCrossfader::default();

        // 阶段 1：循环切句 → 逐句 TTS → 过 fader
        while let Some(end) = next_sentence_end(&buf) {
            let sent: String = buf[..end].to_string();
            buf = buf[end..].to_string();
            info!(target: "voice_server.admin_api", endpoint = "tts", session_id = %sid, sentence = %sent, "切出句子送 TTS");

            let mut stream = match tts.synthesize(&sid, &sent).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(target: "voice_server.admin_api", endpoint = "tts", session_id = %sid, "TTS 调用失败: {}", e);
                    let err_line = ErrorLine { error: format!("tts error: {}", e), code: 1102 };
                    let mut line = serde_json::to_vec(&err_line)
                        .map_err(|e2| actix_web::error::ErrorInternalServerError(e2.to_string()))?;
                    line.push(b'\n');
                    yield Bytes::from(line);
                    return;
                }
            };

            fader.begin_sentence();
            while let Some(item) = stream.next().await {
                let e = match item {
                    Ok(e) => e,
                    Err(e) => {
                        warn!(target: "voice_server.admin_api", endpoint = "tts", session_id = %sid, "TTS 流错误: {}", e);
                        let err_line = ErrorLine { error: format!("tts stream: {}", e), code: 1103 };
                        let mut line = serde_json::to_vec(&err_line)
                            .map_err(|e2| actix_web::error::ErrorInternalServerError(e2.to_string()))?;
                        line.push(b'\n');
                        yield Bytes::from(line);
                        return;
                    }
                };
                let pcm = fader.feed(&e.data);
                if !pcm.is_empty() {
                    global_seq += 1;
                    yield serialize_pcm_line(global_seq, &pcm)?;
                }
                if e.is_last { break; }
            }
            let pcm = fader.end_sentence();
            if !pcm.is_empty() {
                global_seq += 1;
                yield serialize_pcm_line(global_seq, &pcm)?;
            }
        }

        // 阶段 2：残余（末尾没有句末标点的部分）送一次 TTS
        let tail = buf.trim().to_string();
        if !tail.is_empty() {
            info!(target: "voice_server.admin_api", endpoint = "tts", session_id = %sid, sentence = %tail, "TTS 末尾残余句子送 TTS");
            if let Ok(mut stream) = tts.synthesize(&sid, &tail).await {
                fader.begin_sentence();
                while let Some(item) = stream.next().await {
                    if let Ok(e) = item {
                        let pcm = fader.feed(&e.data);
                        if !pcm.is_empty() {
                            global_seq += 1;
                            yield serialize_pcm_line(global_seq, &pcm)?;
                        }
                        if e.is_last { break; }
                    } else {
                        break;
                    }
                }
                let pcm = fader.end_sentence();
                if !pcm.is_empty() {
                    global_seq += 1;
                    yield serialize_pcm_line(global_seq, &pcm)?;
                }
            }
        }

        // 阶段 3：最后一句扣留的句尾不再需要淡化，原样下发
        let pcm = fader.finish();
        if !pcm.is_empty() {
            global_seq += 1;
            yield serialize_pcm_line(global_seq, &pcm)?;
        }

        // 结束标记
        global_seq += 1;
        let line = TtsLine { seq: global_seq, audio: String::new(), is_last: true };
        let mut json = serde_json::to_vec(&line)
            .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
        json.push(b'\n');
        yield Bytes::from(json);

        debug!(target: "voice_server.admin_api", endpoint = "tts", session_id = %sid, "/admin/tts 全部完成");
    }
}

/// POST /admin/llm_tts
/// body: `{"text":"...","session_id":"可选"}`
/// 内部：LLM 流式 → 切句 → 逐句 TTS 流式 → 拼接成统一 seq
/// response: NDJSON `{"seq":N,"audio":"<base64>","is_last":false|true}`
///           最后补一条 `{"seq":N+1,"audio":"","is_last":true}` 作为结束标记
/// （LLM 文本 delta 不透出、服务端日志可见；/admin/asr_llm_tts 会透出）
pub async fn llm_tts(
    req: Json<TtsReq>,
    llm: Data<ArcLlm>,
    tts: Data<ArcTts>,
) -> Result<HttpResponse, actix_web::Error> {
    let sid = req.session_id.clone().unwrap_or_else(|| gen_sid("llm_tts"));
    info!(target: "voice_server.admin_api", endpoint = "llm_tts", session_id = %sid, text_len = req.text.chars().count(), "/admin/llm_tts 收到请求");

    let prompt = req.text.clone();
    let sid_inner = sid.clone();

    let items = llm_tts_items(prompt, sid_inner, llm.get_ref().clone(), tts.get_ref().clone());

    Ok(HttpResponse::Ok()
        .content_type("application/x-ndjson")
        .streaming(llm_tts_lines(items)))
}

/// 把 LlmTtsItem 流映射成 /admin/llm_tts 的 wire 格式（Llm 事件不透出）。
/// 抽成独立函数（而不是 let binding）是为了让 try_stream! 的 error type 能从返回类型推断。
fn llm_tts_lines(
    items: impl Stream<Item = LlmTtsItem> + 'static,
) -> impl Stream<Item = Result<Bytes, actix_web::Error>> + 'static {
    try_stream! {
        let mut items = Box::pin(items);
        while let Some(item) = items.next().await {
            match item {
                LlmTtsItem::Llm { .. } => {} // 现有 wire 格式不含 LLM 文本行
                LlmTtsItem::Tts { seq, audio, is_last } => {
                    yield ndjson_line(&TtsLine { seq, audio, is_last })?;
                }
                LlmTtsItem::Failed { error, code } => {
                    yield ndjson_line(&ErrorLine { error, code })?;
                }
            }
        }
    }
}

/// 把 llm + sentence-split + tts 三个阶段串成一条事件流，
/// 供 /admin/llm_tts、/admin/asr_llm_tts 与 session.rs 的 WS pipeline 共用
/// （后两者额外透出 Llm 文本事件）。
/// 接受 Arc 而非 &Arc 是因为 actix-web 的 streaming() 要求 `Stream + 'static`，Arc 便宜 clone。
pub fn llm_tts_items(
    prompt: String,
    sid: String,
    llm: ArcLlm,
    tts: ArcTts,
) -> impl Stream<Item = LlmTtsItem> + 'static {
    stream! {
        // 阶段 1: 拉 LLM 流
        let mut llm_stream = match llm.chat(&sid, &prompt).await {
            Ok(s) => s,
            Err(e) => {
                warn!(target: "voice_server.admin_api", endpoint = "llm_tts", session_id = %sid, "LLM 调用失败: {}", e);
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
                    warn!(target: "voice_server.admin_api", endpoint = "llm_tts", session_id = %sid, "LLM 流错误: {}", e);
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
                info!(target: "voice_server.admin_api", endpoint = "llm_tts", session_id = %sid, sentence = %sent, "切出句子送 TTS");

                let mut tts_stream = match tts.synthesize(&sid, &sent).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(target: "voice_server.admin_api", endpoint = "llm_tts", session_id = %sid, "TTS 调用失败: {}", e);
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
                            warn!(target: "voice_server.admin_api", endpoint = "llm_tts", session_id = %sid, "TTS 流错误: {}", e);
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
            info!(target: "voice_server.admin_api", endpoint = "llm_tts", session_id = %sid, sentence = %tail, "LLM 末尾残余句子送 TTS");
            if let Ok(mut tts_stream) = tts.synthesize(&sid, &tail).await {
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

        debug!(target: "voice_server.admin_api", endpoint = "llm_tts", session_id = %sid, "LLM→TTS 管线全部完成");
    }
}

/// POST /admin/asr_llm_tts
/// body: 整段音频字节（前端直接转发，服务端统一包成 16kHz mono s16le WAV 喂给 ASR）
/// 内部：ASR → LLM 流式 → 切句 → 逐句 TTS 流式，全链路串成一条 NDJSON 流
/// response: NDJSON 分阶段事件（stage 字段区分）：
///   `{"stage":"asr","text":..,"is_final":..}`
///   `{"stage":"llm","delta":..,"is_final":..}`
///   `{"stage":"tts","seq":..,"audio":base64,"is_last":..}`（末尾 audio 空、is_last:true 结束标记）
///   `{"error":..,"code":1001~1005}`
pub async fn asr_llm_tts(
    body: Bytes,
    asr: Data<ArcAsr>,
    llm: Data<ArcLlm>,
    tts: Data<ArcTts>,
) -> Result<HttpResponse, actix_web::Error> {
    let sid = gen_sid("asr_llm_tts");
    let bytes_len = body.len();
    info!(target: "voice_server.admin_api", endpoint = "asr_llm_tts", session_id = %sid, bytes = bytes_len, "/admin/asr_llm_tts 收到请求");

    // 统一预处理：WAV/裸 PCM 都包成 16kHz mono s16le WAV。失败 → 400 短文本。
    let prepared = match prepare_audio_for_asr(body.to_vec()) {
        Some(b) => b,
        None => {
            return Ok(HttpResponse::BadRequest()
                .content_type("text/plain; charset=utf-8")
                .body("audio empty or not a valid wav/pcm"));
        }
    };

    Ok(HttpResponse::Ok()
        .content_type("application/x-ndjson")
        .streaming(build_asr_llm_tts_stream(
            prepared,
            sid,
            asr.get_ref().clone(),
            llm.get_ref().clone(),
            tts.get_ref().clone(),
        )))
}

/// ASR 阶段内联跑完拿到 prompt，LLM→TTS 阶段复用 llm_tts_items，
/// 统一映射成带 stage 标记的 NDJSON 行。
fn build_asr_llm_tts_stream(
    pcm: Vec<u8>,
    sid: String,
    asr: ArcAsr,
    llm: ArcLlm,
    tts: ArcTts,
) -> impl Stream<Item = Result<Bytes, actix_web::Error>> + 'static {
    try_stream! {
        // 阶段 1: ASR —— 转发识别事件，收最终文本作为 LLM prompt
        let mut asr_stream = match asr.recognize(&sid, None, pcm).await {
            Ok(s) => s,
            Err(e) => {
                warn!(target: "voice_server.admin_api", endpoint = "asr_llm_tts", session_id = %sid, "ASR 调用失败: {}", e);
                yield ndjson_line(&ErrorLine { error: format!("asr error: {}", e), code: 1001 })?;
                return;
            }
        };
        let mut prompt = String::new();
        while let Some(item) = asr_stream.next().await {
            let evt = match item {
                Ok(e) => e,
                Err(e) => {
                    warn!(target: "voice_server.admin_api", endpoint = "asr_llm_tts", session_id = %sid, "ASR 流错误: {}", e);
                    yield ndjson_line(&ErrorLine { error: format!("asr stream: {}", e), code: 1001 })?;
                    return;
                }
            };
            // 取最新非空识别文本；非流式 ASR 客户端只发一个 is_final=true 的完整结果
            if !evt.text.is_empty() {
                prompt = evt.text.clone();
            }
            yield ndjson_line(&StageAsrLine { stage: "asr", text: evt.text, is_final: evt.is_final })?;
        }

        let prompt = prompt.trim().to_string();
        if prompt.is_empty() {
            yield ndjson_line(&ErrorLine { error: "asr result empty".to_string(), code: 1001 })?;
            return;
        }

        // 阶段 2+3: 复用 LLM→TTS 管线
        let mut items = Box::pin(llm_tts_items(prompt, sid.clone(), llm, tts));
        while let Some(item) = items.next().await {
            match item {
                LlmTtsItem::Llm { delta, is_final } => {
                    yield ndjson_line(&StageLlmLine { stage: "llm", delta, is_final })?;
                }
                LlmTtsItem::Tts { seq, audio, is_last } => {
                    yield ndjson_line(&StageTtsLine { stage: "tts", seq, audio, is_last })?;
                }
                LlmTtsItem::Failed { error, code } => {
                    yield ndjson_line(&ErrorLine { error, code })?;
                }
            }
        }

        debug!(target: "voice_server.admin_api", endpoint = "asr_llm_tts", session_id = %sid, "/admin/asr_llm_tts 全部完成");
    }
}