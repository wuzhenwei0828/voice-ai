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

use actix_web::web::{Bytes, Data, Json, Query};
use actix_web::HttpResponse;
use async_stream::try_stream;
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

use crate::client::{asr::wrap_pcm_as_wav, ArcAsr, ArcLlm, ArcTts, ClientError};
use crate::events::{AsrEvent, LlmEvent};
use crate::pipeline::{llm_tts_items, LlmTtsItem, SentenceCrossfader};
use crate::session::{next_sentence_end, parse_asr_emotion_tags};

/// 把 `ClientError` 转成 actix 响应。
///
/// - `ClientError::Api { status, error }`：保留上游 HTTP 状态码 + 渲染 OpenAI 信封 body，
///   这样下游能看到 `model_not_found` / `file_too_large` / `invalid_value` 等真实错误分类
///   （而不是被一律降级成 HTTP 500 + reqwest 错误字符串）。
/// - 其他变体：维持旧行为，HTTP 500 + 错误字符串。
fn api_err_to_response(e: ClientError) -> actix_web::Error {
    if let Some((status, body)) = e.render_api_envelope() {
        let sc = actix_web::http::StatusCode::from_u16(status)
            .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR);
        actix_web::error::InternalError::from_response(
            e.to_string(),
            HttpResponse::build(sc)
                .content_type("application/json")
                .body(body),
        )
        .into()
    } else {
        actix_web::error::ErrorInternalServerError(e.to_string())
    }
}

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
    /// 端侧选中的音色**短名**（如 `"alex"`）。`None` → 用 `tts.voice` 配置默认。
    /// 不在白名单时由 HttpTtsClient 返回 `ClientError::Config`。
    #[serde(default)]
    pub voice: Option<String>,
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
    /// voice-server 内部管线阶段码（1001 asr / 1002 llm / 1003 llm 流 / 1004 tts / 1005 tts 流）
    pub code: u16,
    /// yapi 兼容：上游 OpenAI 信封原文（如 `{"error":{...}}`）。`None` 表示非 OpenAI 错误。
    /// 目前只有 `asr_llm_tts` 阶段的 ASR 调用错误会填；其他阶段保留 `None`。
    /// 前端可以直接 `JSON.parse(type)` 拿上游的 `message/type/param/code`。
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// yapi 兼容：导致错误的字段名。**预留字段**，目前未填充（保留给后续扩展）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub param: Option<String>,
}

impl ErrorLine {
    /// 构造一个不带 yapi 上游信封的 ErrorLine（绝大多数 case 走这条）
    fn new(error: String, code: u16) -> Self {
        Self {
            error,
            code,
            r#type: None,
            param: None,
        }
    }
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
                    let err_line = ErrorLine::new(e.to_string(), 500);
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

// ====== 句间 crossfade（SentenceCrossfader / crossfade 已搬到 crate::pipeline）======
//
// 本模块只消费 `crate::pipeline::SentenceCrossfader`：
//   - `build_tts_sentence_stream`（/admin/tts 的句间拼接）
// `llm_tts_items`（共享 LLM→TTS 管线）本身已搬到 pipeline.rs。


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
        .map_err(api_err_to_response)?;

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
        .chat(&sid, &req.prompt, None)
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
/// body: `{"text":"...","session_id":"可选","voice":"可选短名"}`
/// 内部：用 next_sentence_end 按句子切分，逐句调 tts.synthesize，
///       句间过 crossfade 消除波形拼接尖刺。
/// response: NDJSON `{"seq":N,"audio":"<base64>","is_last":false|true}`
///           最后补一条 `{"seq":N+1,"audio":"","is_last":true}` 作为结束标记
pub async fn tts(
    req: Json<TtsReq>,
    tts: Data<ArcTts>,
) -> Result<HttpResponse, actix_web::Error> {
    let sid = req.session_id.clone().unwrap_or_else(|| gen_sid("tts"));
    info!(
        target: "voice_server.admin_api",
        endpoint = "tts",
        session_id = %sid,
        text_len = req.text.chars().count(),
        voice_override = ?req.voice,
        "/admin/tts 收到请求"
    );

    let text = req.text.clone();
    let sid_inner = sid.clone();
    let voice_override = req.voice.clone();

    // voice_override 由端侧（前端的 voice 下拉）提供；None → HttpTtsClient 走配置兜底
    Ok(HttpResponse::Ok()
        .content_type("application/x-ndjson")
        .streaming(build_tts_sentence_stream(
            text,
            sid_inner,
            tts.get_ref().clone(),
            None,
            voice_override,
        )))
}

/// 把一段长文本按 next_sentence_end 切句，逐句调 TTS，过 SentenceCrossfader 后下发。
/// 与 build_llm_tts_stream 结构对称，只是没有 LLM 阶段。
///
/// `sample_rate_override`：端侧 SessionStart 上报的 TTS 输出采样率。
///   - `Some(n)` —— 覆盖 `TtsConfig.sample_rate`
///   - `None` —— 走配置兜底
///   - /admin/llm_tts 与 /admin/asr_llm_tts 没有 SessionStart，传 `None`。
///
/// `voice_override`：端侧（前端的 voice 下拉）选中的音色短名（如 `"alex"`）。
///   - `Some("alex")` —— 用该短名（HttpTtsClient 拼 model 前缀后发给 provider）
///   - `None` —— 用配置 `TtsConfig.voice` 默认
fn build_tts_sentence_stream(
    text: String,
    sid: String,
    tts: ArcTts,
    sample_rate_override: Option<u32>,
    voice_override: Option<String>,
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

            let mut stream = match tts.synthesize(&sid, &sent, sample_rate_override, voice_override.clone()).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(target: "voice_server.admin_api", endpoint = "tts", session_id = %sid, "TTS 调用失败: {}", e);
                    let err_line = ErrorLine::new(format!("tts error: {}", e), 1102);
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
                        let err_line = ErrorLine::new(format!("tts stream: {}", e), 1103);
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
            if let Ok(mut stream) = tts.synthesize(&sid, &tail, sample_rate_override, voice_override.clone()).await {
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
    info!(
        target: "voice_server.admin_api",
        endpoint = "llm_tts",
        session_id = %sid,
        text_len = req.text.chars().count(),
        voice_override = ?req.voice,
        "/admin/llm_tts 收到请求"
    );

    let prompt = req.text.clone();
    let sid_inner = sid.clone();
    let voice_override = req.voice.clone();

    // /admin/llm_tts 是直接调 LLM，不经过 ASR，没有情绪标签可解析
    let items = llm_tts_items(
        prompt,
        None,
        sid_inner,
        llm.get_ref().clone(),
        tts.get_ref().clone(),
        None,
        voice_override,
    );

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
                    yield ndjson_line(&ErrorLine::new(error, code))?;
                }
            }
        }
    }
}

/// `llm_tts_items()` —— 已搬到 [`crate::pipeline`]。本模块从那里 import 使用。
///
/// 调用方（/admin/llm_tts、/admin/asr_llm_tts）与 `session.rs` 的 WS pipeline
/// 都通过 `crate::pipeline::llm_tts_items` 复用同一份实现，反向依赖已翻转。

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
    // /admin/asr_llm_tts 是裸 PCM body，没法用 JSON 字段传 voice → 用 query 参数
    query: Query<AsrLlmTtsQuery>,
) -> Result<HttpResponse, actix_web::Error> {
    let sid = gen_sid("asr_llm_tts");
    let bytes_len = body.len();
    info!(
        target: "voice_server.admin_api",
        endpoint = "asr_llm_tts",
        session_id = %sid,
        bytes = bytes_len,
        voice_override = ?query.voice,
        "/admin/asr_llm_tts 收到请求"
    );

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
            query.voice.clone(),
        )))
}

/// /admin/asr_llm_tts 的 query 参数：现在仅 `voice`（短名，可选）。
#[derive(Debug, serde::Deserialize)]
pub struct AsrLlmTtsQuery {
    #[serde(default)]
    pub voice: Option<String>,
}

/// GET /admin/voices
///
/// 给前端拉"音色列表 + 默认值"，用于填充 voice 下拉框。
///
/// response: JSON
/// ```json
/// { "voices": ["alex", "anna", ...], "default": "alex" }
/// ```
///
/// 白名单 `SUPPORTED_VOICES` 硬编码在 `client::tts`（与 HttpTtsClient 共享）；
/// 默认值从 `TtsClient::default_voice_short()` 拿（即 yaml `tts.voice`）。
pub async fn voices(tts: Data<ArcTts>) -> Result<HttpResponse, actix_web::Error> {
    use crate::client::tts::SUPPORTED_VOICES;

    let resp = VoicesResp {
        voices: SUPPORTED_VOICES.iter().map(|s| s.to_string()).collect(),
        default: tts.get_ref().default_voice_short().to_string(),
    };
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .json(resp))
}

#[derive(Debug, Serialize)]
struct VoicesResp {
    voices: Vec<String>,
    default: String,
}

/// ASR 阶段内联跑完拿到 prompt，LLM→TTS 阶段复用 llm_tts_items，
/// 统一映射成带 stage 标记的 NDJSON 行。
fn build_asr_llm_tts_stream(
    pcm: Vec<u8>,
    sid: String,
    asr: ArcAsr,
    llm: ArcLlm,
    tts: ArcTts,
    voice_override: Option<String>,
) -> impl Stream<Item = Result<Bytes, actix_web::Error>> + 'static {
    try_stream! {
        // 阶段 1: ASR —— 转发识别事件，收最终文本作为 LLM prompt
        let mut asr_stream = match asr.recognize(&sid, None, pcm).await {
            Ok(s) => s,
            Err(e) => {
                warn!(target: "voice_server.admin_api", endpoint = "asr_llm_tts", session_id = %sid, "ASR 调用失败: {}", e);
                // 如果上游走了 OpenAI 信封，把 code/param 也带出来 —— voice-server NDJSON 的
                // 内部错误码 (1001) 是管线阶段标识，与 yapi 的 `code` 字符串语义不同，
                // 这里统一塞进 ErrorLine，message 里能透出上游 `code: param=xxx`。
                yield ndjson_line(&ErrorLine {
                    error: format!("asr error: {}", e),
                    code: 1001,
                    r#type: e.render_api_envelope().map(|(_, b)| b),
                    param: None,
                })?;
                return;
            }
        };
        let mut prompt = String::new();
        while let Some(item) = asr_stream.next().await {
            let evt = match item {
                Ok(e) => e,
                Err(e) => {
                    warn!(target: "voice_server.admin_api", endpoint = "asr_llm_tts", session_id = %sid, "ASR 流错误: {}", e);
                    yield ndjson_line(&ErrorLine::new(format!("asr stream: {}", e), 1001))?;
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
            yield ndjson_line(&ErrorLine::new("asr result empty".to_string(), 1001))?;
            return;
        }

        // 解析 ASR 情绪标签（如 <|zh|><|SAD|><|Speech|>...），情绪作为 system message 传给 LLM
        let parsed = parse_asr_emotion_tags(&prompt);
        if parsed.text.is_empty() {
            yield ndjson_line(&ErrorLine::new("asr result empty after stripping tags".to_string(), 1001))?;
            return;
        }
        if parsed.emotion.is_some() {
            info!(
                target: "voice_server.admin_api",
                endpoint = "asr_llm_tts",
                session_id = %sid,
                emotion = ?parsed.emotion,
                "ASR 文本含情绪标签"
            );
        }

        // 阶段 2+3: 复用 LLM→TTS 管线；voice_override 来自 query 参数
        let mut items = Box::pin(llm_tts_items(parsed.text, parsed.emotion, sid.clone(), llm, tts, None, voice_override));
        while let Some(item) = items.next().await {
            match item {
                LlmTtsItem::Llm { delta, is_final } => {
                    yield ndjson_line(&StageLlmLine { stage: "llm", delta, is_final })?;
                }
                LlmTtsItem::Tts { seq, audio, is_last } => {
                    yield ndjson_line(&StageTtsLine { stage: "tts", seq, audio, is_last })?;
                }
                LlmTtsItem::Failed { error, code } => {
                    yield ndjson_line(&ErrorLine::new(error, code))?;
                }
            }
        }

        debug!(target: "voice_server.admin_api", endpoint = "asr_llm_tts", session_id = %sid, "/admin/asr_llm_tts 全部完成");
    }
}