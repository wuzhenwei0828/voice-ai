//! FunASR 服务端响应 wire 格式：`FunasrResponseMode` / `FunasrResponse` / `FunasrClose` / `FunasrEvent` + 解析

use async_tungstenite::tungstenite::protocol::CloseFrame;
use serde::Deserialize;

use crate::client::error::ClientError;

/// FunASR 服务端响应模式（对应服务端 JSON 里的 `mode` 字段）。
///
/// - `Online` —— 实时识别流式增量（`mode: "online"`）
/// - `TwoPassOnline` —— 2pass 模式下的流式增量（`mode: "2pass-online"`）
/// - `TwoPassOffline` —— 2pass 模式下的二次纠错结果（`mode: "2pass-offline"`，仅 sentence end 后出现）
/// - `Offline` —— 离线一次性识别（`mode: "offline"`）
/// - `Other(String)` —— 未知 mode（保留原值用于日志；上层一般当 Online 处理）
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunasrResponseMode {
    Online,
    TwoPassOnline,
    TwoPassOffline,
    Offline,
    Other(String),
}

impl FunasrResponseMode {
    /// FunASR 服务端的 `text` 字段在此 mode 下是"累积全文"还是其它。
    /// - 累积全文：必须做 last_text → delta 转换（前端才能"一个字一个字显示"）。
    /// - 最终纠错：必须替换上一行 final（不要 append）。
    pub fn is_cumulative(&self) -> bool {
        matches!(
            self,
            FunasrResponseMode::Online | FunasrResponseMode::TwoPassOnline
        )
    }
    pub fn is_correction(&self) -> bool {
        matches!(self, FunasrResponseMode::TwoPassOffline)
    }
}

/// FunASR 服务端响应解析后的内部结构（比 AsrEvent 多带 mode，供上层做累积→增量转换）
#[derive(Debug, Clone)]
pub struct FunasrResponse {
    pub mode: FunasrResponseMode,
    pub text: String,
    pub is_final: bool,
}

/// FunASR 服务端 Close 帧解析结果。
///
/// FunASR 协议下 Close **= 识别完成**，不是错误。常见取值：
///   - `code = 1000` —— 正常关闭（服务端发的 Close 帧）
///   - `code = 1006` —— abnormal closure（stream 在没收到 Close 帧时就结束了，兜底映射）
///
/// 对齐浏览器 WebSocket `onclose` 的 `code` / `reason` 字段语义。
#[derive(Debug, Clone)]
pub struct FunasrClose {
    pub code: u16,
    pub reason: String,
}

impl FunasrClose {
    /// 构造 normal closure（1000 / 空 reason）。FunASR 服务端发 Close 帧时常不带 reason。
    pub fn normal() -> Self {
        Self {
            code: 1000,
            reason: String::new(),
        }
    }
    /// 构造 abnormal closure（1006）。stream 在没收到 Close 帧就结束时用。
    pub fn abnormal() -> Self {
        Self {
            code: 1006,
            reason: "abnormal closure (no close frame)".into(),
        }
    }
    /// 从 tungstenite `CloseFrame` 构造。
    pub fn from_frame(cf: CloseFrame<'_>) -> Self {
        Self {
            code: u16::from(cf.code),
            reason: cf.reason.into_owned(),
        }
    }
}

/// `FunasrReceiver::next_event` 的返回类型 —— 对应浏览器 WebSocket 三种事件的 pull 风格。
///
/// 调用方一般 `match`：
/// ```ignore
/// match rx.next_event().await {
///     FunasrEvent::Message(resp) => { /* onmessage: 识别结果 */ }
///     FunasrEvent::Close(c)      => { /* onclose: 识别完成（FunASR 不是错误！即便 1006 也要退出 loop）*/ }
///     FunasrEvent::Error(e)      => { /* onerror: 超时 / 读失败 / 协议异常 */ }
/// }
/// ```
///
/// 设计要点：
///   - `Message` / `Close` 是协议正常事件；`Close` 在 FunASR 下 = "识别完成" —— **不是错误**。
///   - `Error` 仅承载 WS 层故障（读失败 / recv 超时 / stream 错误），不含业务逻辑错误。
///   - 解析失败（JSON 非法 / 字段缺失）的服务端帧**不上浮**为 `Error` —— `next_event` 内部
///     warn + 继续收下一帧，避免一帧坏包杀掉整轮 recv。
#[derive(Debug)]
pub enum FunasrEvent {
    /// 识别事件（onmessage）—— 服务端文本帧已 parse 成 `FunasrResponse`
    Message(FunasrResponse),
    /// 服务端结束连接（onclose）。FunASR 协议下 = 识别完成；abnormal closure (code=1006)
    /// 表示 stream 在没收到 Close 帧时就断了 —— 也**不是**错误，至少本地 recv loop 应当退出。
    Close(FunasrClose),
    /// 错误（onerror）—— WS 读失败 / recv 超时
    Error(ClientError),
}

#[derive(Debug, Deserialize)]
pub(super) struct ServerResponse {
    /// `mode`：`offline` | `online` | `2pass-online` | `2pass-offline`。
    /// 服务端偶尔发的 metadata 帧可能缺该字段 —— 空字符串视为"非识别帧"，parse_server_event 返回 Ok(None)。
    #[serde(default)]
    mode: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    is_final: bool,
    #[serde(default)]
    #[allow(dead_code)]
    wav_name: String,
    #[serde(default)]
    #[allow(dead_code)]
    timestamp: Option<serde_json::Value>,
    #[serde(default)]
    #[allow(dead_code)]
    stamp_sents: Option<serde_json::Value>,
}

pub(super) fn classify_mode(raw: &str) -> FunasrResponseMode {
    match raw {
        "online" => FunasrResponseMode::Online,
        "2pass-online" => FunasrResponseMode::TwoPassOnline,
        "2pass-offline" => FunasrResponseMode::TwoPassOffline,
        "offline" => FunasrResponseMode::Offline,
        other => FunasrResponseMode::Other(other.to_string()),
    }
}

/// 解析服务端 transcript 文本帧。
///
/// 返回：
/// - `Ok(Some(resp))` — 识别事件（mode + text + is_final）
/// - `Ok(None)` — 非识别帧（mode 字段缺失或空，服务端偶尔发的 metadata）
/// - `Err(_)` — JSON 解析失败（接收方一般选择 warn + 继续 recv）
pub(super) fn parse_server_event(bytes: &[u8]) -> Result<Option<FunasrResponse>, ClientError> {
    let resp: ServerResponse = serde_json::from_slice(bytes)
        .map_err(|e| ClientError::Decode(format!("decode FunASR response: {}", e)))?;
    if resp.mode.is_empty() {
        return Ok(None);
    }
    Ok(Some(FunasrResponse {
        mode: classify_mode(&resp.mode),
        text: resp.text,
        is_final: resp.is_final,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ===== FunasrResponseMode 分类 =====

    #[test]
    fn response_mode_classify_all_doc_values() {
        assert_eq!(classify_mode("online"), FunasrResponseMode::Online);
        assert_eq!(
            classify_mode("2pass-online"),
            FunasrResponseMode::TwoPassOnline
        );
        assert_eq!(
            classify_mode("2pass-offline"),
            FunasrResponseMode::TwoPassOffline
        );
        assert_eq!(classify_mode("offline"), FunasrResponseMode::Offline);
        match classify_mode("something-new") {
            FunasrResponseMode::Other(s) => assert_eq!(s, "something-new"),
            _ => panic!("unknown mode 应归 Other"),
        }
    }

    #[test]
    fn response_mode_is_cumulative_and_correction() {
        // online / 2pass-online → text 是累积全文，需算 delta
        assert!(FunasrResponseMode::Online.is_cumulative());
        assert!(FunasrResponseMode::TwoPassOnline.is_cumulative());
        assert!(!FunasrResponseMode::Offline.is_cumulative());
        assert!(!FunasrResponseMode::TwoPassOffline.is_cumulative());
        // 2pass-offline → 二次纠错，要 replace_last
        assert!(FunasrResponseMode::TwoPassOffline.is_correction());
        assert!(!FunasrResponseMode::Online.is_correction());
    }

    #[test]
    fn parse_response_exposes_mode() {
        // parse_server_event 必须把 mode 暴露出来（live_asr 累积→delta 转换需要）
        let resp = json!({
            "mode": "2pass-online",
            "wav_name": "x",
            "text": "你",
            "is_final": false
        });
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.mode, FunasrResponseMode::TwoPassOnline);
        assert_eq!(out.text, "你");
        assert!(!out.is_final);
    }

    // ===== 服务端响应解析 =====

    #[test]
    fn parse_partial_2pass_online() {
        let resp = json!({
            "mode": "2pass-online",
            "wav_name": "x",
            "text": "你好",
            "is_final": false,
            "timestamp": "[]",
            "stamp_sents": []
        });
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "你好");
        assert!(!out.is_final);
    }

    #[test]
    fn parse_sentence_boundary_2pass_online_final() {
        let resp = json!({
            "mode": "2pass-online",
            "wav_name": "x",
            "text": "你好世界。",
            "is_final": true
        });
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "你好世界。");
        assert!(out.is_final);
    }

    #[test]
    fn parse_2pass_offline_is_correction_result() {
        // 2pass-offline 由服务端在句子结束后补发，等价于最终文本
        let resp = json!({
            "mode": "2pass-offline",
            "wav_name": "x",
            "text": "你好世界。",
            "is_final": true
        });
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "你好世界。");
        assert!(out.is_final);
    }

    #[test]
    fn parse_offline_single_response() {
        // offline 模式 is_final 文档说永远为 False（实际上服务端只发一次，靠 Close 收尾）
        let resp = json!({
            "mode": "offline",
            "wav_name": "x",
            "text": "完整文本",
            "is_final": false
        });
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "完整文本");
        assert!(!out.is_final);
    }

    #[test]
    fn parse_response_with_timestamp_and_stamp_sents() {
        // 服务端带时间戳模型时返回 timestamp / stamp_sents，必须能容忍这两个字段
        let resp = json!({
            "mode": "2pass-online",
            "wav_name": "x",
            "text": "你好",
            "is_final": false,
            "timestamp": "[[100,200],[200,500]]",
            "stamp_sents": [
                {"text_seg": "你", "punc": "", "start": 100, "end": 200, "ts_list": [[100,200]]}
            ]
        });
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "你好");
    }

    #[test]
    fn parse_response_empty_mode_returns_none() {
        // 服务端偶尔会发 metadata 帧（mode 字段缺失或空），不应报错也不应产生事件
        let resp = json!({"text": "foo", "is_final": false});
        let bytes = serde_json::to_vec(&resp).unwrap();
        assert!(parse_server_event(&bytes).unwrap().is_none());
    }

    #[test]
    fn parse_invalid_json_returns_err() {
        // 单帧解析失败必须让 recv_event 知道 —— 由调用方决定 warn + 继续 vs 中断
        assert!(parse_server_event(b"not json").is_err());
    }

    #[test]
    fn parse_response_missing_text_defaults_empty() {
        // BEGIN 帧 / VAD 未触发时可能 text 字段缺失，必须能容忍
        let resp = json!({"mode": "2pass-online", "is_final": false});
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "");
        assert!(!out.is_final);
    }

    #[test]
    fn parse_response_online_mode_partial() {
        // online 模式（非 2pass）的增量帧
        let resp = json!({
            "mode": "online",
            "wav_name": "x",
            "text": "你好",
            "is_final": false
        });
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "你好");
        assert!(!out.is_final);
    }

    // ===== FunasrClose 构造 =====

    #[test]
    fn funasr_close_normal_is_1000_empty_reason() {
        // 正常关闭：code=1000、reason 空 —— 对齐浏览器 WS normal closure
        let c = FunasrClose::normal();
        assert_eq!(c.code, 1000);
        assert_eq!(c.reason, "");
    }

    #[test]
    fn funasr_close_abnormal_is_1006() {
        // 异常关闭：code=1006、reason 非空 —— next_event 内部用
        let c = FunasrClose::abnormal();
        assert_eq!(c.code, 1006);
        assert!(!c.reason.is_empty());
    }

    #[test]
    fn funasr_close_from_frame_carries_code_and_reason() {
        // 从 tungstenite CloseFrame 构造：code / reason 都应透传
        use async_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
        let frame = CloseFrame {
            code: CloseCode::Away,
            reason: "server shutting down".into(),
        };
        let c = FunasrClose::from_frame(frame);
        assert_eq!(c.code, 1001);
        assert_eq!(c.reason, "server shutting down");
    }

    // ===== FunasrEvent 形态 =====

    #[test]
    fn funasr_event_message_carries_response() {
        // Message 变体必须能装 FunasrResponse 且 Debug 可打印
        let resp = FunasrResponse {
            mode: FunasrResponseMode::Online,
            text: "hi".into(),
            is_final: false,
        };
        let evt = FunasrEvent::Message(resp);
        // Debug 必须能格式化（避免后续日志 panic）
        let _ = format!("{:?}", evt);
        match evt {
            FunasrEvent::Message(r) => {
                assert_eq!(r.text, "hi");
                assert!(!r.is_final);
            }
            _ => panic!("Message 变体应能 match 出 FunasrResponse"),
        }
    }

    #[test]
    fn funasr_event_close_carries_code() {
        // Close 变体装 FunasrClose —— 验证 code 透传
        let evt = FunasrEvent::Close(FunasrClose {
            code: 1000,
            reason: "ok".into(),
        });
        let _ = format!("{:?}", evt);
        match evt {
            FunasrEvent::Close(c) => {
                assert_eq!(c.code, 1000);
                assert_eq!(c.reason, "ok");
            }
            _ => panic!("Close 变体应能 match 出 FunasrClose"),
        }
    }

    #[test]
    fn funasr_event_error_carries_client_error() {
        // Error 变体装 ClientError —— live_asr 上层要把它 Display 化下行
        let evt = FunasrEvent::Error(crate::client::error::ClientError::Ws("test".into()));
        let _ = format!("{:?}", evt);
        match evt {
            FunasrEvent::Error(e) => {
                assert!(matches!(e, crate::client::error::ClientError::Ws(_)));
            }
            _ => panic!("Error 变体应能 match 出 ClientError"),
        }
    }
}
