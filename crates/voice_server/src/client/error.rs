//! 客户端通用错误

use serde::{Deserialize, Serialize};
use std::fmt;

/// OpenAI 兼容错误信封（参见 yapi.md §错误码）。
///
/// 上游 FunASR 服务在非 2xx 时返回：
/// ```json
/// { "error": { "message": "...", "type": "...", "param": "...", "code": "..." } }
/// ```
///
/// 字段：
///   - `message`：人类可读的错误描述
///   - `type_`：`invalid_request_error` | `rate_limit_error` | `api_error`
///   - `param`：导致错误的字段名（可缺省）
///   - `code`：机器可读错误分类字符串（如 `model_not_found` / `file_too_large` / `internal_error`）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAiError {
    pub message: String,
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub param: Option<String>,
    pub code: String,
}

impl fmt::Display for OpenAiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.param {
            Some(p) => write!(f, "[{}] {} (param={})", self.code, self.message, p),
            None => write!(f, "[{}] {}", self.code, self.message),
        }
    }
}

#[derive(Debug, Deserialize)]
struct OpenAiErrorEnvelope {
    error: OpenAiError,
}

/// 尝试把一段 body 解析成 OpenAI 信封；任何一步失败（坏 JSON / 缺字段）返回 `None`，
/// 调用方应降级到 `ClientError::Status(u16)` 旧路径，不要静默吞错。
pub fn parse_openai_error(body: &str) -> Option<OpenAiError> {
    serde_json::from_str::<OpenAiErrorEnvelope>(body).ok().map(|e| e.error)
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("http error: {0}")]
    Http(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("decode error: {0}")]
    Decode(String),
    /// 非 2xx + body 解析成功为 OpenAI 信封（yapi.md §错误码）。
    /// 下游（actix handler）可调 `render_api_envelope()` 直接渲染成 HTTP 状态码 + 信封 body。
    #[error("api error: status={status}, {error}")]
    Api { status: u16, error: OpenAiError },
    /// 非 2xx + body 不是 OpenAI 信封（或解析失败），仅保留状态码
    #[error("service returned status {0}")]
    Status(u16),
    #[error("ws error: {0}")]
    Ws(String),
    /// 调用方在请求级别传了与服务端/上游不兼容的参数（如 TTS sample_rate 与 response_format 不匹配）。
    /// 与 `build_*_client` 启动期 bail 区分 —— 这是请求期才会发现的 misuse。
    #[error("config error: {0}")]
    Config(String),
}

impl ClientError {
    /// 一句话短描述 —— 老的调用方还在用，新增 `Api` 走 `error.code: error.message`
    pub fn to_string_short(&self) -> String {
        match self {
            ClientError::Http(m)
            | ClientError::Io(m)
            | ClientError::Decode(m)
            | ClientError::Ws(m)
            | ClientError::Config(m) => m.clone(),
            ClientError::Status(c) => c.to_string(),
            ClientError::Api { status, error } => format!("{}: {}", status, error),
        }
    }

    /// 如果本错误来自上游 OpenAI 信封，返回 `(status, JSON 信封 body)`，
    /// 给 actix 直接渲染（保留上游 HTTP 状态码 + 信封形态）。
    /// 序列化结果按 yapi.md §错误码 包裹成 `{"error": {...}}`。
    /// 非 `Api` 变体返回 `None`，调用方应走 HTTP 500 fallback。
    pub fn render_api_envelope(&self) -> Option<(u16, String)> {
        match self {
            ClientError::Api { status, error } => {
                // 信封形态：`{"error": {"message", "type", "param", "code"}}`
                // 字段缺失时（如上游没传 param）按 skip_serializing_if 折叠，
                // 保持与上游返回的形状尽量一致。
                let envelope = serde_json::json!({ "error": error });
                let body = serde_json::to_string(&envelope).ok()?;
                Some((*status, body))
            }
            _ => None,
        }
    }
}

pub fn format_err<E: fmt::Display>(e: E) -> ClientError {
    ClientError::Http(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_openai_error_full_envelope() {
        let body = r#"{
            "error": {
                "message": "model 'paraformer' is not supported",
                "type": "invalid_request_error",
                "param": "model",
                "code": "model_not_found"
            }
        }"#;
        let err = parse_openai_error(body).expect("should parse");
        assert_eq!(err.code, "model_not_found");
        assert_eq!(err.type_.as_deref(), Some("invalid_request_error"));
        assert_eq!(err.param.as_deref(), Some("model"));
        assert!(err.message.contains("paraformer"));
    }

    #[test]
    fn parse_openai_error_without_param() {
        // param 字段可缺省
        let body = r#"{
            "error": {
                "message": "internal boom",
                "type": "api_error",
                "code": "internal_error"
            }
        }"#;
        let err = parse_openai_error(body).expect("should parse without param");
        assert_eq!(err.code, "internal_error");
        assert_eq!(err.param, None);
    }

    #[test]
    fn parse_openai_error_invalid_json_returns_none() {
        assert!(parse_openai_error("not json").is_none());
        assert!(parse_openai_error("").is_none());
        assert!(parse_openai_error(r#"{"text":"hi"}"#).is_none()); // 顶层缺 error
    }

    #[test]
    fn parse_openai_error_missing_required_field_returns_none() {
        // 缺 code → 解析失败
        let body = r#"{"error":{"message":"x","type":"api_error"}}"#;
        assert!(parse_openai_error(body).is_none());
    }

    #[test]
    fn render_api_envelope_round_trips_as_json() {
        let body = r#"{
            "error": {
                "message": "boom",
                "type": "api_error",
                "code": "internal_error"
            }
        }"#;
        let err = ClientError::Api {
            status: 500,
            error: parse_openai_error(body).unwrap(),
        };
        let (status, json) = err.render_api_envelope().expect("should render");
        assert_eq!(status, 500);
        // 反序列化回信封确认结构保真
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["error"]["code"], "internal_error");
        assert_eq!(parsed["error"]["type"], "api_error");
    }

    #[test]
    fn render_api_envelope_non_api_returns_none() {
        let err = ClientError::Status(400);
        assert!(err.render_api_envelope().is_none());
        let err = ClientError::Http("conn refused".into());
        assert!(err.render_api_envelope().is_none());
    }

    #[test]
    fn to_string_short_for_api() {
        let body = r#"{"error":{"message":"x","type":"api_error","code":"internal_error"}}"#;
        let err = ClientError::Api {
            status: 500,
            error: parse_openai_error(body).unwrap(),
        };
        let s = err.to_string_short();
        assert!(s.contains("500"));
        assert!(s.contains("internal_error"));
    }

    #[test]
    fn openai_error_display_with_param() {
        let e = OpenAiError {
            message: "model 'foo' not supported".into(),
            type_: Some("invalid_request_error".into()),
            param: Some("model".into()),
            code: "model_not_found".into(),
        };
        let s = e.to_string();
        assert!(s.contains("[model_not_found]"));
        assert!(s.contains("param=model"));
    }

    #[test]
    fn openai_error_display_without_param() {
        let e = OpenAiError {
            message: "boom".into(),
            type_: Some("api_error".into()),
            param: None,
            code: "internal_error".into(),
        };
        let s = e.to_string();
        assert!(s.contains("[internal_error]"));
        assert!(!s.contains("param="));
    }
}
