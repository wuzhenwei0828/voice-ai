//! 客户端通用错误

use std::fmt;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("http error: {0}")]
    Http(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("service returned status {0}")]
    Status(u16),
}

// 兼容旧代码里的 Display impl 路径
impl ClientError {
    pub fn to_string_short(&self) -> String {
        match self {
            ClientError::Http(m) | ClientError::Io(m) | ClientError::Decode(m) => m.clone(),
            ClientError::Status(c) => c.to_string(),
        }
    }
}

pub fn format_err<E: fmt::Display>(e: E) -> ClientError {
    ClientError::Http(e.to_string())
}
