//! LLM 客户端：基于 `async-openai::chat().create_stream()`
//!
//! Wire format（OpenAI-compat）：JSON `{model, messages, stream: true}` + SSE 响应

use async_openai::config::{Config, OpenAIConfig};
use async_openai::error::OpenAIError;
use async_openai::types::{ChatCompletionRequestMessage, CreateChatCompletionRequest};
use async_openai::Client;
use async_trait::async_trait;
use futures_util::StreamExt;
use std::pin::Pin;
use std::sync::Arc;
use tracing::{info, warn};

use crate::client::error::ClientError;
use crate::config::{llm_openai, LlmConfig, ProviderConfig};
use crate::session::LlmEvent;

pub type BoxStream<T> = Pin<Box<dyn futures_util::Stream<Item = T> + Send>>;
pub type ArcLlm = Arc<dyn LlmClient>;

#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn chat(
        &self,
        session_id: &str,
        prompt: &str,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError>;
}

pub struct HttpLlmClient {
    openai: OpenAIConfig,
    client: Client<OpenAIConfig>,
    model: String,
    // 注意：async-openai 暂不暴露往请求里塞自定义 header 的口子，
    // 所以 provider / llm.headers 配置项在 LLM 这里先不消费；TTS 走手搓 reqwest 所以能用。
}

impl HttpLlmClient {
    pub fn new(openai: OpenAIConfig, model: String) -> Self {
        let client = Client::with_config(openai.clone());
        Self { openai, client, model }
    }
}

#[async_trait]
impl LlmClient for HttpLlmClient {
    async fn chat(
        &self,
        session_id: &str,
        prompt: &str,
    ) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
        // ===== 请求：把所有发出去的字段都打出来 =====
        let api_base = self.openai.api_base();
        let full_url = format!("{}/chat/completions", api_base.trim_end_matches('/'));
        let prompt_preview: String = prompt.chars().take(500).collect();
        info!(
            target: "voice_server.llm",
            session_id,
            method = "POST",
            url = %full_url,
            model = %self.model,
            stream = true,
            messages_count = 1usize,
            prompt_chars = prompt.chars().count(),
            prompt_preview = %prompt_preview,
            "LLM 请求即将发送"
        );

        let req = CreateChatCompletionRequest {
            model: self.model.clone(),
            messages: vec![ChatCompletionRequestMessage::User(prompt.to_string().into())],
            stream: Some(true),
            ..Default::default()
        };
        // ===== 请求体 JSON：方便对照实际发出的 payload =====
        match serde_json::to_string(&req) {
            Ok(body) => info!(
                target: "voice_server.llm.req",
                session_id,
                body = %body,
                "LLM 请求体"
            ),
            Err(e) => warn!(
                target: "voice_server.llm.req",
                session_id,
                "LLM 请求体序列化失败: {}",
                e
            ),
        }

        let mut raw_stream = match self.client.chat().create_stream(req).await {
            Ok(s) => {
                info!(
                    target: "voice_server.llm",
                    session_id,
                    "LLM 连接建立，stream 已开始"
                );
                s
            }
            Err(e) => {
                let err_text = e.to_string();
                let (api_message, api_type, api_param, api_code, reqwest_status, is_timeout, is_connect) =
                    match &e {
                        OpenAIError::ApiError(api_err) => (
                            Some(api_err.message.clone()),
                            api_err.r#type.clone(),
                            api_err.param.clone(),
                            api_err.code.clone(),
                            None,
                            false,
                            false,
                        ),
                        OpenAIError::Reqwest(re) => {
                            let s = re.status().map(|s| s.as_u16());
                            (
                                None,
                                None,
                                None,
                                None,
                                s,
                                re.is_timeout(),
                                re.is_connect(),
                            )
                        }
                        _ => (None, None, None, None, None, false, false),
                    };
                warn!(
                    target: "voice_server.llm.err",
                    session_id,
                    url = %full_url,
                    error = %err_text,
                    error_debug = ?e,
                    api_message = api_message.as_deref().unwrap_or(""),
                    api_type = api_type.as_deref().unwrap_or(""),
                    api_param = api_param.as_deref().unwrap_or(""),
                    api_code = api_code.as_deref().unwrap_or(""),
                    reqwest_status = reqwest_status.unwrap_or(0),
                    is_timeout,
                    is_connect,
                    "LLM create_stream 失败"
                );
                return Err(ClientError::Http(err_text));
            }
        };

        let session = session_id.to_string();
        let stream = async_stream::stream! {
            let mut chunk_count: u32 = 0;
            let mut total_delta_chars: usize = 0;
            let mut first_chunk_logged = false;
            while let Some(item) = raw_stream.next().await {
                chunk_count += 1;
                match item {
                    Ok(chunk) => {
                        // ===== 原始 chunk（JSON）：方便排查 delta 为空 / 字段名差异 =====
                        match serde_json::to_string(&chunk) {
                            Ok(raw) => info!(
                                target: "voice_server.llm.resp",
                                session_id = %session,
                                seq = chunk_count,
                                raw = %raw,
                                "LLM 原始 chunk"
                            ),
                            Err(e) => warn!(
                                target: "voice_server.llm.resp",
                                session_id = %session,
                                seq = chunk_count,
                                "LLM chunk 序列化失败: {}",
                                e
                            ),
                        }
                        // 首个 chunk 多打一行摘要（id / model / choices 数）
                        if !first_chunk_logged {
                            first_chunk_logged = true;
                            info!(
                                target: "voice_server.llm.resp",
                                session_id = %session,
                                chunk_id = %chunk.id,
                                chunk_model = %chunk.model,
                                choices_count = chunk.choices.len(),
                                "LLM 首个 chunk 摘要"
                            );
                        }

                        let (delta, is_final) = chunk
                            .choices
                            .into_iter()
                            .next()
                            .map(|c| {
                                let d = c.delta.content.unwrap_or_default();
                                let fin = c.finish_reason.is_some();
                                (d, fin)
                            })
                            .unwrap_or_default();
                        total_delta_chars += delta.chars().count();
                        info!(
                            target: "voice_server.llm",
                            session_id = %session,
                            seq = chunk_count,
                            delta_len = delta.chars().count(),
                            is_final = is_final,
                            delta = %delta,
                            "收到 LLM delta"
                        );
                        yield Ok(LlmEvent { delta, is_final });
                    }
                    Err(e) => {
                        let err_text = e.to_string();
                        let (api_message, api_type, api_param, api_code, reqwest_status, is_timeout, is_connect) =
                            match &e {
                                OpenAIError::ApiError(api_err) => (
                                    Some(api_err.message.clone()),
                                    api_err.r#type.clone(),
                                    api_err.param.clone(),
                                    api_err.code.clone(),
                                    None,
                                    false,
                                    false,
                                ),
                                OpenAIError::Reqwest(re) => {
                                    let s = re.status().map(|s| s.as_u16());
                                    (
                                        None,
                                        None,
                                        None,
                                        None,
                                        s,
                                        re.is_timeout(),
                                        re.is_connect(),
                                    )
                                }
                                _ => (None, None, None, None, None, false, false),
                            };
                        warn!(
                            target: "voice_server.llm.err",
                            session_id = %session,
                            seq = chunk_count,
                            error = %err_text,
                            error_debug = ?e,
                            api_message = api_message.as_deref().unwrap_or(""),
                            api_type = api_type.as_deref().unwrap_or(""),
                            api_param = api_param.as_deref().unwrap_or(""),
                            api_code = api_code.as_deref().unwrap_or(""),
                            reqwest_status = reqwest_status.unwrap_or(0),
                            is_timeout,
                            is_connect,
                            "LLM 流错误"
                        );
                        yield Err(ClientError::Http(err_text));
                    }
                }
            }
            info!(
                target: "voice_server.llm",
                session_id = %session,
                chunk_count,
                total_delta_chars,
                "LLM 流结束"
            );
        };

        Ok(Box::pin(stream))
    }
}

// 注：llm.headers 配置项当前不消费 —— 见 HttpLlmClient 字段上的注释。

pub fn build_llm_client(
    cfg: &LlmConfig,
    provider: Option<&ProviderConfig>,
) -> anyhow::Result<Arc<dyn LlmClient>> {
    let resolved = cfg.resolved(provider);
    let openai = llm_openai(cfg, provider);

    tracing::info!(
        target: "voice_server.factory",
        kind = "http",
        api_base = %resolved.api_base,
        model = %cfg.model,
        "构造 HttpLlmClient"
    );

    Ok(Arc::new(HttpLlmClient::new(openai, cfg.model.clone())))
}
