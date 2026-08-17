//! VoiceService: 实现 webhttp 的 ServiceCallback
//!
//! 每条 WS 连接（按 session_id 区分）对应一个 VoiceSession，存进 DashMap。
//! wsdata 是同步的，所以 on_payload 也是同步（pipeline 任务通过 tokio::spawn 异步跑）。

use std::any::Any;
use std::sync::Arc;

use actix::prelude::Recipient;
use actix_web::web::{self, ServiceConfig};
use dashmap::DashMap;
use tracing::{debug, info, warn};
use voice_proto::decode_payload;

use webhttp::websocket::{ActorMsg, OutMessage};
use webhttp::{ServiceCallback, WsData};

use crate::client::ArcAsr;
use crate::client::ArcLlm;
use crate::client::ArcTts;
use crate::session::VoiceSession;

pub struct VoiceService {
    pub asr: ArcAsr,
    pub llm: ArcLlm,
    pub tts: ArcTts,
    /// session_id -> VoiceSession
    sessions: DashMap<String, VoiceSession>,
    /// web demo 静态文件目录（None = 不挂载）
    web_static_dir: Option<std::path::PathBuf>,
}

impl VoiceService {
    pub fn new(asr: ArcAsr, llm: ArcLlm, tts: ArcTts) -> Self {
        Self {
            asr,
            llm,
            tts,
            sessions: DashMap::new(),
            web_static_dir: None,
        }
    }

    /// 设置 web demo 静态文件目录（必须在 start() 之前调用）
    pub fn with_web_static_dir(mut self, dir: impl Into<std::path::PathBuf>) -> Self {
        self.web_static_dir = Some(dir.into());
        self
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// 把内部的 Arc 客户端借出来，供给 test_api handlers 用（通过 web::Data 注入）
    pub fn arcs(&self) -> (ArcAsr, ArcLlm, ArcTts) {
        (self.asr.clone(), self.llm.clone(), self.tts.clone())
    }
}

impl ServiceCallback for VoiceService {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn api_init(&self, web_app: &mut ServiceConfig) {
        // 单能力验证接口 /test/* （HTTP REST + NDJSON 流）
        // 注意：必须先于 static files 注册，否则 Files 服务会优先匹配 /test/* 路径
        let (asr, llm, tts) = self.arcs();
        web_app.app_data(web::Data::new(asr))
                .app_data(web::Data::new(llm))
                .app_data(web::Data::new(tts))
                .service(
                    web::scope("/test")
                        .route("/asr", web::post().to(crate::test_api::asr))
                        .route("/llm", web::post().to(crate::test_api::llm))
                        .route("/tts", web::post().to(crate::test_api::tts))
                        .route("/llm_tts", web::post().to(crate::test_api::llm_tts))
                        .route("/asr_llm_tts", web::post().to(crate::test_api::asr_llm_tts))
                );

        if let Some(path) = &self.web_static_dir {
            if path.exists() {
                tracing::info!(target: "voice_server.web", static_dir = %path.display(), "挂载 web demo 静态文件");
                use actix_files::Files;
                web_app.service(Files::new("/", path).index_file("index.html"));
            } else {
                tracing::warn!(
                    target: "voice_server.web",
                    static_dir = %path.display(),
                    "静态文件目录不存在，跳过 web demo 挂载"
                );
            }
        }
    }

    fn wsdata(
        &self,
        data: WsData,
        _consumer: Arc<dyn ServiceCallback>,
    ) -> anyhow::Result<ActorMsg> {
        match data {
            WsData::WsMessage { data: in_msg } => {
                let InMessageParts {
                    addr,
                    business,
                    actor,
                    connid,
                    payload,
                } = split_in_message(in_msg);

                let session_id = format!("{}-{}-{}", business, actor, connid);

                let (kind, p) = match decode_payload(&payload) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(target: "voice_server.service", session_id = %session_id, "解码失败: {}", e);
                        return Ok(ActorMsg::Ok);
                    }
                };
                debug!(target: "voice_server.service", session_id = %session_id, ?kind, "收到 WS 消息");

                // 找/建 session
                let mut entry = self.sessions.entry(session_id.clone()).or_insert_with(|| {
                    info!(target: "voice_server.service", session_id = %session_id, "新建 VoiceSession");
                    VoiceSession::new(
                        session_id.clone(),
                        self.asr.clone(),
                        self.llm.clone(),
                        self.tts.clone(),
                        addr,
                    )
                });

                entry.on_payload(p);

                Ok(ActorMsg::Ok)
            }
            WsData::WsConnect { data: connect } => {
                info!(
                    target: "voice_server.service",
                    connid = %connect.conn.connid,
                    actor = %connect.conn.actor,
                    "WS 连接建立"
                );
                Ok(ActorMsg::Ok)
            }
            WsData::WsDisconnect { data: disconnect } => {
                let sid = disconnect.conn.get_session_id();
                info!(
                    target: "voice_server.service",
                    session_id = %sid,
                    "WS 断开，移除 session"
                );
                self.sessions.remove(&sid);
                Ok(ActorMsg::Ok)
            }
        }
    }
}

struct InMessageParts {
    addr: Recipient<OutMessage>,
    business: String,
    actor: String,
    connid: String,
    payload: Vec<u8>,
}

fn split_in_message(m: webhttp::websocket::InMessage) -> InMessageParts {
    InMessageParts {
        addr: m.addr,
        business: m.conn.business,
        actor: m.conn.actor,
        connid: m.conn.connid,
        payload: m.data,
    }
}