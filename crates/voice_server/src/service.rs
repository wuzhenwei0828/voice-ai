//! VoiceService: 实现 webhttp 的 ServiceCallback
//!
//! 每条 WS 连接（按 session_id 区分）对应一个 VoiceSession，存进 DashMap。
//! wsdata 是同步的，所以 on_payload 也是同步（pipeline 任务通过 tokio::spawn 异步跑）。

use std::any::Any;
use std::sync::Arc;

use actix::prelude::Recipient;
use actix_web::web::{self, ServiceConfig};
use dashmap::DashMap;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};
use voice_proto::{decode_message_id, decode_payload};

use webhttp::websocket::{ActorMsg, OutMessage};
use webhttp::{ServiceCallback, WsData};

use crate::agent::LlmAgent;
use crate::client::ArcAsr;
use crate::client::ArcLlm;
use crate::client::ArcTts;
use crate::metrics::{VoiceMetrics, VoiceMetricsSink};
use crate::session::VoiceSession;
use crate::trace_context::new_trace_id;

pub struct VoiceService {
    pub asr: ArcAsr,
    /// 裸 LlmClient，给 admin endpoints 用（无记忆）
    pub llm: ArcLlm,
    /// LlmAgent，session pipeline 用（带 per-session 短期记忆）
    pub agent: Arc<LlmAgent>,
    pub tts: ArcTts,
    /// Prometheus collectors for server-side voice pipeline timings.
    pub metrics: Arc<VoiceMetrics>,
    metrics_sink: Arc<dyn VoiceMetricsSink>,
    /// session_id -> VoiceSession
    sessions: DashMap<String, VoiceSession>,
    /// web admin 静态文件目录（None = 不挂载）
    web_static_dir: Option<std::path::PathBuf>,
    /// 后台任务追踪：所有 spawn 出去的 pipeline JoinHandle 都通过 wsdata
    /// 推到这里。janitor 任务（new() 时启动）循环调用 join_next 把完成的
    /// 任务从 set 里 yield 掉，set 不会无限增长；panic 会被 janitor 捕获并
    /// 打 error 日志（C1）。
    /// 用 tokio::sync::Mutex 是因为 janitor 要在 `join_next().await` 期间持锁；
    /// std::sync::Mutex 跨 .await 持有会死锁。
    pipeline_tasks: Arc<Mutex<JoinSet<()>>>,
}

impl VoiceService {
    pub fn new(asr: ArcAsr, llm: ArcLlm, agent: Arc<LlmAgent>, tts: ArcTts) -> Self {
        let metrics = Arc::new(VoiceMetrics::new());
        Self::new_with_metrics(asr, llm, agent, tts, metrics)
    }

    pub fn new_with_metrics(
        asr: ArcAsr,
        llm: ArcLlm,
        agent: Arc<LlmAgent>,
        tts: ArcTts,
        metrics: Arc<VoiceMetrics>,
    ) -> Self {
        let pipeline_tasks = Arc::new(Mutex::new(JoinSet::new()));
        let metrics_sink: Arc<dyn VoiceMetricsSink> = metrics.clone();

        // janitor：循环 join_next，已完成的任务自动从 set 移除；panic 转 error 日志。
        // 注意：这是 minimal 版本 —— shutdown 时不等待 in-flight pipeline 退出，
        // 因为 webhttp 没暴露 shutdown hook；VoiceSession::drop 已经在 cancel global_cancel，
        // in-flight pipeline 会自行退出但 join 不到。这里只能观察到 panic。
        // join_next 返回的 future 借用 &mut JoinSet，所以 MutexGuard 必须跨 await 持有；
        // tokio::sync::MutexGuard 是 Send，没问题。
        {
            let set = pipeline_tasks.clone();
            tokio::spawn(async move {
                loop {
                    let mut g = set.lock().await;
                    match g.join_next().await {
                        Some(Ok(())) => {}
                        Some(Err(je)) => {
                            if je.is_panic() {
                                error!(
                                    target: "voice_server.service",
                                    "pipeline task panic: {}", je
                                );
                            } else {
                                error!(
                                    target: "voice_server.service",
                                    "pipeline task join error: {}", je
                                );
                            }
                        }
                        None => {
                            // JoinSet 空且被关闭时才返回 None；当前我们没 close，
                            // 所以理论上不会到这里。保险起见退出循环。
                            debug!(target: "voice_server.service", "pipeline JoinSet empty, janitor exits");
                            break;
                        }
                    }
                }
            });
        }

        Self {
            asr,
            llm,
            agent,
            tts,
            metrics,
            metrics_sink,
            sessions: DashMap::new(),
            web_static_dir: None,
            pipeline_tasks,
        }
    }

    /// 设置 web admin 静态文件目录（必须在 start() 之前调用）
    pub fn with_web_static_dir(mut self, dir: impl Into<std::path::PathBuf>) -> Self {
        self.web_static_dir = Some(dir.into());
        self
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// 把内部的 Arc 客户端借出来，供给 admin_api handlers 用（通过 web::Data 注入）
    pub fn arcs(&self) -> (ArcAsr, ArcLlm, ArcTts) {
        (self.asr.clone(), self.llm.clone(), self.tts.clone())
    }
}

impl ServiceCallback for VoiceService {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn api_init(&self, web_app: &mut ServiceConfig) {
        // 单能力 admin 接口 /admin/* （HTTP REST + SSE 流）
        // 注意：必须先于 static files 注册，否则 Files 服务会优先匹配 /admin/* 路径
        let (asr, llm, tts) = self.arcs();
        web_app
            .app_data(web::Data::new(asr))
            .app_data(web::Data::new(llm))
            .app_data(web::Data::new(tts))
            .app_data(web::Data::new(self.metrics.clone()))
            .route("/metrics/voice", web::get().to(crate::metrics::handler))
            .service(
                web::scope("/admin")
                    .route("/voices", web::get().to(crate::admin_api::voices))
                    .route("/tts/format", web::get().to(crate::admin_api::tts_format))
                    .route("/asr", web::post().to(crate::admin_api::asr))
                    .route("/llm", web::post().to(crate::admin_api::llm))
                    .route("/tts", web::post().to(crate::admin_api::tts))
                    .route("/llm_tts", web::post().to(crate::admin_api::llm_tts))
                    .route(
                        "/asr_llm_tts",
                        web::post().to(crate::admin_api::asr_llm_tts),
                    ),
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
                    "静态文件目录不存在，跳过 web admin 挂载"
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
                let trace_id = decode_message_id(&payload).unwrap_or_else(new_trace_id);

                let (_, p) = match decode_payload(&payload) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(target: "voice_server.service", session_id = %session_id, "解码失败: {}", e);
                        return Ok(ActorMsg::Ok);
                    }
                };
                info!(
                    target: "voice_server.service",
                    direction = "inbound",
                    business = %business,
                    actor = %actor,
                    connid = %connid,
                    session_id = %session_id,
                    message_id = %trace_id,
                    kind = p.type_name(),
                    bytes = payload.len(),
                    "WS 收到消息"
                );

                // live-asr 业务：复用 webhttp 路由 + voice-providers WsConnPool 跨会话复用
                // （公共协议：fun-asr / qwen-audio-3.0 / paraformer，docs L3289）
                if business == "live-asr" {
                    crate::live_asr::handle_message(addr, session_id, payload, trace_id);
                    return Ok(ActorMsg::Ok);
                }

                // 找/建 session
                let mut entry = self.sessions.entry(session_id.clone()).or_insert_with(|| {
                    info!(target: "voice_server.service", session_id = %session_id, "新建 VoiceSession");
                    VoiceSession::new_with_metrics(
                        session_id.clone(),
                        self.asr.clone(),
                        self.agent.clone(),
                        self.tts.clone(),
                        addr,
                        self.metrics_sink.clone(),
                    )
                });

                // on_payload 返回 Some(JoinHandle) 表示本条消息 spawn 出去一个 pipeline；
                // 把它推入 service 级 JoinSet 让 janitor 追踪（C1）。
                // 锁 entry 期间 spawn 一个 helper 把 handle 移交给 JoinSet，
                // helper 立即退出但 JoinSet 内的 child task 会 await 这个 handle，
                // 所以 panics / 异常退出都会被 janitor 观察到。
                let spawned = entry.on_payload_with_trace_id(p, trace_id);
                if let Some(handle) = spawned {
                    let set = self.pipeline_tasks.clone();
                    tokio::spawn(async move {
                        let mut g = set.lock().await;
                        g.spawn(async move {
                            // 只看 panic；正常完成 / cancel 都不打日志（janitor 也只打 panic）
                            if let Err(je) = handle.await {
                                if je.is_panic() {
                                    error!(
                                        target: "voice_server.service",
                                        "pipeline task panic: {}", je
                                    );
                                }
                            }
                        });
                    });
                }

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
