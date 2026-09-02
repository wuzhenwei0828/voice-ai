use std::sync::Arc;
use std::time::{Duration, Instant};

use actix_web::{web, HttpResponse};
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Registry, TextEncoder};

use crate::client::llm::ModelTier;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PipelineResult { Success, Failed, Timeout, Cancelled, EmptyResponse }

impl PipelineResult {
    fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::EmptyResponse => "empty_response",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EscalationReason {
    Timeout,
    EmptyResponse,
    ProviderError,
}

impl EscalationReason {
    fn label(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::EmptyResponse => "empty_response",
            Self::ProviderError => "provider_error",
        }
    }
}

pub trait VoiceMetricsSink: Send + Sync {
    fn pipeline_started(&self);
    fn observe_pipeline_duration(&self, duration: Duration);
    fn observe_queue(&self, duration: Duration);
    fn observe_asr(&self, duration: Duration);
    fn observe_llm_first_token(&self, duration: Duration);
    fn observe_llm_complete(&self, duration: Duration);
    fn observe_llm_route(&self, _tier: ModelTier, _duration: Duration) {}
    fn llm_escalated(&self, _reason: EscalationReason) {}
    fn observe_tts_first_audio(&self, duration: Duration);
    fn observe_tts_complete(&self, duration: Duration);
    fn observe_e2e_first_audio(&self, from_input_start: Duration, from_input_end: Duration);
    fn observe_e2e_complete(&self, from_input_start: Duration, from_input_end: Duration);
    fn pipeline_finished(&self, result: PipelineResult);
    fn request_retried(&self) {}
    fn observe_tts_input_wait(&self, _duration: Duration) {}
    fn tts_ws_connect(&self) {}
    fn tts_ws_connect_failed(&self) {}
    fn tts_provider_error(&self) {}
    fn tts_input_chars(&self, _chars: u64) {}
    fn tts_audio_chunk(&self, _bytes: u64) {}
    fn tts_ws_pool_connection_opened(&self) {}
    fn tts_ws_pool_connection_closed(&self) {}
    fn tts_ws_pool_active(&self, _value: i64) {}
    fn tts_ws_pool_idle(&self, _value: i64) {}
    fn tts_ws_pool_waiting(&self, _value: i64) {}
    fn tts_ws_pool_wait_started(&self) {}
    fn tts_ws_pool_wait_finished(&self, _duration: Duration) {}
    fn observe_tts_ws_pool_wait(&self, _duration: Duration) {}
    fn tts_ws_pool_reaped(&self) {}
    fn tts_ws_pool_invalidated(&self) {}
    fn observe_tts_audio_duration(&self, _duration: Duration) {}
    fn observe_tts_audio_chunk_interval(&self, _duration: Duration) {}
    fn observe_tts_realtime_factor(&self, _value: f64) {}
    fn observe_e2e_client_playback_delay(&self, _duration: Duration) {}
}

pub struct NoopMetricsSink;

impl VoiceMetricsSink for NoopMetricsSink {
    fn pipeline_started(&self) {}
    fn observe_pipeline_duration(&self, _: Duration) {}
    fn observe_queue(&self, _: Duration) {}
    fn observe_asr(&self, _: Duration) {}
    fn observe_llm_first_token(&self, _: Duration) {}
    fn observe_llm_complete(&self, _: Duration) {}
    fn observe_tts_first_audio(&self, _: Duration) {}
    fn observe_tts_complete(&self, _: Duration) {}
    fn observe_e2e_first_audio(&self, _: Duration, _: Duration) {}
    fn observe_e2e_complete(&self, _: Duration, _: Duration) {}
    fn pipeline_finished(&self, _: PipelineResult) {}
}

pub struct VoiceMetrics {
    registry: Registry,
    requests_total: IntCounter,
    requests_finished_total: IntCounterVec,
    requests_success_total: IntCounter,
    requests_failed_total: IntCounter,
    requests_timeout_total: IntCounter,
    requests_cancelled_total: IntCounter,
    tts_ws_connect_total: IntCounter,
    tts_ws_connect_failed_total: IntCounter,
    tts_provider_errors_total: IntCounter,
    tts_input_chars_total: IntCounter,
    tts_audio_chunks_total: IntCounter,
    tts_audio_bytes_total: IntCounter,
    tts_ws_pool_connections: IntGauge,
    tts_ws_pool_active_connections: IntGauge,
    tts_ws_pool_idle_connections: IntGauge,
    tts_ws_pool_waiting: IntGauge,
    tts_ws_pool_wait_duration: Histogram,
    tts_ws_pool_reaped_total: IntCounter,
    tts_ws_pool_invalidated_total: IntCounter,
    tts_audio_duration: Histogram,
    tts_audio_chunk_interval: Histogram,
    tts_realtime_factor: Histogram,
    e2e_client_playback_delay: Histogram,
    requests_retried_total: IntCounter,
    tts_input_wait: Histogram,
    e2e_input_to_first_audio: Histogram,
    e2e_utterance_end_to_first_audio: Histogram,
    e2e_input_to_complete: Histogram,
    e2e_utterance_end_to_complete: Histogram,
    asr_duration: Histogram,
    pipeline_queue_duration: Histogram,
    llm_time_to_first_token: Histogram,
    llm_duration: Histogram,
    llm_route_duration: Histogram,
    llm_route_total: IntCounterVec,
    llm_escalation_total: IntCounterVec,
    tts_time_to_first_audio: Histogram,
    tts_generation_duration: Histogram,
    pipeline_duration: Histogram,
}

/// Prometheus-backed implementation name used by the architecture plan.
pub type PrometheusMetricsSink = VoiceMetrics;

impl VoiceMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let requests_total = counter("voice_requests_total", "Voice utterance pipelines started");
        let requests_finished_total = IntCounterVec::new(prometheus::Opts::new("voice_requests_finished_total", "Voice utterance pipelines finished"), &["result"]).expect("valid voice request result counter");
        let requests_success_total = counter("voice_requests_success_total", "Voice pipelines completed successfully");
        let requests_failed_total = counter("voice_requests_failed_total", "Voice pipelines failed");
        let requests_timeout_total = counter("voice_requests_timeout_total", "Voice pipelines timed out");
        let requests_cancelled_total = counter("voice_requests_cancelled_total", "Voice pipelines cancelled");
        let tts_ws_connect_total = counter("voice_tts_ws_connect_total", "TTS WebSocket connection attempts");
        let tts_ws_connect_failed_total = counter("voice_tts_ws_connect_failed_total", "TTS WebSocket connection failures");
        let tts_provider_errors_total = counter("voice_tts_provider_errors_total", "TTS provider errors");
        let tts_input_chars_total = counter("voice_tts_input_chars_total", "Characters submitted to TTS");
        let tts_audio_chunks_total = counter("voice_tts_audio_chunks_total", "TTS audio chunks received");
        let tts_audio_bytes_total = counter("voice_tts_audio_bytes_total", "TTS audio bytes received");
        let tts_ws_pool_connections = gauge("voice_tts_ws_pool_connections", "TTS WebSocket pool connections");
        let tts_ws_pool_active_connections = gauge("voice_tts_ws_pool_active_connections", "TTS WebSocket pool active connections");
        let tts_ws_pool_idle_connections = gauge("voice_tts_ws_pool_idle_connections", "TTS WebSocket pool idle connections");
        let tts_ws_pool_waiting = gauge("voice_tts_ws_pool_waiting", "Requests waiting for a TTS WebSocket pool connection");
        let tts_ws_pool_wait_duration = histogram("voice_tts_ws_pool_wait_duration_seconds", "Seconds spent waiting for a TTS WebSocket pool connection");
        let tts_ws_pool_reaped_total = counter("voice_tts_ws_pool_reaped_total", "TTS WebSocket pool connections reaped after idling");
        let tts_ws_pool_invalidated_total = counter("voice_tts_ws_pool_invalidated_total", "TTS WebSocket pool connections invalidated");
        let tts_audio_duration = histogram("voice_tts_audio_duration_seconds", "Duration represented by TTS audio output");
        let tts_audio_chunk_interval = histogram("voice_tts_audio_chunk_interval_seconds", "Interval between TTS audio chunks");
        let tts_realtime_factor = histogram("voice_tts_realtime_factor", "TTS audio duration divided by generation duration");
        let e2e_client_playback_delay = histogram("voice_e2e_tts_first_audio_to_client_playback_seconds", "Seconds from first TTS audio received to client playback start");
        let requests_retried_total = counter("voice_requests_retried_total", "Voice requests retried");
        let tts_input_wait = histogram("voice_tts_input_wait_seconds", "Seconds from TTS input.text to input.done");
        let e2e_input_to_first_audio = histogram("voice_e2e_input_to_tts_first_audio_seconds", "Seconds from the first input chunk to the first TTS audio chunk");
        let e2e_utterance_end_to_first_audio = histogram("voice_e2e_utterance_end_to_tts_first_audio_seconds", "Seconds from the final input chunk to the first TTS audio chunk");
        let e2e_input_to_complete = histogram("voice_e2e_input_to_tts_complete_seconds", "Seconds from the first input chunk to pipeline completion");
        let e2e_utterance_end_to_complete = histogram("voice_e2e_utterance_end_to_tts_complete_seconds", "Seconds from the final input chunk to pipeline completion");
        let asr_duration = histogram("voice_asr_duration_seconds", "Seconds from ASR start to final ASR event");
        let pipeline_queue_duration = histogram("voice_pipeline_queue_duration_seconds", "Seconds from final input chunk until the ASR request starts");
        let llm_time_to_first_token = histogram("voice_llm_time_to_first_token_seconds", "Seconds from LLM/TTS stream start to the first non-empty LLM delta");
        let llm_duration = histogram("voice_llm_duration_seconds", "Seconds from LLM/TTS stream start to the final LLM delta");
        let llm_route_duration = histogram(
            "voice_llm_route_duration_seconds",
            "Seconds spent selecting the LLM model tier",
        );
        let llm_route_total = IntCounterVec::new(
            prometheus::Opts::new("voice_llm_route_total", "LLM turns selected by model tier"),
            &["route"],
        )
        .expect("valid LLM route counter");
        let llm_escalation_total = IntCounterVec::new(
            prometheus::Opts::new(
                "voice_llm_escalation_total",
                "LLM attempts escalated from fast to strong",
            ),
            &["from", "to", "reason"],
        )
        .expect("valid LLM escalation counter");
        let tts_time_to_first_audio = histogram("voice_tts_time_to_first_audio_seconds", "Seconds from TTS input.done to the first TTS audio chunk");
        let tts_generation_duration = histogram("voice_tts_generation_duration_seconds", "Seconds from TTS input.done to session.done");
        let pipeline_duration = histogram("voice_pipeline_duration_seconds", "Total server-side pipeline duration");

        for collector in [
            Box::new(requests_total.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(requests_finished_total.clone()), Box::new(requests_success_total.clone()), Box::new(requests_failed_total.clone()),
            Box::new(requests_timeout_total.clone()), Box::new(requests_cancelled_total.clone()), Box::new(e2e_input_to_first_audio.clone()),
            Box::new(tts_ws_connect_total.clone()), Box::new(tts_ws_connect_failed_total.clone()), Box::new(tts_provider_errors_total.clone()),
            Box::new(tts_input_chars_total.clone()), Box::new(tts_audio_chunks_total.clone()), Box::new(tts_audio_bytes_total.clone()),
            Box::new(tts_ws_pool_connections.clone()), Box::new(tts_ws_pool_active_connections.clone()), Box::new(tts_ws_pool_idle_connections.clone()), Box::new(tts_ws_pool_waiting.clone()), Box::new(tts_ws_pool_wait_duration.clone()), Box::new(tts_ws_pool_reaped_total.clone()), Box::new(tts_ws_pool_invalidated_total.clone()), Box::new(tts_audio_duration.clone()), Box::new(tts_audio_chunk_interval.clone()), Box::new(tts_realtime_factor.clone()), Box::new(e2e_client_playback_delay.clone()),
            Box::new(requests_retried_total.clone()), Box::new(tts_input_wait.clone()),
            Box::new(e2e_utterance_end_to_first_audio.clone()), Box::new(e2e_input_to_complete.clone()), Box::new(e2e_utterance_end_to_complete.clone()),
            Box::new(asr_duration.clone()), Box::new(pipeline_queue_duration.clone()), Box::new(llm_time_to_first_token.clone()),
            Box::new(llm_duration.clone()), Box::new(llm_route_duration.clone()), Box::new(llm_route_total.clone()),
            Box::new(llm_escalation_total.clone()), Box::new(tts_time_to_first_audio.clone()), Box::new(tts_generation_duration.clone()), Box::new(pipeline_duration.clone()),
        ] {
            registry.register(collector).expect("voice metric names should be unique");
        }
        Self { registry, requests_total, requests_finished_total, requests_success_total, requests_failed_total, requests_timeout_total, requests_cancelled_total, tts_ws_connect_total, tts_ws_connect_failed_total, tts_provider_errors_total, tts_input_chars_total, tts_audio_chunks_total, tts_audio_bytes_total, tts_ws_pool_connections, tts_ws_pool_active_connections, tts_ws_pool_idle_connections, tts_ws_pool_waiting, tts_ws_pool_wait_duration, tts_ws_pool_reaped_total, tts_ws_pool_invalidated_total, tts_audio_duration, tts_audio_chunk_interval, tts_realtime_factor, e2e_client_playback_delay, requests_retried_total, tts_input_wait, e2e_input_to_first_audio, e2e_utterance_end_to_first_audio, e2e_input_to_complete, e2e_utterance_end_to_complete, asr_duration, pipeline_queue_duration, llm_time_to_first_token, llm_duration, llm_route_duration, llm_route_total, llm_escalation_total, tts_time_to_first_audio, tts_generation_duration, pipeline_duration }
    }

    pub fn start_pipeline(self: &Arc<Self>, input_started_at: Instant, input_ended_at: Instant) -> PipelineMetricsGuard {
        PipelineMetricsGuard::start(self.clone(), input_started_at, input_ended_at)
    }

    pub fn render(&self) -> String {
        let mut encoded = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut encoded).expect("encoding metrics should not fail");
        String::from_utf8(encoded).expect("prometheus output is utf8")
    }
}

impl VoiceMetricsSink for VoiceMetrics {
    fn pipeline_started(&self) { self.requests_total.inc(); }
    fn observe_pipeline_duration(&self, d: Duration) { self.pipeline_duration.observe(d.as_secs_f64()); }
    fn observe_queue(&self, d: Duration) { self.pipeline_queue_duration.observe(d.as_secs_f64()); }
    fn observe_asr(&self, d: Duration) { self.asr_duration.observe(d.as_secs_f64()); }
    fn observe_llm_first_token(&self, d: Duration) { self.llm_time_to_first_token.observe(d.as_secs_f64()); }
    fn observe_llm_complete(&self, d: Duration) { self.llm_duration.observe(d.as_secs_f64()); }
    fn observe_llm_route(&self, tier: ModelTier, d: Duration) {
        self.llm_route_duration.observe(d.as_secs_f64());
        self.llm_route_total.with_label_values(&[tier.as_str()]).inc();
    }
    fn llm_escalated(&self, reason: EscalationReason) {
        self.llm_escalation_total
            .with_label_values(&["fast", "strong", reason.label()])
            .inc();
    }
    fn observe_tts_first_audio(&self, d: Duration) { self.tts_time_to_first_audio.observe(d.as_secs_f64()); }
    fn observe_tts_complete(&self, d: Duration) { self.tts_generation_duration.observe(d.as_secs_f64()); }
    fn observe_e2e_first_audio(&self, start: Duration, end: Duration) { self.e2e_input_to_first_audio.observe(start.as_secs_f64()); self.e2e_utterance_end_to_first_audio.observe(end.as_secs_f64()); }
    fn observe_e2e_complete(&self, start: Duration, end: Duration) { self.e2e_input_to_complete.observe(start.as_secs_f64()); self.e2e_utterance_end_to_complete.observe(end.as_secs_f64()); }
    fn pipeline_finished(&self, result: PipelineResult) {
        self.requests_finished_total.with_label_values(&[result.label()]).inc();
        match result { PipelineResult::Success => self.requests_success_total.inc(), PipelineResult::Timeout => self.requests_timeout_total.inc(), PipelineResult::Cancelled => self.requests_cancelled_total.inc(), PipelineResult::Failed | PipelineResult::EmptyResponse => self.requests_failed_total.inc() }
    }
    fn tts_ws_connect(&self) { self.tts_ws_connect_total.inc(); }
    fn tts_ws_connect_failed(&self) { self.tts_ws_connect_failed_total.inc(); }
    fn tts_provider_error(&self) { self.tts_provider_errors_total.inc(); }
    fn tts_input_chars(&self, chars: u64) { self.tts_input_chars_total.inc_by(chars); }
    fn tts_audio_chunk(&self, bytes: u64) { self.tts_audio_chunks_total.inc(); self.tts_audio_bytes_total.inc_by(bytes); }
    fn tts_ws_pool_connection_opened(&self) { self.tts_ws_pool_connections.inc(); }
    fn tts_ws_pool_connection_closed(&self) { self.tts_ws_pool_connections.dec(); }
    fn tts_ws_pool_active(&self, value: i64) { self.tts_ws_pool_active_connections.set(value); }
    fn tts_ws_pool_idle(&self, value: i64) { self.tts_ws_pool_idle_connections.set(value); }
    fn tts_ws_pool_waiting(&self, value: i64) { self.tts_ws_pool_waiting.set(value); }
    fn tts_ws_pool_wait_started(&self) { self.tts_ws_pool_waiting.inc(); }
    fn tts_ws_pool_wait_finished(&self, d: Duration) { self.tts_ws_pool_waiting.dec(); self.tts_ws_pool_wait_duration.observe(d.as_secs_f64()); }
    fn observe_tts_ws_pool_wait(&self, d: Duration) { self.tts_ws_pool_wait_duration.observe(d.as_secs_f64()); }
    fn tts_ws_pool_reaped(&self) { self.tts_ws_pool_reaped_total.inc(); }
    fn tts_ws_pool_invalidated(&self) { self.tts_ws_pool_invalidated_total.inc(); }
    fn observe_tts_audio_duration(&self, d: Duration) { self.tts_audio_duration.observe(d.as_secs_f64()); }
    fn observe_tts_audio_chunk_interval(&self, d: Duration) { self.tts_audio_chunk_interval.observe(d.as_secs_f64()); }
    fn observe_tts_realtime_factor(&self, value: f64) { if value.is_finite() && value >= 0.0 { self.tts_realtime_factor.observe(value); } }
    fn observe_e2e_client_playback_delay(&self, d: Duration) { if d <= Duration::from_secs(30) { self.e2e_client_playback_delay.observe(d.as_secs_f64()); } }
    fn request_retried(&self) { self.requests_retried_total.inc(); }
    fn observe_tts_input_wait(&self, d: Duration) { self.tts_input_wait.observe(d.as_secs_f64()); }
}

pub struct PipelineMetricsGuard {
    metrics: Arc<dyn VoiceMetricsSink>,
    input_started_at: Instant,
    input_ended_at: Instant,
    started_at: Instant,
    first_audio_recorded: bool,
    completed_recorded: bool,
    result: PipelineResult,
}

impl PipelineMetricsGuard {
    pub fn start(metrics: Arc<dyn VoiceMetricsSink>, input_started_at: Instant, input_ended_at: Instant) -> Self {
        metrics.pipeline_started();
        Self { metrics, input_started_at, input_ended_at, started_at: Instant::now(), first_audio_recorded: false, completed_recorded: false, result: PipelineResult::Failed }
    }
    pub fn has_first_audio(&self) -> bool { self.first_audio_recorded }
    pub fn record_first_audio(&mut self, at: Instant) {
        if self.first_audio_recorded { return; }
        self.first_audio_recorded = true;
        self.metrics.observe_e2e_first_audio(at.duration_since(self.input_started_at), at.duration_since(self.input_ended_at));
    }
    pub fn finish(&mut self, result: PipelineResult, at: Instant) {
        if self.completed_recorded { return; }
        self.completed_recorded = true;
        self.result = result;
        self.metrics.observe_e2e_complete(at.duration_since(self.input_started_at), at.duration_since(self.input_ended_at));
    }

    pub fn set_result(&mut self, result: PipelineResult) {
        self.result = result;
    }
}

impl Drop for PipelineMetricsGuard {
    fn drop(&mut self) {
        self.metrics.observe_pipeline_duration(self.started_at.elapsed());
        self.metrics.pipeline_finished(self.result);
    }
}

pub async fn handler(metrics: web::Data<Arc<VoiceMetrics>>) -> HttpResponse {
    HttpResponse::Ok().content_type("text/plain; version=0.0.4").body(metrics.render())
}

fn counter(name: &str, help: &str) -> IntCounter { IntCounter::with_opts(prometheus::Opts::new(name, help)).expect("valid voice counter") }
fn gauge(name: &str, help: &str) -> IntGauge { IntGauge::with_opts(prometheus::Opts::new(name, help)).expect("valid voice gauge") }
fn histogram(name: &str, help: &str) -> Histogram {
    Histogram::with_opts(
        HistogramOpts::new(name, help)
            .buckets(vec![0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]),
    )
    .expect("valid voice histogram")
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test as actix_test, App};
    #[test]
    fn noop_sink_accepts_pipeline_events_without_prometheus() {
        let sink: Arc<dyn VoiceMetricsSink> = Arc::new(NoopMetricsSink);
        let now = Instant::now();
        let mut guard = PipelineMetricsGuard::start(sink, now, now);
        guard.record_first_audio(now + Duration::from_millis(10));
        guard.finish(PipelineResult::Success, now + Duration::from_millis(20));
    }
    #[test]
    fn renders_pipeline_metrics_without_high_cardinality_labels() {
        let metrics = Arc::new(VoiceMetrics::new());
        let now = Instant::now();
        let mut guard = metrics.start_pipeline(now, now);
        guard.record_first_audio(now + Duration::from_millis(25));
        guard.finish(PipelineResult::Success, now + Duration::from_millis(50));
        drop(guard);
        metrics.tts_ws_connect();
        metrics.tts_input_chars(12);
        metrics.tts_audio_chunk(320);
        metrics.observe_tts_first_audio(Duration::from_millis(30));
        metrics.observe_tts_complete(Duration::from_millis(80));
        let output = metrics.render();
        assert!(output.contains("voice_e2e_utterance_end_to_tts_first_audio_seconds"));
        assert!(output.contains("voice_requests_finished_total{result=\"success\"}"));
        assert!(output.contains("voice_tts_audio_bytes_total 320"));
        assert!(output.contains("voice_tts_time_to_first_audio_seconds_count 1"));
        assert!(!output.contains("session_id"));
    }

    #[test]
    fn renders_low_cardinality_llm_route_and_escalation_metrics() {
        let metrics = VoiceMetrics::new();

        metrics.observe_llm_route(ModelTier::Fast, Duration::from_millis(1));
        metrics.llm_escalated(EscalationReason::EmptyResponse);

        let output = metrics.render();
        assert!(output.contains("voice_llm_route_total{route=\"fast\"} 1"));
        assert!(output.contains(
            "voice_llm_escalation_total{from=\"fast\",reason=\"empty_response\",to=\"strong\"} 1"
        ));
        assert!(!output.contains("session_id"));
    }

    #[test]
    fn bounds_client_playback_delay_and_keeps_it_unlabelled() {
        let metrics = VoiceMetrics::new();
        metrics.observe_e2e_client_playback_delay(Duration::from_secs(31));
        metrics.observe_e2e_client_playback_delay(Duration::from_millis(120));
        let output = metrics.render();
        assert!(output.contains("voice_e2e_tts_first_audio_to_client_playback_seconds_count 1"));
        assert!(!output.contains("request_id"));
    }

    #[actix_web::test]
    async fn metrics_handler_exposes_prometheus_text() {
        let metrics = Arc::new(VoiceMetrics::new());
        let app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(metrics))
                .route("/metrics/voice", web::get().to(handler)),
        )
        .await;
        let response = actix_test::call_service(
            &app,
            actix_test::TestRequest::get().uri("/metrics/voice").to_request(),
        )
        .await;
        assert!(response.status().is_success());
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain; version=0.0.4"
        );
        let body = actix_test::read_body(response).await;
        assert!(std::str::from_utf8(&body).unwrap().contains("voice_requests_total"));
    }
}
