use std::sync::Arc;
use std::time::{Duration, Instant};

use actix_web::{web, HttpResponse};
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Registry, TextEncoder,
};

use crate::client::llm::ModelTier;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PipelineResult {
    Success,
    Failed,
    Timeout,
    Cancelled,
    EmptyResponse,
}

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
    fn observe_input_end_to_asr_output_end(&self, _duration: Duration) {}
    fn observe_input_end_to_llm_first_text(&self, _duration: Duration) {}
    fn observe_input_end_to_ws_first_audio_sent(&self, _duration: Duration) {}
    fn observe_llm_first_text_to_tts_first_frame(&self, _duration: Duration) {}
    fn observe_tts_first_frame_to_ws_first_audio_sent(&self, _duration: Duration) {}
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
    fn observe_client_first_audio_received_to_playback(&self, _duration: Duration) {}
    fn observe_client_input_end_to_final_audio_sent(&self, _duration: Duration) {}
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
    client_first_audio_received_to_playback: Histogram,
    client_input_end_to_final_audio_sent: Histogram,
    requests_retried_total: IntCounter,
    tts_input_wait: Histogram,
    e2e_input_to_first_audio: Histogram,
    input_end_to_asr_output_end: Histogram,
    input_end_to_llm_first_text: Histogram,
    input_end_to_tts_first_frame: Histogram,
    input_end_to_ws_first_audio_sent: Histogram,
    llm_first_text_to_tts_first_frame: Histogram,
    tts_first_frame_to_ws_first_audio_sent: Histogram,
    e2e_input_to_complete: Histogram,
    e2e_utterance_end_to_complete: Histogram,
    asr_input_to_output_end: Histogram,
    pipeline_queue_duration: Histogram,
    llm_input_to_first_text: Histogram,
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
        let requests_finished_total = IntCounterVec::new(
            prometheus::Opts::new(
                "voice_requests_finished_total",
                "Voice utterance pipelines finished",
            ),
            &["result"],
        )
        .expect("valid voice request result counter");
        let requests_success_total = counter(
            "voice_requests_success_total",
            "Voice pipelines completed successfully",
        );
        let requests_failed_total =
            counter("voice_requests_failed_total", "Voice pipelines failed");
        let requests_timeout_total =
            counter("voice_requests_timeout_total", "Voice pipelines timed out");
        let requests_cancelled_total = counter(
            "voice_requests_cancelled_total",
            "Voice pipelines cancelled",
        );
        let tts_ws_connect_total = counter(
            "voice_tts_ws_connect_total",
            "TTS WebSocket connection attempts",
        );
        let tts_ws_connect_failed_total = counter(
            "voice_tts_ws_connect_failed_total",
            "TTS WebSocket connection failures",
        );
        let tts_provider_errors_total =
            counter("voice_tts_provider_errors_total", "TTS provider errors");
        let tts_input_chars_total =
            counter("voice_tts_input_chars_total", "Characters submitted to TTS");
        let tts_audio_chunks_total =
            counter("voice_tts_audio_chunks_total", "TTS audio chunks received");
        let tts_audio_bytes_total =
            counter("voice_tts_audio_bytes_total", "TTS audio bytes received");
        let tts_ws_pool_connections = gauge(
            "voice_tts_ws_pool_connections",
            "TTS WebSocket pool connections",
        );
        let tts_ws_pool_active_connections = gauge(
            "voice_tts_ws_pool_active_connections",
            "TTS WebSocket pool active connections",
        );
        let tts_ws_pool_idle_connections = gauge(
            "voice_tts_ws_pool_idle_connections",
            "TTS WebSocket pool idle connections",
        );
        let tts_ws_pool_waiting = gauge(
            "voice_tts_ws_pool_waiting",
            "Requests waiting for a TTS WebSocket pool connection",
        );
        let tts_ws_pool_wait_duration = histogram(
            "voice_tts_ws_pool_wait_duration_seconds",
            "Seconds spent waiting for a TTS WebSocket pool connection",
        );
        let tts_ws_pool_reaped_total = counter(
            "voice_tts_ws_pool_reaped_total",
            "TTS WebSocket pool connections reaped after idling",
        );
        let tts_ws_pool_invalidated_total = counter(
            "voice_tts_ws_pool_invalidated_total",
            "TTS WebSocket pool connections invalidated",
        );
        let tts_audio_duration = histogram(
            "voice_tts_audio_duration_seconds",
            "Duration represented by TTS audio output",
        );
        let tts_audio_chunk_interval = histogram(
            "voice_tts_audio_chunk_interval_seconds",
            "Interval between TTS audio chunks",
        );
        let tts_realtime_factor = histogram(
            "voice_tts_realtime_factor",
            "TTS audio duration divided by generation duration",
        );
        let client_first_audio_received_to_playback = histogram(
            "voice_client_first_audio_received_to_playback_seconds",
            "Seconds from first TTS audio received by the client to playback start",
        );
        let client_input_end_to_final_audio_sent = histogram("voice_client_input_end_to_final_audio_sent_seconds", "Seconds from client input end detection until the final audio frame is accepted by WebSocket.send");
        let requests_retried_total =
            counter("voice_requests_retried_total", "Voice requests retried");
        let tts_input_wait = histogram(
            "voice_tts_input_wait_seconds",
            "Seconds from TTS input.text to input.done",
        );
        let e2e_input_to_first_audio = histogram(
            "voice_e2e_input_to_tts_first_audio_seconds",
            "Seconds from the first input chunk to the first TTS audio chunk",
        );
        let input_end_to_asr_output_end = histogram(
            "voice_input_end_to_asr_output_end_seconds",
            "Seconds from receiving the final input audio frame to the final ASR output",
        );
        let input_end_to_llm_first_text = histogram(
            "voice_input_end_to_llm_first_text_seconds",
            "Seconds from receiving the final input audio frame to the first non-empty LLM text",
        );
        let input_end_to_tts_first_frame = histogram("voice_input_end_to_tts_first_frame_seconds", "Seconds from receiving the final input audio frame to the first non-empty TTS audio frame");
        let input_end_to_ws_first_audio_sent = histogram("voice_input_end_to_ws_first_audio_sent_seconds", "Seconds from receiving the final input audio frame until the first audio WebSocket message enters the outbound queue");
        let llm_first_text_to_tts_first_frame = histogram(
            "voice_llm_first_text_to_tts_first_frame_seconds",
            "Seconds from the first non-empty LLM text to the first non-empty TTS audio frame",
        );
        let tts_first_frame_to_ws_first_audio_sent = histogram("voice_tts_first_frame_to_ws_first_audio_sent_seconds", "Seconds from the first non-empty TTS audio frame until its WebSocket message enters the outbound queue");
        let e2e_input_to_complete = histogram(
            "voice_e2e_input_to_tts_complete_seconds",
            "Seconds from the first input chunk to pipeline completion",
        );
        let e2e_utterance_end_to_complete = histogram(
            "voice_e2e_utterance_end_to_tts_complete_seconds",
            "Seconds from the final input chunk to pipeline completion",
        );
        let asr_input_to_output_end = histogram(
            "voice_asr_input_to_output_end_seconds",
            "Seconds from ASR input to the final ASR output",
        );
        let pipeline_queue_duration = histogram(
            "voice_pipeline_queue_duration_seconds",
            "Seconds from final input chunk until the ASR request starts",
        );
        let llm_input_to_first_text = histogram(
            "voice_llm_input_to_first_text_seconds",
            "Seconds from LLM input to the first non-empty LLM text",
        );
        let llm_duration = histogram(
            "voice_llm_duration_seconds",
            "Seconds from LLM/TTS stream start to the final LLM delta",
        );
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
        let tts_time_to_first_audio = histogram(
            "voice_tts_time_to_first_audio_seconds",
            "Seconds from TTS input.done to the first TTS audio chunk",
        );
        let tts_generation_duration = histogram(
            "voice_tts_generation_duration_seconds",
            "Seconds from TTS input.done to session.done",
        );
        let pipeline_duration = histogram(
            "voice_pipeline_duration_seconds",
            "Total server-side pipeline duration",
        );

        for collector in [
            Box::new(requests_total.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(requests_finished_total.clone()),
            Box::new(requests_success_total.clone()),
            Box::new(requests_failed_total.clone()),
            Box::new(requests_timeout_total.clone()),
            Box::new(requests_cancelled_total.clone()),
            Box::new(e2e_input_to_first_audio.clone()),
            Box::new(tts_ws_connect_total.clone()),
            Box::new(tts_ws_connect_failed_total.clone()),
            Box::new(tts_provider_errors_total.clone()),
            Box::new(tts_input_chars_total.clone()),
            Box::new(tts_audio_chunks_total.clone()),
            Box::new(tts_audio_bytes_total.clone()),
            Box::new(tts_ws_pool_connections.clone()),
            Box::new(tts_ws_pool_active_connections.clone()),
            Box::new(tts_ws_pool_idle_connections.clone()),
            Box::new(tts_ws_pool_waiting.clone()),
            Box::new(tts_ws_pool_wait_duration.clone()),
            Box::new(tts_ws_pool_reaped_total.clone()),
            Box::new(tts_ws_pool_invalidated_total.clone()),
            Box::new(tts_audio_duration.clone()),
            Box::new(tts_audio_chunk_interval.clone()),
            Box::new(tts_realtime_factor.clone()),
            Box::new(client_first_audio_received_to_playback.clone()),
            Box::new(client_input_end_to_final_audio_sent.clone()),
            Box::new(requests_retried_total.clone()),
            Box::new(tts_input_wait.clone()),
            Box::new(input_end_to_asr_output_end.clone()),
            Box::new(input_end_to_llm_first_text.clone()),
            Box::new(input_end_to_tts_first_frame.clone()),
            Box::new(input_end_to_ws_first_audio_sent.clone()),
            Box::new(llm_first_text_to_tts_first_frame.clone()),
            Box::new(tts_first_frame_to_ws_first_audio_sent.clone()),
            Box::new(e2e_input_to_complete.clone()),
            Box::new(e2e_utterance_end_to_complete.clone()),
            Box::new(asr_input_to_output_end.clone()),
            Box::new(pipeline_queue_duration.clone()),
            Box::new(llm_input_to_first_text.clone()),
            Box::new(llm_duration.clone()),
            Box::new(llm_route_duration.clone()),
            Box::new(llm_route_total.clone()),
            Box::new(llm_escalation_total.clone()),
            Box::new(tts_time_to_first_audio.clone()),
            Box::new(tts_generation_duration.clone()),
            Box::new(pipeline_duration.clone()),
        ] {
            registry
                .register(collector)
                .expect("voice metric names should be unique");
        }
        Self {
            registry,
            requests_total,
            requests_finished_total,
            requests_success_total,
            requests_failed_total,
            requests_timeout_total,
            requests_cancelled_total,
            tts_ws_connect_total,
            tts_ws_connect_failed_total,
            tts_provider_errors_total,
            tts_input_chars_total,
            tts_audio_chunks_total,
            tts_audio_bytes_total,
            tts_ws_pool_connections,
            tts_ws_pool_active_connections,
            tts_ws_pool_idle_connections,
            tts_ws_pool_waiting,
            tts_ws_pool_wait_duration,
            tts_ws_pool_reaped_total,
            tts_ws_pool_invalidated_total,
            tts_audio_duration,
            tts_audio_chunk_interval,
            tts_realtime_factor,
            client_first_audio_received_to_playback,
            client_input_end_to_final_audio_sent,
            requests_retried_total,
            tts_input_wait,
            e2e_input_to_first_audio,
            input_end_to_asr_output_end,
            input_end_to_llm_first_text,
            input_end_to_tts_first_frame,
            input_end_to_ws_first_audio_sent,
            llm_first_text_to_tts_first_frame,
            tts_first_frame_to_ws_first_audio_sent,
            e2e_input_to_complete,
            e2e_utterance_end_to_complete,
            asr_input_to_output_end,
            pipeline_queue_duration,
            llm_input_to_first_text,
            llm_duration,
            llm_route_duration,
            llm_route_total,
            llm_escalation_total,
            tts_time_to_first_audio,
            tts_generation_duration,
            pipeline_duration,
        }
    }

    pub fn start_pipeline(
        self: &Arc<Self>,
        input_started_at: Instant,
        input_ended_at: Instant,
    ) -> PipelineMetricsGuard {
        PipelineMetricsGuard::start(self.clone(), input_started_at, input_ended_at)
    }

    pub fn render(&self) -> String {
        let mut encoded = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut encoded)
            .expect("encoding metrics should not fail");
        String::from_utf8(encoded).expect("prometheus output is utf8")
    }
}

impl VoiceMetricsSink for VoiceMetrics {
    fn pipeline_started(&self) {
        self.requests_total.inc();
    }
    fn observe_pipeline_duration(&self, d: Duration) {
        self.pipeline_duration.observe(d.as_secs_f64());
    }
    fn observe_queue(&self, d: Duration) {
        self.pipeline_queue_duration.observe(d.as_secs_f64());
    }
    fn observe_asr(&self, d: Duration) {
        self.asr_input_to_output_end.observe(d.as_secs_f64());
    }
    fn observe_llm_first_token(&self, d: Duration) {
        self.llm_input_to_first_text.observe(d.as_secs_f64());
    }
    fn observe_llm_complete(&self, d: Duration) {
        self.llm_duration.observe(d.as_secs_f64());
    }
    fn observe_llm_route(&self, tier: ModelTier, d: Duration) {
        self.llm_route_duration.observe(d.as_secs_f64());
        self.llm_route_total
            .with_label_values(&[tier.as_str()])
            .inc();
    }
    fn llm_escalated(&self, reason: EscalationReason) {
        self.llm_escalation_total
            .with_label_values(&["fast", "strong", reason.label()])
            .inc();
    }
    fn observe_tts_first_audio(&self, d: Duration) {
        self.tts_time_to_first_audio.observe(d.as_secs_f64());
    }
    fn observe_tts_complete(&self, d: Duration) {
        self.tts_generation_duration.observe(d.as_secs_f64());
    }
    fn observe_e2e_first_audio(&self, start: Duration, end: Duration) {
        self.e2e_input_to_first_audio.observe(start.as_secs_f64());
        self.input_end_to_tts_first_frame.observe(end.as_secs_f64());
    }
    fn observe_input_end_to_asr_output_end(&self, d: Duration) {
        self.input_end_to_asr_output_end.observe(d.as_secs_f64());
    }
    fn observe_input_end_to_llm_first_text(&self, d: Duration) {
        self.input_end_to_llm_first_text.observe(d.as_secs_f64());
    }
    fn observe_input_end_to_ws_first_audio_sent(&self, d: Duration) {
        self.input_end_to_ws_first_audio_sent
            .observe(d.as_secs_f64());
    }
    fn observe_llm_first_text_to_tts_first_frame(&self, d: Duration) {
        self.llm_first_text_to_tts_first_frame
            .observe(d.as_secs_f64());
    }
    fn observe_tts_first_frame_to_ws_first_audio_sent(&self, d: Duration) {
        self.tts_first_frame_to_ws_first_audio_sent
            .observe(d.as_secs_f64());
    }
    fn observe_e2e_complete(&self, start: Duration, end: Duration) {
        self.e2e_input_to_complete.observe(start.as_secs_f64());
        self.e2e_utterance_end_to_complete
            .observe(end.as_secs_f64());
    }
    fn pipeline_finished(&self, result: PipelineResult) {
        self.requests_finished_total
            .with_label_values(&[result.label()])
            .inc();
        match result {
            PipelineResult::Success => self.requests_success_total.inc(),
            PipelineResult::Timeout => self.requests_timeout_total.inc(),
            PipelineResult::Cancelled => self.requests_cancelled_total.inc(),
            PipelineResult::Failed | PipelineResult::EmptyResponse => {
                self.requests_failed_total.inc()
            }
        }
    }
    fn tts_ws_connect(&self) {
        self.tts_ws_connect_total.inc();
    }
    fn tts_ws_connect_failed(&self) {
        self.tts_ws_connect_failed_total.inc();
    }
    fn tts_provider_error(&self) {
        self.tts_provider_errors_total.inc();
    }
    fn tts_input_chars(&self, chars: u64) {
        self.tts_input_chars_total.inc_by(chars);
    }
    fn tts_audio_chunk(&self, bytes: u64) {
        self.tts_audio_chunks_total.inc();
        self.tts_audio_bytes_total.inc_by(bytes);
    }
    fn tts_ws_pool_connection_opened(&self) {
        self.tts_ws_pool_connections.inc();
    }
    fn tts_ws_pool_connection_closed(&self) {
        self.tts_ws_pool_connections.dec();
    }
    fn tts_ws_pool_active(&self, value: i64) {
        self.tts_ws_pool_active_connections.set(value);
    }
    fn tts_ws_pool_idle(&self, value: i64) {
        self.tts_ws_pool_idle_connections.set(value);
    }
    fn tts_ws_pool_waiting(&self, value: i64) {
        self.tts_ws_pool_waiting.set(value);
    }
    fn tts_ws_pool_wait_started(&self) {
        self.tts_ws_pool_waiting.inc();
    }
    fn tts_ws_pool_wait_finished(&self, d: Duration) {
        self.tts_ws_pool_waiting.dec();
        self.tts_ws_pool_wait_duration.observe(d.as_secs_f64());
    }
    fn observe_tts_ws_pool_wait(&self, d: Duration) {
        self.tts_ws_pool_wait_duration.observe(d.as_secs_f64());
    }
    fn tts_ws_pool_reaped(&self) {
        self.tts_ws_pool_reaped_total.inc();
    }
    fn tts_ws_pool_invalidated(&self) {
        self.tts_ws_pool_invalidated_total.inc();
    }
    fn observe_tts_audio_duration(&self, d: Duration) {
        self.tts_audio_duration.observe(d.as_secs_f64());
    }
    fn observe_tts_audio_chunk_interval(&self, d: Duration) {
        self.tts_audio_chunk_interval.observe(d.as_secs_f64());
    }
    fn observe_tts_realtime_factor(&self, value: f64) {
        if value.is_finite() && value >= 0.0 {
            self.tts_realtime_factor.observe(value);
        }
    }
    fn observe_client_first_audio_received_to_playback(&self, d: Duration) {
        if d <= Duration::from_secs(30) {
            self.client_first_audio_received_to_playback
                .observe(d.as_secs_f64());
        }
    }
    fn observe_client_input_end_to_final_audio_sent(&self, d: Duration) {
        if d <= Duration::from_secs(30) {
            self.client_input_end_to_final_audio_sent
                .observe(d.as_secs_f64());
        }
    }
    fn request_retried(&self) {
        self.requests_retried_total.inc();
    }
    fn observe_tts_input_wait(&self, d: Duration) {
        self.tts_input_wait.observe(d.as_secs_f64());
    }
}

pub struct PipelineMetricsGuard {
    metrics: Arc<dyn VoiceMetricsSink>,
    input_started_at: Instant,
    input_ended_at: Instant,
    started_at: Instant,
    llm_first_text_at: Option<Instant>,
    tts_first_frame_at: Option<Instant>,
    first_audio_ws_sent_recorded: bool,
    completed_recorded: bool,
    result: PipelineResult,
}

impl PipelineMetricsGuard {
    pub fn start(
        metrics: Arc<dyn VoiceMetricsSink>,
        input_started_at: Instant,
        input_ended_at: Instant,
    ) -> Self {
        metrics.pipeline_started();
        Self {
            metrics,
            input_started_at,
            input_ended_at,
            started_at: Instant::now(),
            llm_first_text_at: None,
            tts_first_frame_at: None,
            first_audio_ws_sent_recorded: false,
            completed_recorded: false,
            result: PipelineResult::Failed,
        }
    }
    pub fn has_first_audio(&self) -> bool {
        self.tts_first_frame_at.is_some()
    }
    pub fn record_first_audio(&mut self, at: Instant) {
        self.record_tts_first_frame(at);
    }
    pub fn record_asr_output_end(&self, asr_started_at: Instant, at: Instant) {
        self.metrics
            .observe_input_end_to_asr_output_end(at.duration_since(self.input_ended_at));
        self.metrics.observe_asr(at.duration_since(asr_started_at));
    }
    pub fn record_llm_first_text(&mut self, llm_started_at: Instant, at: Instant) {
        if self.llm_first_text_at.is_some() {
            return;
        }
        self.llm_first_text_at = Some(at);
        self.metrics
            .observe_input_end_to_llm_first_text(at.duration_since(self.input_ended_at));
        self.metrics
            .observe_llm_first_token(at.duration_since(llm_started_at));
    }
    pub fn record_tts_first_frame(&mut self, at: Instant) {
        if self.tts_first_frame_at.is_some() {
            return;
        }
        self.tts_first_frame_at = Some(at);
        self.metrics.observe_e2e_first_audio(
            at.duration_since(self.input_started_at),
            at.duration_since(self.input_ended_at),
        );
        if let Some(llm_first_text_at) = self.llm_first_text_at {
            self.metrics
                .observe_llm_first_text_to_tts_first_frame(at.duration_since(llm_first_text_at));
        }
    }
    pub fn record_first_audio_ws_sent(&mut self, at: Instant) {
        if self.first_audio_ws_sent_recorded {
            return;
        }
        let Some(tts_first_frame_at) = self.tts_first_frame_at else {
            return;
        };
        self.first_audio_ws_sent_recorded = true;
        self.metrics
            .observe_input_end_to_ws_first_audio_sent(at.duration_since(self.input_ended_at));
        self.metrics
            .observe_tts_first_frame_to_ws_first_audio_sent(at.duration_since(tts_first_frame_at));
    }
    pub fn finish(&mut self, result: PipelineResult, at: Instant) {
        if self.completed_recorded {
            return;
        }
        self.completed_recorded = true;
        self.result = result;
        self.metrics.observe_e2e_complete(
            at.duration_since(self.input_started_at),
            at.duration_since(self.input_ended_at),
        );
    }

    pub fn set_result(&mut self, result: PipelineResult) {
        self.result = result;
    }
}

impl Drop for PipelineMetricsGuard {
    fn drop(&mut self) {
        self.metrics
            .observe_pipeline_duration(self.started_at.elapsed());
        self.metrics.pipeline_finished(self.result);
    }
}

pub async fn handler(metrics: web::Data<Arc<VoiceMetrics>>) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4")
        .body(metrics.render())
}

fn counter(name: &str, help: &str) -> IntCounter {
    IntCounter::with_opts(prometheus::Opts::new(name, help)).expect("valid voice counter")
}
fn gauge(name: &str, help: &str) -> IntGauge {
    IntGauge::with_opts(prometheus::Opts::new(name, help)).expect("valid voice gauge")
}
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
    fn renders_latency_contract_metrics() {
        let metrics = Arc::new(VoiceMetrics::new());
        let now = Instant::now();
        let input_ended_at = now + Duration::from_millis(10);
        let mut guard = metrics.start_pipeline(now, input_ended_at);
        guard.record_asr_output_end(
            now + Duration::from_millis(20),
            now + Duration::from_millis(30),
        );
        guard.record_llm_first_text(
            now + Duration::from_millis(40),
            now + Duration::from_millis(50),
        );
        guard.record_tts_first_frame(now + Duration::from_millis(70));
        guard.record_first_audio_ws_sent(now + Duration::from_millis(75));
        guard.finish(PipelineResult::Success, now + Duration::from_millis(80));
        drop(guard);
        metrics.observe_client_first_audio_received_to_playback(Duration::from_millis(25));
        metrics.observe_client_input_end_to_final_audio_sent(Duration::from_millis(2));

        let output = metrics.render();
        for name in [
            "voice_input_end_to_asr_output_end_seconds",
            "voice_input_end_to_llm_first_text_seconds",
            "voice_input_end_to_tts_first_frame_seconds",
            "voice_input_end_to_ws_first_audio_sent_seconds",
            "voice_asr_input_to_output_end_seconds",
            "voice_llm_input_to_first_text_seconds",
            "voice_llm_first_text_to_tts_first_frame_seconds",
            "voice_tts_first_frame_to_ws_first_audio_sent_seconds",
            "voice_client_first_audio_received_to_playback_seconds",
            "voice_client_input_end_to_final_audio_sent_seconds",
        ] {
            assert!(
                output.contains(&format!("{name}_count 1")),
                "missing {name}"
            );
        }
        for retired in [
            "voice_e2e_utterance_end_to_tts_first_audio_seconds",
            "voice_asr_duration_seconds",
            "voice_llm_time_to_first_token_seconds",
            "voice_e2e_tts_first_audio_to_client_playback_seconds",
        ] {
            assert!(
                !output.contains(retired),
                "retired metric remains: {retired}"
            );
        }
        assert!(!output.contains("session_id"));
        assert!(!output.contains("request_id"));
        assert!(!output.contains("message_id"));
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
    fn bounds_client_reported_durations_and_keeps_them_unlabelled() {
        let metrics = VoiceMetrics::new();
        metrics.observe_client_first_audio_received_to_playback(Duration::from_secs(31));
        metrics.observe_client_first_audio_received_to_playback(Duration::from_millis(120));
        metrics.observe_client_input_end_to_final_audio_sent(Duration::from_secs(31));
        metrics.observe_client_input_end_to_final_audio_sent(Duration::from_millis(4));
        let output = metrics.render();
        assert!(output.contains("voice_client_first_audio_received_to_playback_seconds_count 1"));
        assert!(output.contains("voice_client_input_end_to_final_audio_sent_seconds_count 1"));
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
            actix_test::TestRequest::get()
                .uri("/metrics/voice")
                .to_request(),
        )
        .await;
        assert!(response.status().is_success());
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain; version=0.0.4"
        );
        let body = actix_test::read_body(response).await;
        assert!(std::str::from_utf8(&body)
            .unwrap()
            .contains("voice_requests_total"));
    }
}
