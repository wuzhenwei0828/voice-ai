use std::sync::Arc;
use std::time::{Duration, Instant};

use actix_web::{web, HttpResponse};
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, Registry, TextEncoder};

/// Server-side Prometheus metrics for one voice utterance pipeline.
///
/// Request identifiers are deliberately absent from labels. They remain in logs and spans,
/// where they can be used to investigate an individual sample without exploding cardinality.
pub struct VoiceMetrics {
    registry: Registry,
    requests_total: IntCounter,
    requests_finished_total: IntCounterVec,
    requests_success_total: IntCounter,
    requests_failed_total: IntCounter,
    requests_timeout_total: IntCounter,
    requests_cancelled_total: IntCounter,
    e2e_input_to_first_audio: Histogram,
    e2e_utterance_end_to_first_audio: Histogram,
    e2e_input_to_complete: Histogram,
    e2e_utterance_end_to_complete: Histogram,
    asr_duration: Histogram,
    pipeline_queue_duration: Histogram,
    llm_time_to_first_token: Histogram,
    llm_duration: Histogram,
    tts_time_to_first_audio: Histogram,
    tts_generation_duration: Histogram,
    pipeline_duration: Histogram,
}

impl VoiceMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let requests_total = counter("voice_requests_total", "Voice utterance pipelines started");
        let requests_finished_total = IntCounterVec::new(
            prometheus::Opts::new("voice_requests_finished_total", "Voice utterance pipelines finished"),
            &["result"],
        )
        .expect("valid voice request result counter");
        let requests_success_total = counter("voice_requests_success_total", "Voice pipelines completed successfully");
        let requests_failed_total = counter("voice_requests_failed_total", "Voice pipelines failed");
        let requests_timeout_total = counter("voice_requests_timeout_total", "Voice pipelines timed out");
        let requests_cancelled_total = counter("voice_requests_cancelled_total", "Voice pipelines cancelled");
        let e2e_input_to_first_audio = histogram(
            "voice_e2e_input_to_tts_first_audio_seconds",
            "Seconds from the first input chunk to the first TTS audio chunk",
        );
        let e2e_utterance_end_to_first_audio = histogram(
            "voice_e2e_utterance_end_to_tts_first_audio_seconds",
            "Seconds from the final input chunk to the first TTS audio chunk",
        );
        let e2e_input_to_complete = histogram(
            "voice_e2e_input_to_tts_complete_seconds",
            "Seconds from the first input chunk to pipeline completion",
        );
        let e2e_utterance_end_to_complete = histogram(
            "voice_e2e_utterance_end_to_tts_complete_seconds",
            "Seconds from the final input chunk to pipeline completion",
        );
        let asr_duration = histogram("voice_asr_duration_seconds", "Seconds from ASR start to final ASR event");
        let pipeline_queue_duration = histogram(
            "voice_pipeline_queue_duration_seconds",
            "Seconds from final input chunk until the ASR request starts",
        );
        let llm_time_to_first_token = histogram(
            "voice_llm_time_to_first_token_seconds",
            "Seconds from LLM/TTS stream start to the first non-empty LLM delta",
        );
        let llm_duration = histogram(
            "voice_llm_duration_seconds",
            "Seconds from LLM/TTS stream start to the final LLM delta",
        );
        let tts_time_to_first_audio = histogram(
            "voice_tts_time_to_first_audio_seconds",
            "Seconds from LLM/TTS stream start to the first TTS audio chunk",
        );
        let tts_generation_duration = histogram(
            "voice_tts_generation_duration_seconds",
            "Seconds from LLM/TTS stream start to the final TTS audio chunk",
        );
        let pipeline_duration = histogram("voice_pipeline_duration_seconds", "Total server-side pipeline duration");

        for collector in [
            Box::new(requests_total.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(requests_finished_total.clone()),
            Box::new(requests_success_total.clone()),
            Box::new(requests_failed_total.clone()),
            Box::new(requests_timeout_total.clone()),
            Box::new(requests_cancelled_total.clone()),
            Box::new(e2e_input_to_first_audio.clone()),
            Box::new(e2e_utterance_end_to_first_audio.clone()),
            Box::new(e2e_input_to_complete.clone()),
            Box::new(e2e_utterance_end_to_complete.clone()),
            Box::new(asr_duration.clone()),
            Box::new(pipeline_queue_duration.clone()),
            Box::new(llm_time_to_first_token.clone()),
            Box::new(llm_duration.clone()),
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
            e2e_input_to_first_audio,
            e2e_utterance_end_to_first_audio,
            e2e_input_to_complete,
            e2e_utterance_end_to_complete,
            asr_duration,
            pipeline_queue_duration,
            llm_time_to_first_token,
            llm_duration,
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
        self.requests_total.inc();
        PipelineMetricsGuard {
            metrics: self.clone(),
            input_started_at,
            input_ended_at,
            started_at: Instant::now(),
            first_audio_recorded: false,
            completed_recorded: false,
            result: "failed",
        }
    }

    pub fn observe_asr(&self, duration: Duration) {
        self.asr_duration.observe(duration.as_secs_f64());
    }

    pub fn observe_queue(&self, duration: Duration) {
        self.pipeline_queue_duration.observe(duration.as_secs_f64());
    }

    pub fn observe_llm_first_token(&self, duration: Duration) {
        self.llm_time_to_first_token.observe(duration.as_secs_f64());
    }

    pub fn observe_llm_duration(&self, duration: Duration) {
        self.llm_duration.observe(duration.as_secs_f64());
    }

    pub fn observe_tts_first_audio(&self, duration: Duration) {
        self.tts_time_to_first_audio.observe(duration.as_secs_f64());
    }

    pub fn observe_tts_generation(&self, duration: Duration) {
        self.tts_generation_duration.observe(duration.as_secs_f64());
    }

    pub fn render(&self) -> String {
        let families = self.registry.gather();
        let mut encoded = Vec::new();
        TextEncoder::new()
            .encode(&families, &mut encoded)
            .expect("encoding metrics should not fail");
        String::from_utf8(encoded).expect("prometheus output is utf8")
    }
}

pub struct PipelineMetricsGuard {
    metrics: Arc<VoiceMetrics>,
    input_started_at: Instant,
    input_ended_at: Instant,
    started_at: Instant,
    first_audio_recorded: bool,
    completed_recorded: bool,
    result: &'static str,
}

impl PipelineMetricsGuard {
    pub fn has_first_audio(&self) -> bool {
        self.first_audio_recorded
    }

    pub fn record_first_audio(&mut self, at: Instant) {
        if self.first_audio_recorded {
            return;
        }
        self.first_audio_recorded = true;
        self.metrics
            .e2e_input_to_first_audio
            .observe(at.duration_since(self.input_started_at).as_secs_f64());
        self.metrics
            .e2e_utterance_end_to_first_audio
            .observe(at.duration_since(self.input_ended_at).as_secs_f64());
    }

    pub fn finish(&mut self, result: &'static str, at: Instant) {
        if self.completed_recorded {
            return;
        }
        self.completed_recorded = true;
        self.result = result;
        self.metrics
            .e2e_input_to_complete
            .observe(at.duration_since(self.input_started_at).as_secs_f64());
        self.metrics
            .e2e_utterance_end_to_complete
            .observe(at.duration_since(self.input_ended_at).as_secs_f64());
    }
}

impl Drop for PipelineMetricsGuard {
    fn drop(&mut self) {
        self.metrics
            .pipeline_duration
            .observe(self.started_at.elapsed().as_secs_f64());
        self.metrics
            .requests_finished_total
            .with_label_values(&[self.result])
            .inc();
        match self.result {
            "success" => self.metrics.requests_success_total.inc(),
            "timeout" => self.metrics.requests_timeout_total.inc(),
            "cancelled" => self.metrics.requests_cancelled_total.inc(),
            _ => self.metrics.requests_failed_total.inc(),
        }
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

fn histogram(name: &str, help: &str) -> Histogram {
    Histogram::with_opts(HistogramOpts::new(name, help)).expect("valid voice histogram")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_pipeline_metrics_without_high_cardinality_labels() {
        let metrics = Arc::new(VoiceMetrics::new());
        let now = Instant::now();
        let mut guard = metrics.start_pipeline(now, now);
        guard.record_first_audio(now + Duration::from_millis(25));
        guard.finish("success", now + Duration::from_millis(50));
        drop(guard);

        let output = metrics.render();
        assert!(output.contains("voice_e2e_utterance_end_to_tts_first_audio_seconds"));
        assert!(output.contains("voice_requests_finished_total{result=\"success\"}"));
        assert!(!output.contains("session_id"));
    }
}
