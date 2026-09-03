'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  parsePrometheus,
  histogramQuantile,
  buildDashboardSnapshot,
} = require('./metrics-dashboard.js');

const fixture = `
# HELP voice_requests_total Voice requests
# TYPE voice_requests_total counter
voice_requests_total 21
voice_requests_finished_total{result="success"} 16
voice_requests_finished_total{result="failed"} 2
voice_requests_finished_total{result="timeout"} 1
voice_requests_finished_total{result="cancelled"} 1
voice_input_end_to_asr_output_end_seconds_bucket{le="0.1"} 2
voice_input_end_to_asr_output_end_seconds_bucket{le="0.5"} 8
voice_input_end_to_asr_output_end_seconds_bucket{le="1"} 10
voice_input_end_to_asr_output_end_seconds_bucket{le="+Inf"} 10
voice_input_end_to_asr_output_end_seconds_sum 4.2
voice_input_end_to_asr_output_end_seconds_count 10
voice_tts_ws_pool_connections 4
voice_tts_ws_pool_active_connections 1
voice_tts_ws_pool_idle_connections 3
voice_llm_route_total{route="fast"} 12
voice_llm_route_total{route="strong"} 8
`;

test('parses labelled Prometheus samples without treating comments as data', () => {
  const samples = parsePrometheus(fixture);
  assert.equal(samples.length, 16);
  assert.deepEqual(samples.find((sample) => sample.name === 'voice_llm_route_total'), {
    name: 'voice_llm_route_total',
    labels: { route: 'fast' },
    value: 12,
  });
});

test('interpolates a quantile from cumulative histogram buckets', () => {
  const samples = parsePrometheus(fixture);
  assert.ok(Math.abs(histogramQuantile(samples, 'voice_input_end_to_asr_output_end_seconds', 0.5) - 0.3) < 1e-9);
  assert.equal(histogramQuantile(samples, 'voice_input_end_to_asr_output_end_seconds', 0.95), 0.875);
});

test('builds request, latency, pool and route summaries from one scrape', () => {
  const samples = parsePrometheus(fixture);
  const metricNames = [
    'voice_input_end_to_asr_output_end_seconds',
    'voice_input_end_to_llm_first_text_seconds',
    'voice_input_end_to_tts_first_frame_seconds',
    'voice_input_end_to_ws_first_audio_sent_seconds',
    'voice_asr_input_to_output_end_seconds',
    'voice_llm_input_to_first_text_seconds',
    'voice_llm_first_text_to_tts_first_frame_seconds',
    'voice_tts_first_frame_to_ws_first_audio_sent_seconds',
    'voice_client_first_audio_received_to_playback_seconds',
    'voice_client_input_end_to_final_audio_sent_seconds',
  ];
  for (const metric of metricNames.slice(1)) {
    samples.push(
      { name: `${metric}_bucket`, labels: { le: '1' }, value: 10 },
      { name: `${metric}_bucket`, labels: { le: '+Inf' }, value: 10 },
      { name: `${metric}_count`, labels: {}, value: 10 },
    );
  }
  const snapshot = buildDashboardSnapshot(samples);
  assert.deepEqual(snapshot.requests, {
    total: 21, success: 16, failed: 2, timeout: 1, cancelled: 1, successRate: 80,
  });
  assert.deepEqual(snapshot.pool, { connections: 4, active: 1, idle: 3, waiting: 0 });
  assert.deepEqual(snapshot.routes, { fast: 12, strong: 8 });
  assert.ok(Math.abs(snapshot.latencies[0].p50 - 0.3) < 1e-9);
  assert.ok(Math.abs(snapshot.latencies[0].p90 - 0.75) < 1e-9);
  assert.ok(snapshot.latencies.slice(0, 10).every((item) => item.count === 10 && Number.isFinite(item.p90) && item.p90 > 0));
  assert.deepEqual(snapshot.latencies.slice(0, 10).map((item) => item.metric), [
    'voice_input_end_to_asr_output_end_seconds',
    'voice_input_end_to_llm_first_text_seconds',
    'voice_input_end_to_tts_first_frame_seconds',
    'voice_input_end_to_ws_first_audio_sent_seconds',
    'voice_asr_input_to_output_end_seconds',
    'voice_llm_input_to_first_text_seconds',
    'voice_llm_first_text_to_tts_first_frame_seconds',
    'voice_tts_first_frame_to_ws_first_audio_sent_seconds',
    'voice_client_first_audio_received_to_playback_seconds',
    'voice_client_input_end_to_final_audio_sent_seconds',
  ]);
});
