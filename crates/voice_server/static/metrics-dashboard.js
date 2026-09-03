(function initMetricsDashboard(root) {
  'use strict';

  const LATENCY_METRICS = [
    ['服务端尾帧 → ASR 结束', 'voice_input_end_to_asr_output_end_seconds'],
    ['服务端尾帧 → LLM 首字', 'voice_input_end_to_llm_first_text_seconds'],
    ['服务端尾帧 → TTS 首帧', 'voice_input_end_to_tts_first_frame_seconds'],
    ['服务端尾帧 → 首音频 WS 发出', 'voice_input_end_to_ws_first_audio_sent_seconds'],
    ['ASR 输入 → ASR 结束', 'voice_asr_input_to_output_end_seconds'],
    ['LLM 输入 → LLM 首字', 'voice_llm_input_to_first_text_seconds'],
    ['LLM 首字 → TTS 首帧', 'voice_llm_first_text_to_tts_first_frame_seconds'],
    ['TTS 首帧 → 首音频 WS 发出', 'voice_tts_first_frame_to_ws_first_audio_sent_seconds'],
    ['端侧收到首帧 → 开始播放', 'voice_client_first_audio_received_to_playback_seconds'],
    ['端侧输入结束 → 尾帧发出', 'voice_client_input_end_to_final_audio_sent_seconds'],
    ['输入开始到完成', 'voice_e2e_input_to_tts_complete_seconds'],
    ['说完到完成', 'voice_e2e_utterance_end_to_tts_complete_seconds'],
    ['Pipeline 排队', 'voice_pipeline_queue_duration_seconds'],
    ['LLM 完成', 'voice_llm_duration_seconds'],
    ['TTS 输入等待', 'voice_tts_input_wait_seconds'],
    ['TTS 首音频', 'voice_tts_time_to_first_audio_seconds'],
    ['TTS 生成完成', 'voice_tts_generation_duration_seconds'],
    ['Pipeline 总耗时', 'voice_pipeline_duration_seconds'],
  ];

  function parseLabels(source) {
    const labels = {};
    const pattern = /([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"])*)"/g;
    let match;
    while ((match = pattern.exec(source)) !== null) {
      labels[match[1]] = match[2].replace(/\\n/g, '\n').replace(/\\"/g, '"').replace(/\\\\/g, '\\');
    }
    return labels;
  }

  function parsePrometheus(text) {
    const samples = [];
    for (const rawLine of text.split(/\r?\n/)) {
      const line = rawLine.trim();
      if (!line || line.startsWith('#')) continue;
      const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+([^\s]+)(?:\s+\d+)?$/);
      if (!match) continue;
      const value = Number(match[3]);
      if (Number.isNaN(value)) continue;
      samples.push({ name: match[1], labels: parseLabels(match[2] || ''), value });
    }
    return samples;
  }

  function sampleValue(samples, name, labels = {}) {
    const sample = samples.find((candidate) => candidate.name === name &&
      Object.entries(labels).every(([key, value]) => candidate.labels[key] === value));
    return sample ? sample.value : 0;
  }

  function histogramQuantile(samples, metric, quantile) {
    const buckets = samples
      .filter((sample) => sample.name === `${metric}_bucket` && sample.labels.le !== undefined)
      .map((sample) => ({ upper: sample.labels.le === '+Inf' ? Infinity : Number(sample.labels.le), count: sample.value }))
      .filter((bucket) => !Number.isNaN(bucket.upper))
      .sort((a, b) => a.upper - b.upper);
    if (!buckets.length) return null;
    const finiteBuckets = buckets.filter((bucket) => Number.isFinite(bucket.upper));
    const total = sampleValue(samples, `${metric}_count`) || buckets[buckets.length - 1].count;
    if (!total || !finiteBuckets.length) return null;
    const target = Math.min(1, Math.max(0, quantile)) * total;
    let previousUpper = 0;
    let previousCount = 0;
    for (const bucket of finiteBuckets) {
      if (bucket.count >= target) {
        const inBucket = bucket.count - previousCount;
        if (inBucket <= 0) return bucket.upper;
        const fraction = (target - previousCount) / inBucket;
        return previousUpper + (bucket.upper - previousUpper) * fraction;
      }
      previousUpper = bucket.upper;
      previousCount = bucket.count;
    }
    return finiteBuckets[finiteBuckets.length - 1].upper;
  }

  function buildDashboardSnapshot(samples) {
    const total = sampleValue(samples, 'voice_requests_total');
    const success = sampleValue(samples, 'voice_requests_finished_total', { result: 'success' });
    const failed = sampleValue(samples, 'voice_requests_finished_total', { result: 'failed' }) +
      sampleValue(samples, 'voice_requests_finished_total', { result: 'empty_response' });
    const timeout = sampleValue(samples, 'voice_requests_finished_total', { result: 'timeout' });
    const cancelled = sampleValue(samples, 'voice_requests_finished_total', { result: 'cancelled' });
    const finished = success + failed + timeout + cancelled;
    return {
      requests: { total, success, failed, timeout, cancelled, successRate: finished ? success / finished * 100 : 0 },
      pool: {
        connections: sampleValue(samples, 'voice_tts_ws_pool_connections'),
        active: sampleValue(samples, 'voice_tts_ws_pool_active_connections'),
        idle: sampleValue(samples, 'voice_tts_ws_pool_idle_connections'),
        waiting: sampleValue(samples, 'voice_tts_ws_pool_waiting'),
      },
      routes: {
        fast: sampleValue(samples, 'voice_llm_route_total', { route: 'fast' }),
        strong: sampleValue(samples, 'voice_llm_route_total', { route: 'strong' }),
      },
      latencies: LATENCY_METRICS.map(([label, metric]) => ({
        label,
        metric,
        count: sampleValue(samples, `${metric}_count`),
        p50: histogramQuantile(samples, metric, 0.5),
        p90: histogramQuantile(samples, metric, 0.9),
        p95: histogramQuantile(samples, metric, 0.95),
        p99: histogramQuantile(samples, metric, 0.99),
      })),
    };
  }

  function formatCount(value) {
    return new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 0 }).format(value || 0);
  }

  function formatDuration(seconds) {
    if (seconds === null || !Number.isFinite(seconds)) return '--';
    if (seconds < 0.001) return `${(seconds * 1e6).toFixed(0)} us`;
    if (seconds < 1) return `${(seconds * 1000).toFixed(seconds < 0.01 ? 1 : 0)} ms`;
    return `${seconds.toFixed(seconds < 10 ? 2 : 1)} s`;
  }

  function setText(id, value) {
    const element = document.getElementById(id);
    if (element) element.textContent = value;
  }

  function render(snapshot, rawText) {
    setText('request-total', formatCount(snapshot.requests.total));
    setText('success-rate', `${snapshot.requests.successRate.toFixed(1)}%`);
    setText('request-success', formatCount(snapshot.requests.success));
    setText('request-failed', formatCount(snapshot.requests.failed));
    setText('request-timeout', formatCount(snapshot.requests.timeout));
    setText('request-cancelled', formatCount(snapshot.requests.cancelled));
    setText('pool-connections', formatCount(snapshot.pool.connections));
    setText('pool-active', formatCount(snapshot.pool.active));
    setText('pool-idle', formatCount(snapshot.pool.idle));
    setText('pool-waiting', formatCount(snapshot.pool.waiting));
    setText('route-fast', formatCount(snapshot.routes.fast));
    setText('route-strong', formatCount(snapshot.routes.strong));
    setText('raw-metrics', rawText.trim());

    const body = document.getElementById('latency-body');
    if (body) {
      body.replaceChildren(...snapshot.latencies.map((item) => {
        const row = document.createElement('tr');
        const cells = [item.label, formatDuration(item.p50), formatDuration(item.p90), formatDuration(item.p95), formatDuration(item.p99), formatCount(item.count)];
        for (const value of cells) {
          const cell = document.createElement('td');
          cell.textContent = value;
          row.appendChild(cell);
        }
        return row;
      }));
    }
  }

  async function refresh() {
    const button = document.getElementById('refresh-button');
    if (button) button.disabled = true;
    setText('dashboard-status', '正在刷新');
    try {
      const response = await fetch('/metrics/voice', { cache: 'no-store' });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const rawText = await response.text();
      render(buildDashboardSnapshot(parsePrometheus(rawText)), rawText);
      setText('dashboard-status', '数据正常');
      setText('last-updated', new Date().toLocaleTimeString('zh-CN', { hour12: false }));
      document.body.dataset.state = 'online';
    } catch (error) {
      setText('dashboard-status', '读取失败');
      setText('last-updated', String(error));
      document.body.dataset.state = 'error';
    } finally {
      if (button) button.disabled = false;
    }
  }

  const api = { parsePrometheus, histogramQuantile, buildDashboardSnapshot, formatDuration };
  if (typeof module === 'object' && module.exports) module.exports = api;
  root.VoiceMetricsDashboard = api;

  if (typeof document !== 'undefined') {
    document.addEventListener('DOMContentLoaded', () => {
      document.getElementById('refresh-button')?.addEventListener('click', refresh);
      refresh();
      window.setInterval(refresh, 5000);
    });
  }
})(typeof window !== 'undefined' ? window : globalThis);
