# Voice Metrics Collection Architecture

## 1. Decision

采用“业务埋点 API + Prometheus 默认实现”的方式，不通过日志解析生成业务指标。

```text
VoiceSession / Pipeline / TTS WS
            |
            | 只依赖 VoiceMetricsSink
            v
    VoiceMetricsSink trait
       |              |
       |              +--> NoopMetricsSink（测试/关闭指标）
       |
       +--> PrometheusMetricsSink
                    |
                    v
             /metrics/voice
                    |
                    v
              Prometheus
                    |
                    v
              Grafana / Alerting
```

业务代码不认识 `prometheus::Registry`、`Histogram` 或 Prometheus label；它只发送有限集合的业务事件和耗时。Prometheus 实现负责把事件转换成 Counter、Gauge、Histogram，并以 HTTP exposition format 暴露。

## 2. Why This Method

### 2.1 不从日志采集业务指标

日志是单次事件记录，适合排查某个 `trace_id`；指标是时间窗口内的聚合数据，适合计算请求率、错误率、p95/p99 和 SLO。用日志正则解析指标会依赖日志格式，无法稳定处理丢日志、重复日志和异步日志延迟，也不适合高频音频 chunk。

日志和指标的关系如下：

```text
指标异常：voice_e2e_utterance_end_to_tts_first_audio_seconds p95 上升
    |
    +--> Grafana/告警发现整体问题
    |
    +--> 通过 trace_id/request_id 查结构化日志和 Trace
             |
             +--> 定位 ASR、LLM、TTS 或客户端播放具体事件
```

### 2.2 为什么第一阶段使用 Prometheus

- 当前 Server 是在线服务，Prometheus 的 Counter/Histogram 正好覆盖请求量、结果和延迟分布。
- Server 已经有 HTTP 路由，增加 scrape endpoint 的部署成本低。
- `prometheus` Rust client 在进程内更新，无需每个请求同步访问外部系统。
- Prometheus 通过 `rate()`、`histogram_quantile()` 等查询完成聚合，不要求业务代码自己维护 p95/p99。

### 2.3 为什么保留 OpenTelemetry 演进空间

OpenTelemetry 将 Metrics、Logs、Traces 视为不同信号，并提供 Collector 统一接收和转发。后续若需要统一接入 Trace、日志和多个观测后端，只需新增 `OpenTelemetryMetricsSink` 或替换 exporter，不需要改 ASR/LLM/TTS 业务流程。

## 3. Coupling Boundary

### 3.1 允许的耦合

业务流程必须知道“什么时候发生了首包、完成、失败、取消”，因为这些时间点只有业务代码能准确捕获。因此 `run_pipeline` 和 `TtsWsClient` 会调用抽象接口：

```rust
pub trait VoiceMetricsSink: Send + Sync {
    fn pipeline_started(&self);
    fn observe_queue(&self, duration: Duration);
    fn observe_asr(&self, duration: Duration);
    fn observe_llm_first_token(&self, duration: Duration);
    fn observe_llm_complete(&self, duration: Duration);
    fn observe_tts_first_audio(&self, duration: Duration);
    fn observe_tts_complete(&self, duration: Duration);
    fn observe_e2e_first_audio(&self, from_input_start: Duration, from_input_end: Duration);
    fn observe_e2e_complete(&self, from_input_start: Duration, from_input_end: Duration);
    fn pipeline_finished(&self, result: PipelineResult);
}

#[derive(Clone, Copy)]
pub enum PipelineResult {
    Success,
    Failed,
    Timeout,
    Cancelled,
    EmptyResponse,
}
```

### 3.2 禁止的耦合

- `VoiceSession`、`run_pipeline` 和 TTS client 不直接操作 `Registry`。
- 业务代码不直接构造 Histogram/Counter。
- 指标更新不发网络请求，不阻塞等待 Prometheus。
- 指标实现不能改变业务返回值、取消逻辑、协议字段和重试语义。
- `trace_id`、`message_id`、`session_id`、`request_id`、prompt、文本和音频内容不得作为指标 label。

### 3.3 依赖关系

```text
service.rs
  └── Arc<dyn VoiceMetricsSink>
        ├── session/mod.rs
        │     └── pipeline.rs
        └── client/tts_ws.rs

metrics.rs
  ├── VoiceMetricsSink trait
  ├── PrometheusMetricsSink
  ├── NoopMetricsSink
  └── /metrics/voice handler（仅 Prometheus 实现需要）
```

## 4. Collection Protocol

### 4.1 Prometheus Pull

Server 启动一个进程级 `Arc<dyn VoiceMetricsSink>`，默认实例是 `PrometheusMetricsSink`。Prometheus 配置：

```yaml
scrape_configs:
  - job_name: voice-server
    metrics_path: /metrics/voice
    static_configs:
      - targets: ["voice-server:8080"]
```

`/metrics/voice` 只返回聚合后的 Counter/Histogram，不返回单次请求详情。Scrape 失败不会影响业务请求；业务埋点失败也不能阻断 pipeline。

### 4.2 Metric Types

| 数据 | 类型 | 说明 |
|---|---|---|
| 请求量、成功/失败/超时/取消、TTS WS 错误 | Counter | 只增不减，查询时使用 `rate()` |
| 阶段和端到端延迟 | Histogram | 由 Prometheus 计算 p50/p95/p99 |
| 当前连接池连接数、等待数 | Gauge | 表示当前状态，可升可降 |

Histogram bucket 初始使用语音交互常用的秒级边界：`0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30`；上线后根据真实基线调整。

## 5. Timing and Context

服务端使用 `Instant` 计算同一进程内耗时：

```text
input_started_at
  → input_ended_at
  → asr_started_at / asr_final_at
  → llm_started_at / llm_first_token_at / llm_completed_at
  → tts_first_audio_at / tts_completed_at
```

客户端播放延迟不能用客户端和 Server 的墙钟直接相减。客户端应测量“收到首个 TTS 音频到实际开始播放”的本地相对时长，再通过带 `message_id` 的事件上报；Server 只校验范围并观察该 duration。

## 6. Result Classification

| 结果 | 触发条件 | Counter |
|---|---|---|
| `success` | TTS 正常完成并发送完成状态 | `voice_requests_success_total` |
| `failed` | Provider、解码、连接或协议错误 | `voice_requests_failed_total` |
| `timeout` | ASR/TTS/连接超时 | `voice_requests_timeout_total` |
| `cancelled` | 用户 Interrupt、SessionEnd 或断连 | `voice_requests_cancelled_total` |
| `empty_response` | ASR、LLM 或 TTS 没有有效结果 | `voice_requests_failed_total{result="empty_response"}` |

`PipelineMetricsGuard` 在 future 被提前 drop 或 panic unwind 时默认归类为 `failed`，避免请求只增加 started 而没有 finished 结果。

## 7. Migration Steps

1. 将当前 `VoiceMetrics` 改名或拆为 `PrometheusMetricsSink`，保留现有 Registry 和指标名称。
2. 在同一 `metrics.rs` 定义 `VoiceMetricsSink`、`PipelineResult` 和 `NoopMetricsSink`。
3. 将 `VoiceService`、`VoiceSession`、`run_pipeline` 和 `TtsWsClient` 的依赖类型改为 `Arc<dyn VoiceMetricsSink>`。
4. 把 RAII guard 的结果计数和 E2E Histogram 逻辑保留在 Prometheus sink 的内部实现中，业务层只负责发送事件。
5. 为 sink trait 写 fake 实现，pipeline 测试只断言收到哪些事件和 duration 范围，不依赖 Prometheus 文本格式。
6. 最后补 `/metrics/voice` 的 Actix 集成测试，验证 Prometheus adapter 的 exposition output。

## 8. Acceptance Criteria

- 业务 pipeline 可以在没有 Prometheus collector 的情况下使用 `NoopMetricsSink` 运行。
- 替换 Prometheus 实现不会修改 ASR、LLM、TTS 的业务函数签名和协议行为，只改变 metrics sink 注入。
- 指标 endpoint 能被 Prometheus scrape，且包含核心 E2E Histogram 和结果 Counter。
- 日志采集不是指标计算链路；日志只保存事件和 trace/request 关联字段。
- 指标更新、endpoint scrape、sink trait 和 pipeline 事件均有独立测试。
