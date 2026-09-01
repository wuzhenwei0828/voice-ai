# Voice AI 全链路指标统计 Spec

## 1. 背景

当前语音链路包含客户端输入、实时 ASR、LLM 流式生成、TTS WebSocket 合成和客户端播放多个阶段。仅统计单个 Provider 的耗时，无法回答以下问题：

- 用户开始说话后，多久能收到第一段 TTS 音频？
- 用户说完后，多久能听到系统回应？
- 延迟主要消耗在 ASR、LLM、TTS，还是服务端排队？
- 不同 Provider、模型、音色和音频格式的性能差异是什么？

本 Spec 定义统一的时间点、指标名称、标签和验收标准，为后续 Prometheus/OTel 指标实现提供依据。

## 2. 目标

1. 统计从用户输入到 TTS 首包的全链路延迟。
2. 区分“用户开始输入”和“用户结束输入”两个延迟起点。
3. 拆分 ASR、LLM、TTS 及内部排队耗时，定位主要延迟来源。
4. 统计请求量、成功率、超时、取消、重试和 Provider 错误。
5. 支持按 endpoint、Provider、模型、音色和音频格式进行聚合比较。
6. 使用现有 `trace_id`、`session_id`、`request_id` 关联单次请求日志，但不将它们作为指标标签。

## 3. 非目标

- 本期不统计 trace_id 是否缺失、message_id 是否重复等链路完整性指标。
- 本期不做音频识别准确率、语义质量或音色主观评分。
- 本期不把 prompt、文本内容、session_id、trace_id、message_id 放入指标标签。
- 本期不要求客户端和服务端时钟直接相减来计算跨机器耗时。

## 4. 时间点定义

每个 `request_id` 对应一轮用户语音请求，服务端记录以下单调时钟时间点：

| 时间点 | 定义 | 触发位置 |
|---|---|---|
| `input_started_at` | 收到本轮第一个音频 chunk | 第一条 `AudioChunk` |
| `input_ended_at` | 收到本轮最后一个音频 chunk | `AudioChunk.is_last=true` 或结束事件 |
| `asr_started_at` | 开始调用 ASR Provider | ASR 请求发出前 |
| `asr_final_at` | 收到可用于后续处理的最终 ASR 文本 | ASR final 事件 |
| `llm_started_at` | 开始调用 LLM Provider | LLM 请求发出前 |
| `llm_first_token_at` | 收到第一个非空 LLM delta | 第一个有效 delta |
| `llm_completed_at` | LLM 流结束 | LLM stream 完成 |
| `tts_input_sent_at` | 向 TTS Provider 发送文本 | `input.text` 发送成功 |
| `tts_input_done_at` | 向 TTS Provider 发送 `input.done` 成功 | TTS 输入结束 |
| `tts_first_audio_at` | 收到第一个二进制音频 chunk | TTS WebSocket 首个音频帧 |
| `tts_completed_at` | 收到 `session.done` | TTS WebSocket 会话完成 |
| `client_audio_started_at` | 客户端开始播放首个音频 chunk | 客户端播放器实际开始播放 |

服务端阶段耗时使用 `Instant` 或 OpenTelemetry monotonic duration 计算。跨客户端和服务端的指标优先由客户端上报相对时间，或者只统计服务端可观测的区间，避免受机器时钟偏差影响。

## 5. 核心全链路指标

所有延迟指标使用 Histogram，单位为秒，至少展示 p50、p90、p95、p99。

### 5.1 用户体验延迟

| 指标 | 计算方式 | 用途 |
|---|---|---|
| `voice_e2e_input_to_tts_first_audio_seconds` | `tts_first_audio_at - input_started_at` | 从用户开始说话到系统开始回复，包含用户讲话时长 |
| `voice_e2e_utterance_end_to_tts_first_audio_seconds` | `tts_first_audio_at - input_ended_at` | 用户说完到听到首包，作为主要响应延迟指标 |
| `voice_e2e_input_to_tts_complete_seconds` | `tts_completed_at - input_started_at` | 从开始输入到 TTS 全部完成 |
| `voice_e2e_utterance_end_to_tts_complete_seconds` | `tts_completed_at - input_ended_at` | 用户说完到完整回复结束 |
| `voice_e2e_tts_first_audio_to_client_playback_seconds` | `client_audio_started_at - tts_first_audio_at` | 服务端首包到客户端真正播放的传输/缓冲延迟 |

其中 `voice_e2e_utterance_end_to_tts_first_audio_seconds` 是本系统的主 SLI，因为它最接近用户感知的“我说完后多久得到回应”。

### 5.2 阶段延迟

```text
voice_pipeline_queue_duration_seconds
  = asr_started_at - input_ended_at

voice_asr_duration_seconds
  = asr_final_at - asr_started_at

voice_llm_time_to_first_token_seconds
  = llm_first_token_at - llm_started_at

voice_llm_duration_seconds
  = llm_completed_at - llm_started_at

voice_tts_input_wait_seconds
  = tts_input_done_at - tts_input_sent_at

voice_tts_time_to_first_audio_seconds
  = tts_first_audio_at - tts_input_done_at

voice_tts_generation_duration_seconds
  = tts_completed_at - tts_input_done_at
```

说明：LLM 和 TTS 都是流式处理，首包延迟与完整完成耗时必须分开统计。

## 6. 请求量、结果和异常指标

使用 Counter 统计：

```text
voice_requests_total
voice_requests_success_total
voice_requests_failed_total
voice_requests_timeout_total
voice_requests_cancelled_total
voice_requests_retried_total
voice_tts_ws_connect_total
voice_tts_ws_connect_failed_total
voice_tts_ws_reconnect_total
voice_tts_provider_errors_total
```

错误指标应包含有限集合的 `stage` 或 `error_type`，例如：

```text
stage = connect | send_input | receive | asr | llm | tts | playback
error_type = timeout | provider | decode | connection_closed | empty_response | cancelled
```

需要区分以下结果：

- 正常完成
- 用户主动打断
- 服务端超时
- Provider 返回错误
- WebSocket 建连或收包失败
- ASR/LLM/TTS 返回空结果

## 7. TTS 输出和连接池指标

### 7.1 音频输出

```text
voice_tts_input_chars_total
voice_tts_audio_chunks_total
voice_tts_audio_bytes_total
voice_tts_audio_duration_seconds
voice_tts_audio_chunk_interval_seconds
voice_tts_realtime_factor
```

实时率定义：

```text
realtime_factor = 音频播放时长 / TTS 生成耗时
```

### 7.2 TTS WebSocket 连接池

使用 Gauge 统计当前状态，使用 Counter 统计状态变更：

```text
voice_tts_ws_pool_connections
voice_tts_ws_pool_active_connections
voice_tts_ws_pool_idle_connections
voice_tts_ws_pool_waiting
voice_tts_ws_pool_wait_duration_seconds
voice_tts_ws_pool_invalidated_total
voice_tts_ws_pool_reaped_total
```

## 8. 标签约束

允许使用的低基数标签：

```text
endpoint
business
provider
model
voice
transport
audio_format
result
stage
error_type
```

禁止作为指标标签：

```text
trace_id
message_id
session_id
request_id
prompt
text
wav_name
```

这些字段保留在结构化日志和 Trace span 中，用于定位单个请求。

## 9. 事件日志要求

指标用于聚合统计，日志用于单次链路排查。每轮请求至少应能通过 `trace_id` 或 `request_id` 找到以下事件：

```text
input_started
input_ended
asr_final
llm_first_token
tts_input_done
tts_first_audio
tts_completed
```

TTS WebSocket 事件日志保持 `info` 级别：

- `session.config` 发送成功
- `input.text`、`input.done`、`session.close` 发送成功
- `audio.start`、`audio.done`、`session.done` 接收成功
- 首个音频 chunk 到达及耗时
- TTS 回复完成及耗时

高频音频 chunk 不要求每个都输出 `info` 日志，只统计 chunk 数量、字节数和间隔指标。

## 10. 仪表盘和 SLO

第一版仪表盘至少包含：

1. 用户说完到 TTS 首包的 p50/p95/p99。
2. 用户开始输入到 TTS 首包的 p50/p95/p99。
3. ASR final 延迟。
4. LLM 首 token 延迟。
5. TTS 首包延迟。
6. TTS 完成耗时。
7. 端到端成功率、超时率、取消率。
8. 按 Provider、模型、音色的延迟对比。
9. TTS WebSocket 建连失败率和连接池等待时间。

建议先将以下指标作为 SLI，具体阈值由线上基线确定：

```text
voice_e2e_utterance_end_to_tts_first_audio_seconds
voice_requests_success_total / voice_requests_total
voice_tts_ws_connect_failed_total / voice_tts_ws_connect_total
voice_requests_timeout_total / voice_requests_total
```

## 11. 验收标准

- 能分别得到“用户开始输入到 TTS 首包”和“用户说完到 TTS 首包”两个 Histogram。
- 每个完整请求都能计算 ASR、LLM、TTS 和端到端耗时；异常结束请求记录明确结果。
- TTS WebSocket `session.done` 日志包含回复耗时，首个音频 chunk 日志包含首包耗时。
- 指标可按 endpoint、Provider、模型和音色聚合。
- 指标标签不包含 trace_id、session_id、request_id 或文本内容。
- 可通过 `trace_id`/`request_id` 从指标异常跳转到对应结构化日志和完整链路。
- 用户主动打断、超时、Provider 错误和连接池等待不会被归类为正常成功请求。

## 12. 待确认事项

- 客户端是否需要上报 `client_audio_started_at`，用于统计服务端首包到实际播放的延迟。
- 是否将静音检测完成作为 `input_ended_at`，还是继续以 `is_last=true` 作为统一口径。
- 线上 SLO 的目标值和 Histogram bucket 边界，需要根据一段真实流量基线确定。

## 13. 采集架构与解耦决策

本 Spec 的指标实现采用“业务埋点抽象 + Prometheus 默认实现”：`VoiceSession`、pipeline 和 TTS WebSocket client 只依赖 `VoiceMetricsSink`，不直接依赖 Prometheus Registry 或 Histogram 类型。Prometheus adapter 负责 Counter/Histogram 注册和 `/metrics/voice` exposition endpoint；测试和关闭指标场景使用 `NoopMetricsSink`。

指标不通过日志解析生成。日志和 Trace 继续保存 `trace_id`、`session_id`、`request_id`，用于从聚合指标跳转到单次请求；这些字段禁止作为指标标签。完整的技术方案、依赖边界、Prometheus/OpenTelemetry 演进路径见 [`2026-09-01-voice-metrics-collection-architecture.md`](2026-09-01-voice-metrics-collection-architecture.md)。
