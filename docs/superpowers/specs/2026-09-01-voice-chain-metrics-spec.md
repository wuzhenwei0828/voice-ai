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
| `input_ended_at` | 服务端收到触发本轮 pipeline 的最后一个音频 chunk | `AudioChunk.is_last=true`；服务端强制截断时取触发截断的当前 chunk |
| `asr_started_at` | 开始调用 ASR Provider | ASR 请求发出前 |
| `asr_final_at` | 收到可用于后续处理的最终 ASR 文本 | ASR final 事件 |
| `llm_started_at` | 开始调用 LLM Provider | LLM 请求发出前 |
| `llm_first_text_at` | 收到第一个 `trim()` 后非空的 LLM delta | 第一个有效文本 delta |
| `llm_completed_at` | LLM 流结束 | LLM stream 完成 |
| `tts_input_sent_at` | 向 TTS Provider 发送文本 | `input.text` 发送成功 |
| `tts_input_done_at` | 向 TTS Provider 发送 `input.done` 成功 | TTS 输入结束 |
| `tts_first_audio_at` | 收到第一个二进制音频 chunk | TTS WebSocket 首个音频帧 |
| `tts_completed_at` | 收到 `session.done` | TTS WebSocket 会话完成 |
| `first_audio_ws_sent_at` | 首个非空 TTS 音频 WS 消息成功进入下行发送队列 | `Recipient::try_send` 成功返回 |
| `client_input_ended_at` | 端侧检测到本轮用户输入结束 | 端侧收到 `is_last=true` 音频回调 |
| `client_final_audio_sent_at` | 尾帧被端侧 WebSocket 接受并进入发送队列 | `WebSocket.send()` 无异常返回 |
| `client_first_audio_received_at` | 端侧收到本轮首个非空 TTS 音频帧 | WebSocket `tts_audio` 首帧 |
| `client_audio_started_at` | 端侧播放器实际排程首帧播放 | `AudioBufferSource.start()` 对应时间 |

服务端阶段耗时使用 `Instant` 或 OpenTelemetry monotonic duration 计算。跨客户端和服务端的指标优先由客户端上报相对时间，或者只统计服务端可观测的区间，避免受机器时钟偏差影响。

## 5. 核心全链路指标

所有延迟指标使用 Histogram，单位为秒，至少展示 p50、p90、p95、p99。

### 5.1 本期链路延迟合同

| # | 指标 | 计算方式 | 时钟所有者 |
|---|---|---|---|
| 1 | `voice_input_end_to_asr_output_end_seconds` | `asr_final_at - input_ended_at` | Server |
| 2 | `voice_input_end_to_llm_first_text_seconds` | `llm_first_text_at - input_ended_at` | Server |
| 3 | `voice_input_end_to_tts_first_frame_seconds` | `tts_first_audio_at - input_ended_at` | Server |
| 4 | `voice_input_end_to_ws_first_audio_sent_seconds` | `first_audio_ws_sent_at - input_ended_at` | Server |
| 5 | `voice_asr_input_to_output_end_seconds` | `asr_final_at - asr_started_at` | Server |
| 6 | `voice_llm_input_to_first_text_seconds` | `llm_first_text_at - llm_started_at` | Server |
| 7 | `voice_llm_first_text_to_tts_first_frame_seconds` | `tts_first_audio_at - llm_first_text_at` | Server |
| 8 | `voice_tts_first_frame_to_ws_first_audio_sent_seconds` | `first_audio_ws_sent_at - tts_first_audio_at` | Server |
| 9 | `voice_client_first_audio_received_to_playback_seconds` | `client_audio_started_at - client_first_audio_received_at` | Client，上报相对时长 |
| 10 | `voice_client_input_end_to_final_audio_sent_seconds` | `client_final_audio_sent_at - client_input_ended_at` | Client，上报相对时长 |

第 1 至 4 项的“输入结束”统一指服务端收到尾帧音频的时间。第 4、8 项的“WS 发送成功”统一指消息成功进入服务端下行发送队列，不声明客户端已经收到。第 9、10 项全部使用端侧单调时钟；客户端只上报相对时长，服务端不直接相减两台机器的时间戳。

旧指标中与本合同语义相同的指标直接重命名，不双写旧名称：

```text
voice_e2e_utterance_end_to_tts_first_audio_seconds -> voice_input_end_to_tts_first_frame_seconds
voice_asr_duration_seconds                         -> voice_asr_input_to_output_end_seconds
voice_llm_time_to_first_token_seconds              -> voice_llm_input_to_first_text_seconds
voice_e2e_tts_first_audio_to_client_playback_seconds -> voice_client_first_audio_received_to_playback_seconds
```

### 5.2 阶段延迟

```text
voice_pipeline_queue_duration_seconds
  = asr_started_at - input_ended_at

voice_asr_input_to_output_end_seconds
  = asr_final_at - asr_started_at

voice_llm_input_to_first_text_seconds
  = llm_first_text_at - llm_started_at

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
llm_first_text
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

1. 本期 10 个链路 Histogram 的 p50/p90/p95/p99。
2. ASR、LLM、TTS 和 WS 下行发送各阶段的延迟拆分。
3. 端侧尾帧发送和首帧播放缓冲延迟。
4. TTS 完成耗时。
5. 端到端成功率、超时率、取消率。
6. 按 Provider、模型、音色的延迟对比。
7. TTS WebSocket 建连失败率和连接池等待时间。

建议先将以下指标作为 SLI，具体阈值由线上基线确定：

```text
voice_input_end_to_ws_first_audio_sent_seconds
voice_requests_success_total / voice_requests_total
voice_tts_ws_connect_failed_total / voice_tts_ws_connect_total
voice_requests_timeout_total / voice_requests_total
```

## 11. 验收标准

- 能在 `/metrics/voice` 得到本期定义的 10 个链路 Histogram，且旧的 4 个同义指标名不再暴露。
- 第 1 至 8 项只使用服务端 `Instant`；第 9、10 项只接收客户端单调时钟计算出的相对毫秒数。
- 客户端指标消息只允许固定枚举值，拒绝空 `message_id`、非有限值、负值和超过 30 秒的值，同一 `message_id + metric` 只记录一次。
- 每个完整请求都能计算 ASR、LLM、TTS 和端到端耗时；异常结束请求记录明确结果。
- TTS WebSocket `session.done` 日志包含回复耗时，首个音频 chunk 日志包含首包耗时。
- 指标可按 endpoint、Provider、模型和音色聚合。
- 指标标签不包含 trace_id、session_id、request_id 或文本内容。
- 可通过 `trace_id`/`request_id` 从指标异常跳转到对应结构化日志和完整链路。
- 用户主动打断、超时、Provider 错误和连接池等待不会被归类为正常成功请求。

## 12. 实现状态

- [x] Server pipeline、ASR、LLM、TTS WebSocket 和结果分类指标已通过 `VoiceMetricsSink` 接入。
- [x] TTS WebSocket 连接池 Gauge、等待/回收/失效 Counter，以及音频时长、chunk 间隔、实时率指标已接入。
- [x] 使用固定枚举的 `client_metric_report` 上报“首帧接收到播放”和“输入结束到尾帧发送”两个端侧相对时长；Server 做 0-30 秒边界校验并按 `message_id + metric` 去重。
- [x] 将 ASR、LLM、TTS 首帧的旧同义指标重命名，并新增输入结束、LLM 首文本和首个音频 WS 发送之间的组合时延。
- [x] Prometheus 暴露地址为 `/metrics/voice`；业务逻辑只依赖 `VoiceMetricsSink`，测试和关闭指标使用 `NoopMetricsSink`。
- [ ] OpenTelemetry exporter 尚未启用，后续可新增 sink adapter，不改业务埋点调用方。
- `input_ended_at` 已确定为服务端收到触发 pipeline 的尾帧音频；强制时长/缓冲截断时取触发截断的当前音频帧。
- 线上 SLO 的目标值和 Histogram bucket 边界，需要根据一段真实流量基线确定。

## 13. 采集架构与解耦决策

本 Spec 的指标实现采用“业务埋点抽象 + Prometheus 默认实现”：`VoiceSession`、pipeline 和 TTS WebSocket client 只依赖 `VoiceMetricsSink`，不直接依赖 Prometheus Registry 或 Histogram 类型。Prometheus adapter 负责 Counter/Histogram 注册和 `/metrics/voice` exposition endpoint；测试和关闭指标场景使用 `NoopMetricsSink`。

指标不通过日志解析生成。日志和 Trace 继续保存 `trace_id`、`session_id`、`request_id`，用于从聚合指标跳转到单次请求；这些字段禁止作为指标标签。完整的技术方案、依赖边界、Prometheus/OpenTelemetry 演进路径见 [`2026-09-01-voice-metrics-collection-architecture.md`](2026-09-01-voice-metrics-collection-architecture.md)。
