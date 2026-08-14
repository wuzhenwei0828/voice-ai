# voice-app 实施进度

> 用于跨会话继续工作时快速恢复上下文。每完成一项 / 改一个决策就在这里追加。
> **最近更新**：2026-08-13

---

## 一、当前状态

**整体进度**：落地清单 9/15 ≈ **56%**

```
Phase 1 MVP ████████████████████ 100% (4/4)   ✅ 完成
Phase 2 半双工 ██████████████░░░░░░  66% (2/3)  ⚠️ 还差 TTS 存盘
Phase 3 全双工 ████████████░░░░░░░░  75% (3/4)  ⚠️ 还差客户端 VAD
Phase 4 生产化 ░░░░░░░░░░░░░░░░░░░░   0% (0/4)  ❌ 未开始
```

**能用的能力**：
- 固定 wav 文件输入 → 流式推到服务端 → 文字 + 流式 LLM 回复 + 流式 TTS 音频字节
- 半双工对话 demo 完整跑通（打断机制服务端支持，CLI 也接了 `q` 输入）

**下一步优先级**（按依赖关系）：
1. **TTS 存盘验证**（Phase 2 第 6 项，1 小时）：客户端把 TTS bytes 写 wav 文件，肉眼/播放器验证音质
2. **客户端 VAD**（Phase 3 第 8 项，半天）：接 Silero VAD，自动切句
3. **麦克风实时采集**（Phase 3 配套，半天）：cpal 接音频输入
4. **TTS 真实播放**（Phase 2 第 7 项，半天）：cpal 接音频输出
5. **限流**（Phase 4 第 12 项，1 天）：每会话 max 1 路 in-flight
6. **监控埋点**（Phase 4 第 14 项，1 天）：Prometheus 指标（首字延迟、TTS 成功率、ASR 准确率）

---

## 二、文件结构

```
voice-app/
├── Cargo.toml                           workspace
├── README.md                            启动命令 + 端到端联调指南
├── PROGRESS.md                          ← 本文件
├── crates/
│   ├── voice-proto/                     VoicePayload 协议定义 + encode/decode
│   ├── voice-config/                    共享 LogConfig + YAML 加载 + init_logging
│   ├── voice-server/                    基于 webhttp 的语音服务端
│   │   ├── src/
│   │   │   ├── config.rs                VoiceConfig (含 asr/llm/tts ClientConfig)
│   │   │   ├── clients.rs               AsrClient/LlmClient/TtsClient trait + HTTP impl
│   │   │   ├── session.rs               VoiceSession 状态机 + pipeline + CancellationToken
│   │   │   ├── service.rs               VoiceService: webhttp::ServiceCallback 实现
│   │   │   └── bin/voice_server.rs      启动入口
│   ├── voice-client/                    终端 SDK
│   │   ├── src/
│   │   │   ├── lib.rs                   VoiceClient 封装 webclient
│   │   │   ├── callback.rs              VoiceCallback trait + 默认实现
│   │   │   └── bin/voice_terminal.rs    CLI demo
│   ├── mock-asr/                        mock ASR 服务
│   ├── mock-llm/                        mock LLM 服务
│   └── mock-tts/                        mock TTS 服务
├── configs/                             默认 YAML 配置
│   ├── voice-mock-asr.yaml
│   ├── voice-mock-llm.yaml
│   ├── voice-mock-tts.yaml
│   ├── voice-voice_server.yaml
│   └── voice-voice_terminal.yaml
└── logs/                                启动后自动生成 *.log
```

---

## 三、Phase 1 ✅ MVP（100%）

| 步骤 | 状态 | 文件 | 备注 |
|------|------|------|------|
| 1. VoicePayload 协议 | ✅ | `voice-proto/src/lib.rs` | `enum VoicePayload`，2 个单测 |
| 2. wav 流式上传 | ✅ | `voice_terminal.rs::push_from_wav` | 20ms 一帧，模拟采集节奏 |
| 3. ASR mock + 文本 echo | ✅ | `mock-asr/src/main.rs` + `clients.rs::HttpAsrClient` | POST /recognize，NDJSON 流 |
| 4. 客户端打印 ASR | ✅ | `voice_client/src/callback.rs` | `DefaultVoiceCallback.on_payload` |

---

## 四、Phase 2 ⚠️ 半双工对话（66%，缺 6、7）

| 步骤 | 状态 | 文件 | 备注 |
|------|------|------|------|
| 5. LLM 流式接入 | ✅ | `mock-llm/src/main.rs` + `clients.rs::HttpLlmClient` | POST /chat，NDJSON 流，固定模板 |
| **6. TTS 流式 + 存盘验证** | ❌ | `voice_terminal.rs` 待改 | 客户端收 TtsAudio 后**写 wav 文件**（不只是缓存到 Vec） |
| 7. 终端 TTS 本地播放 | ❌ | `voice_terminal.rs::push_from_mic` 旁边加 `play_audio` | cpal 接 default output stream |

---

## 五、Phase 3 ⚠️ 全双工 + 体验（75%，缺 8）

| 步骤 | 状态 | 文件 | 备注 |
|------|------|------|------|
| 8. 客户端 VAD | ❌ | `voice_terminal.rs` 待加 `VoiceActivityDetector` | 推荐 Silero VAD（ONNX）；能量阈值法可作 MVP |
| 9. 打断机制 | ✅ | 服务端 `session.rs::CancellationToken` + 客户端 `voice_terminal.rs::Interrupt` | CLI 输入 `q` 触发 |
| 10. LLM 按句切分送 TTS | ✅ | `session.rs::next_sentence_end` | 按 `。！？.? !` 切，**中文句号也要切** |
| 11. ASR partial 实时上屏 | ✅ | 服务端流式 `AsrPartial { is_final: false }` + 客户端 `on_payload` 区分 |

---

## 六、Phase 4 ❌ 生产化（0%）

| 步骤 | 状态 | 备注 |
|------|------|------|
| 12. 限流 | ❌ | 用 `tokio::sync::Semaphore` 每会话 1 路 in-flight；全局 token bucket |
| 13. 多租户 + 配额 | ❌ | 接 `voice_session_id → tenant_id` 映射；从 JWT 取 |
| 14. 监控埋点 | ❌ | 加 `prometheus` crate；指标：asr_first_byte_latency / llm_first_token_latency / tts_first_byte_latency / pipeline_total_latency / 各阶段错误计数 |
| 15. 内容审核 + 安全 | ❌ | LLM 输入/输出过敏感词；token 不进日志 |

---

## 七、配置能力 ✅

### YAML 配置（已实现）

每个 binary 加载 `./voice-<bin_name>.yaml`，支持：

| 字段 | 类型 | 说明 |
|------|------|------|
| `log.level` | trace\|debug\|info\|warn\|error | 全局日志级别 |
| `log.file` | path | 空=stdout；非空写到该文件（不轮转） |
| `log.format` | pretty\|json | pretty=人类可读；json=结构化 |
| `server.port` | u16 | 服务监听端口 |
| `server.worker_num` | usize | webhttp Worker 数量 |
| `asr.endpoint` / `llm.endpoint` / `tts.endpoint` | string | base URL |
| `asr.path` / `llm.path` / `tts.path` | string | 拼接在 endpoint 后；空=endpoint 是完整 URL |
| `asr.method` / `llm.method` / `tts.method` | POST\|GET\|PUT | HTTP method |
| `asr.model` / `llm.model` / `tts.model` | string | 同时发 `X-Model` header + body 字段 |
| `asr.authorization` / 同上 | string | 完整 `Authorization: Bearer xxx` 值 |
| `asr.headers` / 同上 | map[string]string | 自定义 header（厂商特有字段如 `X-Region`） |
| `asr.timeout_ms` / 同上 | u64 | HTTP 超时 |
| `terminal.url` | ws://... | voice_terminal 连的服务端地址 |
| `terminal.file` | string | wav 路径（不填=用麦克风 stub） |
| `terminal.interrupt` | bool | 是否监听 stdin `q` 触发打断 |

### 优先级链

```
CLI 参数 > 环境变量 (VOICE_*) > YAML 配置文件 > 内置默认
```

### 环境变量支持

```bash
VOICE_LOG_LEVEL / VOICE_LOG_FILE      # log 段
VOICE_PORT                            # server.port
VOICE_<ASR|LLM|TTS>_URL               # endpoint
VOICE_<ASR|LLM|TTS>_AUTHORIZATION     # authorization
VOICE_<ASR|LLM|TTS>_MODEL             # model
```

---

## 八、关键技术决策（不要重新讨论）

### 协议层

- **VoicePayload 用 enum + tag**（`#[serde(tag="type")]`）：上行音频 / 下行结果共用 wire format
- **AudioChunk 用 Indication 单向推**：避免每个分片带 event_id
- **状态控制（SessionStart/End/Interrupt）走 ClientCommand/ServerCommand**：需要 event_id

### 服务端

- **复用 webhttp 框架**：HTTP+WS+Worker+Room 全套都现成的，业务只挂 `ServiceCallback`
- **VoiceSession 简化状态机**：Idle/Listening/Processing/Speaking；并发允许 Listening + 后台 pipeline
- **CancellationToken 打断**：任何阶段被 Interrupt 即停；下一段 LLM 不再生成即可
- **Sentence splitter 用 `next_sentence_end` 函数**：遍历 char_indices 找标点，返回标点**之后**的字节索引（避免 `drain` 切 UTF-8 中间字节）
- **logs/ 在 cwd 下**：不是相对 binary 路径；启动脚本 cd 到 configs/

### 客户端

- **复用 webclient 框架**：自动重连、JWT、心跳都现成的
- **降级方案**：MVP 用 wav 整段做 is_last=true；真实 VAD 待 Phase 3 接入

### 错误修复历史

1. **webhttp 与 env_logger 冲突**：把 `webhttp/src/lib.rs` 的 `env_logger::init_from_env` 改成 `try_init_from_env().ok()`，不 panic
2. **wsdata 是同步的**：on_payload 也得同步；pipeline 用 `tokio::spawn` 异步跑
3. **actix-rt 不能在 `#[tokio::main]` 里跑**：用 `#[actix_web::main]`（含 actix-rt）
5. **UTF-8 drain 切分**：用 `next_sentence_end` 函数算 byte index，不用 `drain(..=idx)`（idx 是 char 起始字节）
6. **tracing-subscriber 默认 features 含 tracing-log**：和 env_logger 抢全局 logger。voice-config 走 `tracing-subscriber::fmt::try_init`（无 tracing-log），env_logger 后续 try_init 静默失败

---

## 十、已知问题与 TODO

### Bug

1. **空字符串 TTS**：LLM delta 末尾孤立标点（如 "。"）触发 `text="。"` 的 TTS 请求 → 生产里应 `if text.trim().is_empty() { continue; }`
2. **VoiceSession 的 DashMap 持有**：当前 `on_payload` 是同步的，但每个 session 只处理一条消息就丢（Spawn 出 Actor 之后 session 就没被持久持有）。需要改成 session 在 DashMap 里跨消息调用 `on_payload` —— 影响多轮上下文。
3. **log guard leak**：`voice-config::init_logging` 里 `Box::leak` 了 writer guard（避免 stdout guard 提前 drop）；无害但不优雅
4. **mock 服务 graceful shutdown**：Ctrl-C 时正在处理的请求会被砍掉；生产里要等 in-flight 结束

### 性能

- **Pipeline 异步顺序**：当前 ASR 串 LLM 串 TTS（除了 TTS 内部 chunk 并行）；Phase 2 pipeline overlap 已经实现（LLM 按句切分，TTS 边出边推）
- **reqwest Client 每次新建**：每个请求 `reqwest::Client::new()` 有 TLS 握手开销，应该复用同一个 Client

### 测试

- 缺单元测试：除了 voice-proto 的 2 个测试，session/clients/service 都没测
- 缺集成测试：没有跑完整 wav → mock-asr → mock-llm → mock-tts → 字节数校验的自动化
- mock 服务没有 CI 自检

### 文档

- `README.md` 的端到端命令还是用 `/tmp/voice-app-test/*.log` 路径，**没更新到 YAML + configs/ + logs/ 的新布局**
- 没有架构图（流程图、状态机图）—— 按 diagram skill 可以补一份

---

## 十一、下次开工从哪里开始

**如果是新会话**，按这个顺序：

1. **读本文档**（5 分钟）
2. **跑一遍启动命令验证环境没坏**：
   ```bash
   cd /Users/wuzhenwei/Code/github/voice-app/configs
   ../target/debug/mock-asr &
   ../target/debug/mock-llm &
   ../target/debug/mock-tts &
   ../target/debug/voice_server &
   ../target/debug/voice_terminal --config voice-voice_terminal.yaml --file /tmp/test.wav
   ```
3. **看 logs/*.log 验证 pipeline 完整跑通**
4. **从"下一步优先级"挑一项继续**

**如果想推 Phase 2 第 6 项（TTS 存盘）**：在 `voice_terminal.rs` 的 `TtsAudio` 回调里加：
```rust
// 累计够 N 字节或 is_last=true 时写 wav 文件
let mut file = std::fs::OpenOptions::new()
    .create(true).append(true).open("tts_output.wav")?;
file.write_all(&pcm_bytes)?;
```
注意：当前 mock-tts 生成的是 s16le 16kHz 正弦波 PCM，wav header 需要手动加（44 字节）。