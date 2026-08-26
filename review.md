# 代码审查：qwen3-asr-flash-realtime 流式 ASR 接入

> 范围：本次新增的所有文件 + 纯增量接线行。审查维度 = 正确性、并发与线程安全、性能、配置与错误处理、可测性、可维护性。

## 总体结论

**无 Critical，无 Major；5 条 Minor**。全部 162/0 测试通过；既有功能冒烟（`/`、`/admin/*`、`/ws/voice/*`）行为不变。

---

## 对照 plan 的交付

| 计划项 | 状态 |
|---|---|
| A1-A3 adapter + 帧构造 + 事件解析 | ✅ 22 个单测 |
| A4 mod.rs 增量 + select_asr_adapter | ✅ 纯增量 |
| A5 make_realtime_dialer（含 OpenAI-Beta） | ✅ header 单测验证 |
| A6 RealtimeEvent + parse_realtime_event | ✅ 富解析 + 委托 |
| A7-A8 start_realtime_session + MockWs 集成 | ✅ 5 个集成测试（含 ScriptedServer） |
| B1 依赖增量 | ✅ workspace + voice_server Cargo.toml 增量 |
| B2 配置 + env 覆盖 + 端点解析 | ✅ 单测覆盖（yaml full/default/unknown keys/env 覆盖/解析 → endpoint） |
| B3 事件 → 浏览器 JSON 映射 | ✅ 单测 |
| B4 actix-ws handler | ✅ WS 协议级冒烟通过（无 key 错误路径） |
| B5 路由接线 | ✅ `Files` 注册之前的纯增量 scope |
| B6 cargo test --workspace | ✅ 162/0 |
| C1-C3 前端 + 无 key 冒烟 | ✅ 页面 + JS + WS 错误响应验证 |
| D0 diff 审计 | ✅ 5 个已有文件：只 +pub mod / +match arm / +route / +Cargo dep / +config 示例段；2 处 `-` 行均为「EOF 无换行 + 在后追加」的内容零变化 diff 呈现 |
| D1 README + PROGRESS 追加 | ✅ |
| D2 review.md | ✅ 本文档 |

---

## 审查发现（按严重度排序）

### 🟡 Minor — 后续可改进，不影响本次交付

#### M1. resolve_realtime_endpoint 错误用 `ClientError::Decode`
**位置**：`crates/voice-providers/src/asr/qwen3_realtime.rs::resolve_realtime_endpoint`
**描述**：配置缺失返回 `ClientError::Decode(...)`，语义上是「配置错误」而非「解码错误」。但 `ClientError` 没有 `Config` 变体；增加变体会破坏现有 `match` 的穷尽性（现有 match 都用 `_` 通配，添加是 source-compatible，但语义扩展超出本次范围）。
**影响**：仅错误类型名称贴切度；消息内容明确（含 "workspace" 关键字，定位无障碍）。
**建议**：下一轮可加 `ClientError::Config(String)` 变体并迁移。

#### M2. actix-ws 的 `MessageStream` 非 Send，使用 `actix_web::rt::spawn`
**位置**：`crates/voice_server/src/asr_stream_api.rs::ws_asr_stream`
**描述**：WS handler 用 `actix_web::rt::spawn`（而非 `tokio::spawn`），session 跑在 actix 当前 arbiter 线程上，而非独立 tokio worker。低 worker_num 配置下，慢客户端可能拖累 arbiter。
**影响**：与现有 `VoiceService::wsdata` 用 `tokio::spawn` 不对称；但其只处理 VoicePayload（同步），不会持非 Send 类型。
**建议**：在 PROGRESS 记录此差异；若后续多并发 WS 客户端成为性能问题，可在 webhttp 框架侧考虑 ws 流可 Send 化的方案。

#### M3. env 变量测试的并发安全
**位置**：`crates/voice_server/src/asr_stream_api.rs::tests::env_overrides_yaml`
**描述**：单测直接 `std::env::set_var`，与同 crate 其他并行测试在同一进程。当前 voice_server 内仅这一个测试写 env，安全；若未来新增 env 写入测试需加 Mutex 串行化或每个测试用独立临时 env 名称。
**影响**：低；当前测试在 finally-equivalent（末尾 restore）还原所有变量。
**建议**：后续若引入第二个 env 写入测试，加 `#[serial]` 或共享 mutex。

#### M4. 前端 worklet 无客户端 VAD，VAD 边沿事件仅靠服务端
**位置**：`crates/voice_server/static/asr_realtime.js::WORKLET_SRC`
**描述**：服务端 VAD 配置 400ms 静音断句；若服务端配置改动，前端无对应反馈机制。
**影响**：当前服务端配置由 YAML/env 控制，前端只是消费者，无需同步。
**建议**：在 `started` 事件中带 `vad: {silence_ms: 400}` 元信息便于前端展示；本次未做（YAGNI）。

#### M5. 错误事件后 forwarder 仍向 channel 推 Ended
**位置**：`crates/voice_server/src/asr_stream_api.rs::start_provider_session`（forwarder）
**描述**：provider stream Err 后，forwarder 仍继续 drain 然后推 `BridgeItem::Ended`。主循环在收到 Err 时已置 `provider_ended=true`，后续 Ended 因 `if !provider_ended` 守卫被禁用。无功能问题。
**影响**：无；属健壮性余量。
**建议**：保留现状。

### ℹ️ 同步声明（informational，非问题）

- voice-providers 之前 staged 但未接入 workspace（缺 `async-tungstenite`/`bytes`/`prost[-build]` workspace.dependencies 定义）—— 本次纯增量补齐，不算回归；之前状态无法构建（README 描述的"已有测试全过"实际未跑过 workspace 级测试）
- `select_asr_adapter` 新增三个 model arm 不会干扰现有 arm 的匹配优先级（按顺序，`paraformer-realtime-v2` 仍在最前）
- 浏览器独立页面（不改 index.html）—— 用户导航到 `/asr_realtime.html` 直接访问；README 文档了直链

---

## 关键设计决策回顾

| 决策 | 理由 |
|---|---|
| 不复用 `session.rs::StreamingAsrSession` | 公共协议在首个 is_final 即断流；Realtime 多句 VAD 需要以 `session.finished` 为终态 |
| 新 dialer（`make_realtime_dialer`） | 必须带 `OpenAI-Beta: realtime=v1` 头，公共 dialer 没带 |
| 端点解析放新文件 `resolve_realtime_endpoint` | `provider.rs::build_asr_endpoint` 按 TTS 规则用通用域名（`dashscope.aliyuncs.com`），与 ASR realtime 的业务空间专属域名不符；不动 provider.rs |
| 纯增量接线而非零改动 | 现有 module/路由声明必须扩展才能接线；纯增量是「不改动已有逻辑」的最强约束 |
| mpsc 桥 + 守卫分支 | 复用 actix-ws MessageStream 简化主循环；禁用 provider 事件分支防止 busy loop |
| ScriptedServer mock | `ws_pool::test_helpers::MockWs` 空队列即报错，与后台任务"等下一事件"语义冲突；新写确定性 mock |
| actix_web::rt::spawn | MessageStream 非 Send；tokio::spawn 编译失败 |
| Lazy `OnceLock<Runtime>` | 首次 WS 连接时解析配置 + 建连接池；之后复用 |

---

## 测试矩阵

| 覆盖项 | 位置 | 状态 |
|---|---|---|
| open_request（session.update 形状） | voice-providers 单测 | ✅ |
| audio_frame（base64 往返） | voice-providers 单测 | ✅ |
| stop_frame（session.finish） | voice-providers 单测 | ✅ |
| parse_event 全事件类型（含 unknown/error/非JSON） | voice-providers 单测 | ✅ |
| parse_realtime_event 富解析 | voice-providers 单测 | ✅ |
| endpoint 解析（显式含/缺 model / workspace / region / 缺配置报错） | voice-providers 单测 | ✅ |
| handshake header（Auth + OpenAI-Beta + Bearer 兼容 + 空 key 省略） | voice-providers 单测 | ✅ |
| Realtime 会话：多句不断流 | voice-providers 集成 | ✅ |
| Realtime 会话：上行帧序列 + 3200B 切片 + base64 长度 | voice-providers 集成 | ✅ |
| Realtime 会话：VAD 边沿 + finish 后残余 final | voice-providers 集成 | ✅ |
| Realtime 会话：abandon → 连接归还 idle | voice-providers 集成 | ✅ |
| Realtime 会话：服务端 error 终止流 | voice-providers 集成 | ✅ |
| YAML asr_stream: 段解析（full/default/unknown keys） | voice_server 单测 | ✅ |
| env 覆盖 YAML | voice_server 单测 | ✅ |
| RealtimeEvent → 浏览器 JSON 映射 | voice_server 单测 | ✅ |
| 配置 + 端点联动（cfg → resolve_endpoint） | voice_server 单测 | ✅ |
| WS 协议级：start 无 key 错误 | 真实 WS 冒烟 | ✅ |
| WS 协议级：未知 type 错误 | 真实 WS 冒烟 | ✅ |
| 静态资源 + 旧路由冒烟 | curl | ✅ |

## 不需要修复 —— ✅