# 设计：新增 qwen3-asr-flash-realtime 流式 ASR 接入 + 前端实时识别页面

> 状态：待用户确认
> 约束：不改动已有代码逻辑；已有文件只做**纯增量**接线（`pub mod` 声明、match 分支、路由注册、Cargo 依赖行）

## 1. 目标与现状缺口

**目标**：接入百炼 `qwen3-asr-flash-realtime`（Qwen3-ASR-Flash-Realtime）流式识别模型，浏览器麦克风 → voice_server → DashScope Realtime WSS，实时回显 partial / final 识别结果。

**现状缺口**：
- `voice-providers/src/provider.rs::build_asr_endpoint` 已为 `qwen3-asr*` 做了 Realtime 端点路由（`wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=...`），但 `select_asr_adapter` **没有对应分支**，配置该模型即报错
- 该模型走 **OpenAI-Realtime 风格协议**，与现有 `qwen.rs` 的 DashScope 公共协议（run-task / 裸 PCM 二进制）完全不同：
  - 上行全为 JSON 文本帧；音频须 **base64** 编码后放进 `input_audio_buffer.append`
  - 建连后需发 `session.update`（modalities / input_audio_format / sample_rate / turn_detection）
  - 结束发 `session.finish`（不是 finish-task）
  - 下行事件按 `type` 字段分发；句终由服务端 VAD（`turn_detection.server_vad`）触发
- 握手需额外请求头 `OpenAI-Beta: realtime=v1`（现有 `make_real_dialer` 不发）
- `voice_server` 尚未依赖 `voice-providers`——本次是首个接入点
- 现有 `StreamingAsrSession`（asr/session.rs）在收到首个 `is_final=true` 后即停止 drain——对 Realtime 协议的多句连续听写语义不对（每句 VAD 断句都产生一次 completed，真正的终态是 `session.finished`）

## 2. 协议细节（依据 docs/qwen-asr-docs/real-asr-code.md L528-860, L2182-2369）

**建连 URL**（docs/qwen-asr-docs/real-asr-code.md 全部 8 处示例一致，ASR realtime 用**业务空间专属域名**，与 TTS 的 `dashscope.aliyuncs.com` 不同）：
```
华北2（北京）: wss://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/api-ws/v1/realtime?model=qwen3-asr-flash-realtime
新加坡:       wss://{WorkspaceId}.ap-southeast-1.maas.aliyuncs.com/api-ws/v1/realtime?model=qwen3-asr-flash-realtime
```
> 现有 `provider.rs::build_asr_endpoint` 按通用域名拼 realtime URL——那是 TTS 规则，对 ASR 不适用。按约束不改它，**新模块自带端点解析**：
> 1. `asr_stream.endpoint` 显式配置 → 原样使用（缺 `?model=` 自动追加）
> 2. 否则 `workspace_id` **必填** → `wss://{workspace_id}.{region}.maas.aliyuncs.com/api-ws/v1/realtime?model={model}`（region 默认 `cn-beijing`）
> 3. 两者都没有 → 建连时返回明确错误（提示配 workspace_id 或 endpoint）

**建连请求头**：
```
Authorization: Bearer <DASHSCOPE_API_KEY>
OpenAI-Beta: realtime=v1
```

**上行（C→S，JSON 文本帧）**：
```jsonc
// ① 建连后：session.update（VAD 模式，断句静音 400ms——文档推荐对话场景值）
{"type":"session.update","session":{
  "modalities":["text"],"input_audio_format":"pcm","sample_rate":16000,
  "turn_detection":{"type":"server_vad","threshold":0.0,"silence_duration_ms":400}}}
// ② 音频：每 ~100ms 一帧（3200B PCM s16le 16k mono），base64
{"type":"input_audio_buffer.append","audio":"<base64>"}
// ③ 结束
{"type":"session.finish"}
```

**下行（S→C，按 `type` 分发）**：
| type | 含义 | 映射 |
|---|---|---|
| `conversation.item.input_audio_transcription.text` | 增量 partial：`text` + `stash` 拼接 | Partial |
| `conversation.item.input_audio_transcription.completed` | 句终 final：`transcript` | Final |
| `input_audio_buffer.speech_started` / `speech_stopped` | 服务端 VAD 起止 | SpeechStarted / SpeechStopped |
| `session.created` / `session.updated` | 控制事件 | 忽略 |
| `session.finished` | 会话终态 | Finished（流结束） |
| `error` | 错误（`error.code` / `error.message`） | Error |

注：该模型当前不返回时间戳；`emotion` 字段暂不透出（后续增强点）。

## 3. 架构

```
浏览器 asr_realtime.html
  │ AudioWorklet: mic → 16k s16le PCM
  │ WS: /stream/asr（新）
  │   ↑ binary PCM 帧 / ↓ JSON 事件
  ▼
voice_server::asr_stream_api（新文件）
  │ actix-ws 会话循环：select! { 浏览器帧, provider 事件 }
  ▼
voice_providers::asr::qwen3_realtime（新文件）
  ├─ Qwen3RealtimeAdapter      impl AsrModelAdapter（帧构造 + 事件解析）
  ├─ make_realtime_dialer      Authorization + OpenAI-Beta 头
  └─ start_realtime_session    增量会话（send_audio / finish），事件流到 session.finished 才结束
        │ 复用 ws_pool::WsPool / TungsteniteWs / GaxFrame
        ▼
   DashScope Realtime WSS
```

**为什么不复用 `StreamingAsrSession`**：它在 finish 后收到首个 `is_final=true` 即断流；Realtime 多句听写需要收完所有句子、以 `session.finished` 为终态。新会话循环放新文件，不动 session.rs。

**Adapter 双层解析**：新文件提供 `parse_realtime_event() -> RealtimeEvent`（富事件枚举）；trait 方法 `parse_event()` 委托它并映射成 `AsrEvent`（Partial→is_final=false，Final→is_final=true，其余→Ok(None)），保持批量路径 `build_asr_client` 兼容。

## 4. 模块设计

### 4.1 voice-providers（新 `src/asr/qwen3_realtime.rs`）

```rust
pub struct Qwen3RealtimeAdapter { model: String, silence_ms: u32 }
impl AsrModelAdapter for Qwen3RealtimeAdapter { ... }   // open_request/audio_frame/stop_frame/parse_event

pub enum RealtimeEvent {
    Partial { text: String },      // text + stash
    Final { text: String },        // transcript
    SpeechStarted, SpeechStopped,  // 服务端 VAD
    Finished,                      // session.finished，流终止
}

pub fn make_realtime_dialer(endpoint, api_key) -> Dialer      // +OpenAI-Beta 头
pub struct RealtimeAsrSession { cmd_tx }                       // send_audio / finish / abandon
pub async fn start_realtime_session(pool, adapter, dialer, sr, ch, task_id)
    -> Result<(RealtimeAsrSession, BoxStream<Result<RealtimeEvent, ClientError>>)>
// 后台任务 select! { cmd_rx → 发帧, conn.recv → parse → event_tx }，
// Finished / Error / 连接断开 → 终止并 release 连接
```

音频帧内部按 CHUNK_BYTES(3200) 切片——`send_audio` 无长度限制（与 StreamingAsrSession 约定一致）。

### 4.2 voice_server（新 `src/asr_stream_api.rs`）

**配置**（惰性 OnceLock，优先级 env > YAML `asr_stream:` 段 > 默认）：
```yaml
asr_stream:                       # 可选段；缺省时用默认值 + env
  model: "qwen3-asr-flash-realtime"
  api_key: "sk-..."               # 或 env DASHSCOPE_API_KEY / VOICE_ASR_STREAM_API_KEY
  workspace_id: "llm-xxxxxx"      # 必填（除非显式给 endpoint）——ASR realtime 专属域名需要
  region: "cn-beijing"            # cn-beijing | ap-southeast-1；默认 cn-beijing
  endpoint: null                  # 显式覆盖（完整 URL，缺 ?model= 自动追加）
  silence_duration_ms: 400
```
环境变量：`VOICE_ASR_STREAM_{API_KEY,WORKSPACE_ID,MODEL,REGION,ENDPOINT}` + `DASHSCOPE_API_KEY`。
YAML 从 `resolve_config_path("voice_server", ...)` 找到的同一份 config 读取（复用 lib 导出的解析函数，不重复实现搜索逻辑）。
端点解析顺序：`endpoint` 显式 > `{workspace_id}.{region}.maas.aliyuncs.com/api-ws/v1/realtime?model=` > 报错。

**WS 协议**（浏览器 ↔ voice_server）：
- ↑ text `{"type":"start"}` → 建连 DashScope、发 session.update；回 `{"type":"started","session_id":...}`
- ↑ binary（raw PCM s16le 16k mono）→ `send_audio`
- ↑ text `{"type":"finish"}` → `session.finish()`；DashScope 事件收完后回 `{"type":"finished"}`
- ↑ text `{"type":"stop"}` / 连接断开 → drop session（abandon，释放连接）
- ↓ `{"type":"partial","text":...}` / `{"type":"final","text":...}` / `{"type":"speech_started"}` / `{"type":"speech_stopped"}` / `{"type":"error","message":...}`

### 4.3 前端（新 `static/asr_realtime.html` + `static/asr_realtime.js`）

- 独立页面，访问 `http://127.0.0.1:8080/asr_realtime.html`（`Files::new("/")` 自动服务，**零** index.html 改动）；复用现有 `style.css`
- 布局：状态徽标（连接/识别中/空闲）+ VAD 指示（说话中/静音）+「开始识别 / 结束」按钮 + 转写区（当前句 partial 行实时替换 + 已完成句子列表逐行固定）
- 采集：getUserMedia + AudioWorklet（16k 重采样 + s16le 量化，worklet 经 Blob URL 内联，与 app.js 同款方案），100ms 帧二进制直发
- 结束语义：「结束」按钮发 finish，收到 `finished` 事件后关 WS；页面刷新/关页 → WS 断开 → 服务端 abandon

## 5. 文件清单

**新文件（全部核心逻辑）**：
| 文件 | 内容 |
|---|---|
| `crates/voice-providers/src/asr/qwen3_realtime.rs` | adapter + dialer + realtime session + 单元测试 |
| `crates/voice-providers/tests/qwen3_asr_realtime_test.rs` | 集成测试（mock WS，无网络） |
| `crates/voice_server/src/asr_stream_api.rs` | WS 端点 + 配置 + 协议映射 |
| `crates/voice_server/static/asr_realtime.html` | 页面骨架 |
| `crates/voice_server/static/asr_realtime.js` | 采集 + WS 客户端 + UI |

**已有文件纯增量接线（不改任何已有逻辑）**：
| 文件 | 增量 |
|---|---|
| `crates/voice-providers/src/asr/mod.rs` | `pub mod qwen3_realtime;` + `select_asr_adapter` 两个新分支（稳定版 + 快照版） |
| `Cargo.toml`（workspace） | `[workspace.dependencies]` 加 `voice-providers` |
| `crates/voice_server/Cargo.toml` | `voice-providers` + `actix-ws = "0.3"` |
| `crates/voice_server/src/lib.rs` | `pub mod asr_stream_api;` |
| `crates/voice_server/src/service.rs` | `api_init` 注册 `/stream/asr` 路由（在 Files 挂载之前） |
| `crates/voice_server/src/config/config.yaml.temp` | 追加 `asr_stream:` 示例段 |
| `README.md` / `PROGRESS.md` | 追加文档段（非代码） |

## 6. 测试策略（TDD）

1. **adapter 单测**（先写，Red→Green）：session.update JSON 形状 / append base64 帧 / finish 帧 / 各 type 事件解析（partial 含 stash 拼接、final、VAD、finished、error、非 JSON 容忍）/ 快照版模型名透传
2. **端点解析单测**：endpoint 显式（含/缺 `?model=`）> workspace_id+region 构造 > 缺两者报错
3. **realtime session 集成测试**：mock `WebSocketLike`（参照现有 ws_pool 测试的 mock 设施）模拟 DashScope 回包序列：partial→final→（继续）final→finished，断言事件流完整且不在首个 final 断流
4. **voice_server 协议映射单测**：RealtimeEvent → 浏览器 JSON 行的映射函数
5. **端到端**：手动验证（需真实 API key + WorkspaceId），README 记录验证步骤

## 7. 风险与边界

- **OpenAI-Beta 头缺失即握手失败**——新 dialer 负责带上；若线上协议头有变（文档为准），只动新文件
- **Mock 测试无法覆盖真实握手/鉴权**：真实联调留待用户拿 key 验证（与项目现状一致，voice-providers 本就标注"未做真实 DashScope 联调"）
- **每连接一条 DashScope WSS**：复用 ws_pool 连接池上限（默认 16）天然限流；超出时 acquire 超时返回错误事件
- **60s 限制**：文档提示非 VAD 模式音频累计不超 60s；本设计用 VAD 模式（默认），不受此限
- 页面无入口链接（不改 index.html）——README 记录直链 `/asr_realtime.html`
