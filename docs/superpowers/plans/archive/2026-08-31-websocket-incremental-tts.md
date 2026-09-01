# WebSocket TTS 增量输入改造方案

> **给执行代理：** 必须使用 `superpowers:subagent-driven-development`（推荐）或 `superpowers:executing-plans`，按任务逐项执行。每一步使用复选框跟踪状态。

**目标：** 让 vLLM-Omni WebSocket TTS 真正消费 LLM 的增量输出：连续发送多个 `input.text`，在句子边界发送 `input.done`，同时保持 HTTP TTS 的现有行为不变。

**架构：** 保留 `TtsClient::synthesize` 作为“完整文本合成”兼容接口，HTTP 和 admin 接口继续使用它。为 `TtsClient` 增加可选的会话式增量输入接口；`TtsWsClient` 实现该接口并通过全局有界连接池复用多条 WebSocket，`HttpTtsClient` 返回“不支持增量会话”。LLM→TTS 公共流水线检测该能力后，WebSocket 使用简单字符过滤和句边界 flush，HTTP 继续使用现有的按句合成路径。

**技术栈：** Rust workspace、Tokio、`async_trait`、`futures_util`、`async_stream`、`async-tungstenite`、现有 `tracing` 和单元测试设施。

**协议依据：** `/Users/wuzhenwei/Code/github/vllm-omni/docs/serving/speech_api_websocket.md`

## 全局约束

- WebSocket 控制消息使用 JSON 文本帧，音频使用二进制帧。
- `session.config` 必须是每条物理连接的第一条业务消息；每个新的下游对话开始时都要先发送一次，后续 utterance 在同一对话内保持粘性。
- `input.text` 可以发送多次；只有 `input.done` 会触发合成。
- 当前文本仍在 buffer 中时，不发送新的 `session.config`。
- `audio.done(error=true)` 和 `error` 控制帧都必须使当前 utterance 失败，但不能把 provider 详细错误泄露到用户侧协议。
- 现有 HTTP TTS 和 admin 接口行为必须保持不变。
- 日志不记录 API Key；默认只记录 endpoint、session id、字符数、帧数和字节数。为排查增量清洗问题，在 `debug` 级别记录过滤后的文本片段，不记录原始未清洗文本；`text_preview` 最多保留前 200 个 Unicode 字符，并把换行转义为 `\n`。

## 协议对齐结论（实现前逐项确认）

以下结论以 vLLM-Omni `speech_api_websocket.md` 为准，并明确区分上游协议要求和本项目的实现策略。

### 1. 增量文本和 Markdown 标签清洗

不做跨 delta 的 Markdown 语法解析，也不维护链接、代码围栏或粗体的闭合状态。每个 LLM delta 直接经过字符过滤：保留文字、数字、空格以及会影响语音节奏/断句的标点（例如逗号、句号、问号、感叹号和换行），删除不影响语音和断句的装饰符号，例如括号、引号、emoji、反引号、`*`、`_`、`~` 等。

```text
LLM delta -> 删除装饰符号 -> input.text(过滤后的片段)
```

过滤器可以对每个 delta 独立执行，不需要等待标签闭合；过滤后为空的 delta 不发送。句边界检测使用过滤后的文本，因此保留的句末标点仍可触发 `input.done`。流结束时只需 flush 尚未发送的文本，不需要复杂的 `finish()` 解析。每次过滤后以 `debug` 记录 `original_chars`、`filtered_chars` 和 `text_preview`；`text_preview` 最多保留前 200 个 Unicode 字符并转义换行。

### 2. `session.config` 发送时机

每条物理 WebSocket 连接握手成功后，第一条业务消息必须是 `session.config`。它配置音色、格式、采样率相关参数和 `stream_audio`。每个新的下游对话/请求开始时，即使配置与上一轮相同，也必须在首条 `input.text` 前重新发送一次 `session.config`；同一轮内部的后续 utterance 不重复发送，不能在已有 `input.text` 缓冲时发送。

### 3. `session.done` 和下一句发送顺序

同一条连接必须严格执行：

```text
input.text* -> input.done -> audio.start / binary / audio.done -> session.done -> input.text*
```

收到当前 utterance 的 `session.done` 之前，不能发送下一句的 `input.text`。收到后仍不释放当前 lease：同一个 `TtsInputSession` 的后续句子继续使用同一条物理连接，直接发送下一组 `input.text`，不重复发送 `session.config`。只有当前 `TtsInputSession` 的整次 LLM 回复全部结束后，才释放 lease；如果此时连接已经不可用，才移除旧连接并在需要继续发送时获取新连接。LLM reader 不能停止消费，必须用有界文本队列暂存已经生成但尚未发送的安全片段，writer 在每个 utterance 的 `session.done` 后继续发送队列内容。

示例：同一轮回复包含多个句子时，连接和配置的时序如下。

```text
用户：你好
LLM：你好啊。有什么可以帮助你的吗？

创建 TtsInputSession-1
获取连接 C1
C1: session.config

C1: input.text("你好啊。")
C1: input.done
C1: audio...
C1: session.done

# 当前 TtsInputSession-1 仍持有 C1，不归还连接池
C1: input.text("有什么可以帮助你的吗？")
C1: input.done
C1: audio...
C1: session.done

# 整轮 LLM 回复完成
归还 C1 到连接池
```

如果第二句开始时 C1 已不可用，则只在切换连接时重新发送配置：

```text
C1: 第一句处理完成，收到 session.done
C1: 连接失效，移除连接池

获取连接 C2
C2: session.config
C2: input.text("有什么可以帮助你的吗？")
C2: input.done
C2: audio...
C2: session.done

整轮回复完成，归还 C2 到连接池
```

下一轮对话会创建新的 `TtsInputSession`。即使复用 C1，也必须重新发送 `session.config`：

```text
用户：今天天气怎么样？
LLM：今天天气不错。

创建 TtsInputSession-2
复用连接 C1
C1: session.config
C1: input.text("今天天气不错。")
C1: input.done
C1: audio...
C1: session.done
```

### 4. 无 Ping 保活下的空闲回收和业务超时

上游 TTS 当前不支持 Ping/Pong 保活，因此客户端不主动发送 Ping，也不把 Pong 作为连接健康依据。上游已知超时为：WebSocket 建立后 10 秒内必须收到第一条 `session.config`；服务端完成上一条消息处理并发送响应后，连续 30 秒没有收到任何客户端消息才关闭连接。TTS 正在生成期间不会进入这段空闲计时。

客户端采用“及时配置 + 主动空闲回收”：

- WebSocket 握手成功后立即发送 `session.config`，不进入连接池后再延迟发送；从握手成功到 `session.config` 发送完成单独受 10 秒 deadline 约束，超时则关闭 socket，不进入可复用池。WebSocket 握手本身继续使用连接超时配置，不与这 10 秒混为一段。
- 每次成功发送 `session.config`、`input.text` 或 `input.done` 后更新 `last_sent_at`，用于发送诊断。只有当前 `TtsInputSession` 的整次回复全部结束并归还 lease 后，连接才允许转为 `Idle`，并将 `idle_since` 设置为归还时间；回收 deadline 固定为 `idle_since + 20 秒`。
- 同一轮内部某个 utterance 收到 `session.done` 后，连接仍处于当前 lease 的 `InUse` 状态，继续等待并发送下一句，不启动空闲回收，也不使用 `last_sent_at` 推断上游已经空闲。只有整轮回复归还连接后，若 20 秒内没有新 owner 发送消息，才主动发送 `session.close` 并移除连接。
- 选择 20 秒而不是 9 秒：它相对上游从响应完成后开始计算的 30 秒 idle timeout 保留约 10 秒安全余量，足以吸收定时器调度和网络抖动，同时给相邻对话轮次保留合理的连接复用窗口。
- 新 owner 在 deadline 前成功获取连接时，原子地将状态改为 `InUse`；新对话先发送 `session.config`，同一对话后续 utterance 继续持有同一 lease 并直接发送 `input.text`，只有连接失效切换到新连接时才重新发送 `session.config`；每次成功发送后更新 `last_sent_at`。
- 空闲关闭任务执行前必须重新锁住连接池，校验连接仍是同一 `generation`、状态仍为 `Idle`、`idle_since` 未被清空且 `now - idle_since >= 20 秒`，避免旧定时器关闭已经被新请求占用的连接。

防误杀规则保持不变：发送 `input.done` 后等待音频和 `session.done` 时，连接处于 `InUse`，不执行 20 秒空闲回收，而使用 TTS 请求级超时（默认 300 秒）。Close、读错误或请求级超时都使当前 utterance 失败并移除连接，下次使用时重新握手并立即发送 `session.config`。

### 5. 并发下的连接复用和占用确认

不能按 `session_id` 固定一条上游连接：同一个下游会话可能同时有多个请求，固定映射会把这些请求串行化并限制吞吐；也不能用一条全局 WebSocket 串行化所有请求。应使用全局有界连接池，容量代表允许的上游并发数；任意空闲连接都可以被下一个请求复用，同一条连接在一个 lease 期间仍只能由一个 owner 使用。

```rust
struct TtsWsConnection {
    stream: WsStream,
    state: ConnectionState, // Connecting | Idle | InUse | Closed
    generation: u64,
    lease_id: u64,
    owner_session_id: Option<String>,
    last_sent_at: Instant,
    idle_since: Option<Instant>,
    cancel: CancellationToken,
}
struct TtsWsPool {
    entries: Mutex<Vec<Arc<TtsWsEntry>>>,
    notify: Notify,
    max_connections: usize, // 默认 4，可配置
}
```

获取连接时先锁住 pool：找到 `Idle` entry 就原子地改为 `InUse`，分配递增的 `lease_id` 和当前下游 `session_id`；没有空闲连接且总数小于 `max_connections` 时，先预留一个 `Connecting` entry，再释放 pool 锁并执行握手和首条 `session.config`；达到上限时等待 `Notify`，并与请求取消信号做 `select`。建连或首配失败时移除预留 entry 并唤醒等待者。不同并发请求可以拿到不同连接，同一个逻辑 `session_id` 也可以同时拿到多条连接。

lease 携带 `session_id + generation + lease_id`，所有发送、接收、释放和关闭操作都校验这三个值。每条新 lease 在确认上一 owner 已收到 `session.done` 后才能复用连接，并且无论配置是否变化都先发送新的 `session.config`。连接池只负责容量和占用，不负责同一 `session_id` 的下游响应排序；需要有序输出的调用方必须在 pipeline 层用 request/utterance 序号维护顺序。

lease 不得通过“长时间持有连接池 Mutex”实现：`next_event()` 等待上游时必须允许取消路径发出取消信号。实现上使用 entry 内部的可取消 I/O 状态（或独立读写任务）；`close()` 先标记 lease 失效并触发 `CancellationToken`，再关闭底层 socket。单个 utterance 收到 `session.done` 后 lease 仍保持 `InUse`；当前 `TtsInputSession` 整轮完成后才释放为 `Idle`。异常、取消、读写错误或超时则使 entry 失效并从池移除。lease 丢弃时也必须触发异步清理，不能把仍可能被使用的连接直接标成 `Idle`。

只有当前 `TtsInputSession` 的整轮回复完成并释放 lease 后才启用空闲回收，并按 `idle_since + 20 秒` 启动。回收任务执行前重新锁住对应 entry，校验定时器捕获的 `generation + lease_id` 仍对应本次释放、状态为 `Idle`、`idle_since` 未被清空且 `now - idle_since >= 20 秒`；旧定时器不能关闭已经重新分配给新 owner 的连接。日志记录逻辑 session、owner、generation、state、`idle_ms` 和关闭原因。

`max_connections` 是 provider 总并发上限，默认值设为 4 并通过配置覆盖；后续可根据 provider 限流和实例资源调大或调小。池锁只保护 entry 状态和 lease 分配，绝不跨越网络 I/O，确保连接池不会成为并发瓶颈。

> 以上五点是实现前的对齐基线。若其中任一点需要改变，应先修改本节和后续任务，再写代码。

### 任务 1：增加增量 TTS 会话接口

**文件：**
- 修改：`crates/voice_server/src/client/tts.rs:30-70`
- 修改：`crates/voice_server/src/client/mod.rs:1-40`
- 测试：`crates/voice_server/src/client/tts.rs` 测试模块

**接口：**
- 新增 `TtsInputSession`：

```rust
#[async_trait]
pub trait TtsInputSession: Send {
    async fn send_text(&mut self, text: &str) -> Result<(), ClientError>;
    async fn flush(&mut self) -> Result<(), ClientError>;
    async fn next_event(&mut self) -> Option<Result<TtsEvent, ClientError>>;
    async fn close(&mut self) -> Result<(), ClientError>;
}
```

- 扩展 `TtsClient`：

```rust
async fn open_input_session(
    &self,
    session_id: &str,
    sample_rate_override: Option<u32>,
    voice_override: Option<String>,
) -> Result<Option<Box<dyn TtsInputSession>>, ClientError> {
    Ok(None)
}
```

- 从 `client/mod.rs` 以及 crate 根的现有 TTS re-export 中导出 `TtsInputSession`。

- [x] **步骤 1：先写失败测试**

增加测试专用的 `RecordingInputSession` 和 `RecordingTts`，验证 trait object 可以发送文本、flush、读取终端事件和关闭；同时验证默认 HTTP 风格 mock 的 `open_input_session` 返回 `None`。

- [x] **步骤 2：运行测试确认失败**

运行：`cargo test -p voice_server client::tts::tests::incremental_tts_session_interface`

预期：因为接口尚不存在而失败。

- [x] **步骤 3：实现最小接口**

增加上述 trait 和默认方法。保持 `synthesize` 不变；默认 `open_input_session` 返回 `Ok(None)`，确保现有 mock 和 HTTP 实现无需立即增加代码。

- [x] **步骤 4：运行测试确认通过**

运行：`cargo test -p voice_server client::tts::tests::incremental_tts_session_interface`

预期：通过。

- [ ] **步骤 5：提交（未执行；本次未创建 git commit）**

```bash
git add crates/voice_server/src/client/tts.rs crates/voice_server/src/client/mod.rs
git commit -m "feat: add incremental TTS session interface"
```

### 任务 2：实现 vLLM-Omni WebSocket 增量输入会话

**文件：**
- 修改：`crates/voice_server/src/client/tts_ws.rs:185-440`
- 修改：`crates/voice_server/src/client/tts.rs:640-690`
- 测试：`crates/voice_server/src/client/tts_ws.rs` 测试模块

**接口和行为：**
- 新增 `TtsWsInputSession`，实现 `TtsInputSession`。
- `TtsWsClient::open_input_session(...)` 返回 `Some(Box<TtsWsInputSession>)`。
- 由全局有界连接池管理多个 `WsStream`；一次流水线为整个 `TtsInputSession` 获取任意空闲 entry 的独占 lease，多个 utterance 之间持续持有该 lease，整轮回复结束后才释放为 `Idle`。协议错误、取消和超时关闭连接并从池中移除；连接失效时才允许当前会话切换到新 entry。达到 `max_connections` 时等待空闲连接，不按 `session_id` 固定映射。
- `send_text` 只发送 `{"type":"input.text","text":...}`，不能隐式发送 `input.done`。
- `flush` 发送 `{"type":"input.done"}`，每个待处理 utterance 只能发送一次。
- `next_event` 将二进制帧映射为非终端 `TtsEvent`，将 `session.done` 映射为空音频的终端 `TtsEvent`，将 `audio.done(error=true)` 和 `error` 映射为 `ClientError`。

- [x] **步骤 1：先写协议和会话失败测试**

增加消息序列测试，验证顺序严格为：

```rust
assert_eq!(messages, vec![
    r#"{"type":"session.config","voice":"vivian","response_format":"pcm"}"#,
    r#"{"type":"input.text","text":"你好"}"#,
    r#"{"type":"input.text","text":"，世界。"}"#,
    r#"{"type":"input.done"}"#,
]);
```

增加接收测试：二进制帧序号递增；`session.done` 产生 `is_last=true`；provider 错误会清理连接。

- [x] **步骤 2：运行测试确认失败**

运行：`cargo test -p voice_server client::tts_ws::tests::incremental`

预期：因为增量会话实现尚不存在而失败。

- [x] **步骤 3：实现会话状态机**

把建连、发送首条 `session.config` 抽成复用 helper。实现全局有界连接池、可取消的 owner lease 和 generation 校验；不同并发请求可使用不同连接，同一连接同一时刻只能有一个 owner；同一 `TtsInputSession` 的多个 utterance 持续使用同一 lease，只有整轮完成或连接失效才释放/切换。达到上限时等待 `Notify`，请求取消应立即退出等待。lease 正常完成整轮后释放，错误/取消/超时使连接失效并移除；不得在等待 `next_event()` 时长时间持有阻塞池锁。实现 `TtsWsInputSession` 的发送、flush、接收和关闭逻辑，并复用现有控制消息解析器。保留 `synthesize`，将其实现为一次 `send_text`、一次 `flush` 和循环 `next_event` 的兼容包装。

在 `voice_server.tts.ws` 下记录 `send_text`、`flush`、二进制帧、`audio.done`、`session.done`、错误和连接归还/关闭日志。`send_text` 的 `debug` 日志增加过滤后的文本片段（例如 `text_preview`，按字符截断并转义换行），同时记录 session id、endpoint、文本长度、序号、帧数和字节数；不得记录未清洗的原始 delta 或 API Key。

- [x] **步骤 4：运行测试确认通过**

运行：`cargo test -p voice_server client::tts_ws::tests::incremental`

预期：通过。

- [ ] **步骤 5：提交（未执行；本次未创建 git commit）**

```bash
git add crates/voice_server/src/client/tts.rs crates/voice_server/src/client/tts_ws.rs
git commit -m "feat: support incremental vllm websocket TTS input"
```

### 任务 3：增加简单的增量文本过滤器

**文件：**
- 修改：`crates/voice_server/src/pipeline/text.rs:1-180`
- 测试：`crates/voice_server/src/pipeline/text.rs` 测试模块

**接口：**

```rust
#[derive(Debug, Default)]
pub struct IncrementalTtsCleaner;

impl IncrementalTtsCleaner {
    pub fn clean(delta: &str) -> String;
}
```

- `clean` 对单个 delta 直接过滤字符，不维护跨 delta 状态。
- 保留文字、数字、空格、逗号、句号、问号、感叹号、冒号、分号和换行等可能影响朗读节奏或断句的字符。
- 删除括号、引号、emoji、反引号、`*`、`_`、`~` 等装饰符号；过滤结果为空时，调用方不发送 `input.text`。

- [x] **步骤 1：先写失败测试**

覆盖装饰符号过滤和普通文本：

```rust
assert_eq!(
    IncrementalTtsCleaner::clean("请查看 [帮助](文档)😊**。"),
    "请查看 帮助文档。"
);
```

额外断言：引号、括号、emoji、反引号和 `*` 等被删除；逗号、句号、问号、感叹号和换行被保留；普通中英文 delta 可以立即输出。

- [x] **步骤 2：运行测试确认失败**

运行：`cargo test -p voice_server pipeline::text::tests::incremental`

预期：因为 `IncrementalTtsCleaner::clean` 尚不存在而失败。

- [x] **步骤 3：实现无状态过滤器**

实现一个无状态字符过滤器；不要解析 Markdown 结构，也不要缓存待闭合标签。使用明确的保留/删除字符分类，确保句末标点原样保留供 `next_sentence_end` 检测。

- [x] **步骤 4：运行测试确认通过**

运行：`cargo test -p voice_server pipeline::text::tests::incremental`

预期：通过。

- [ ] **步骤 5：提交（未执行；本次未创建 git commit）**

```bash
git add crates/voice_server/src/pipeline/text.rs
git commit -m "feat: add incremental TTS text filter"
```

### 任务 4：让 LLM 流水线使用 WebSocket 增量 TTS

**文件：**
- 修改：`crates/voice_server/src/pipeline/llm_tts.rs:144-285`
- 修改：`crates/voice_server/src/pipeline/text.rs`
- 测试：`crates/voice_server/src/pipeline/llm_tts.rs` 测试模块

**行为：**
- 流水线开始时调用 `tts.open_input_session(&sid, sample_rate_override, voice_override.clone()).await`。
- 返回 `Some(session)` 时走 WebSocket 增量路径；返回 `None` 时保持现有 HTTP 按句路径。
- 增量路径维护：

```rust
let mut boundary_buf = String::new();
let mut ws_session = session;
```

- 每个 LLM 事件先立即 yield `LlmTtsItem::Llm`；再调用 `IncrementalTtsCleaner::clean(&evt.delta)`。过滤结果非空时调用一次 `send_text`，同时追加到 `boundary_buf`。
- 使用 `next_sentence_end` 反复检测句边界。每发现一句，调用 `flush`，循环 `next_event` 直到该 utterance 的 terminal 事件；音频继续经过现有 crossfader 和序号映射，然后清空边界缓冲。
- LLM `is_final` 时，如果 `boundary_buf` 仍有无句末标点的文本，则最后 flush 一次；最后保留现有全局空音频结束标记。
- 取消或发送/接收失败时调用 `ws_session.close().await`，并保持现有安全错误消息及 1004/1005 错误码语义。

- [x] **步骤 1：先写失败测试**

增加 `RecordingIncrementalTts`，其 session 记录 `send_text` 和 `flush`，每次 flush 返回一帧音频和一个终端事件。使用以下 LLM delta：

```text
"第一句" + "。第二句" + "。"
```

确定断言：

```text
input.text 依次收到 "第一句"、"。第二句"、"。"
flush 调用次数为 2
每个 flush 有一个 utterance 终端事件，流水线末尾另有 1 个全局结束标记
```

再增加一个带装饰符号的 delta，例如 `"**第一句**😊。"`，断言发送的是 `"第一句。"`，并让测试使用的日志字段能验证 `text_preview` 等于过滤后的文本。增加无标点尾句测试，以及 HTTP mock 回归测试，确认 `open_input_session` 返回 `None` 时仍按完整句子调用 `synthesize`。

- [x] **步骤 2：运行测试确认失败**

运行：`cargo test -p voice_server pipeline::llm_tts::tests::incremental`

预期：因为流水线尚未调用 `open_input_session` 而失败。

- [x] **步骤 3：实现增量分支**

保持现有 HTTP 分支结构，只有 PCM 输出、错误映射等确实共享的部分才抽 helper；不能让 HTTP 客户端依赖 WebSocket 专用类型。

- [x] **步骤 4：运行聚焦测试确认通过**

运行：`cargo test -p voice_server pipeline::llm_tts::tests::incremental pipeline::llm_tts::tests::cleans_markdown_before_calling_tts pipeline::llm_tts::tests::merges_short_sentence_with_the_next_tts_request`

预期：新增增量测试和既有 HTTP 行为测试全部通过。

- [ ] **步骤 5：提交（未执行；本次未创建 git commit）**

```bash
git add crates/voice_server/src/pipeline/llm_tts.rs crates/voice_server/src/pipeline/text.rs
git commit -m "feat: feed LLM deltas into websocket TTS"
```

### 任务 5：验证取消、连接复用和日志

**文件：**
- 修改：`crates/voice_server/src/session/pipeline.rs:303-400`
- 修改：`crates/voice_server/src/client/tts_ws.rs`
- 修改：`crates/voice_server/src/bin/voice_server.rs`
- 修改：`crates/voice_server/src/config/config.yaml.template:38-50`
- 测试：`crates/voice_server/src/session/pipeline.rs`、`crates/voice_server/src/client/tts_ws.rs`

**行为：**
- 会话取消必须中止正在等待的 `next_event`，关闭上游 WebSocket，不能伪造成功结束事件。
- 增量流水线正常结束后，连接应可供下一条流水线复用；发生错误时必须清理并允许下次重新连接。
- 在 TTS 配置中增加 `tts.max_connections`（默认 4），限制 provider 总并发连接数；池满时请求等待空闲 lease，不能退化为按 `session_id` 串行。
- 握手成功后立即发送 `session.config`，从握手完成到首配发送完成不能超过 10 秒。
- 整个 `TtsInputSession` 回复结束并归还连接后才允许回收；回收 deadline 为归还时间加 20 秒。达到 deadline 时主动发送 `session.close` 并移除连接；仍被当前轮次持有的 `InUse` 连接不执行空闲回收。
- 启动日志继续显示 `tts_kind=websocket` 和 `/audio/speech/stream`；运行日志显示增量 `input.text` 和 `input.done` 的元数据。

- [x] **步骤 1：先写失败的生命周期测试**

覆盖：接收音频期间取消；同一个 `TtsInputSession` 的多句回复持续使用同一条连接且句间不重复发送 `session.config`；连接失效后才切换到第二条连接并重新发送 `session.config`；两个并发请求可同时占用两条不同连接；达到 `max_connections` 后第三个请求等待且取消可释放等待；同一条连接不会同时出现两个 owner；同一个 `session_id` 的并发请求不被错误串行化；整轮正常完成后连接可被任意后续请求复用；收到 provider `error` 后清理并重新连接；握手完成后首配超过 10 秒不进入连接池；整轮归还后按 `idle_since + 20 秒` 主动关闭；合成耗时超过 20 秒但尚未完成整轮时不触发空闲回收；旧 generation 的回收定时器不能关闭已经重新分配给新 owner 的连接。日志捕获设施可用时，断言日志中不出现 API Key 或原始未过滤文本。

- [x] **步骤 2：运行测试确认失败**

运行：`cargo test -p voice_server session::pipeline::tests::cancellation client::tts_ws::tests::reuse`

预期：新增加的复用/取消断言在生命周期逻辑完成前失败。

- [x] **步骤 3：实现生命周期和配置说明**

在 session pipeline 中将 LLM reader、文本队列和 WebSocket writer 解耦；使用有界队列持续消费 LLM，在 `session.done` 前暂存已经生成但尚未发送的安全片段。使用 `tokio::select!` 包裹每个 `send_text`、`flush` 和 `next_event` await。WebSocket 客户端实现握手后 10 秒首配 deadline，以及仅在收到 `session.done` 后基于 `idle_since`、连接 generation 和 `Idle` 状态的 20 秒主动 `session.close` 回收；不主动发送 Ping 保活帧。增加配置模板注释，说明 WebSocket transport 使用增量 `input.text` 加句边界 `input.done`，HTTP 仍使用完整文本。

- [x] **步骤 4：运行生命周期测试确认通过**

运行：`cargo test -p voice_server session::pipeline::tests::cancellation client::tts_ws::tests::reuse`

预期：通过。

- [ ] **步骤 5：提交（未执行；本次未创建 git commit）**

```bash
git add crates/voice_server/src/session/pipeline.rs crates/voice_server/src/client/tts_ws.rs crates/voice_server/src/bin/voice_server.rs crates/voice_server/src/config/config.yaml.template
git commit -m "test: verify websocket TTS cancellation and reuse"
```

### 任务 6：完整验证和协议冒烟测试

**文件：**
- 仅测试：现有 `crates/voice_server` 测试模块
- 可选测试 helper：`crates/voice_server/src/client/tts_ws.rs` 测试模块

- [x] **步骤 1：运行格式和静态检查**

运行：`cargo fmt --all -- --check`

预期：本方案涉及的 Rust 文件没有格式差异。若工作区其他既有文件导致全局检查失败，则只对本次修改文件运行 `rustfmt --check`，并单独报告无关失败。

- [x] **步骤 2：运行完整 crate 测试**

运行：`cargo test -p voice_server`

预期：所有既有测试和新增测试通过。

- [x] **步骤 3：运行编译检查**

运行：`cargo check -p voice_server`

预期：退出码为 0。

- [x] **步骤 4：运行本地 fake provider WebSocket 冒烟测试**

启动仅供测试的 WebSocket 服务端，严格检查以下顺序：

```text
session.config -> input.text* -> input.done -> audio.start -> binary -> audio.done -> session.done
```

使用包含至少两句话的 LLM 响应，验证服务端在第一个 `input.done` 前观察到多个 `input.text`，总 flush 次数为 2，两个句子之间没有重新握手。

- [x] **步骤 5：检查最终差异**

运行：`git diff --check` 和 `git status --short`。

预期：没有空白错误；实现阶段只修改方案中列出的文件。
