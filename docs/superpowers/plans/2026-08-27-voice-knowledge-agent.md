# Voice Knowledge Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在现有浏览器语音链路上增加面向普通用户的中文知识问答 Agent 体验，并为未来知识库搜索预留可注入接口。

**Architecture:** 沿用现有单 WebSocket 会话和 `ASR -> LLM -> TTS` 流水线，新增服务端生成的 `AgentStatus` 事件表达用户安全的阶段状态。知识搜索抽象为可选 trait，第一版默认使用空实现并跳过真实搜索；终端页面消费状态事件，开发调试能力通过入口进入。

**Tech Stack:** Rust workspace、`serde`/MessagePack、Tokio `CancellationToken`、Actix/webhttp WebSocket、原生 HTML/CSS/JavaScript。

**Spec:** `docs/superpowers/specs/2026-08-27-voice-knowledge-agent-design.md`

## Global Constraints

- 第一版固定使用中文；协议保留 `language` 扩展口子。
- 第一版不接入本地 RAG、外部搜索或 MCP；只实现可注入的 `KnowledgeSearch` 接口和空/Mock 实现。
- 不向终端用户暴露 chain-of-thought、Prompt、工具参数、JSON、URL 或内部错误栈。
- 来源只展示文本，不提供外部网页跳转。
- 保留现有 ASR、LLM、TTS 调试能力，通过“开发模式”入口进入。
- 新增事件必须兼容既有 `AsrPartial`、`LlmDelta`、`TtsAudio` 和旧客户端解码路径。
- 新请求到达时必须取消旧请求并清空旧 TTS 播放队列。
- 本计划不执行 Git add、commit 或其他提交操作。

---

### Task 1: 扩展语音协议

**Files:**
- Modify: `crates/voice-proto/src/lib.rs`
- Test: `crates/voice-proto/src/lib.rs` 内 `#[cfg(test)]` 模块

**Interfaces:**
- Produces `AgentPhase`, `AgentStatus` 和 `VoicePayload::AgentStatus`。
- `AgentStatus` 字段固定为 `session_id: String`、`phase: AgentPhase`、`label: String`、`tool: Option<String>`、`request_id: u64`、`done: bool`。
- 为既有 `AsrPartial`、`LlmDelta`、`TtsAudio` 增加 `#[serde(default)] request_id: u64`，让前端能丢弃旧轮次迟到事件；缺字段时按 `0` 解码，兼容旧客户端。

- [ ] **Step 1: 写失败测试**

新增测试验证：

```rust
#[test]
fn round_trip_agent_status() {
    let payload = VoicePayload::AgentStatus {
        session_id: "s".into(),
        phase: AgentPhase::Searching,
        label: "正在查资料".into(),
        tool: Some("knowledge_search".into()),
        request_id: 7,
        done: false,
    };
    let bytes = encode_indication(&payload).unwrap();
    let (_, decoded) = decode_payload(&bytes).unwrap();
    assert!(matches!(decoded, VoicePayload::AgentStatus {
        phase: AgentPhase::Searching,
        request_id: 7,
        ..
    }));
}

#[test]
fn legacy_stream_event_without_request_id_decodes() {
    #[derive(serde::Serialize)]
    struct OldWire<'a> {
        #[serde(rename = "type")]
        t: &'a str,
        session_id: &'a str,
        delta: &'a str,
        is_final: bool,
    }
    let raw = rmp_serde::to_vec_named(&OldWire {
        t: "llm_delta",
        session_id: "s",
        delta: "你好",
        is_final: true,
    }).unwrap();
    let decoded: VoicePayload = rmp_serde::from_slice(&raw).unwrap();
    match decoded {
        VoicePayload::LlmDelta { request_id, .. } => assert_eq!(request_id, 0),
        other => panic!("unexpected payload: {other:?}"),
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p voice-proto round_trip_agent_status`

Expected: FAIL because `AgentPhase` and `VoicePayload::AgentStatus` do not exist.

- [ ] **Step 3: 实现最小协议变更**

在 `VoicePayload` 之前定义 `AgentPhase`，使用 `#[serde(rename_all = "snake_case")]`；在下行事件区域增加 `AgentStatus` variant；在 `session_id()` 中返回其 session id。为 `tool` 和既有流式事件的 `request_id` 增加 `#[serde(default)]`；`AgentStatus.request_id` 和 `done` 仍为必填字段。

同时更新 `AsrPartial`、`LlmDelta`、`TtsAudio` 的所有构造点和匹配分支，新增字段统一放在 `request_id` 末尾；新服务端事件填当前轮次 id，旧客户端缺字段时解为 `0`。

- [ ] **Step 4: 运行协议测试**

Run: `cargo test -p voice-proto`

Expected: PASS，既有 round-trip 测试和新增测试全部通过。

### Task 2: 增加可选知识搜索边界

**Files:**
- Create: `crates/voice_server/src/agent/knowledge_search.rs`
- Modify: `crates/voice_server/src/agent/mod.rs`
- Modify: `crates/voice_server/src/lib.rs`
- Test: `crates/voice_server/src/agent/knowledge_search.rs` 内单元测试

**Interfaces:**
- Produces `KnowledgeSearch` trait、`SearchResult`、`Source`、`SearchError`、`NoopKnowledgeSearch`。
- `KnowledgeSearch::search(&self, session_id: &str, query: &str, cancel: CancellationToken) -> BoxFuture<Result<SearchResult, SearchError>>`。

- [ ] **Step 1: 写失败测试**

新增以下测试，固定空实现和取消语义：

```rust
#[tokio::test]
async fn noop_search_returns_empty_result() {
    let search = NoopKnowledgeSearch;
    let result = search.search("s", "天气", CancellationToken::new()).await.unwrap();
    assert!(result.context.is_empty());
    assert!(result.sources.is_empty());
}

#[tokio::test]
async fn noop_search_honors_cancellation() {
    let search = NoopKnowledgeSearch;
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        search.search("s", "天气", cancel).await,
        Err(SearchError::Cancelled)
    ));
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p voice_server agent::knowledge_search`

Expected: FAIL because the module and types do not exist.

- [ ] **Step 3: 实现 trait 和空实现**

使用 `async_trait`（项目已有依赖）定义 trait。`NoopKnowledgeSearch` 默认不访问网络；方法开始时检查 `cancel.is_cancelled()`，否则返回空 context 和空 sources。`SearchError` 至少包含 `Cancelled`、`Unavailable(String)`、`Timeout`，并实现 `Display`/`Error`。

- [ ] **Step 4: 暴露模块并运行测试**

在 `agent/mod.rs` 增加 `pub mod knowledge_search;` 和必要 re-export；在 `lib.rs` re-export trait 和数据类型。

Run: `cargo test -p voice_server agent::knowledge_search`

Expected: PASS。

### Task 3: 在会话流水线中发送用户安全状态

**Files:**
- Modify: `crates/voice_server/src/session/pipeline.rs`
- Modify: `crates/voice_server/src/session/mod.rs`
- Modify: `crates/voice_server/src/session/state.rs`（仅在现有状态枚举需要补齐时）
- Test: `crates/voice_server/src/session/pipeline.rs` 或 `crates/voice_server/tests/` 新增状态事件测试

**Interfaces:**
- Produces `run_pipeline` 内部的 `send_agent_status(...)` 辅助函数。
- `VoiceSession` 持有 `next_request_id: u64`，每次 `trigger_pipeline()` 先递增并将该 id 传入 `run_pipeline`；该 pipeline 的 `AgentStatus`、`AsrPartial`、`LlmDelta`、`TtsAudio` 共用同一个 id。

- [ ] **Step 1: 写失败测试**

增加纯函数测试，验证阶段到中文文案映射：

```rust
assert_eq!(agent_label(AgentPhase::Transcribing), "正在理解");
assert_eq!(agent_label(AgentPhase::Composing), "正在组织答案");
assert_eq!(agent_label(AgentPhase::Speaking), "正在回答");
```

增加事件序列测试，使用现有 mock ASR/LLM/TTS 和测试 `Recipient`，验证正常请求至少产生：`Transcribing -> Composing -> Speaking`；默认空搜索实现不产生 `Searching`；同一轮的 `AsrPartial`、`LlmDelta`、`TtsAudio` 和 `AgentStatus` request id 一致。

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p voice_server session::pipeline`

Expected: FAIL because status helper and emissions do not exist.

- [ ] **Step 3: 实现状态发送**

在 `VoiceSession::trigger_pipeline()` 分配 request id，并将 id 作为 `run_pipeline(..., request_id, ...)` 参数传递。在 ASR 流开始前发送 `Transcribing`；ASR final 非空后发送 `Composing`。当未来注入的搜索 provider 实际执行时，才发送 `Searching`，默认 `NoopKnowledgeSearch` 路径直接跳过。收到第一段可播放 TTS 前发送 `Speaking`，工具/LLM/TTS 失败时发送 `Error`，`label` 只能来自固定中文映射。

沿用现有 `CancellationToken`：每次发送状态前检查取消；取消后不发送旧 request 的后续状态或 TTS。所有新建的 `AsrPartial`、`LlmDelta`、`TtsAudio` 均填入当前 `request_id`；不要把工具参数或 provider 错误原文放入 `label`。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p voice_server session::pipeline`

Expected: PASS；既有 ASR/LLM/TTS 测试保持通过。

### Task 4: 重构终端用户主界面

**Files:**
- Modify: `crates/voice_server/static/index.html`
- Modify: `crates/voice_server/static/style.css`
- Modify: `crates/voice_server/static/app.js`
- Test: 浏览器手工验证；如仓库已有前端测试框架则在对应目录添加状态映射测试，否则使用浏览器控制台断言

**Interfaces:**
- Consumes binary `VoicePayload::AgentStatus` 解码后的对象，以及既有 ASR/LLM/TTS 事件。
- Produces DOM elements `agent-phase`、`agent-substatus`、`source-list`、`source-details`、`dev-mode-link` 和 `retry-action`。

- [ ] **Step 1: 写前端状态映射测试/断言**

为 `app.js` 抽出无 DOM 依赖的 `agentStatusLabel(phase)` 和 `isCurrentRequest(requestId)`；验证未知 phase 回退为“正在处理”，旧 request id 返回 false，中文状态映射完整。

- [ ] **Step 2: 实现主舞台布局**

在 `index.html` 的 pipeline Tab 中保留现有麦克风、扬声器、打断、结束控制；新增头像状态、主状态、次状态、字幕区、折叠来源区、错误重试按钮和“开发模式”入口。默认首页不展示 ASR/LLM/TTS 单能力面板。

- [ ] **Step 3: 接入 AgentStatus 事件**

在 `app.js` 的下行消息分发中处理 `agent_status`：更新 `data-state`、状态文字和可选进度播报。`transcribing` 不播报；`composing` 默认不播报；`searching` 仅在服务端标记需要进度且当前 request 尚未播报时播放一次“我查一下”。不显示 `tool` 参数。

- [ ] **Step 4: 接入来源和错误 UI**

为未来来源事件保留只读文本渲染函数，仅显示标题、站点、更新时间；不要生成 `<a href>`。错误状态显示用户安全文案和重试按钮；重试只重新提交最近一条用户文本，不复用已取消的 TTS 队列。

- [ ] **Step 5: 保持打断行为**

在新 request id 到达或用户点击打断时调用现有 `stopTtsPlayback()`，清空队列、撤销 Blob URL、恢复 Listening 状态。延迟到达的旧 `AgentStatus`、`LlmDelta`、`TtsAudio` 不得覆盖新轮次。

- [ ] **Step 6: 响应式视觉验证**

Run: `cargo run -p voice_server`，浏览器打开 `http://127.0.0.1:8080`。

验证 1440px、1024px、390px 三种视口：状态文字、字幕、来源折叠区和底部控制不重叠；麦克风权限拒绝、未连接、处理中、错误、打断均有稳定布局。

### Task 5: 增加开发模式入口

**Files:**
- Modify: `crates/voice_server/static/index.html`
- Modify: `crates/voice_server/static/app.js`
- Modify: `crates/voice_server/static/style.css`

**Interfaces:**
- Produces a client-side `data-mode="user|developer"` switch or equivalent URL/hash state.
- Existing `tab_asr.js`、`tab_llm.js`、`tab_tts.js`、`tab_llm_tts.js`、`tab_asr_llm_tts.js` remain loaded and functional in developer mode.

- [ ] **Step 1: 添加入口和模式状态**

在终端主界面提供明确的“开发模式”按钮；点击后切换到调试导航，使用 URL hash（例如 `#developer`）使刷新后模式可恢复。默认无 hash 时进入 user mode。

- [ ] **Step 2: 保持调试页兼容**

不要删除现有 Tab DOM 或脚本；仅通过容器显示/隐藏控制。开发模式下现有 `/admin/*` 测试按钮、音色选择和 ASR realtime 页面入口保持可用。

- [ ] **Step 3: 验证模式切换**

手工验证：用户模式首页不显示单能力面板；点击“开发模式”后可以访问全部既有调试 Tab；返回用户模式后语音会话状态、麦克风和扬声器控制不丢失。

### Task 6: 集成验证和文档同步

**Files:**
- Modify: `docs/superpowers/specs/2026-08-27-voice-knowledge-agent-design.md`（仅记录实现偏差或已完成范围）
- Test: workspace tests and browser smoke checks

- [ ] **Step 1: 运行格式和协议测试**

Run: `cargo fmt --all -- --check`  
Run: `cargo test -p voice-proto`  
Run: `cargo test -p voice_server`

Expected: 全部 PASS；若存在与本功能无关的既有失败，记录具体测试名和失败原因，不修改无关模块。

- [ ] **Step 2: 运行端到端冒烟**

启动服务后验证：建立 WebSocket、发送 `SessionStart`、说一句中文问题、收到 ASR final、收到 `AgentStatus`、收到 LLM 文本和 TTS 音频。第一版默认不应出现 `Searching`，因为真实搜索 provider 未启用。

- [ ] **Step 3: 验证取消和错误路径**

使用模拟延迟 provider 或测试 double 验证：新问题会取消旧 request；搜索/LLM/TTS 失败只显示安全文案；用户打断立即停止播放；旧事件不会覆盖新状态。

- [ ] **Step 4: 更新设计文档实现状态**

在设计文档中补充已完成的 Phase 1/2 范围和未实现的真实搜索 provider，保持“第一版不接入真实知识库”的决策明确。不要添加 Git 提交记录。

## Execution Notes

按 Task 1 → Task 2 → Task 3 → Task 4 → Task 5 → Task 6 顺序执行。每个任务完成后先运行该任务的定向测试，再进入下一任务；只有 Task 6 的全量测试和浏览器冒烟通过后，才可将实现标记为完成。
