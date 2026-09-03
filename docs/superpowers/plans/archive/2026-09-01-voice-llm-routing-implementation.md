# Voice LLM Routing Implementation Plan

> 状态：已归档

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 LLM 选型基线中的 `fast`/`strong` 双模型、独立完整 System Prompt、首版长度路由和一次安全兜底接入现有 `ASR -> LLM -> TTS` Rust 链路。

**Architecture:** 保留一个 `LlmAgent` 作为 session pipeline 的唯一 LLM 入口。该 Agent 持有 fast/strong 两个已构造完成的 `HttpLlmClient`、一个 `MemoryStore` 和长度路由器；每次 `chat()` 只根据当前用户输入选择 client。每个 `HttpLlmClient` 在构造时固定自己的完整 System Prompt 模板，并在请求时内部渲染 ASR hint。Agent 不读取、不选择、不拼接 Prompt；`llm_tts_items`、`VoiceSession` 和 `VoiceService` 也不感知模型与 Prompt 细节。

**Tech Stack:** Rust 2021、Tokio、Actix、Serde/YAML、Reqwest OpenAI-compatible Chat Completions、Prometheus、现有 `MemoryStore` 与 `llm_tts_items`。

**Spec:** `docs/superpowers/specs/2026-09-01-voice-llm-selection-baseline.md`

## Global Constraints

- 以用户当前手动修改后的选型文档为唯一 Prompt 和模型职责基线，不恢复此前版本。
- `fast` 固定为 `Qwen/Qwen3-8B`、`/no_think`、纯文本输出，不向请求注册任何工具。
- `strong` 固定为 `Qwen/Qwen3-30B-A3B`，使用独立完整 System Prompt；`reasoning_content`、`<think>` 和工具 JSON 不得进入 TTS 或会话记忆。
- 同一个 `LlmAgent` 维护一份会话历史；两个底层 client 共享该历史，但各自在构造时固定 Prompt、模型和生成参数。
- 模型路由器独立放在 `agent/router.rs`，但只由 `LlmAgent` 调用；session pipeline 不感知路由。
- 首版路由只判断当前输入 `trim()` 后的 Unicode 字符数：少于 15 个字符走 fast，达到或超过 15 个字符走 strong。
- 首版不实现关键词、闲聊、工具意图、风险、复杂度、ASR 置信度或额外 LLM 分类；这些能力等真实流量证明有必要后再迭代。
- fast 只允许在 `LlmAgent` 尚未向下游 yield 任何非空 delta 时升级一次 strong。已经输出文本后不得重跑，避免重复或前后矛盾的语音。
- strong 工具调用协议不属于本计划，长度路由器也不判断工具意图。
- `/admin/llm`、`/admin/llm_tts` 默认继续使用 fast 原始客户端，避免调试接口被会话路由逻辑改变。
- 保留现有 HTTP stream drop 即取消请求的行为，不切换回 `async-openai`。
- 不修改 ASR、分句、TTS wire 协议和前端播放队列。
- 工作区已有用户改动；只编辑本计划列出的相关文件，不回退其他文件。
- 除非用户明确要求，不执行 `git add`、`git commit` 或分支操作。

---

### Task 1: 扩展双模型配置

**Files:**
- Modify: `crates/voice_server/src/config.rs`
- Modify: `crates/voice_server/src/config/config.yaml`
- Modify: `crates/voice_server/src/config/config.yaml.template`
- Modify: `crates/voice_server/src/client/llm.rs`
- Test: `crates/voice_server/src/config.rs` 内 `#[cfg(test)]` 模块
- Test: `crates/voice_server/src/client/llm.rs` 内 `#[cfg(test)]` 模块

**Interfaces:**
- 保留 `VoiceConfig.llm: LlmConfig` 作为 fast 配置，避免破坏现有配置和 admin 接口。
- 新增 `VoiceConfig.llm_strong: Option<LlmConfig>`；缺省时启动代码克隆 fast 配置，仅切换 Prompt，并记录降级警告。
- 新增 `LlmConfig.top_k: Option<u32>`，由 `HttpLlmClient` 按 OpenAI-compatible 扩展字段透传。
- 环境变量继续使用 `VOICE_LLM_*` 覆盖 fast，新增 `VOICE_LLM_STRONG_*` 覆盖 strong。

- [ ] **Step 1: 写双模型配置失败测试**

在 `config.rs` 增加以下覆盖点：

```rust
#[test]
fn parses_fast_and_strong_config() {
    let yaml = r#"
        llm:
          model: Qwen/Qwen3-8B
          max_completion_tokens: 512
          temperature: 0.7
          top_p: 0.8
          top_k: 20
        llm_strong:
          model: Qwen/Qwen3-30B-A3B
          max_completion_tokens: 1024
          temperature: 0.6
          top_p: 0.95
          top_k: 20
    "#;
    let cfg: VoiceConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(cfg.llm.model, "Qwen/Qwen3-8B");
    assert_eq!(cfg.llm.top_k, Some(20));
    assert_eq!(cfg.llm_strong.as_ref().unwrap().model, "Qwen/Qwen3-30B-A3B");
}
```

增加环境变量测试，使用现有测试锁/清理方式验证 `VOICE_LLM_STRONG_MODEL`、`VOICE_LLM_STRONG_TIMEOUT_MS`、`VOICE_LLM_STRONG_TOP_K` 只覆盖 strong，不改变 fast。

- [ ] **Step 2: 运行配置测试确认失败**

Run: `cargo test -p voice_server config::tests::parses_fast_and_strong_config -- --exact`

Expected: FAIL，因为 `llm_strong` 和 `top_k` 尚不存在。

- [ ] **Step 3: 实现双模型配置和兼容覆盖**

`VoiceConfig::apply_env_overrides()` 中先按 `LLM` 覆盖 fast；只有 YAML 已配置 strong 或存在任一 `VOICE_LLM_STRONG_*` 时才创建 strong 配置并按 `LLM_STRONG` 覆盖。不要因为没有 strong 环境变量就凭空创建空模型。首版长度阈值固定在独立路由器中，不增加 YAML 或环境变量配置。

- [ ] **Step 4: 写并验证 `top_k` 请求体失败测试**

扩展 `request_body_serializes_configured_openai_options`：

```rust
let cfg = LlmConfig {
    model: "test-model".into(),
    top_k: Some(20),
    ..LlmConfig::default()
};
let body = serde_json::to_value(
    HttpLlmClient::from_config(ProviderConfig::default(), &cfg)
        .request_body(&[ChatMessage { role: "user".into(), content: "你好".into() }])
).unwrap();
assert_eq!(body["top_k"], 20);
```

Run: `cargo test -p voice_server client::llm::tests::request_body_serializes_configured_openai_options -- --exact`

Expected: FAIL，因为请求结构还没有 `top_k`。

- [ ] **Step 5: 实现 `top_k` 透传并更新 YAML**

把 `top_k` 加入 `LlmConfig`、`HttpLlmClient` 和 `ChatReq`，使用 `skip_serializing_if = "Option::is_none"`。配置示例固定为：

```yaml
llm:
  model: "Qwen/Qwen3-8B"
  max_completion_tokens: 512
  temperature: 0.7
  top_p: 0.8
  top_k: 20

llm_strong:
  model: "Qwen/Qwen3-30B-A3B"
  max_completion_tokens: 1024
  temperature: 0.6
  top_p: 0.95
  top_k: 20

```

- [ ] **Step 6: 运行配置和客户端测试**

Run: `cargo test -p voice_server config::tests`

Run: `cargo test -p voice_server client::llm::tests`

Expected: PASS；旧的单 `llm:` YAML 和 `VOICE_LLM_*` 测试继续通过。

### Task 2: 将完整 Prompt 固定为 HttpLlmClient 属性

**Files:**
- Create: `crates/voice_server/src/client/prompt.rs`
- Create: `crates/voice_server/src/client/prompt.yaml`
- Modify: `crates/voice_server/src/client/mod.rs`
- Modify: `crates/voice_server/src/client/llm.rs`
- Delete: `crates/voice_server/src/agent/prompts.rs`
- Delete: `crates/voice_server/src/agent/prompts.yaml`
- Modify: `crates/voice_server/src/agent/mod.rs`
- Modify: `crates/voice_server/src/lib.rs`
- Test: `crates/voice_server/src/client/prompt.rs` 内 `#[cfg(test)]` 模块
- Test: `crates/voice_server/src/client/llm.rs` 内 `#[cfg(test)]` 模块

**Interfaces:**
- `LlmPromptTemplates { fast: String, strong: String }` 只负责从编译期 YAML 加载两份模板。
- `ModelTier::{Fast, Strong}` 定义在 `client::llm`，由 client、Agent 路由和指标共用。
- `HttpLlmClient` 新增私有属性 `system_prompt_template: Option<Arc<str>>`；构造完成后不可修改。
- 新增 `build_llm_client_with_prompt(cfg, provider, system_prompt_template)`；原 `build_llm_client` 保留并传 `None`，兼容不需要固定 Prompt 的调用方和测试。
- `HttpLlmClient` 内部渲染 `{emotion}` 并自动把唯一一条固定 system message 放在调用方 messages 之前。
- `LlmAgent` 不导入 `client::prompt`，也不持有任何 Prompt 类型或文本。

- [ ] **Step 1: 写两份模板加载失败测试**

```rust
#[test]
fn embedded_templates_are_complete_and_distinct() {
    let prompts = LlmPromptTemplates::from_embedded().unwrap();
    assert!(prompts.fast.starts_with("/no_think"));
    assert!(prompts.fast.contains("不要尝试调用工具"));
    assert!(!prompts.fast.contains("工具调用前校验"));
    assert!(prompts.strong.contains("强执行模型"));
    assert!(prompts.strong.contains("工具调用前校验"));
    assert!(!prompts.strong.starts_with("/no_think"));
    assert_eq!(prompts.fast.matches("{emotion}").count(), 1);
    assert_eq!(prompts.strong.matches("{emotion}").count(), 1);
}
```

- [ ] **Step 2: 运行模板测试确认失败**

Run: `cargo test -p voice_server client::prompt::tests::embedded_templates_are_complete_and_distinct -- --exact`

Expected: FAIL，因为 client 目录中还没有 Prompt 模板模块。

- [ ] **Step 3: 迁移用户确认的完整 Prompt**

将选型文档第 3.4.1、3.4.2 节中用户手动修改后的两个 `text` 代码块逐字复制到 `client/prompt.yaml` 的 `fast` 和 `strong` 字段。不改写、不抽取公共段、不添加第三份模板；迁移并验证后删除旧 `agent/prompts.rs` 和 `agent/prompts.yaml`，避免形成两套 Prompt 来源。

- [ ] **Step 4: 写 HttpLlmClient 固定 Prompt 失败测试**

```rust
#[test]
fn client_prepends_its_fixed_prompt_and_renders_emotion() {
    let client = HttpLlmClient::from_config_with_prompt(
        ProviderConfig::default(),
        &LlmConfig { model: "fast".into(), ..LlmConfig::default() },
        Some(Arc::<str>::from("固定提示。推断信息：{emotion}")),
    );
    let messages = vec![ChatMessage { role: "user".into(), content: "你好".into() }];
    let rendered = client.messages_with_system_prompt(&messages, Some("开心"));
    assert_eq!(rendered[0].role, "system");
    assert_eq!(rendered[0].content, "固定提示。推断信息：开心");
    assert_eq!(rendered[1].role, "user");
}
```

再增加无 emotion 测试，断言 `{emotion}` 替换为 `无`；`system_prompt_template=None` 时 messages 原样返回，不添加 system message。

- [ ] **Step 5: 运行 client Prompt 测试确认失败**

Run: `cargo test -p voice_server client::llm::tests::client_prepends_its_fixed_prompt_and_renders_emotion -- --exact`

Expected: FAIL，因为 `HttpLlmClient` 尚未持有 Prompt。

- [ ] **Step 6: 实现固定 Prompt 属性和请求内渲染**

目标字段和辅助函数：

```rust
pub struct HttpLlmClient {
    system_prompt_template: Option<Arc<str>>,
    // 保留现有连接、模型和生成参数字段
}

fn messages_with_system_prompt(
    &self,
    messages: &[ChatMessage],
    emotion_hint: Option<&str>,
) -> Vec<ChatMessage>
```

渲染只在客户端内部执行：`template.replace("{emotion}", emotion_hint.filter(|v| !v.is_empty()).unwrap_or("无"))`。固定 Prompt 必须位于 messages 第 0 条；调用方历史和当前 user 的相对顺序保持不变。

- [ ] **Step 7: 扩展 LlmClient 的消息入口以透传请求上下文**

把 trait 方法签名改为：

```rust
async fn chat_with_messages(
    &self,
    session_id: &str,
    messages: &[ChatMessage],
    emotion_hint: Option<&str>,
) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError>;
```

`HttpLlmClient::chat()` 只构造当前 user message，再调用上述方法；不再独立拼 ASR system message。更新仓库内所有 mock 和调用点，明确 `None` 或原有 hint。

- [ ] **Step 8: 运行模板、客户端和编译检查**

Run: `cargo test -p voice_server client::prompt::tests`

Run: `cargo test -p voice_server client::llm::tests`

Run: `cargo check -p voice_server --all-targets`

Expected: PASS；Agent 模块中不存在 Prompt 加载器，Prompt 只由构造完成的 `HttpLlmClient` 持有。

### Task 3: 实现 LlmAgent 内部的长度路由器

**Files:**
- Create: `crates/voice_server/src/agent/router.rs`
- Modify: `crates/voice_server/src/agent/mod.rs`
- Modify: `crates/voice_server/src/lib.rs`
- Test: `crates/voice_server/src/agent/router.rs` 内 `#[cfg(test)]` 模块

**Interfaces:**
- `ModelRouter` 是无状态对象，`DEFAULT_STRONG_MIN_CHARS` 固定为 15。
- `ModelRouter::route(&self, input: &str) -> ModelTier`。
- 字符数定义为 `input.trim().chars().count()`：去除首尾空白，按 Unicode 字符计数；标点和中间空白也计入长度。
- `ModelRouter` 是 `LlmAgent` 持有的内部组件；它不访问网络、不调用 LLM、不读取 `MemoryStore`。session pipeline 不得直接调用它。

- [ ] **Step 1: 写 15 字边界失败测试**

```rust
#[test]
fn routes_by_trimmed_unicode_character_count() {
    let router = ModelRouter::default();
    assert_eq!(router.route("你好"), ModelTier::Fast);
    assert_eq!(router.route("一二三四五六七八九十甲乙丙丁"), ModelTier::Fast);
    assert_eq!(router.route("一二三四五六七八九十甲乙丙丁戊"), ModelTier::Strong);
    assert_eq!(router.route("  一二三四五六七八九十甲乙丙丁  "), ModelTier::Fast);
}
```

再覆盖空字符串和纯空白均走 fast；补充含中文、ASCII、Emoji、标点的用例，证明使用 `chars().count()`，不使用 UTF-8 字节数。

- [ ] **Step 2: 运行路由测试确认失败**

Run: `cargo test -p voice_server agent::router::tests`

Expected: FAIL，因为 `router` 模块尚不存在。

- [ ] **Step 3: 实现独立长度路由模块**

```rust
pub const DEFAULT_STRONG_MIN_CHARS: usize = 15;

#[derive(Default)]
pub struct ModelRouter;

impl ModelRouter {
    pub fn route(&self, input: &str) -> ModelTier {
        if input.trim().chars().count() < DEFAULT_STRONG_MIN_CHARS {
            ModelTier::Fast
        } else {
            ModelTier::Strong
        }
    }
}
```

除 `trim()` 和字符计数外不做任何文本标准化、特征提取或语义判断。生产和测试都直接验证固定阈值 15，不在本版本暴露动态配置入口。

- [ ] **Step 4: 运行路由测试和静态检查**

Run: `cargo test -p voice_server agent::router::tests`

Run: `cargo clippy -p voice_server --all-targets -- -D warnings`

Expected: PASS。

### Task 4: 将双模型和路由封装进单个 LlmAgent

**Files:**
- Modify: `crates/voice_server/src/agent/llmagent.rs`
- Modify: `crates/voice_server/src/bin/voice_server.rs`
- Test: `crates/voice_server/src/agent/llmagent.rs` 内 `#[cfg(test)]` 模块

**Interfaces:**
- `LlmAgent` 继续实现 `LlmClient`，仍是 `VoiceSession` 和 `llm_tts_items` 看见的唯一对象。
- `LlmAgent` 内部只持有 `fast_llm: ArcLlm`、`strong_llm: ArcLlm`、`store`、`ModelRouter` 和 `metrics`，不持有 Prompt。
- `LlmAgent::chat()` 把当前用户输入交给 `ModelRouter`，只根据返回的 tier 选择底层 client；System Prompt 由被选中的 client 内部自动注入。实现时将现有参数变量从易混淆的 `prompt` 改名为 `user_input`。
- `LlmAgent::chat_with_messages()` 不参与记忆和 Prompt 处理，但根据最后一条 user message 选择底层 client，并原样透传 `emotion_hint`。
- `LlmAgent::new`、`with_window`、`with_store` 保持兼容：只传一个 client 时，fast/strong 都引用该 client。

- [ ] **Step 1: 写单 Agent 动态选模型失败测试**

```rust
#[tokio::test]
async fn one_agent_routes_each_turn_to_the_selected_model() {
    let fast = Arc::new(RecordingLlm::responding("fast"));
    let strong = Arc::new(RecordingLlm::responding("strong"));
    let agent = test_agent(fast.clone(), strong.clone());

    drain(agent.chat("s", "你好", None).await.unwrap()).await;
    drain(agent.chat("s", "帮我比较两个方案并给出详细执行计划", None).await.unwrap()).await;

    assert_eq!(fast.calls(), 1);
    assert_eq!(strong.calls(), 1);
    assert_eq!(fast.last_user_message(), "你好");
    assert_eq!(strong.last_user_message(), "帮我比较两个方案并给出详细执行计划");
}
```

第二轮 strong 请求还必须包含第一轮的 user/assistant 历史，证明模型切换没有切换 Agent 或 MemoryStore。Prompt 是否正确由 Task 2 的 `HttpLlmClient` 测试负责，不在 Agent mock 中断言。

- [ ] **Step 2: 运行单 Agent 路由测试确认失败**

Run: `cargo test -p voice_server agent::llmagent::tests::one_agent_routes_each_turn_to_the_selected_model -- --exact`

Expected: FAIL，因为当前 `LlmAgent` 只有一个 `llm` 字段。

- [ ] **Step 3: 重构 LlmAgent 字段和构造函数**

目标结构：

```rust
pub struct LlmAgent {
    fast_llm: ArcLlm,
    strong_llm: ArcLlm,
    store: Arc<dyn MemoryStore>,
    router: ModelRouter,
    metrics: Arc<dyn VoiceMetricsSink>,
}
```

新增完整构造入口：

```rust
pub fn with_models(
    fast_llm: ArcLlm,
    strong_llm: ArcLlm,
    store: Arc<dyn MemoryStore>,
    router: ModelRouter,
    metrics: Arc<dyn VoiceMetricsSink>,
) -> Self
```

旧构造入口将同一个 client clone 到 fast/strong，并使用默认 router 和 `NoopMetricsSink`，保证现有单模型测试与外部调用不被强制修改。

- [ ] **Step 4: 在 `chat()` 内完成动态选择和消息拼装**

每轮固定执行：

```text
读取共享历史
  -> ModelRouter::route(当前 user input)
  -> [history..., current user]
  -> tier 对应的底层 LlmClient
  -> 原样透传 emotion_hint，由 client 注入其固定 Prompt
  -> 流式返回并在成功结束后写共享记忆
```

Agent 的消息组装函数不接收 tier 或 emotion：

```rust
fn build_messages(
    &self,
    history: &[Message],
    user_input: &str,
) -> Vec<ChatMessage>
```

该函数只生成历史和当前 user，不得创建任何 system message。调用选定 client 的 `chat_with_messages(session_id, &messages, emotion_hint)` 后，client 才把自己的固定 system message 放到第 0 条。

- [ ] **Step 5: 更新启动装配，但不改变 Service/Session 类型**

```rust
let strong_cfg = cfg.llm_strong.as_ref().unwrap_or(&cfg.llm);
let templates = LlmPromptTemplates::from_embedded()?;
let fast_llm = voice_server::build_llm_client_with_prompt(
    &cfg.llm, provider_cfg, Arc::<str>::from(templates.fast),
)?;
let strong_llm = voice_server::build_llm_client_with_prompt(
    strong_cfg, provider_cfg, Arc::<str>::from(templates.strong),
)?;
let agent = Arc::new(LlmAgent::with_models(
    fast_llm.clone(),
    strong_llm,
    store,
    ModelRouter::default(),
    metrics.clone(),
));
```

`VoiceService::new_with_metrics(asr, fast_llm, agent, tts, metrics)` 签名保持不变；admin 继续拿 fast 原始 client。`llm_strong` 缺失时记录 warning，说明 strong 路由复用 fast 型号。启动日志记录固定阈值 `strong_min_chars=15`。

- [ ] **Step 6: 运行 Agent 和构造回归测试**

Run: `cargo test -p voice_server agent::llmagent::tests`

Run: `cargo test -p voice_server session::tests`

Run: `cargo check -p voice_server --all-targets`

Expected: PASS；`VoiceSession` 仍持有一个 `Arc<LlmAgent>`，无需新增 Runtime 类型。

### Task 5: 增加路由与升级指标

**Files:**
- Modify: `crates/voice_server/src/metrics.rs`
- Test: `crates/voice_server/src/metrics.rs` 内 `#[cfg(test)]` 模块

**Interfaces:**
- `VoiceMetricsSink::observe_llm_route(tier, duration)`。
- `VoiceMetricsSink::llm_escalated(reason)`。
- `LlmAgent` 在每轮 `chat()` 内记录路由耗时和结果，在内部 fast -> strong 切换时记录升级。
- 指标名固定为 `voice_llm_route_duration_seconds`、`voice_llm_route_total{route}`、`voice_llm_escalation_total{from,to,reason}`。
- label 值只来自枚举的 `as_str()`，不接受用户文本、session id、request id 或任意错误字符串。

- [ ] **Step 1: 写指标失败测试**

```rust
#[test]
fn renders_low_cardinality_route_and_escalation_metrics() {
    let metrics = VoiceMetrics::new();
    metrics.observe_llm_route(ModelTier::Fast, Duration::from_millis(1));
    metrics.llm_escalated(EscalationReason::EmptyResponse);
    let output = metrics.render();
    assert!(output.contains("voice_llm_route_total{route=\"fast\"} 1"));
    assert!(output.contains("voice_llm_escalation_total{from=\"fast\",reason=\"empty_response\",to=\"strong\"} 1"));
    assert!(!output.contains("session_id"));
}
```

- [ ] **Step 2: 运行指标测试确认失败**

Run: `cargo test -p voice_server metrics::tests::renders_low_cardinality_route_and_escalation_metrics -- --exact`

Expected: FAIL，因为 route/escalation collector 尚不存在。

- [ ] **Step 3: 实现 trait 默认方法和 Prometheus collector**

为 `VoiceMetricsSink` 的两个新方法提供空默认实现，减少现有 mock 的改动；`VoiceMetrics` 注册 histogram、route counter vec 和 escalation counter vec。新增：

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EscalationReason {
    Timeout,
    EmptyResponse,
    ProviderError,
}
```

`NoopMetricsSink` 保持零副作用。

- [ ] **Step 4: 运行指标测试**

Run: `cargo test -p voice_server metrics::tests`

Expected: PASS，已有指标名称和无高基数标签测试保持通过。

### Task 6: 在单个 LlmAgent 内实现一次安全兜底

**Files:**
- Modify: `crates/voice_server/src/agent/llmagent.rs`
- Modify: `crates/voice_server/src/session/pipeline.rs`（仅删除已完成 TODO 并更新职责注释）
- Test: `crates/voice_server/src/agent/llmagent.rs` 内 `#[cfg(test)]` 模块
- Test: `crates/voice_server/src/session/pipeline.rs` 内现有回归测试

**Interfaces:**
- 路由、client 选择和 fast -> strong 兜底全部封装在 `LlmAgent::chat()`；Prompt 仍完全属于各自 client。
- `LlmAgent` 对外仍返回一个 `BoxStream<Result<LlmEvent, ClientError>>`；下游不知道最终使用了哪个模型。
- fast 在第一次向下游 yield 非空 delta 前发生建连失败、流失败、空 final 或空流结束时，可以切换一次 strong。
- fast 已经 yield 非空 delta 后发生错误时，错误直接透传；不得再调用 strong。
- strong 仍为空或失败时返回原错误，不循环重试。

- [ ] **Step 1: 写 Agent 兜底失败测试**

```rust
#[tokio::test]
async fn fast_empty_response_retries_strong_before_yielding() {
    let fast = Arc::new(RecordingLlm::empty_final());
    let strong = Arc::new(RecordingLlm::responding("我来处理。"));
    let agent = test_agent(fast.clone(), strong.clone());

    let events = collect(agent.chat("s", "你好", None).await.unwrap()).await.unwrap();
    let text = events.iter().map(|e| e.delta.as_str()).collect::<Vec<_>>().concat();
    assert_eq!(text, "我来处理。");
    assert_eq!(fast.calls(), 1);
    assert_eq!(strong.calls(), 1);
    assert_eq!(agent.memory_len("s").await, 2);
}
```

再分别覆盖 fast `chat_with_messages()` 建连返回 Err、stream 首事件返回 Err、stream 无事件直接结束三种情况，均只能调用 strong 一次。

- [ ] **Step 2: 运行兜底测试确认失败**

Run: `cargo test -p voice_server agent::llmagent::tests::fast_empty_response_retries_strong_before_yielding -- --exact`

Expected: FAIL，因为当前 Agent 没有第二个底层模型。

- [ ] **Step 3: 提取单次 client 调用**

实现不感知 Prompt 的私有辅助函数：

```rust
async fn start_attempt(
    llm: ArcLlm,
    session_id: String,
    messages: Vec<ChatMessage>,
    emotion_hint: Option<String>,
) -> Result<BoxStream<Result<LlmEvent, ClientError>>, ClientError> {
    llm.chat_with_messages(&session_id, &messages, emotion_hint.as_deref()).await
}
```

`chat()` 在返回 `'static` stream 前 clone fast/strong client、store、metrics、session id、历史消息、当前 user input 和 emotion hint；stream 内不得借用 `&self`。strong 兜底继续传同一份历史消息和 emotion hint，strong client 自动注入自己的固定 Prompt。

- [ ] **Step 4: 在 Agent 返回流中实现切换状态机**

状态固定为：

```text
FastPending -> FastVisible -> Completed
FastPending -> StrongPending -> StrongVisible -> Completed
FastVisible + error -> Failed
StrongPending/StrongVisible + error -> Failed
```

使用 `async_stream::try_stream!` 消费当前 attempt。fast 的空 delta 不算 visible；fast 的空 final 不向下游 yield，而是触发 strong。第一次非空 delta yield 后设置 `visible = true`，后续不得切换模型。

空 fast attempt 不能写记忆。最终对下游可见的 attempt 收到第一段非空 delta 时写入一次 user；收到成功 final 时写入完整 assistant。strong 兜底成功后总计写入两条；fast 已输出后再失败时保留 user、但不写不完整的 assistant。

- [ ] **Step 5: 写“输出后禁止兜底”和共享历史测试**

```rust
#[tokio::test]
async fn does_not_retry_after_fast_has_yielded_visible_text() {
    let fast = Arc::new(RecordingLlm::text_then_error("部分回答"));
    let strong = Arc::new(RecordingLlm::responding("重复回答"));
    let agent = test_agent(fast, strong.clone());
    let result = collect(agent.chat("s", "你好", None).await.unwrap()).await;
    assert!(result.is_err());
    assert_eq!(strong.calls(), 0);
}
```

再验证上一轮走 fast、下一轮走 strong 时，Agent 传给 strong client 的 messages 包含上一轮 user/assistant 历史、当前 user、不包含任何 system message，并原样带上当前 emotion hint。strong Prompt 的注入只在 `HttpLlmClient` 测试中验证。

- [ ] **Step 6: 接入指标和结构化日志**

`chat()` 完成路由后调用：

```rust
self.metrics.observe_llm_route(tier, route_started_at.elapsed());
```

发生升级时调用 `llm_escalated`，日志字段只包含 `session_id`、`from=fast`、`to=strong` 和枚举 reason；不记录完整用户文本。删除 `session/pipeline.rs` 中“模型路由待实现”的 TODO，替换为“模型路由由 LlmAgent 内部完成”的注释。

- [ ] **Step 7: 运行 Agent 和语音 pipeline 回归测试**

Run: `cargo test -p voice_server agent::llmagent::tests`

Run: `cargo test -p voice_server pipeline::llm_tts::tests`

Run: `cargo test -p voice_server session::pipeline::tests`

Expected: PASS；`llm_tts_items` 和 `run_pipeline` 的函数签名不变，取消仍通过 drop Agent stream 立即关闭当前 HTTP 请求。

### Task 7: 启动验证、配置文档和回归检查

**Files:**
- Modify: `README.md`（仅增加双模型配置和当前不支持工具协议的说明）
- Modify: `docs/superpowers/specs/2026-09-01-voice-llm-selection-baseline.md`（仅在实现与文档存在偏差时记录，不改用户 Prompt）
- Verify: workspace tests and本地启动日志

**Interfaces:**
- 启动日志必须分别显示 fast/strong 型号和 `strong_min_chars=15`，不打印 API key。
- README 明确：`llm` 是 fast；`llm_strong` 是 strong；缺少 strong 时复用 fast 型号；路由只按 `trim()` 后字符数判断；fast 不注册工具；当前 strong 也尚无 `tools/tool_calls` 执行协议。

- [ ] **Step 1: 更新 README 配置示例**

写入与 `config.yaml.template` 一致的 `llm`、`llm_strong` 示例，并列出环境变量前缀：

```text
VOICE_LLM_*
VOICE_LLM_STRONG_*
```

不要在 README 复制真实 `config.yaml` 中的 endpoint、token 或私有网络地址。

- [ ] **Step 2: 运行格式检查**

Run: `cargo fmt --all -- --check`

Expected: PASS。

- [ ] **Step 3: 运行定向测试**

Run: `cargo test -p voice_server config::tests`

Run: `cargo test -p voice_server client::prompt::tests`

Run: `cargo test -p voice_server agent::llmagent::tests`

Run: `cargo test -p voice_server agent::router::tests`

Run: `cargo test -p voice_server metrics::tests`

Run: `cargo test -p voice_server pipeline::llm_tts::tests`

Run: `cargo test -p voice_server session::pipeline::tests`

Expected: 全部 PASS。

- [ ] **Step 4: 运行 crate 和 workspace 回归**

Run: `cargo test -p voice_server`

Run: `cargo test --workspace`

Run: `cargo clippy -p voice_server --all-targets -- -D warnings`

Expected: 全部 PASS；若存在与本功能无关的既有失败，记录准确测试名和错误，不修改无关模块。

- [ ] **Step 5: 本地启动冒烟**

Run: `cargo run -p voice_server --bin voice_server`

验证日志和行为：

```text
“你好”                               -> route=fast, char_count=2
“一二三四五六七八九十甲乙丙丁”       -> route=fast, char_count=14
“一二三四五六七八九十甲乙丙丁戊”     -> route=strong, char_count=15
“  一二三四五六七八九十甲乙丙丁  ”   -> route=fast, char_count=14
```

确认 fast 请求体的 model 为 `Qwen/Qwen3-8B`，system message 以 `/no_think` 开头；strong 请求体的 model 为 `Qwen/Qwen3-30B-A3B`，system message 不含 `/no_think`；任何响应中的 `reasoning_content` 不出现在 LLM delta、TTS 文本和会话记忆中。

- [ ] **Step 6: 核对最终差异**

Run: `git diff --check`

Run: `git status --short`

Expected: 无空白错误；只出现本计划声明的文件和用户原有未提交改动。不要自动提交。

## 前端播报打断专项

前端播报打断、ASR 非空事件和 `message_id` 结果过滤已单独整理到
`docs/superpowers/plans/archive/2026-09-02-message-id-asr-filtering.md`，后续以该中文计划和对应规范为准。

## 后续独立计划

以下内容不混入本次实现，避免路由和工具协议同时改变核心链路：

1. OpenAI-compatible `tools` / `tool_calls` 请求与 SSE 增量解析。
2. 工具注册表、schema 校验、权限、幂等性和高风险二次确认状态机。
3. strong 的多工具依赖编排及工具结果回填。
4. 基于真实流量评估是否引入关键词、意图、风险、复杂度、ASR 置信度或小分类模型。
5. 将长度阈值配置化，并依据 fast/strong SLO 和成本数据调优。
