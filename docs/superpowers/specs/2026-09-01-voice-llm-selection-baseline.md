# 语音对话 LLM 选型基线

> 状态：Draft
> 适用架构：`ASR → LLM → TTS` 级联链路
> 更新时间：2026-09-01

## 1. 文档目的

本文档定义语音对话场景中 LLM 的选型标准、运行约束和评测方法，作为后续模型比较、接入和线上调优的共同基线。

本文档只讨论级联架构：

```text
用户音频
  → ASR（整句或流式识别）
  → LLM（文本流式生成）
  → 分句器
  → TTS（按句流式合成）
  → 客户端播放
```

## 2. 当前架构决策

### 2.1 默认链路

当前默认采用：

```text
前端 VAD / 端点检测
  → 非流式 ASR（整句返回 final 文本）
  → 流式 LLM
  → 按标点或安全边界切句
  → 流式输出 TTS
  → Web Audio 播放队列
```

选择非流式 ASR 的原因是：整句识别准确率和工程稳定性优先，LLM 只在收到可信的 final 文本后启动，避免 partial 文本变化导致重复生成。后续如果实测 ASR 成为主要延迟来源，再评估流式 ASR，但不改变 LLM 选型原则。

### 2.2 LLM 的职责

LLM 负责：

- 理解 ASR 输出的用户意图和上下文。
- 生成适合口语播放的简短回复。
- 在需要时调用 RAG、业务 API 或其他工具。
- 按协议输出可被分句器和 TTS 消费的增量文本。
- 正确响应用户打断、取消和超时。

LLM 不负责：

- 音频识别、端点检测或音色合成。
- 直接决定客户端播放队列。
- 将内部思考过程输出给 TTS。
- 代替业务代码执行有副作用的操作。

## 3. LLM 接口约束

### 3.1 输入

每次 LLM 请求至少包含：

- 当前轮 ASR final 文本。
- 经裁剪或摘要后的会话上下文。
- 系统提示词和业务规则。
- 可选的检索结果。
- 可用工具定义及参数约束。

ASR partial 文本默认不发送给 LLM。只有满足端点条件并得到 final 文本后，才创建一轮 LLM 请求。

### 3.2 输出

LLM 必须支持文本流式输出，业务层至少能区分：

```text
delta      增量文本
completed  生成完成
cancelled  被用户打断或会话取消
failed     Provider 或协议失败
```

输出应满足以下约束：

- 首句尽快可播放，不等待完整回答。
- 默认使用短句、口语化表达，避免 Markdown、表格和大段列表。
- 工具调用参数必须是结构化数据，不能把 JSON 直接送入 TTS。
- 推理过程、隐藏标签和不可播字符必须在进入 TTS 前过滤。
- 用户打断后立即停止继续生成，并丢弃尚未播放的文本和音频。

### 3.3 流式文本到 TTS

分句器在以下边界之一成立时提交 TTS：

1. 句末标点：`。！？.!?`。
2. 达到安全长度上限，例如 20–40 个中文字符。
3. 检测到自然停顿或换行，且当前片段已经达到最小长度。
4. LLM 完成时提交剩余文本。

不能为了追求首包而切出单字、半个词或未闭合的数字/代码/URL。分句器应保留一个可取消的缓冲区，打断时清空该缓冲区。

### 3.4 模型 System Prompt 参考

参考 LiveKit、Pipecat 和 Deepgram 的语音 Agent Prompt，结合本项目的 ASR 情绪提示、LLM 流式输出、工具调用和 TTS 分句逻辑，以下两段均可独立作为完整 System Prompt。它们不再通过运行时拼接基础 Prompt，避免两个模型的行为约束发生漂移。

#### 3.4.1 `fast`：Qwen/Qwen3-8B

```text
/no_think 你是实时中文语音助手，请直接和用户进行对话。用户的输入是语音经 ASR 转写后的文字，你的输出会经过TTS模型转成语音播报给用户。

工作边界：
1. 默认使用中文，直接回应问候、闲聊、简单常识问答和已有上下文可以回答的 FAQ，不要尝试调用工具。
2. 默认回答不超过两句话、约 40 个汉字；只提供用户当前能执行的结论或下一步。用户没有追问时，不主动补充背景、例外和长篇解释。
3. 不确定时直接说不知道，不要编造事实。
4. 不主动提及自己是 AI / 语言模型。
5. ASR 转写可能含错别字、谐音字、漏字、重复字或口水词，需结合上下文推断真实意图，不要把字面错别字当事实。
6. 不要向用户指出、复述或纠正输入里的错别字，也不要说"你是不是想说 XXX"，像直接听到他说的一样自然回应。
7. 如果结合上下文仍无法判断意图，用口语请用户换个说法再说一遍（如"再说一遍呗，我没太听清"），不要硬猜。
8. 用户的问题如果缺少回答所需的关键信息（地点、时间、对象、偏好等），可以自然地反问 1-2 个关键问题来收窄范围，不必硬答、不要瞎猜。

表达格式：使用短句和口语化中文，适合语音播报；不要使用 Markdown、表格、项目符号、Emoji、XML、JSON、代码块、内部标签或思考过程，不要输出任何结构化内容。

以下内容由语音识别模型从用户语音中推断，仅供理解语气和场景参考，可能不准确。请自然回答用户，可以据此调整语气和措辞，但不要在回复中提及、复述或暗示这些信号，不要把事件当作用户明确说出的事实，也不要解释判断过程。
推断信息：{emotion}
```

#### 3.4.2 `strong`：Qwen/Qwen3-30B-A3B

```text
你是实时中文语音助手的强执行模型。你的职责是处理快速模型不应承担的复杂、多步骤、强约束或高风险请求，并在内部完成可靠的分析和执行规划。
用户的输入是语音经 ASR 转写后的文字.
你的输出会直接进入 TTS。可以在内部进行多步推理，但对用户只能输出最终结论、必要的确认问题和下一步动作，绝不能暴露推理内容。

工作流程：
1. 先在内部识别用户目标、约束、已有上下文、缺失信息、事实冲突和操作风险，再决定回答、澄清或调用工具。不要因为请求表面简单就跳过约束检查。
2. 对需要多个步骤的任务，先形成完整计划，再按依赖顺序执行；能够并行的只读查询可以并行，存在写入或副作用时必须串行并校验前一步结果。
3. 工具调用前校验权限、参数完整性、资源标识、幂等性和用户授权范围；工具返回后核对结果是否真正满足目标。工具失败、结果矛盾或权限不足时，说明可验证的状态，不得声称已经完成。
4. 涉及金额、删除、转账、权限、合同、对外发送或其他不可逆操作时，先明确告知动作和影响，取得用户确认后再执行。确认不能从沉默、历史习惯或模糊措辞推断。
5. 面对不确定或互相冲突的信息，优先指出关键不确定性并提出最少的澄清问题；不要用猜测填补会影响结果的缺口。低风险且不影响目标的缺口可以采用明确标注的默认值。
6. 任务完成后只总结结果、未完成项和用户下一步需要知道的内容。默认不超过三句话；用户明确要求过程、解释或详细结果时，再按需展开，但仍保持适合语音播放。
7. 用户打断、取消或提出新问题时，立即终止上一轮计划和未提交的输出，以最新请求为准；已发生的外部副作用必须如实说明。
8. ASR 转写可能含错别字、谐音字、漏字、重复字或口水词，需结合上下文推断真实意图，不要把字面错别字当事实。
9. 不要向用户指出、复述或纠正输入里的错别字，也不要说"你是不是想说 XXX"，像直接听到他说的一样自然回应。
10. 用户的问题如果缺少回答所需的关键信息（地点、时间、对象、偏好等），可以自然地反问 1-2 个关键问题来收窄范围，不必硬答、不要瞎猜。

工具与输出约束：工具调用必须严格遵循 schema。工具名、JSON 参数、内部计划、reasoning、调用过程和内部错误绝不能进入语音文本；只有工具完成后的可验证结果才能对用户播报。除非工具协议明确要求结构化调用，不要输出 XML、JSON、代码块、内部标签或思考过程。

最终表达格式：默认使用中文、短句、口语化表达，不使用 Markdown、表格、项目符号、Emoji 或复杂特殊符号。不要为了显得专业而播报分析过程或冗余背景。

以下内容由语音识别模型从用户语音中推断，仅供理解语气和场景参考，可能不准确。请自然回答用户，可以据此调整语气和措辞，但不要在回复中提及、复述或暗示这些信号，不要把事件当作用户明确说出的事实，也不要解释判断过程。
推断信息：{emotion}
```

两份完整 Prompt 的行为差异如下；此表仅用于设计和评测，不是运行时需要拼接的公共 Prompt：

| 维度 | `fast`：Qwen/Qwen3-8B | `strong`：Qwen/Qwen3-30B-A3B |
|---|---|---|
| 任务范围 | 明确、低风险、单轮可完成 | 复杂、多步骤、强约束或高风险 |
| 推理策略 | 关闭 thinking，不展开分析 | 可在内部完成计划、推理和校验，但不外显 |
| 工具策略 | 不注册工具，不查询外部系统，不执行任何操作 | 按依赖编排多次调用，逐步校验权限、参数和结果 |
| 不确定性 | 一个关键问题；低风险时采用默认值 | 主动识别冲突和缺口，不能用猜测补齐关键事实 |
| 完成判断 | 仅对已有上下文或一次工具结果给结论 | 必须核对每个步骤和最终结果，失败时不得声称完成 |
| 输出目标 | 两句话以内、约 40 个汉字，优先首包速度 | 默认三句话以内，优先完整性和可靠性 |
| 不适合的请求 | 复杂任务应尽快停止并转交 | 承担复杂任务，但仍需遵守确认和可验证性约束 |

`fast` 使用非推理模式；`strong` 只在复杂或高风险路由时使用推理模式。无论使用哪种模型，发送给 TTS 的都只能是最终回答文本，不能包含 `reasoning_content`、`<think>` 内容或工具 JSON。

## 4. 选型维度与优先级

### 4.1 优先级

默认优先级如下：语音响应延迟 > 中文口语理解与回复自然度 > 工具调用可靠性 > 稳定性与并发 > 成本 > 长上下文能力 > 泛化推理能力。

具体业务可调整权重，但不能只用文字 benchmark 或单轮准确率做决定。

### 4.2 必测能力

| 维度 | 关注点 | 主要指标 |
|---|---|---|
| 首包延迟 | LLM 多久开始产生可播文本 | `voice_llm_time_to_first_token_seconds` |
| 流式能力 | 是否稳定输出增量文本 | 首 token 成功率、delta 间隔 |
| 中文口语 | 省略、同音词、数字、地址、口头纠正 | 意图准确率、槽位准确率 |
| 回复可播性 | 是否短句、少歧义、少特殊符号 | TTS 清洗率、人工自然度评分 |
| 工具调用 | 是否选对工具并填对参数 | 调用成功率、参数完整率 |
| 多轮一致性 | 是否记住约束并避免跑题 | 多轮任务完成率 |
| 打断取消 | 取消后是否停止输出 | 取消生效延迟、残留音频率 |
| 稳定性 | 超时、限流、空响应和重试 | 成功率、超时率、错误率 |
| 成本 | 输入、输出和缓存 token 成本 | 每轮和每分钟成本 |
| 部署约束 | 区域、数据合规、显存和运维 | 可用区、资源占用、SLA |

### 4.3 参考项目的选型启示

成熟语音项目通常使用通用文本 LLM，而不是专门的“语音 LLM”：

| 参考项目 | 示例 LLM | 选择背景 |
|---|---|---|
| LiveKit Agents Starter | `google/gemma-4-31b-it` | LiveKit Inference 的延迟优化开源默认模型，可由平台直接托管 |
| Pipecat Quickstart | `gpt-4.1` | OpenAI 流式和工具调用集成成熟，便于快速跑通完整 Pipeline |
| Deepgram + LiveKit 教程 | `gpt-4o` | 常见的 OpenAI 集成基线；教程明确允许替换为其他兼容模型 |

这些型号是各自框架的默认或教程选择，不是跨项目的统一排名。选择时需要注意：

1. **区分平台默认和模型绝对能力。** LiveKit 的 Gemma 4 31B 由其基础设施托管并针对延迟优化，优势包含平台服务条件，不能直接等同于裸模型在本地的性能。
2. **优先验证流式和工具调用。** 语音对话需要尽早产生可播文本，并能稳定返回结构化工具参数；普通文字 benchmark 不能替代这两项测试。
3. **把语音约束放在 Prompt 和编排层。** 参考项目都要求回复简短、少格式、适合直接朗读；LLM 型号本身不会自动解决分句、TTS 排队和打断。
4. **评估 SDK 和运行时适配成本。** 成熟项目选择 GPT、Gemma 等型号，部分原因是框架已有插件、流式事件和工具调用解析；更换本地模型时，必须验证 OpenAI-compatible 协议、SSE 事件和取消语义。
5. **云端样例不能直接证明本地适用。** GPT-4.1、GPT-4o 的网络延迟、价格和数据区域与本地 Qwen/Gemma 不同；本地选型必须在目标硬件和并发下重测。
6. **使用同一套 Pipeline 做公平比较。** 固定 ASR、分句器、TTS、Prompt 和上下文，只替换 LLM，并比较首 token、首句 TTS、工具调用成功率和完整任务成功率。

参考：[LiveKit Voice AI Quickstart](https://docs.livekit.io/agents/start/voice-ai/)、[Pipecat Quickstart](https://docs.pipecat.ai/pipecat/get-started/quickstart)、[Deepgram Voice Agent with LiveKit](https://developers.deepgram.com/docs/build-voice-agent-with-livekit-and-deepgram)。

## 5. 模型分层策略

### 5.1 快速模型：默认处理普通轮次

适用于：

- 问候、闲聊和简单问答。
- 基于已有上下文即可完成的简单业务指令。
- 已由 RAG 提供明确答案的事实性问题。

要求：低首 token 延迟、稳定流式输出、较低价格和较高并发。语音场景默认关闭深度 thinking 或使用非推理模式，以避免模型生成不可见的思考 token，拉长首包时间。

### 5.2 强模型：只处理复杂轮次

适用于：

- 多条件、多步骤业务办理。
- 需要比较、规划或跨多份资料推理的问题。
- 快速模型超时、空响应、主动请求升级或用户明确要求详细解释的情况。

强模型不应作为所有语音轮次的默认模型，否则通常会牺牲交互延迟和成本。可采用“先快速回答，检测到复杂度后升级”的路由方式；升级时需要把当前轮完整上下文传给强模型。

### 5.3 本地部署模型

本地模型的判断标准是端到端实测，而不是参数规模：

- 先在目标硬件上测并发、首 token 和持续输出速度。
- 普通语音轮次优先使用 Instruct / non-thinking 模式。
- 复杂轮次才启用 thinking，并设置推理预算上限。
- 评估量化后中文口语、工具调用和长上下文是否明显退化。
- 将模型服务与语音编排服务解耦，保留 OpenAI-compatible 或内部统一接口。

Qwen3 文档明确支持 thinking / non-thinking 切换，可作为本地模型路由的实现参考。DeepSeek 当前 API 文档列出的 Flash/Pro 系列支持工具调用，适合放在级联链路的文本 LLM 位置；具体版本、价格和可用性必须在接入前重新核对。
参考：[Qwen3 thinking 模式](https://qwen.readthedocs.io/en/stable/getting_started/quickstart.html)、[DeepSeek 模型与价格](https://api-docs.deepseek.com/quick_start/pricing/)。

### 5.4 模型路由决策

默认不额外调用 LLM 作为模型裁判。路由粒度是“一轮 ASR final”，由 `LlmAgent` 内部的长度路由器选择初始模型：

```text
ASR final → LlmAgent → 长度路由器 → fast / strong client → 流式 LLM → 分句 → TTS
```

首版规则固定为：

1. 对当前 ASR final 执行 `trim()`，再用 Unicode 字符数 `chars().count()` 计算长度。
2. 字符数小于 15 时选择 `fast`。
3. 字符数达到或超过 15 时选择 `strong`。
4. `fast` 在尚未输出任何非空文本前发生超时、空响应或 Provider 临时错误时，最多升级一次 `strong`。

标点和输入内部空白也计入字符数，只有首尾空白被移除。首版不根据问候、闲聊、工具意图、风险、复杂度或 ASR 置信度改变初始路由，因此短的复杂请求仍可能走 fast，长的简单请求也可能走 strong；这是有意保留的简单基线，后续用真实流量评估是否升级规则。

### 5.4.1 端侧播报打断与 ASR 事件

端侧正在播放 TTS 时，不因本地 VAD 检测到声音或收到空 ASR 事件而停止播报。收到非空 `asr_partial` 或非空 `asr_final`（文本执行 `trim()` 后非空，包含任意语种文字即可）时，确认用户确实说话，并立即停止当前 TTS 播放队列。

规则如下：

1. 本地 VAD 只负责发送音频，不直接停止 TTS，也不发送 `Interrupt`。
2. `asr_partial` / `asr_final` 的文本为空或只含首尾空白时，不停止 TTS、不更新用户文本、不改变当前播报请求。
3. 每轮语音输入只生成一个 `message_id`，该轮所有上行音频和服务端 ASR、LLM、TTS、状态事件复用同一个值。
4. 收到 trim 后非空的 `asr_partial` 或 `asr_final` 时：如果其 `message_id` 不等于端侧当前有效 ID，则先将当前有效 ID 更新为该值，再停止当前 TTS 播放队列；相同 ID 的后续 partial/final 只更新识别文本，不重复停止。
5. 端侧设置当前有效 ID 后，只处理 `message_id` 等于该 ID 的 ASR、LLM、TTS 和 pipeline 状态事件；其它 pipeline 事件丢弃。`session_ack` 等连接级事件不受该过滤规则限制。
6. 本阶段不根据旧事件的文本内容做额外丢弃策略；是否展示旧 ASR 文本由当前有效 `message_id` 过滤规则决定。
7. 服务端内部可以为每次处理尝试生成 `request_id`，用于取消、重试和并发控制，但 `request_id` 不出现在 WebSocket 下行协议或端侧事件类型中。

典型时序：

```text
message_id=0 正在播报
message_id=1 返回空 ASR partial             -> 继续播放 message_id=0
message_id=1 返回非空 ASR partial            -> 当前有效 ID=1，停止 message_id=0
message_id=1 后续返回非空 ASR final          -> 继续处理，不重复停止
message_id=0 的迟到 LLM/TTS/status           -> 丢弃
```

这里的 `message_id` 表示用户输入的一句话：端侧在一轮语音开始时生成，整轮 `audio_chunk` 复用同一个值；下一轮语音才生成新的值。音频上行帧不携带客户端生成的 `request_id`；`request_id` 仅由服务端内部创建，用于处理尝试的取消、重试和并发控制，不下发给端侧。连接级控制消息可以有自己的 `message_id`，但不参与 pipeline 结果过滤。

服务端为每轮有效音频创建一个 pipeline，并将该轮的 `message_id` 作为 pipeline 属性贯穿处理。下行 `AsrPartial`、`LlmDelta`、`TtsAudio` 和 `AgentStatus` 必须携带该 `message_id`，端侧据此进行结果配对和过滤；按请求产生的错误若能关联 pipeline，也必须携带该 `message_id`。同一句话重试时复用原 `message_id`，服务端内部可以生成新的 `request_id`，但端侧不可见。

### 5.5 模型路由器：必须自研的实现内容

Provider SDK 只负责模型请求和流式事件，不提供本项目的模型分层策略，因此路由器由项目自己实现。首版单独抽为 `agent/router.rs`，但只包含长度判断，不引入规则引擎或分类 SDK。

| 组件 | 必须实现的内容 | 输出 |
|---|---|---|
| `ModelRouter` | 无状态对象，固定 `DEFAULT_STRONG_MIN_CHARS=15` | 独立路由对象 |
| 长度计算 | `input.trim().chars().count()` | `usize` 字符数 |
| 路由结果 | `< 15` 为 `ModelTier::Fast`，`>= 15` 为 `ModelTier::Strong` | `ModelTier` |
| 升级策略 | fast 在首个非空 delta 前超时、空响应或 Provider 临时错误 | 最多一次 strong 兜底 |
| 观测 | 路由耗时、模型层级、升级原因、模型版本 | Metrics + Trace 字段 |
| 测试 | 0、14、15 字边界，首尾空白，中文、ASCII、Emoji 和标点 | 可重复测试集 |

第一版不实现通用闲聊分类模型，也不实现关键词、规则注册表、特征提取或 ASR 置信度路由。15 字阈值先固定在 `ModelRouter` 内，不增加配置项；有真实数据后再决定是否配置化。

与现有代码的边界：

```text
client/prompt.yaml       fast/strong 两份完整 System Prompt 的唯一文件来源
client/{asr,llm,tts}.rs  Provider SDK/HTTP/WebSocket 适配；HttpLlmClient 构造后持有固定 Prompt
agent/router.rs           本项目自研长度路由器
agent/llmagent.rs         持有两个 client 和路由器，维护共享会话历史
session/pipeline.rs       ASR final → 单个 LlmAgent → 切句 → TTS，不感知路由
config/                   fast/strong 模型和生成参数
metrics.rs                路由耗时、模型层级和升级指标
```

替换 Provider 只改 `client` 适配和配置；修改路由策略只改 `router`，不应改动 session pipeline、TTS 分句协议或客户端播放逻辑。

路由指标使用低基数标签：

```text
voice_llm_route_duration_seconds
voice_llm_route_total{route="fast|strong"}
voice_llm_escalation_total{from="fast", to="strong", reason="timeout|empty_response|provider_error"}
```

## 6. 延迟与 SLO 基线

完整指标定义以 [`2026-09-01-voice-chain-metrics-spec.md`](2026-09-01-voice-chain-metrics-spec.md) 为准。LLM 选型重点关注：

| 指标 | 定义 | 选型用途 |
|---|---|---|
| `voice_llm_time_to_first_token_seconds` | `llm_first_token_at - llm_started_at` | 判断 LLM 是否适合实时首句输出 |
| `voice_llm_duration_seconds` | `llm_completed_at - llm_started_at` | 判断完整回复耗时 |
| `voice_e2e_utterance_end_to_tts_first_audio_seconds` | 用户说完到 TTS 首包 | 主用户体验 SLI |
| `voice_tts_time_to_first_audio_seconds` | LLM 文本提交完成到 TTS 首包 | 区分 LLM 与 TTS 瓶颈 |
| `voice_requests_timeout_total / voice_requests_total` | 超时请求比例 | 判断模型服务稳定性 |

建议初始目标，待真实流量基线后校准：

- 用户说完到 TTS 首包：p50 < 0.8s，p95 < 1.5s。
- LLM 首 token：p50 < 0.3s，p95 < 0.8s。
- 正常请求成功率：≥ 99%。
- 因 LLM 超时导致的请求比例：< 1%。

这些是工程启动目标，不是 Provider 的保证值。所有模型必须在相同 ASR、上下文和 TTS 条件下比较，并分别固定第 3.4.1、3.4.2 节的完整 System Prompt。

## 7. 评测方案

### 7.1 测试集

测试集至少包含：

- 普通中文闲聊和寒暄。
- 口语省略、重复、纠正和停顿。
- 数字、金额、日期、地址、专有名词。
- FAQ 和 RAG 问答。
- 单步和多步工具调用。
- 多轮约束、追问和上下文切换。
- 用户打断、取消、重复发送和 ASR 错字。
- 噪声、方言或中英混说样本。

每个样本保存 ASR 输出、模型版本、提示词版本、工具结果和各时间点，但不将原始文本放入 Prometheus 标签。

### 7.2 评分

建议使用加权评分，而不是单一总分：

```text
总分 = 0.30 × 端到端延迟
     + 0.25 × 任务/意图正确率
     + 0.20 × 工具调用正确率（仅对 strong/工具路由样本统计）
     + 0.10 × 回复可播性
     + 0.10 × 稳定性
     + 0.05 × 成本
```

延迟、成本等反向指标应先归一化，避免某个量纲支配结果。任何安全、合规或工具误操作问题都可以直接否决模型，不以总分抵消。

### 7.3 A/B 测试要求

- 固定 ASR、TTS、VAD、网络区域和提示词版本。
- 至少记录 p50/p95/p99，不只看平均值。
- 区分首 token、首句 TTS 和完整回复三个时间点。
- 线上灰度先从 5% 流量开始，观察至少一个完整业务周期。
- 新模型出现超时、空响应、工具误调用或可播性退化时，支持按模型版本快速回滚。

## 8. 落地顺序与伪代码

第一阶段只实现长度路由和一次强模型兜底。fast 请求不注册工具；当前路由器不识别工具意图：

```text
非流式 ASR final
  → 单个 LlmAgent 内的长度路由器
  → fast 或 strong HttpLlmClient
  → 句末/安全长度分句
  → 流式 TTS
```

实现顺序：

1. 在 `agent/router.rs` 定义 `ModelRouter`，固定 `strong_min_chars=15`。
2. 为 0、14、15 字边界和 Unicode 字符计数编写单元测试。
3. 让单个 `LlmAgent` 持有 fast/strong 两个 client，并在每轮 ASR final 后调用路由器。
4. 为 `fast` 和 `strong` 配置独立模型名、Prompt、超时和生成参数；Prompt 在 `HttpLlmClient` 构造时固定。
5. 实现 fast 失败后的单次升级，且只允许发生在任何非空文本输出之前。
6. 接入路由和升级指标，记录模型层级与模型版本。
7. 使用固定测试集验证长度边界、共享历史、超时、空响应和取消。

### 8.1 路由伪代码

以下伪代码描述首版实现。`ModelRouter` 只负责选择初始模型；Prompt 已在两个 `HttpLlmClient` 构造时固定，`LlmAgent` 不感知 Prompt。

```text
class ModelRouter:
    strong_min_chars = 15

    function route(asr_final):
        char_count = unicode_char_count(trim(asr_final))
        if char_count < strong_min_chars:
            return "fast"
        return "strong"


async function LlmAgent.handle_voice_turn(asr_final, emotion_hint, cancel_signal):
    tier = router.route(asr_final)
    metrics.record_route(tier)
    messages = shared_history + [user(asr_final)]
    client = fast_client if tier == "fast" else strong_client

    result = await client.chat_with_messages(
        messages=messages,
        emotion_hint=emotion_hint,
        cancel_signal=cancel_signal,
        stream_to=sentence_buffer_and_tts
    )

    if result.cancelled:
        metrics.record_result("cancelled")
        return

    if result.success:
        metrics.record_result("success")
        return

    if tier == "fast" and no_visible_text(result) and is_escalatable(result):
        metrics.record_escalation(
            from_model="fast",
            to_model="strong",
            reason=result.reason
        )

        fallback_result = await strong_client.chat_with_messages(
            messages=messages,
            emotion_hint=emotion_hint,
            cancel_signal=cancel_signal,
            stream_to=sentence_buffer_and_tts
        )

        metrics.record_result(classify_result(fallback_result))
        return

    metrics.record_result(classify_result(result))
```

`is_escalatable` 至少覆盖首 token 超时、空响应和 Provider 临时错误。fast 一旦产生任何非空 delta 就不得升级，避免向 TTS 输出两份前后矛盾的回答。

## 9. 与现有观测系统的对接

LLM 业务代码只依赖 `VoiceMetricsSink`，不直接依赖 Prometheus 类型。所有模型都使用相同的指标名称和低基数标签：

```text
provider
model
business
endpoint
result
error_type
```

禁止将以下字段作为指标标签：

```text
trace_id
session_id
request_id
prompt
text
```

指标用于比较模型整体表现，日志和 Trace 用于定位单轮请求。模型切换不得改变 ASR、TTS、取消语义和协议行为。

## 10. 最终推荐结论

在当前 `ASR → LLM → TTS` 级联架构、本地部署优先和中文语音对话约束下，第一版 LLM 选型收敛为：

| 角色 | 推荐型号 | Prompt | 初始模式 | 初始参数建议 | 主要用途 |
|---|---|---|---|---|---|
| `fast` | `Qwen/Qwen3-8B` | 第 3.4.1 节完整 Prompt | non-thinking | `max_tokens=256~512`、`temperature=0.7`、`top_p=0.8`、`top_k=20`；不注册 tools | 问候、闲聊、简单常识问答、已有上下文 FAQ |
| `strong` | `Qwen/Qwen3-30B-A3B` | 第 3.4.2 节完整 Prompt | 复杂请求 thinking，普通请求可关闭 | `max_tokens=512~1024`、`temperature=0.6`、`top_p=0.95`、`top_k=20` | 多步骤规划、多条件约束、多工具依赖和高风险请求 |

fast/strong 两个 `HttpLlmClient` 分别使用第 3.4.1 和第 3.4.2 节的完整 System Prompt。Prompt 是 client 的内部只读属性，在 client 构造完成时确定；`LlmAgent` 只持有两个已构造好的 client，不读取、选择或拼接 Prompt。两份 Prompt 保持相同的安全、ASR 辅助信号和 TTS 约束，但针对模型角色分别强化低延迟或复杂分析要求。不要维护第三份共享 Prompt。

```text
第 3.4.1 节完整 Prompt → Qwen/Qwen3-8B
第 3.4.2 节完整 Prompt → Qwen/Qwen3-30B-A3B
```

两份 Prompt 的公共约束需要同步维护；模型专属差异只放在对应完整 Prompt 内，避免运行时拼接导致版本不可追踪。

路由方式固定为：

```text
ASR final
  → 单个 LlmAgent 内的自研长度路由器
  → fast（少于 15 字）或 strong（达到或超过 15 字）
  → 流式输出
  → 只保留最终回答文本
  → 分句
  → TTS
```

最终结论：

1. `Qwen/Qwen3-8B` 作为默认快速模型，关闭 thinking，优先保证首 token 和并发。
2. `Qwen/Qwen3-30B-A3B` 作为强模型，首版在输入达到 15 字或 fast 尚未输出文本便失败时使用。
3. 不额外调用 LLM 判断闲聊；首版模型选择只由 `trim()` 后的 Unicode 字符数决定。
4. `strong` 的 reasoning 内容不能进入 TTS 或会话历史，只保留最终回答。
5. 两个模型均通过统一的 OpenAI-compatible LLM 接口接入，Provider 差异放在 adapter 和配置中；当前版本不实现 tools/tool_calls 协议。
6. 当前项目先用相同 ASR、分句器和 TTS 测试这两个模型，并分别固定第 3.4.1、3.4.2 节的完整 Prompt，再依据首 token、首音频、任务成功率、工具调用和成本决定是否调整型号。

外部参考模型只用于对照，不作为当前默认生产方案：

```text
google/gemma-4-31b-it  开源模型质量/延迟参考
gpt-4.1                 云端质量上限参考
```

如果目标硬件无法承载 `Qwen/Qwen3-30B-A3B`，先保留 `Qwen/Qwen3-8B` 单模型运行，并将 `gpt-4.1` 或其他兼容云端模型作为强模型对照；切换前必须重新评估数据区域、网络延迟和成本。

## 11. 待确认事项

- 真实流量是否证明需要用意图、风险、复杂度或 ASR 置信度替代单纯长度路由。
- “快速模型”和“强模型”的首批候选及目标硬件。
- 中文方言、行业词和专有名词测试集来源。
- LLM 首 token 和端到端首音频的最终 SLO。
- 失败后的重试次数、降级模型和用户可见提示。
- 云端数据区域、日志脱敏和本地部署的合规要求。
