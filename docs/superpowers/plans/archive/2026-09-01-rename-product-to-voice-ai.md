# Voice AI Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将项目对外名称统一为 `voice-ai`，不保留旧名称兼容。

**Architecture:** 仅修改产品展示、文档、部署注释和 IDE 元数据；Rust 内部 crate、模块及二进制名称保持不变。配置搜索列表和浏览器存储 key 直接切换到 `voice-ai`。

**Tech Stack:** Rust, Cargo, React, TypeScript, Electron, static HTML/JavaScript, Markdown

**Spec:** `docs/superpowers/specs/2026-09-01-voice-ai-renaming-design.md`

## Global Constraints

- 对外项目名使用 `voice-ai`，界面展示使用 `Voice AI`。
- 保留 `voice_server`、`voice-proto`、`voice_desktop` 等内部技术标识。
- 用户配置只读取 `$HOME/.config/voice-ai/`。
- 浏览器存储只使用 `voice-ai.activeTab`。
- 不覆盖或回退工作区中已有的用户修改。

---

### Task 1: 用户配置目录兼容

**Files:**
- Modify: `crates/voice_server/src/logging.rs`
- Test: `crates/voice_server/src/logging.rs`

**Interfaces:**
- Consumes: `candidate_paths(bin_name: &str) -> Vec<PathBuf>`
- Produces: 只包含 `voice-ai` 用户目录的配置候选路径

- [x] **Step 1: Write the failing test**

新增 `candidate_paths_only_use_voice_ai_user_config`，断言 `.config/voice-ai/voice-voice_server.yaml` 存在且旧目录路径不存在。

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p voice_server logging::tests::candidate_paths_only_use_voice_ai_user_config -- --exact`

Expected: FAIL，因为实现仍包含旧用户配置目录。

- [x] **Step 3: Write minimal implementation**

在 `candidate_paths` 的用户级配置部分依次追加：

```rust
let config_root = PathBuf::from(home).join(".config");
out.push(config_root.join("voice-ai").join(&yaml));
```

同步更新函数文档中的搜索顺序说明。

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p voice_server logging::tests::candidate_paths_only_use_voice_ai_user_config -- --exact`

Expected: PASS。

### Task 2: 产品名称和项目元数据

**Files:**
- Modify: `README.md`
- Modify: `PROGRESS.md`
- Modify: `crates/voice-proto/Cargo.toml`
- Modify: `crates/voice_server/static/index.html`
- Modify: `crates/voice_server/static/*.js`
- Modify: `voice_desktop/index.html`
- Modify: `voice_desktop/package.json`
- Modify: `voice_desktop/electron/main.ts`
- Modify: `voice_desktop/src/features/conversation/ConversationPage.tsx`
- Modify: `deploy/caddy/Caddyfile`
- Modify: `diagrams.md`
- Modify: `diagrams.html`
- Modify: `docs/superpowers/specs/2026-08-27-voice-knowledge-agent-design.md`
- Rename: `.idea/voice-app.iml` to `.idea/voice-ai.iml`
- Modify: `.idea/modules.xml`

**Interfaces:**
- Consumes: 已确认的产品名 `voice-ai` / `Voice AI`
- Produces: 一致的用户可见标题、仓库文档和项目元数据

- [x] **Step 1: Update product-facing copy**

把 README、进度文档、架构图、设计文档和部署注释中的项目名改为 `voice-ai`；把 Web、React 和 Electron 标题改为 `Voice AI`。

- [x] **Step 2: Update browser storage key**

保留 Rust 内部标识；把静态 JavaScript 文件头和 localStorage key 改为 `voice-ai`。

- [x] **Step 3: Update IDE metadata**

将 `.idea/voice-app.iml` 重命名为 `.idea/voice-ai.iml`，并更新 `.idea/modules.xml` 的引用。

- [x] **Step 4: Scan remaining old names**

Run: `rg -n --hidden --glob '!target/**' --glob '!voice_desktop/node_modules/**' --glob '!.git/**' 'voice-app|Voice App|voice app' .`

Expected: 只剩用于断言旧配置目录不存在的回归测试，以及改名过程文档中的必要引用。

### Task 3: Verification

**Files:**
- Verify only

**Interfaces:**
- Consumes: Tasks 1-2 的代码和文档变更
- Produces: Rust 与桌面端构建测试证据

- [x] **Step 1: Run Rust tests**

Run: `cargo test -p voice_server`

Expected: 全部通过。

- [x] **Step 2: Run desktop tests and build**

Run: `npm test` in `voice_desktop`

Run: `npm run build` in `voice_desktop`

Expected: 两条命令退出码均为 0。

- [x] **Step 3: Validate diff quality**

Run: `git diff --check`

Expected: 无空白错误。

- [x] **Step 4: Review final scope**

检查 `git diff`，确认未重命名 Rust crate、模块、二进制或 `voice_desktop` 包名，且未回退已有修改。
