# Voice AI 改名设计

**日期**：2026-09-01  
**状态**：已确认  
**目标名称**：`voice-ai`

## 范围

- 仓库标题、部署名称、架构文档和产品展示统一使用 `voice-ai` 或展示形式 `Voice AI`。
- Web 页面、Electron 窗口和桌面端页面标题统一显示 `Voice AI`。
- 用户级配置只搜索 `$HOME/.config/voice-ai/`，不兼容旧目录。

## 保留项

- Rust crate、模块、二进制和日志 target 保留现有技术标识，如 `voice_server`、`voice-proto`。
- `voice_desktop` 包名和目录名保持不变，避免扩大构建与发布变更范围。
- 浏览器 localStorage key 使用 `voice-ai.activeTab`，不读取旧 key。
- 本地 checkout 目录改为 `voice-ai`；远端仓库 slug 仍由仓库托管平台单独调整。

## 验收标准

1. 产品可见位置显示 `Voice AI`。
2. 文档和部署说明以 `voice-ai` 作为项目名。
3. `candidate_paths` 只包含新的用户配置目录。
4. Rust 测试、桌面端测试和桌面端构建通过。
