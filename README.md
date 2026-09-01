# voice-ai

终端语音 → 服务端 ASR → LLM → TTS → 回放的完整链路，端到端跑通。

## 项目结构

```
voice-ai/
├── Cargo.toml                            # workspace
├── README.md
└── crates/
    ├── voice-proto/                      # VoicePayload 协议 + 编解码
    │   ├── Cargo.toml
    │   └── src/lib.rs
    ├── voice_server/                     # 服务端（基于 webhttp）
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── client/                   # ASR / LLM / TTS client 与协议实现
    │       ├── session/                  # VoiceSession 状态机与完整流水线
    │       ├── pipeline/                 # LLM → TTS 增量处理与音频拼接
    │       ├── agent/                    # Agent 记忆、Prompt 与搜索边界
    │       ├── service.rs                # VoiceService 实现
    │       ├── config/                   # YAML 配置与模板
    │       └── bin/voice_server.rs       # 启动入口
    └── ws_payload_helper/                # MessagePack 调试辅助工具
└── voice_desktop/                        # Electron + React 桌面端（独立 npm 项目）
```

> 注：终端 CLI 已停用，当前主线是浏览器页面和 Electron 桌面端。

## 构建和启动

### 1. 构建服务端

在项目根目录执行：

```bash
cargo build --release -p voice_server
```

生成的可执行文件为 `target/release/voice_server`。

### 2. 配置并启动 voice_server

默认配置文件是 `crates/voice_server/src/config/config.yaml`。生产环境建议复制一份到项目外部，并通过 `--config` 显式指定，避免把密钥写进仓库：

```bash
# 使用自定义配置文件
cargo run --release -p voice_server -- --config /absolute/path/to/voice-voice_server.yaml

# 或直接使用仓库内的默认配置
RUST_LOG=info cargo run --release -p voice_server

# 默认监听 8080；可用环境变量覆盖：
# HTTP_PORT=9000
# VOICE_PROVIDER_API_BASE=https://...
# VOICE_PROVIDER_API_KEY=sk-xxx
# VOICE_ASR_API_KEY=sk-xxx
# VOICE_LLM_API_BASE=https://...
# VOICE_LLM_API_KEY=sk-xxx
# VOICE_LLM_TIMEOUT_MS=30000
# VOICE_LLM_HEADERS='{"X-Tenant":"tenant-a"}'
# VOICE_TTS_API_KEY=sk-xxx
# VOICE_ASR_MODEL=model-name
# VOICE_LLM_MODEL=model-name
# VOICE_LLM_MAX_COMPLETION_TOKENS=512
# VOICE_LLM_TEMPERATURE=0.7
# VOICE_LLM_TOP_P=0.9
# VOICE_LLM_REASONING_EFFORT=low
# VOICE_LLM_INCLUDE_USAGE=true
# VOICE_TTS_MODEL=model-name
# VOICE_TTS_VOICE=vivian
# VOICE_TTS_SAMPLE_RATE=24000
```

启动后检查服务：

```bash
curl http://127.0.0.1:8080/health
```

### 3. 浏览器端到端页面

打开：

```text
http://127.0.0.1:8080
```

浏览器页面会通过 WebSocket 连接 `ws://127.0.0.1:8080/ws/voice/web/admin`。需要浏览器允许麦克风权限，并且配置中的 ASR、LLM、TTS 服务可访问。

### 4. Electron 桌面端

桌面端不会启动或打包服务端，需要先启动上面的 `voice_server`，然后在 `voice_desktop` 目录执行：

```bash
cd voice_desktop
npm install

# 开发模式：两个终端分别运行
npm run dev
npm run electron
```

启动 Electron 后，在设置中填写远程服务端地址，例如 `http://127.0.0.1:8080`。生产构建和 macOS 安装包：

```bash
npm run typecheck
npm test
npm run build
npm run package:mac
```

安装包输出到 `voice_desktop/release/`。

打断：浏览器前端「打断」按钮 = 显式 Interrupt（发 WS Interrupt + 清本地 TTS 队列）。**跨句打断**走自动路径：新问句 ASR `is_final` + 非空文本 → 前端只清本地 TTS 队列 + 停止播放，**不发** WS Interrupt（服务端自有 pipeline cancel 逻辑：非空 ASR → cancel 上一个 LLM/TTS pipeline，见 `crates/voice_server/src/session/pipeline.rs`）。
