# voice-app

终端语音 → 服务端 ASR → LLM → TTS → 回放的完整链路，端到端跑通。

## 项目结构

```
voice-app/
├── Cargo.toml                            # workspace
├── README.md
└── crates/
    ├── voice-proto/                      # VoicePayload 协议 + 编解码
    │   ├── Cargo.toml
    │   └── src/lib.rs
    ├── voice-server/                     # 服务端（基于 webhttp）
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── clients.rs                # ASR / LLM / TTS trait + HTTP 实现
    │       ├── session.rs                # VoiceSession 状态机 + pipeline
    │       ├── service.rs                # VoiceService 实现 webhttp::ServiceCallback
    │       ├── test_api.rs               # 单能力验证 HTTP 接口（/test/*）
    │       └── bin/voice_server.rs       # 启动入口
    ├── voice-client/                     # Rust 终端 SDK + CLI
    │   ├── Cargo.toml
    │   └── src/
    │       ├── lib.rs
    │       ├── callback.rs               # VoiceCallback trait + 默认实现
    │       └── bin/voice_terminal.rs     # CLI demo（wav 文件推流 + 接收回放）
    └── ws-payload-helper/                # （辅助工具）
```

## 端到端跑通

### 1. 启动 voice_server

```bash
# 直接用 voice-voice_server.yaml 里配置的 ASR/LLM/TTS 服务
RUST_LOG=info cargo run --release -p voice-server

# 默认端口 8080；可用环境变量覆盖：
# VOICE_PORT=9000
# VOICE_ASR_URL=http://...
# VOICE_LLM_URL=http://...
# VOICE_TTS_URL=http://...
# VOICE_<ASR|LLM|TTS>_AUTHORIZATION=Bearer sk-xxx
# VOICE_<ASR|LLM|TTS>_MODEL=model-name
```

### 2. 准备测试 wav

```bash
# 生成一个 1.5s 的 16kHz mono PCM wav（用 Python / sox / ffmpeg 都可以）
python3 -c "
import wave, struct, math
sr=16000
with wave.open('test.wav','wb') as w:
    w.setnchannels(1); w.setsampwidth(2); w.setframerate(sr)
    for i in range(int(sr*1.5)):
        v=int(0.5*32767*math.sin(2*math.pi*1000*i/sr))
        w.writeframes(struct.pack('<h', v))
"
```

### 3. 跑终端 demo

```bash
RUST_LOG=info cargo run -p voice-client --bin voice_terminal -- \
  --url ws://127.0.0.1:8080/ws/voice/cli/demo \
  --file test.wav
```

打断：在终端 demo 运行时按 `q` + Enter 触发 Interrupt。

### 4. 观察日志（每个关键节点都有）

服务端日志（节选，siliconflow 真实服务示例）：
```
INFO voice_server.service: WS 连接建立
INFO voice_server.service: 新建 VoiceSession session_id=voice-cli-demo
INFO voice_server.session: VoiceSession 创建 session_id=voice-cli-demo
INFO voice_server.session: 收到 SessionStart session_id=voice-cli-demo
INFO voice_server.session: 状态转换 from=Idle to=Listening
INFO voice_server.session: VAD 句尾，触发 pipeline bytes=48000 elapsed_ms=1659
INFO voice_server.session: 状态转换 from=Listening to=Processing
INFO voice_server.session: pipeline 开始 session_id=voice-cli-demo
INFO voice_server.asr: ASR POST 请求 (48000 bytes)
INFO voice_server.asr: 收到 ASR partial/final text=你好世界 is_final=true
INFO voice_server.session: ASR final 完成，进入 LLM
INFO voice_server.llm: LLM POST 请求 prompt_len=4
INFO voice_server.llm: 收到 LLM delta delta_len=6 is_final=false delta=你好世界。
INFO voice_server.session: LLM 切出完整句子，送 TTS sentence=你好世界。
INFO voice_server.tts: TTS POST 请求 text_len=4
INFO voice_server.tts: 收到 TTS chunk seq=0 bytes=32000 is_last=false
INFO voice_server.tts: 收到 TTS chunk seq=1 bytes=0 is_last=true
INFO voice_server.session: pipeline 全部完成
INFO voice_server.session: 收到 SessionEnd session_id=voice-cli-demo reason=normal exit
INFO voice_server.service: WS 断开，移除 session
```

## 当前实现覆盖的落地清单

| 阶段 | 状态 | 说明 |
|------|------|------|
| Phase 1 MVP | ✅ | VoicePayload + ASR 文本 |
| Phase 2 半双工对话 | ✅ | LLM 流式 + TTS 流式 + 按句切分 |
| Phase 3 全双工 | ⏳ 部分 | 打断机制（服务端支持 Interrupt），客户端 VAD 还在浏览器侧做 |
| Phase 4 生产化 | ❌ | 限流、监控、审核、多租户未做 |

## 单能力验证接口

`voice-server` 暴露了 4 个 HTTP REST + NDJSON 流接口，方便单点验证 ASR / LLM / TTS：

| 路径 | 输入 | 输出 |
|------|------|------|
| `POST /test/asr` | raw PCM body | NDJSON `{text, is_final}` |
| `POST /test/llm` | JSON `{prompt}` | NDJSON `{delta, is_final}` |
| `POST /test/tts` | JSON `{text}` | NDJSON `{seq, audio(base64), is_last}` |
| `POST /test/llm_tts` | JSON `{text}` | NDJSON TTS chunks（LLM 切句后串 TTS） |

前端页面默认 5 个 tab：全流程对话 / ASR / LLM / TTS / LLM→TTS，可以分别跑每个能力。

```bash
# curl 示例
curl -sN -X POST http://localhost:8080/test/llm \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"讲个笑话"}'

curl -sN -X POST http://localhost:8080/test/tts \
  -H 'Content-Type: application/json' \
  -d '{"text":"你好世界"}' | jq -c '{seq, is_last, len:(.audio|length)}'
```

## 关键设计决策

1. **VoicePayload 用 enum + tag** —— 上行（音频流）和下行（ASR/LLM/TTS 结果）共用一个 wire format
2. **不用 webproto 改 webproto** —— VoicePayload 定义在 `voice-proto` crate 内，复用 webproto 的 `Message<VoicePayload>` 信封（向后兼容）
3. **AudioChunk 用 Indication 单向推** —— 高频上行避免每个分片带 event_id
4. **状态机 Idle/Listening/Processing/Speaking** —— 简化版（实际 Listening 和 Processing/Speaking 互不冲突，可以并发）
5. **CancellationToken 打断** —— 任何阶段被 Interrupt 即停；目前未在 TTS chunk-by-chunk 上做取消（下一段 LLM 不再生成即可）
6. **logging 全面覆盖** —— SessionStart/AudioChunk/VAD/ASR partial+final/LLM delta/切句/TTS chunk/SessionEnd/Interrupt/状态转换/WsConnect/WsDisconnect 全打 `tracing::*!`

## 已知 bug 与改进点

1. **空句 TTS**：当 LLM delta 末尾出现孤立标点（如 "。"）会触发空字符串 TTS 请求——生产里应该跳过 text.trim().is_empty()
2. **VAD 简化**：终端 CLI 用 wav 文件整段作为 is_last=true，没有真正做能量/静音 VAD
3. **麦克风采集**：`push_from_mic` 当前是 stub，需要接 cpal 实时流
4. **Opus 编码**：当前直接传 PCM s16le，要上生产应该前端 Opus 编码
5. **音频重采样**：wav 假设已经是 16kHz mono，实际终端可能有 44.1kHz / 立体声，需要重采样

## 关键依赖

| 依赖 | 用途 |
|------|------|
| webhttp | HTTP + WebSocket 服务端框架（基于 actix-web） |
| webclient | WebSocket 客户端（基于 tokio-tungstenite） |
| webproto | 二进制消息协议（MessagePack + 信封） |
| tokio + tokio-util | async runtime + CancellationToken |
| reqwest | HTTP client（连 ASR/LLM/TTS 真实服务） |
| tracing | 日志 |
| clap | CLI 参数解析 |
| hound | wav 文件读取 |