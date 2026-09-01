# 实施计划：wspool 抽象重构（WsPool trait + WsConnPool 原语 + AsrWsPool 门面 + AsrWsError）

> 依据：用户对话确认方向（外部 API 不暴露 dialer/adapter 构造细节）。
> 约束：纯增量；不改既有的 `start_session` / `WsConnPool` 行为；既有测试保留不变。
> 流程：TDD（先 Red 后 Green）；4 个新任务 + 1 个回归验证。

## 目标重构

**当前（用户痛点）**：
```rust
// live_asr_api.rs:267-275（业务层看到 5 个内部细节）
let dialer = make_real_dialer(endpoint, api_key, workspace_id);
let adapter = Box::new(QwenAsrAdapter::for_model(model));
let (session, events) = start_session(
    pool.clone(), adapter, dialer,
    sample_rate, channels, sid,
).await?;
```

**目标**：
```rust
// live_asr_api.rs（业务层只问"给我一个 asr WS client"）
let (session, events) = rt.asr_pool.start_session(sid).await?;
```

**新分层**：
```
┌────────────────────────────────────────────────────┐
│  trait WsPool  （接口 / 新）                  │
│    start_session(sid) -> (StreamingAsrSession,    │
│                           BoxStream<AsrEvent>)  │
└────────────────────┬───────────────────────────────┘
                     │ impl
┌────────────────────▼───────────────────────────────┐
│  struct AsrWsPool                                  │
│    inner: Arc<WsConnPool>                          │
│    cfg: AsrPoolConfig                              │
│  ─ start_session 内部 ─                          │
│    resolve_endpoint(cfg) → WSS URL                 │
│    dialer = make_real_dialer(...)  ← 封装       │
│    adapter = QwenAsrAdapter::for_model(...) ← 封装 │
│    start_session(inner, adapter, dialer, ...)     │
└────────────────────┬───────────────────────────────┘
                     │ 用
┌────────────────────▼───────────────────────────────┐
│  struct WsConnPool （原 WsPool，重命名）        │
│    acquire_or_dial(kind, dialer) → PooledConn     │
│    Dialer / WebSocketLike / PooledConn / LaneKind  │
│    —— 维持原样，只改名                              │
└────────────────────────────────────────────────────┘
```

## 关键设计决定

1. **trait 名 = `WsPool`**（接口），低层原语重命名为 **`WsConnPool`**（明示是 conn pool 原语），门面是 **`AsrWsPool`**（DashScope 公共协议 ASR 的实现）
2. **`AsrWsError` 独立 enum**（不直接复用 `ClientError`）：
   - `Config(String)` — endpoint / api_key / model / workspace_id 缺失或非法
   - `Pool(PoolError)` — wspool 拨号/获取失败
   - `Session(ClientError)` — run-task 发送 / stream 初始化失败
   - `Handshake(String)` — 预留（握手语义失败）
   - 提供 `From<PoolError>` + `From<ClientError>`
3. **`WsPool::start_session` 返回值**：`Result<(StreamingAsrSession, BoxStream<Result<AsrEvent, ClientError>>), AsrWsError>`
   - event_stream 内部仍用 `ClientError`（runtime/streaming 错误，与既有测试契约一致）
   - start_session 自身用 `AsrWsError`（configuration/handshake 错误）
4. **`AsrWsPool::start_session` 内部仍调 `voice_providers::asr::session::start_session` 自由函数**（不内联到 trait 方法里）—— 这样既有集成测试（qwen_asr_realtime_test 等）不动
5. **`AsrWsPool` 构造支持注入 dialer**（用于 mock 测试）：
   - `AsrWsPool::new(cfg)` —— 生产路径，内部用 `make_real_dialer` + 自动解析 endpoint
   - `AsrWsPool::with_dialer(cfg, dialer)` —— 测试路径，跳过 dialer 构造
6. **`resolve_endpoint` 移到 `asr_ws_pool.rs`** —— 它是 `AsrWsPool` 的内部依赖；从 `live_asr_api.rs` 删除并把测试搬过去

## 任务清单

- [ ] **T1** `voice-providers/src/ws_pool.rs`：`WsPool` 重命名为 `WsConnPool`（全文件 grep 替换；内部 + 同文件内 impl 块；`lib.rs` re-export）
- [ ] **T2** `voice-providers/src/asr/asr_ws_pool.rs` 新建：先写测试（`AsrWsError` Display/From、`resolve_endpoint` 5 用例、`AsrWsPool::start_session` 用 mock dialer 的集成测试 1-2 个），再实现
- [ ] **T3** `voice-providers/src/lib.rs`：re-export `WsPool / AsrWsPool / AsrWsError / AsrPoolConfig`
- [ ] **T4** `voice-server/src/live_asr_api.rs`：runtime 改存 `Arc<dyn WsPool>`；`on_session_start` 改调 `rt.asr_pool.start_session(sid).await?`；删除本地 `resolve_endpoint` + `make_real_dialer` import + 适配器构造 + 相关 4 单测
- [ ] **T5** 验证：cargo test --workspace 全过；既有 live_asr_api 端点冒烟（无 key 错误路径）；diff 审计（只有 lib.rs + ws_pool.rs 改名 + live_asr_api.rs 业务简化；旧测试不动）

## 测试矩阵

| 覆盖项 | 位置 | 状态 |
|---|---|---|
| `WsConnPool` 重命名（语义不变）| 既有 `ws_pool_test.rs` | ✅ 自动覆盖 |
| `AsrWsError` Display（4 变体）| 新 `asr_ws_pool.rs` tests | TDD |
| `AsrWsError::From<PoolError>` / `From<ClientError>` | 新 `asr_ws_pool.rs` tests | TDD |
| `resolve_endpoint`（5 用例：显式 / workspace / sg region / 缺配置 / 缺工作空间）| 新 `asr_ws_pool.rs` tests（从 live_asr_api.rs 搬） | TDD |
| `AsrWsPool::start_session` 集成（mock dialer：发起 run-task → 收到 task-started → send_audio → 收到 result-generated → finish → 收到 task-finished → 流结束 → 连接归还 WsConnPool.idle）| 新 `asr_ws_pool.rs` tests 或新 `tests/asr_ws_pool_test.rs` | TDD |
| `AsrWsPool::new` 配置缺失返回 `AsrWsError::Config` | 同上 | TDD |
| `live_asr_api` 旧单测（endpoint 解析 + 配置 default）| 搬到 asr_ws_pool.rs 后从 live_asr_api.rs 删除 | 移动 |
| `ws_pool` 既有 ws_pool_test.rs 全部测试 | `crates/voice-providers/tests/ws_pool_test.rs` | ✅ 不动 |
| `qwen_asr_realtime_test` / `qwen_asr_test` 等既有集成测试 | `tests/` 各文件 | ✅ 不动 |
| 真实 WS 协议级冒烟（live_asr_api 端点无 key 错误）| python websockets 客户端 | 手动验证 |

## 验证命令

```bash
cargo test --workspace                                   # 全量测试
cargo test -p voice-providers asr::asr_ws_pool           # 新模块测试
cargo test -p voice-server  live_asr                    # live_asr_api 残留测试
git diff HEAD -- crates/voice-providers crates/voice-server   # diff 审计
HTTP_PORT=8899 cargo run -p voice_server &
python3 -c 'import asyncio,json,websockets,msgpack;
async def t():
    async with websockets.connect("ws://127.0.0.1:8899/ws/live-asr/web/x") as w:
        await w.send(msgpack.packb({"Indication":{"data":{"type":"session_start","session_id":"x","sample_rate":16000,"channels":1,"codec":"pcm","language":"zh-CN"}}}))
        msg=await asyncio.wait_for(w.recv(),timeout=5)
        print(msgpack.unpackb(msg,raw=False))
asyncio.run(t())'                                        # 错误消息验证
```

## 回滚策略

全部新建 + 重命名（编译器可验证引用一致）。`git revert` 一条命令即恢复。
