//! Redis 后端的短期记忆 —— 集群 / 多实例共享
//!
//! ## 数据模型
//! 每个 session 一条 Redis list，key = `{key_prefix}{session_id}`
//!   - `LPUSH key msg`：把新消息塞到 list 头部（新→旧顺序里最新在 index 0）
//!   - `LTRIM key 0 (N-1)`：截断到 N 条
//!   - `LRANGE key 0 -1`：读全部，按 list 顺序返回（最新→最旧）→ 反转得到（最旧→最新）
//!   - `EXPIRE key ttl_secs`：闲置 TTL，防止冷 session 永久占内存
//!
//! ## Connection
//! 用 `redis::aio::ConnectionManager` —— 单连接复用、自动重连、跨任务可 clone。
//! 每次操作 clone 一份 handle 出去即可。
//!
//! ## 失败语义
//! Redis 不可达时 `history` / `append` 直接报错并向上抛（CallError），不静默吞。
//! 这保证"集群视角的真相"是单一来源；不会出现"实例 A 以为写成功了，实例 B 看不到"的脑裂。

use async_trait::async_trait;
use redis::{aio::ConnectionManager, AsyncCommands};
use tracing::warn;

use crate::agent::memory::{MemoryStore, Message};

const DEFAULT_KEY_PREFIX: &str = "voice:memory:";
const DEFAULT_TTL_SECS: u64 = 3600; // 1 小时，闲置超过这个时间的 session 被自动驱逐

pub struct RedisStore {
    manager: ConnectionManager,
    window_size: usize,
    key_prefix: String,
    ttl_secs: u64,
}

impl RedisStore {
    /// 构造。`url` 例：`redis://127.0.0.1:6379/`。
    /// 启动时会建立 ConnectionManager（一次 ping 验证可达），失败立即返回。
    pub async fn connect(
        url: &str,
        window_size: usize,
    ) -> anyhow::Result<Self> {
        Self::connect_with_prefix(url, window_size, DEFAULT_KEY_PREFIX.to_string(), DEFAULT_TTL_SECS).await
    }

    pub async fn connect_with_prefix(
        url: &str,
        window_size: usize,
        key_prefix: String,
        ttl_secs: u64,
    ) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;
        let manager = ConnectionManager::new(client).await?;
        Ok(Self {
            manager,
            window_size: window_size.max(1),
            key_prefix,
            ttl_secs,
        })
    }

    pub fn window_size(&self) -> usize {
        self.window_size
    }

    fn key(&self, session_id: &str) -> String {
        format!("{}{}", self.key_prefix, session_id)
    }
}

#[async_trait]
impl MemoryStore for RedisStore {
    async fn history(&self, session_id: &str) -> Vec<Message> {
        let mut conn = self.manager.clone();
        let key = self.key(session_id);
        // list 的 index 0 是最新 → 反转得到最旧 → 最新
        let raw: Vec<String> = match conn.lrange(&key, 0, -1).await {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    target: "voice_server.agent.redis",
                    session_id,
                    error = %e,
                    "Redis LRANGE 失败"
                );
                return Vec::new();
            }
        };
        let mut msgs: Vec<Message> = raw
            .into_iter()
            .rev()
            .filter_map(|s| match serde_json::from_str::<Message>(&s) {
                Ok(m) => Some(m),
                Err(e) => {
                    warn!(
                        target: "voice_server.agent.redis",
                        session_id,
                        error = %e,
                        "短期记忆 JSON 反序列化失败，跳过该条"
                    );
                    None
                }
            })
            .collect();
        msgs.reverse();
        msgs
    }

    async fn append(&self, session_id: &str, msg: Message) {
        let mut conn = self.manager.clone();
        let key = self.key(session_id);
        let payload = match serde_json::to_string(&msg) {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    target: "voice_server.agent.redis",
                    session_id,
                    error = %e,
                    "短期记忆 JSON 序列化失败，跳过该条"
                );
                return;
            }
        };
        if let Err(e) = conn.lpush::<&str, &str, i64>(&key, payload.as_str()).await {
            warn!(
                target: "voice_server.agent.redis",
                session_id,
                error = %e,
                "Redis LPUSH 失败"
            );
            return;
        }
        // 截断到 N 条；window_size 是 usize，转 isize 防止负数
        let upper = self.window_size as isize - 1;
        if let Err(e) = conn.ltrim::<&str, ()>(&key, 0, upper).await {
            warn!(
                target: "voice_server.agent.redis",
                session_id,
                error = %e,
                "Redis LTRIM 失败"
            );
        }
        // 设 TTL —— 闲置太久自动释放 Redis 内存
        if let Err(e) = conn.expire::<&str, bool>(&key, self.ttl_secs as i64).await {
            warn!(
                target: "voice_server.agent.redis",
                session_id,
                error = %e,
                "Redis EXPIRE 失败（不影响主流程）"
            );
        }
    }

    async fn clear(&self, session_id: &str) {
        let mut conn = self.manager.clone();
        let key = self.key(session_id);
        if let Err(e) = conn.del::<&str, i64>(&key).await {
            warn!(
                target: "voice_server.agent.redis",
                session_id,
                error = %e,
                "Redis DEL 失败"
            );
        }
    }

    async fn len(&self, session_id: &str) -> usize {
        let mut conn = self.manager.clone();
        let key = self.key(session_id);
        match conn.llen::<&str, usize>(&key).await {
            Ok(n) => n,
            Err(e) => {
                warn!(
                    target: "voice_server.agent.redis",
                    session_id,
                    error = %e,
                    "Redis LLEN 失败"
                );
                0
            }
        }
    }
}

