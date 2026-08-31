//! 知识检索抽象。
//!
//! Agent 只依赖 [`KnowledgeSearch`] trait，不关心检索来自本地索引、HTTP 服务还是
//! 数据库。当前提供 [`NoopKnowledgeSearch`] 作为默认实现，便于在未配置知识库时
//! 保持对话链路可运行；接入真实检索器时只需实现同一个 trait。

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// 可注入的知识搜索边界。
#[async_trait]
pub trait KnowledgeSearch: Send + Sync {
    /// 根据用户问题检索上下文，并在取消时尽快停止外部工作。
    async fn search(
        &self,
        session_id: &str,
        query: &str,
        cancel: CancellationToken,
    ) -> Result<SearchResult, SearchError>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchResult {
    /// 注入 LLM prompt 的检索上下文。
    pub context: String,
    /// 可展示给用户或用于审计的来源列表。
    pub sources: Vec<Source>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    /// 文档或网页标题。
    pub title: String,
    /// 发布方或来源站点。
    pub publisher: String,
    /// 来源最后更新时间，格式由具体检索器决定。
    pub updated_at: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    #[error("search cancelled")]
    Cancelled,
    #[error("search unavailable: {0}")]
    Unavailable(String),
    #[error("search timed out")]
    Timeout,
}

/// 默认实现，不执行任何网络请求。
pub struct NoopKnowledgeSearch;

#[async_trait]
impl KnowledgeSearch for NoopKnowledgeSearch {
    async fn search(
        &self,
        _session_id: &str,
        _query: &str,
        cancel: CancellationToken,
    ) -> Result<SearchResult, SearchError> {
        if cancel.is_cancelled() {
            return Err(SearchError::Cancelled);
        }

        Ok(SearchResult::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn noop_search_returns_empty_result() {
        let search = NoopKnowledgeSearch;
        let result = search
            .search("s", "天气", CancellationToken::new())
            .await
            .unwrap();
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
}
