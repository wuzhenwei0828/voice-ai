use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// 可注入的知识搜索边界。
#[async_trait]
pub trait KnowledgeSearch: Send + Sync {
    async fn search(
        &self,
        session_id: &str,
        query: &str,
        cancel: CancellationToken,
    ) -> Result<SearchResult, SearchError>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchResult {
    pub context: String,
    pub sources: Vec<Source>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub title: String,
    pub publisher: String,
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
