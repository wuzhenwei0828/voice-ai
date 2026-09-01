use actix_web::HttpRequest;
use async_stream::stream;
use futures_util::{Stream, StreamExt};
use tracing::Span;
use uuid::Uuid;

tokio::task_local! {
    static TRACE_ID: String;
}

pub fn new_trace_id() -> String {
    Uuid::new_v4().to_string()
}

pub fn trace_id_from_request(request: &HttpRequest) -> String {
    request
        .headers()
        .get("trace_id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(new_trace_id)
}

pub fn current_trace_id() -> Option<String> {
    TRACE_ID.try_with(Clone::clone).ok()
}

pub async fn scope<F: std::future::Future>(trace_id: String, future: F) -> F::Output {
    TRACE_ID.scope(trace_id, future).await
}

/// Poll a response stream inside the request's trace context so lazy SSE work
/// (including upstream provider calls) keeps the same trace ID.
pub fn scope_stream<S>(trace_id: String, span: Span, inner: S) -> impl Stream<Item = S::Item>
where
    S: Stream + 'static,
    S::Item: 'static,
{
    stream! {
        let mut inner = Box::pin(inner);
        loop {
            let next = TRACE_ID.scope(trace_id.clone(), async {
                let _entered = span.enter();
                inner.next().await
            }).await;
            match next {
                Some(item) => yield item,
                None => break,
            }
        }
    }
}
