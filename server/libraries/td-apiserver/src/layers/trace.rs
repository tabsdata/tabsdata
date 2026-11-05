//
//  Copyright 2024 Tabsdata Inc.
//

use http::{HeaderName, HeaderValue, Request};
use std::fmt::Debug;
use std::fmt::Display;
use td_common::id::id;
use tower_http::LatencyUnit;
use tower_http::request_id::PropagateRequestIdLayer;
use tower_http::set_header::{MakeHeaderValue, SetRequestHeaderLayer};
use tower_http::trace::{
    DefaultOnEos, DefaultOnFailure, DefaultOnRequest, DefaultOnResponse, HttpMakeClassifier,
    MakeSpan, TraceLayer,
};
use tracing::{Level, Span, span};

const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
const UNKNOWN: &str = "unknown";

/// Mints the request's id, logs the request in a span carrying it, and
/// echoes it on the response, as `x-request-id`.
pub fn layer() -> (
    SetRequestHeaderLayer<MakeId>,
    PropagateRequestIdLayer,
    TraceLayer<HttpMakeClassifier, RequestMakeSpan>,
) {
    (
        SetRequestHeaderLayer::overriding(X_REQUEST_ID, MakeId),
        PropagateRequestIdLayer::new(X_REQUEST_ID),
        TraceLayer::new_for_http()
            .make_span_with(RequestMakeSpan)
            .on_request(DefaultOnRequest::new().level(Level::DEBUG))
            .on_response(
                DefaultOnResponse::new()
                    .level(Level::DEBUG)
                    .latency_unit(LatencyUnit::Micros),
            )
            .on_failure(
                DefaultOnFailure::new()
                    .level(Level::ERROR)
                    .latency_unit(LatencyUnit::Micros),
            )
            .on_eos(
                DefaultOnEos::new()
                    .level(Level::TRACE)
                    .latency_unit(LatencyUnit::Micros),
            ),
    )
}

/// Mints a request's [`Id`] (a UUIDv7) as its `x-request-id`, replacing any
/// the client sent.
#[derive(Clone, Copy, Default)]
pub struct MakeId;

impl<T> MakeHeaderValue<T> for MakeId {
    fn make_header_value(&mut self, _request: &T) -> Option<HeaderValue> {
        HeaderValue::from_str(&id().to_string()).ok()
    }
}

/// Similar to [`tower_http::trace::DefaultMakeSpan`], but with a unique ID per span.
#[derive(Debug, Clone, Default)]
pub struct RequestMakeSpan;

impl<B: Debug> MakeSpan<B> for RequestMakeSpan {
    fn make_span(&mut self, request: &Request<B>) -> Span {
        let id = request
            .headers()
            .get(X_REQUEST_ID)
            .and_then(|minted| minted.to_str().ok())
            .unwrap_or(UNKNOWN);
        log_span(&id, request)
    }
}

fn log_span<B>(id: &impl Display, request: &Request<B>) -> Span {
    fn header<B>(request: &Request<B>, header: HeaderName) -> String {
        const UNKNOWN_HEADER: HeaderValue = HeaderValue::from_static(UNKNOWN);
        format!(
            "{:?}",
            request.headers().get(header).unwrap_or(&UNKNOWN_HEADER)
        )
    }

    span!(
        Level::INFO,
        "request",
        id = %id,
        method = %request.method(),
        uri = %request.uri(),
        version = ?request.version(),
        host = %header(request, http::header::HOST),
        user_agent = %header(request, http::header::USER_AGENT),
        project_id = tracing::field::Empty,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::routing::get;
    use http::{Method, Uri, Version};
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use td_common::id::Id;
    use tower::ServiceExt;
    use tracing::subscriber::set_default;
    use tracing::{Instrument, info};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::{fmt, registry};

    struct WriterGuard {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for WriterGuard {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut lock = self.buffer.lock().unwrap();
            lock.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_http_span() {
        let logs = Arc::new(Mutex::new(Vec::new()));

        let logs_clone = logs.clone();
        let layer = fmt::layer()
            .with_writer(move || WriterGuard {
                buffer: logs_clone.clone(),
            })
            .with_ansi(false)
            .with_level(true);

        let subscriber = registry().with(layer);
        let _guard = set_default(subscriber);

        let method = Method::POST;
        let uri = Uri::try_from("http://example.com/api/test").unwrap();
        let version = Version::HTTP_2;
        let host = HeaderValue::from_str("example.com").unwrap();
        let user_agent = HeaderValue::from_str("test-agent/1.0").unwrap();
        let request = Request::builder()
            .method(&method)
            .uri(&uri)
            .version(version)
            .header(http::header::HOST, &host)
            .header(http::header::USER_AGENT, &user_agent)
            .body(())
            .unwrap();
        let id = id();
        let log_span = log_span(&id, &request);

        async {
            info!("This is a test log message");
        }
        .instrument(log_span)
        .await;

        let logs = logs.lock().unwrap().to_vec();
        let log_output = String::from_utf8_lossy(&logs);
        assert!(log_output.contains(&format!(
            "request{{id={id} method={method} uri={uri} version={version:?} host={host:?} user_agent={user_agent:?}}}"
        )));
    }

    #[tokio::test]
    async fn test_the_response_echoes_the_id_the_span_logs() {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let logs_clone = logs.clone();
        let capture = fmt::layer()
            .with_writer(move || WriterGuard {
                buffer: logs_clone.clone(),
            })
            .with_ansi(false);
        let _guard = set_default(registry().with(capture));

        let router = Router::new()
            .route("/", get(|| async { info!("handled") }))
            .layer(layer());
        let sent = id().to_string();
        let response = router
            .oneshot(
                Request::get("/")
                    .header(X_REQUEST_ID, &sent)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let echoed = response.headers()[X_REQUEST_ID].to_str().unwrap();
        assert!(Id::try_from(echoed).is_ok(), "{echoed}");
        assert_ne!(echoed, sent, "the client's id was used");
        let logs = logs.lock().unwrap().to_vec();
        let log_output = String::from_utf8_lossy(&logs);
        assert!(
            log_output.contains(&format!("request{{id={echoed} ")),
            "{log_output}"
        );
    }
}
