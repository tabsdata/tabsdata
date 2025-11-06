//
//  Copyright 2024 Tabs Data Inc.
//

use http::{HeaderName, HeaderValue, Request};
use std::fmt::Debug;
use td_common::id::{Id, id};
use tower_http::LatencyUnit;
use tower_http::trace::{
    DefaultOnEos, DefaultOnFailure, DefaultOnRequest, DefaultOnResponse, HttpMakeClassifier,
    MakeSpan, TraceLayer,
};
use tracing::{Level, Span, span};

#[derive(Default)]
pub struct TraceService;

impl TraceService {
    pub fn layer() -> TraceLayer<HttpMakeClassifier, RequestMakeSpan> {
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
            )
    }
}

/// Similar to [`tower_http::trace::DefaultMakeSpan`], but with a unique ID per span.
#[derive(Debug, Clone, Default)]
pub struct RequestMakeSpan;

impl<B: Debug> MakeSpan<B> for RequestMakeSpan {
    fn make_span(&mut self, request: &Request<B>) -> Span {
        log_span(&id(), request)
    }
}

fn log_span<B>(id: &Id, request: &Request<B>) -> Span {
    fn header<B>(request: &Request<B>, header: HeaderName) -> String {
        const UNKNOWN_HEADER: HeaderValue = HeaderValue::from_static("unknown");
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
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Method, Uri, Version};
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
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
        // Collect logs in a buffer
        let logs = Arc::new(Mutex::new(Vec::new()));

        // Custom layer to capture span data
        let logs_clone = logs.clone();
        let layer = fmt::layer()
            .with_writer(move || WriterGuard {
                buffer: logs_clone.clone(),
            })
            .with_ansi(false)
            .with_level(true);

        let subscriber = registry().with(layer);
        let _guard = set_default(subscriber);

        // Create some logs
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

        // Inspect the logs
        let logs = logs.lock().unwrap().to_vec();
        let log_output = String::from_utf8_lossy(&logs);
        assert!(log_output.contains(&format!(
            "request{{id={id} method={method} uri={uri} version={version:?} host={host:?} user_agent={user_agent:?}}}"
        )));
    }
}
