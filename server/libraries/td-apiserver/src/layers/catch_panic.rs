//
// Copyright 2026 Tabsdata Inc.
//

use axum::response::{IntoResponse, Response};
use std::any::Any;
use ta_apiserver::status::error_status::ErrorStatus;
use td_error::{ApiError, api_error};
use tower_http::catch_panic::CatchPanicLayer;
use tracing::error;

type Answer = fn(Box<dyn Any + Send + 'static>) -> Response;

/// A handler that panics answers 500 like any other internal error, instead of
/// the connection being dropped.
pub fn layer() -> CatchPanicLayer<Answer> {
    CatchPanicLayer::custom(answer as Answer)
}

fn answer(panic: Box<dyn Any + Send + 'static>) -> Response {
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("a non-string panic payload");
    error!(panic = message, "http.panic: the handler panicked");
    ErrorStatus::from(api_error!(ApiError::InternalError, "The handler panicked")).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::routing::get;
    use http::{Request, StatusCode};
    use tower::ServiceExt;

    async fn panicking() -> &'static str {
        panic!("boom")
    }

    #[tokio::test]
    async fn test_a_panicking_handler_answers_500() {
        let router = Router::new().route("/", get(panicking)).layer(layer());
        let response = router
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
