//
// Copyright 2026 Tabsdata Inc.
//

//! A request naming another tabsdata version than the server's in
//! [`VERSION_HEADER`] answers 426. Requests without the header pass, and so
//! does every request while the server does not know its own version. A
//! compatibility guard, not a security control.

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http::{HeaderName, HeaderValue, StatusCode};
use ta_apiserver::status::error_status::ErrorStatus;
use ta_services::service::Ctx;
use td_error::api_error;
use td_services::execution::services::runtime_info::{RuntimeContext, UNKNOWN_VERSION};

pub const VERSION_HEADER: HeaderName = HeaderName::from_static("x-td-version");

pub async fn client_version_layer(
    State(runtime): State<Ctx<RuntimeContext>>,
    request: Request,
    next: Next,
) -> Response {
    let server = runtime.info().version.to_string();
    let known = !server.is_empty() && server != UNKNOWN_VERSION;
    let mut response = match request.headers().get(&VERSION_HEADER) {
        Some(client) if known && client.as_bytes() != server.as_bytes() => {
            let client = String::from_utf8_lossy(client.as_bytes());
            ErrorStatus::from(api_error!(
                status = StatusCode::UPGRADE_REQUIRED,
                "The server runs tabsdata {server} but the client runs {client}; \
                 install tabsdata {server} to use this server"
            ))
            .into_response()
        }
        _ => next.run(request).await,
    };
    if let Ok(value) = HeaderValue::from_str(&server) {
        response.headers_mut().insert(VERSION_HEADER, value);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::{VERSION_HEADER, client_version_layer};
    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::extract::Request;
    use axum::middleware::from_fn_with_state;
    use axum::routing::get;
    use http::StatusCode;
    use ta_services::service::Ctx;
    use td_services::execution::services::runtime_info::RuntimeContext;
    use td_services::execution::services::runtime_info::UNKNOWN_VERSION;
    use tower::ServiceExt;

    const SERVER: &str = "2.3.0";

    fn router(server: &str) -> Router {
        let runtime = Ctx::new(RuntimeContext::with_version(server));
        Router::new()
            .route("/", get(|| async { "ok" }))
            .layer(from_fn_with_state(runtime, client_version_layer))
    }

    async fn call(client: Option<&str>) -> (StatusCode, Option<String>, String) {
        call_server(SERVER, client).await
    }

    async fn call_server(
        server: &str,
        client: Option<&str>,
    ) -> (StatusCode, Option<String>, String) {
        let mut request = Request::get("/");
        if let Some(client) = client {
            request = request.header(VERSION_HEADER, client);
        }
        let response = router(server)
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let header = response
            .headers()
            .get(VERSION_HEADER)
            .map(|value| value.to_str().unwrap().to_string());
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, header, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn the_same_version_passes() {
        let (status, header, body) = call(Some(SERVER)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(header.as_deref(), Some(SERVER));
        assert_eq!(body, "ok");
    }

    #[tokio::test]
    async fn no_version_passes() {
        let (status, header, _) = call(None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(header.as_deref(), Some(SERVER));
    }

    #[tokio::test]
    async fn another_version_is_refused_with_both_versions() {
        let (status, header, body) = call(Some("2.2.0")).await;
        assert_eq!(status, StatusCode::UPGRADE_REQUIRED);
        assert_eq!(header.as_deref(), Some(SERVER));
        assert!(body.contains("2.2.0"), "{body}");
        assert!(body.contains(SERVER), "{body}");
    }

    #[tokio::test]
    async fn a_server_not_knowing_its_version_refuses_nobody() {
        let (status, _, _) = call_server(UNKNOWN_VERSION, Some("2.2.0")).await;
        assert_eq!(status, StatusCode::OK);
    }
}
