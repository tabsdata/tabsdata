//
// Copyright 2026 Tabsdata Inc.
//

//! Token gate for the internal API router (replaces the loopback-IP filter).
//!
//! Every internal `HttpRequest` the scheduler embeds in a worker message
//! carries an `Authorization: Bearer` internal-endpoint token bound to its
//! `(method, scope, bindings)`. This layer verifies the token's signature and
//! validates it against the actual request, then injects the internal
//! `RequestContext` (system user, internal-token provenance — the provenance
//! is what grants the RBAC bypass).

use axum::RequestExt;
use axum::extract::{MatchedPath, RawPathParams, Request, State};
use axum::http;
use axum::middleware::Next;
use axum::response::Response;
use std::collections::BTreeMap;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_services::service::Ctx;
use td_common::jwt::{InternalTokenClaims, JwtConfig, decode_internal_token};
use td_error::{TdError, td_error};
use td_objects::dxo::crudl::RequestContext;
use tracing::{Instrument, Level, error, span, warn};

#[td_error]
enum InternalTokenError {
    #[error("Missing internal token")]
    Missing = 4010,
    #[error("Invalid internal token: {0}")]
    Invalid(String) = 4011,
    #[error("Internal token does not authorize this request")]
    Mismatch = 4012,
}

pub async fn internal_token_layer(
    State(jwt): State<Ctx<JwtConfig>>,
    mut request: Request,
    next: Next,
) -> Result<Response, ErrorStatus> {
    let token = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| TdError::from(InternalTokenError::Missing))?
        .to_string();

    let claims = decode_internal_token(&jwt, &token).map_err(|e| {
        warn!("internal token decode failed: {e:#?}");
        TdError::from(InternalTokenError::Invalid(e.to_string()))
    })?;

    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string());
    let params = request
        .extract_parts::<RawPathParams>()
        .await
        .ok()
        .map(|p| {
            p.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>()
        });

    if !token_authorizes_request(
        &claims,
        request.method().as_str(),
        path.as_deref(),
        params.as_ref(),
    ) {
        error!(
            scope = %claims.scope,
            method = %claims.method,
            ?path,
            "internal token does not authorize this request"
        );
        return Err(TdError::from(InternalTokenError::Mismatch).into());
    }

    // The internal provenance is what grants the RBAC bypass — identity
    // alone (the system user) does not.
    request.extensions_mut().insert(RequestContext::internal());

    let log_span = span!(Level::INFO, "internal_token_request", scope = %claims.scope);
    Ok(next.run(request).instrument(log_span).await)
}

/// True iff the token authorizes exactly this request: same `method`, the
/// matched route template equals the token's `scope`, and the request's decoded
/// path params equal the token's `bindings` exactly (as a set).
fn token_authorizes_request(
    claims: &InternalTokenClaims,
    method: &str,
    matched: Option<&str>,
    params: Option<&BTreeMap<String, String>>,
) -> bool {
    claims.method == method
        && matched == Some(claims.scope.as_str())
        && params == Some(&claims.bindings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apiserver::{BASE_API_URL, BASE_URL, V1};
    use crate::router::internal::FUNCTION_RUN_CALLBACK_PATH;
    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use axum::http::StatusCode;
    use axum::middleware::from_fn_with_state;
    use axum::routing::get;
    use std::collections::BTreeMap;
    use td_common::id;
    use td_common::jwt::{INTERNAL_TOKEN_KIND, encode_internal_token};
    use tower::ServiceExt;

    fn claims(method: &str, scope: &str, bindings: &[(&str, &str)]) -> InternalTokenClaims {
        InternalTokenClaims {
            jti: id::id(),
            kind: INTERNAL_TOKEN_KIND.to_string(),
            method: method.to_string(),
            scope: scope.to_string(),
            bindings: params(bindings),
        }
    }

    fn params(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    const SCOPE: &str = "/api/v1/internal/function-runs/{function_run_id}/workers/{worker_id}";

    #[test]
    fn authorizes_matching_request() {
        let c = claims(
            "POST",
            SCOPE,
            &[("function_run_id", "F"), ("worker_id", "W")],
        );
        assert!(token_authorizes_request(
            &c,
            "POST",
            Some(SCOPE),
            Some(&params(&[("function_run_id", "F"), ("worker_id", "W")])),
        ));
    }

    #[test]
    fn rejects_wrong_method() {
        let c = claims(
            "POST",
            SCOPE,
            &[("function_run_id", "F"), ("worker_id", "W")],
        );
        assert!(!token_authorizes_request(
            &c,
            "GET",
            Some(SCOPE),
            Some(&params(&[("function_run_id", "F"), ("worker_id", "W")])),
        ));
    }

    #[test]
    fn rejects_wrong_binding() {
        // A token for worker W must not authorize worker W2.
        let c = claims(
            "POST",
            SCOPE,
            &[("function_run_id", "F"), ("worker_id", "W")],
        );
        assert!(!token_authorizes_request(
            &c,
            "POST",
            Some(SCOPE),
            Some(&params(&[("function_run_id", "F"), ("worker_id", "W2")])),
        ));
    }

    #[test]
    fn rejects_extra_binding() {
        // A token bound only to function_run_id must not authorize a request
        // whose route also binds worker_id (param sets differ).
        let c = claims("POST", SCOPE, &[("function_run_id", "F")]);
        assert!(!token_authorizes_request(
            &c,
            "POST",
            Some(SCOPE),
            Some(&params(&[("function_run_id", "F"), ("worker_id", "W")])),
        ));
    }

    #[test]
    fn rejects_different_endpoint_same_binding() {
        // A status token (GET) must not authorize download-state (GET) even
        // though both bind only function_version_id: the matched template differs.
        let c = claims(
            "GET",
            "/api/v1/internal/function-versions/{function_version_id}/execution-status",
            &[("function_version_id", "FV")],
        );
        assert!(!token_authorizes_request(
            &c,
            "GET",
            Some("/api/v1/internal/function-versions/{function_version_id}/download-state"),
            Some(&params(&[("function_version_id", "FV")])),
        ));
    }

    #[test]
    fn rejects_missing_match_or_params() {
        // Fail closed if the router gave us no matched path or no path params.
        let c = claims(
            "GET",
            SCOPE,
            &[("function_run_id", "F"), ("worker_id", "W")],
        );
        let p = params(&[("function_run_id", "F"), ("worker_id", "W")]);
        assert!(!token_authorizes_request(&c, "GET", None, Some(&p)));
        assert!(!token_authorizes_request(&c, "GET", Some(SCOPE), None));
    }

    #[tokio::test]
    async fn nested_router_authorizes_matching_token() {
        let jwt = Ctx::new(JwtConfig::new("test-secret".to_string(), 3600));

        // Mint the token the same way the scheduler does for this endpoint.
        let scope = format!("{BASE_URL}{FUNCTION_RUN_CALLBACK_PATH}");
        let bindings = BTreeMap::from([
            ("function_run_id".to_string(), "F".to_string()),
            ("worker_id".to_string(), "W".to_string()),
        ]);
        let token = encode_internal_token(&jwt, "GET", &scope, bindings).unwrap();

        let inner = Router::new()
            .route(FUNCTION_RUN_CALLBACK_PATH, get(|| async { StatusCode::OK }))
            .route_layer(from_fn_with_state(jwt.clone(), internal_token_layer));
        let app = Router::new().nest(BASE_API_URL, Router::new().nest(V1, inner));

        let request = Request::builder()
            .method("GET")
            .uri("/api/v1/internal/function-runs/F/workers/W")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a correctly minted token for the endpoint must authorize the request"
        );
    }

    #[tokio::test]
    async fn nested_router_rejects_wrong_worker() {
        let jwt = Ctx::new(JwtConfig::new("test-secret".to_string(), 3600));

        let scope = format!("{BASE_URL}{FUNCTION_RUN_CALLBACK_PATH}");
        let bindings = BTreeMap::from([
            ("function_run_id".to_string(), "F".to_string()),
            ("worker_id".to_string(), "W".to_string()),
        ]);
        let token = encode_internal_token(&jwt, "GET", &scope, bindings).unwrap();

        let inner = Router::new()
            .route(FUNCTION_RUN_CALLBACK_PATH, get(|| async { StatusCode::OK }))
            .route_layer(from_fn_with_state(jwt.clone(), internal_token_layer));
        let app = Router::new().nest(BASE_API_URL, Router::new().nest(V1, inner));

        let request = Request::builder()
            .method("GET")
            .uri("/api/v1/internal/function-runs/F/workers/W2")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
