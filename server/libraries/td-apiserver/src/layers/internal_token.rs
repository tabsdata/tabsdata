//
// Copyright 2026 Tabs Data Inc.
//

//! Token gate for the internal API router (replaces the loopback-IP filter).
//!
//! Every internal `HttpRequest` the scheduler embeds in a worker message
//! carries an `Authorization: Bearer` internal-endpoint token bound to its
//! `(method, scope, bindings)`. This layer verifies the token's signature and
//! validates it against the actual request, then injects the system
//! `RequestContext` the internal handlers expect.

use axum::extract::{Request, State};
use axum::http;
use axum::middleware::Next;
use axum::response::Response;
use std::sync::Arc;
use ta_apiserver::status::error_status::ErrorStatus;
use td_authz::jwt::{InternalTokenClaims, JwtConfig, decode_internal_token};
use td_error::{TdError, td_error};
use td_objects::dxo::crudl::RequestContext;
use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
use tracing::{Instrument, Level, span, warn};

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
    State(jwt): State<Arc<JwtConfig>>,
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

    // Validate against the actual request.
    if !token_authorizes_request(&claims, request.method().as_str(), request.uri().path()) {
        warn!(
            scope = %claims.scope,
            method = %claims.method,
            request_path = %request.uri().path(),
            "internal token does not authorize this request"
        );
        return Err(TdError::from(InternalTokenError::Mismatch).into());
    }

    // Same system identity the loopback filter injected, so handler behavior
    // is unchanged.
    request.extensions_mut().insert(RequestContext::with(
        AccessTokenId::default(),
        UserId::system(),
        RoleId::sys_admin(),
    ));

    let log_span = span!(Level::INFO, "internal_token_request", scope = %claims.scope);
    Ok(next.run(request).instrument(log_span).await)
}

/// True iff the token authorizes exactly this request: same method, and the
/// authorized concrete path (the token's `scope` pattern with its `bindings`
/// substituted) equals the request path. A `scope` placeholder with no
/// matching binding leaves a literal `{name}` that can't match a real path,
/// so under-binding fails closed.
fn token_authorizes_request(claims: &InternalTokenClaims, method: &str, path: &str) -> bool {
    if claims.method != method {
        return false;
    }
    let mut expected_path = claims.scope.clone();
    for (name, value) in &claims.bindings {
        expected_path = expected_path.replace(&format!("{{{name}}}"), value);
    }
    expected_path == path
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use td_authz::jwt::INTERNAL_TOKEN_KIND;
    use td_common::id;

    fn claims(method: &str, scope: &str, bindings: &[(&str, &str)]) -> InternalTokenClaims {
        InternalTokenClaims {
            jti: id::id(),
            kind: INTERNAL_TOKEN_KIND.to_string(),
            method: method.to_string(),
            scope: scope.to_string(),
            bindings: bindings
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>(),
        }
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
            "/api/v1/internal/function-runs/F/workers/W"
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
            "/api/v1/internal/function-runs/F/workers/W"
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
            "/api/v1/internal/function-runs/F/workers/W2"
        ));
    }

    #[test]
    fn rejects_under_binding() {
        // Missing worker_id binding leaves a literal placeholder → no match.
        let c = claims("POST", SCOPE, &[("function_run_id", "F")]);
        assert!(!token_authorizes_request(
            &c,
            "POST",
            "/api/v1/internal/function-runs/F/workers/W"
        ));
    }

    #[test]
    fn rejects_different_endpoint_same_binding() {
        // A status token (GET) must not authorize download-state (GET) even
        // though both bind only function_version_id.
        let c = claims(
            "GET",
            "/api/v1/internal/function-versions/{function_version_id}/execution-status",
            &[("function_version_id", "FV")],
        );
        assert!(!token_authorizes_request(
            &c,
            "GET",
            "/api/v1/internal/function-versions/FV/download-state"
        ));
    }
}
