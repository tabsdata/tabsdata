//
// Copyright 2026 Tabs Data Inc.
//

//! Security-mode endpoint gating.
//!
//! The apiserver runs in one of two views, selected by the `security_mode`
//! config switch:
//!
//! - `simple`: the flat `/permission-grants` facade is available; the raw RBAC
//!   role / role-permission / user-role *management* endpoints are blocked.
//! - `rbac`:  the RBAC management endpoints are available; the `/permission-grants`
//!   facade is blocked.
//!
//! Endpoints used by both views (users, projects, collections, functions,
//! tables, ...) are never gated.
//!
//! Each gate is a middleware wrapping the mode-specific router subtree, so a
//! request to a blocked endpoint short-circuits with a clear `403` ("not
//! available in <mode> mode; use <other> mode") instead of a silent `404`.

use crate::config::SecurityMode;
use axum::extract::Request;
use axum::middleware::{Next, from_fn};
use axum::response::Response;
use ta_apiserver::status::error_status::ErrorStatus;
use td_error::{ApiError, api_error};
use utoipa_axum::router::OpenApiRouter;

/// Reject the request with `403` when `running` is not the mode the wrapped
/// router requires. `required` is the mode under which the wrapped endpoints
/// are available.
async fn gate(
    running: SecurityMode,
    required: SecurityMode,
    request: Request,
    next: Next,
) -> Result<Response, ErrorStatus> {
    if running == required {
        Ok(next.run(request).await)
    } else {
        let (this, other) = match required {
            SecurityMode::Rbac => ("simple", "rbac"),
            SecurityMode::Simple => ("rbac", "simple"),
        };
        Err(api_error!(
            ApiError::Forbidden,
            "This endpoint is not available in '{this}' security mode; \
             switch the apiserver to '{other}' security mode to use it"
        )
        .into())
    }
}

/// Build a router carrying `routers`, gated so it is reachable only when the
/// running `mode` equals `required`. In the wrong mode every request to these
/// paths returns `403` with a clear message (rather than a silent `404`).
pub fn gated_router(
    mode: SecurityMode,
    required: SecurityMode,
    router: OpenApiRouter,
) -> OpenApiRouter {
    router.layer(from_fn(move |request: Request, next: Next| {
        gate(mode, required, request, next)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use axum::Router;
    use http::{Method, StatusCode};
    use tower::ServiceExt;
    use utoipa_axum::router::OpenApiRouter;

    /// A trivial inner router exposing `GET /probe -> 200`, gated to `required`.
    fn probe_router(mode: SecurityMode, required: SecurityMode) -> Router {
        let inner = OpenApiRouter::new().route("/probe", get(|| async { StatusCode::OK }));
        let (router, _) = gated_router(mode, required, inner).split_for_parts();
        router
    }

    async fn probe(router: Router) -> StatusCode {
        router
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/probe")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    /// RBAC-management gate: blocked (403) in simple mode, allowed in rbac mode.
    #[tokio::test]
    async fn test_rbac_gate_blocks_in_simple_mode() {
        // simple mode -> the rbac-only subtree is blocked.
        assert_eq!(
            probe(probe_router(SecurityMode::Simple, SecurityMode::Rbac)).await,
            StatusCode::FORBIDDEN
        );
        // rbac mode -> the rbac-only subtree passes through to the handler.
        assert_eq!(
            probe(probe_router(SecurityMode::Rbac, SecurityMode::Rbac)).await,
            StatusCode::OK
        );
    }

    /// Simple facade gate: blocked (403) in rbac mode, allowed in simple mode.
    #[tokio::test]
    async fn test_simple_gate_blocks_in_rbac_mode() {
        // rbac mode -> the simple facade subtree is blocked.
        assert_eq!(
            probe(probe_router(SecurityMode::Rbac, SecurityMode::Simple)).await,
            StatusCode::FORBIDDEN
        );
        // simple mode -> the simple facade subtree passes through to the handler.
        assert_eq!(
            probe(probe_router(SecurityMode::Simple, SecurityMode::Simple)).await,
            StatusCode::OK
        );
    }

    async fn get_status(router: Router, uri: &str) -> StatusCode {
        router
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    /// A route in an *ungated* sibling subtree is unaffected by the gate, even
    /// in the mode where the gated subtree is blocked. Mirrors v1.rs, where the
    /// gated router is `.layer()`-ed before being `.merge()`-d alongside the
    /// always-on routers — so the gate only wraps its own subtree's routes.
    #[tokio::test]
    async fn test_gate_does_not_touch_sibling_routes() {
        // Gated (rbac-only) subtree carrying /rbac, running in simple mode.
        let gated = gated_router(
            SecurityMode::Simple,
            SecurityMode::Rbac,
            OpenApiRouter::new().route("/rbac", get(|| async { StatusCode::OK })),
        );
        // Always-on sibling carrying /open.
        let open = OpenApiRouter::new().route("/open", get(|| async { StatusCode::OK }));
        let (router, _) = open.merge(gated).split_for_parts();

        // Sibling route is reachable (gate does not wrap it).
        assert_eq!(get_status(router.clone(), "/open").await, StatusCode::OK);
        // Gated route is blocked in the wrong mode.
        assert_eq!(get_status(router, "/rbac").await, StatusCode::FORBIDDEN);
    }

    // -- Real-router gating, exactly as composed in `v1.rs`. ----------------

    mod real_routers {
        use super::*;
        use crate::router::permissions::PermissionsRouter;
        use crate::router::roles::RolesRouter;
        use crate::router::simple_permissions::SimplePermissionsRouter;
        use crate::router::user_roles::UserRolesRouter;
        use axum::Extension;
        use ta_apiserver::router::RouterExtension;
        use ta_services::factory::ServiceFactory;
        use td_database::pool::TdPool;
        use td_objects::dxo::crudl::RequestContext;
        use td_objects::rest_urls::{LIST_PERMISSION_GRANTS_BY_USER, LIST_ROLES};
        use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
        use td_services::{Context, Services};

        /// Build the two gated subtrees the way `v1.rs` does and merge them,
        /// returning an axum `Router` with a `RequestContext` extension (so a
        /// request that passes the gate can reach the handler).
        fn build(db: TdPool, mode: SecurityMode) -> Router {
            let services = Services::build(&Context::with_defaults(db));
            let rbac_management = gated_router(
                mode,
                SecurityMode::Rbac,
                OpenApiRouter::new()
                    .merge(PermissionsRouter::router(services.clone()))
                    .merge(RolesRouter::router(services.clone()))
                    .merge(UserRolesRouter::router(services.clone())),
            );
            let simple_facade = gated_router(
                mode,
                SecurityMode::Simple,
                OpenApiRouter::new()
                    .merge(SimplePermissionsRouter::router(services.clone())),
            );
            let (router, _) = OpenApiRouter::new()
                .merge(rbac_management)
                .merge(simple_facade)
                .split_for_parts();
            let context = RequestContext::with(
                AccessTokenId::default(),
                UserId::admin()
            );
            router.layer(Extension(context))
        }

        /// In `simple` mode the RBAC role-listing endpoint is blocked (403) and
        /// the `/permission-grants` facade is reachable (not 403, not 404).
        #[td_test::test(db)]
        async fn test_simple_mode_blocks_rbac_allows_facade(db: TdPool) {
            let router = build(db, SecurityMode::Simple);

            let roles = get_status(router.clone(), LIST_ROLES).await;
            assert_eq!(roles, StatusCode::FORBIDDEN, "roles must be blocked in simple mode");

            let facade = get_status(router, LIST_PERMISSION_GRANTS_BY_USER).await;
            assert_ne!(facade, StatusCode::FORBIDDEN, "facade must not be gate-blocked in simple mode");
            assert_ne!(facade, StatusCode::NOT_FOUND, "facade route must be mounted in simple mode");
        }

        /// In `rbac` mode the RBAC role-listing endpoint is reachable (not 403,
        /// not 404) and the `/permission-grants` facade is blocked (403).
        #[td_test::test(db)]
        async fn test_rbac_mode_allows_rbac_blocks_facade(db: TdPool) {
            let router = build(db, SecurityMode::Rbac);

            let roles = get_status(router.clone(), LIST_ROLES).await;
            assert_ne!(roles, StatusCode::FORBIDDEN, "roles must not be gate-blocked in rbac mode");
            assert_ne!(roles, StatusCode::NOT_FOUND, "roles route must be mounted in rbac mode");

            let facade = get_status(router, LIST_PERMISSION_GRANTS_BY_USER).await;
            assert_eq!(facade, StatusCode::FORBIDDEN, "facade must be blocked in rbac mode");
        }
    }
}
