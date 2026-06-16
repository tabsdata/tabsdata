//
// Copyright 2026 Tabs Data Inc.
//

//! Security-mode endpoint selection.
//!
//! The apiserver exposes ONE authorization surface, chosen by the
//! `security_mode` config switch:
//!
//! - `simple`: the flat `/permission-grants` facade (tag `Permissions (Simple)`).
//! - `rbac`:  the raw RBAC management endpoints — role CRUD, role-permission and
//!   user-role membership (tags `Roles` / `Permissions` / `User Roles`).
//!
//! Only the active mode's subtree is mounted. The inactive mode's endpoints are
//! dropped entirely — absent from routing (a request 404s) AND from the
//! generated OpenAPI doc, since the doc is produced from the mounted routes. So
//! a `simple`-mode server shows only `Permissions (Simple)`, never the raw
//! `Roles` / `Permissions` / `User Roles` surface, and vice versa.
//!
//! Endpoints used by both views (users, projects, collections, functions,
//! tables, ...) are never mode-specific.

use crate::config::SecurityMode;
use utoipa_axum::router::OpenApiRouter;

/// Select the authorization-endpoint subtree for the running `mode`: the
/// `simple` facade in `simple` mode, the RBAC management endpoints in `rbac`
/// mode. The other mode's subtree is discarded (not mounted, not in the
/// OpenAPI doc).
pub fn mode_authz_router(
    mode: SecurityMode,
    rbac_management: OpenApiRouter,
    simple_facade: OpenApiRouter,
) -> OpenApiRouter {
    match mode {
        SecurityMode::Simple => simple_facade,
        SecurityMode::Rbac => rbac_management,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::permissions::PermissionsRouter;
    use crate::router::roles::RolesRouter;
    use crate::router::simple_permissions::SimplePermissionsRouter;
    use crate::router::user_roles::UserRolesRouter;
    use axum::Extension;
    use axum::Router;
    use axum::body::Body;
    use axum::extract::Request;
    use http::{Method, StatusCode};
    use ta_apiserver::router::RouterExtension;
    use ta_services::factory::ServiceFactory;
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::rest_urls::{LIST_PERMISSION_GRANTS_BY_USER, LIST_ROLES};
    use td_objects::types::basic::{AccessTokenId, UserId};
    use td_services::{Context, Services};
    use tower::ServiceExt;

    /// Build the mode-selected authz subtree exactly as `v1.rs` composes it,
    /// returning the axum `Router` (with a `RequestContext` extension so a
    /// mounted route can reach its handler) and the OpenAPI doc derived from
    /// the mounted routes.
    fn build(db: TdPool, mode: SecurityMode) -> (Router, utoipa::openapi::OpenApi) {
        let services = Services::build(&Context::with_defaults(db));
        let rbac_management = OpenApiRouter::new()
            .merge(PermissionsRouter::router(services.clone()))
            .merge(RolesRouter::router(services.clone()))
            .merge(UserRolesRouter::router(services.clone()));
        let simple_facade =
            OpenApiRouter::new().merge(SimplePermissionsRouter::router(services.clone()));
        let (router, openapi) =
            mode_authz_router(mode, rbac_management, simple_facade).split_for_parts();
        let context = RequestContext::with(AccessTokenId::default(), UserId::admin());
        (router.layer(Extension(context)), openapi)
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

    /// `simple` mode mounts ONLY the `/permission-grants` facade. The RBAC
    /// management endpoints are absent (404, not 403) and absent from the
    /// OpenAPI doc — so the server surfaces only `Permissions (Simple)`.
    #[td_test::test(db)]
    async fn test_simple_mode_exposes_only_simple_facade(db: TdPool) {
        let (router, openapi) = build(db, SecurityMode::Simple);

        // OpenAPI doc: RBAC paths absent, facade present.
        assert!(
            !openapi.paths.paths.contains_key(LIST_ROLES),
            "RBAC role endpoint must not appear in the OpenAPI doc in simple mode"
        );
        assert!(
            openapi.paths.paths.contains_key(LIST_PERMISSION_GRANTS_BY_USER),
            "the simple-permission facade must appear in the OpenAPI doc in simple mode"
        );

        // Routing: RBAC -> 404 (not mounted), facade -> mounted.
        assert_eq!(
            get_status(router.clone(), LIST_ROLES).await,
            StatusCode::NOT_FOUND,
            "RBAC role endpoint must not be mounted in simple mode"
        );
        assert_ne!(
            get_status(router, LIST_PERMISSION_GRANTS_BY_USER).await,
            StatusCode::NOT_FOUND,
            "the simple-permission facade must be mounted in simple mode"
        );
    }

    /// `rbac` mode mounts ONLY the RBAC management endpoints; the simple facade
    /// is absent (404) and absent from the OpenAPI doc.
    #[td_test::test(db)]
    async fn test_rbac_mode_exposes_only_rbac_management(db: TdPool) {
        let (router, openapi) = build(db, SecurityMode::Rbac);

        assert!(
            openapi.paths.paths.contains_key(LIST_ROLES),
            "RBAC role endpoint must appear in the OpenAPI doc in rbac mode"
        );
        assert!(
            !openapi
                .paths
                .paths
                .contains_key(LIST_PERMISSION_GRANTS_BY_USER),
            "the simple-permission facade must not appear in the OpenAPI doc in rbac mode"
        );

        assert_ne!(
            get_status(router.clone(), LIST_ROLES).await,
            StatusCode::NOT_FOUND,
            "RBAC role endpoint must be mounted in rbac mode"
        );
        assert_eq!(
            get_status(router, LIST_PERMISSION_GRANTS_BY_USER).await,
            StatusCode::NOT_FOUND,
            "the simple-permission facade must not be mounted in rbac mode"
        );
    }
}
