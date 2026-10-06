//
// Copyright 2025 Tabsdata Inc.
//

// Reference one edition symbol so the linker keeps te-apiserver's route
// submissions in the final link.
#[used]
static _TD_FORCE_EDITION_ROUTES: fn() = te_apiserver::link;

pub(crate) mod auth;
pub(crate) mod collections;
pub(crate) mod executions;
pub(crate) mod function_runs;
pub(crate) mod functions;
pub(crate) mod groups;
pub(crate) mod internal;
pub(crate) mod logical_versions;
pub(crate) mod metrics;
pub(crate) mod permissions;
pub(crate) mod ping;
pub(crate) mod projects;
pub(crate) mod resources;
pub(crate) mod roles;
pub(crate) mod server_status;
pub(crate) mod simple_permissions;
pub(crate) mod tables;
pub(crate) mod tokens;
pub(crate) mod transactions;
pub(crate) mod user_roles;
pub(crate) mod users;
pub(crate) mod workers;

#[cfg(test)]
mod tests {
    use crate::router::roles::LIST_ROLE_PATH;
    use crate::router::simple_permissions::LIST_BY_USER_PATH;
    use axum::body::Body;
    use axum::extract::Request;
    use axum::{Extension, Router};
    use http::{Method, StatusCode};
    use std::collections::HashSet;
    use ta_apiserver::router::{
        Auth, AuthClass, Position, RouterRegistration, RouterStates, Scope, SecurityMode,
        Server as Listener, Version, mount_router, registered, routes_accepting,
    };
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, UserId};
    use td_services::Services;
    use te_services::ExtendedServices;
    use tower::ServiceExt;
    use utoipa::openapi::path::ParameterIn;

    type Placement = (Listener, Option<Version>, Scope, &'static [SecurityMode]);

    const FLAT: Placement = (
        Listener::Public,
        Some(Version::V1),
        Scope::Flat,
        SecurityMode::ALL,
    );
    const PROJECT: Placement = (
        Listener::Public,
        Some(Version::V1),
        Scope::Project,
        SecurityMode::ALL,
    );
    const RBAC: Placement = (
        Listener::Public,
        Some(Version::V1),
        Scope::Flat,
        &[SecurityMode::Rbac],
    );
    const SIMPLE: Placement = (
        Listener::Public,
        Some(Version::V1),
        Scope::Flat,
        &[SecurityMode::Simple],
    );
    const INTERNAL: Placement = (
        Listener::Internal,
        Some(Version::V1),
        Scope::Flat,
        SecurityMode::ALL,
    );

    fn placement(r: &RouterRegistration) -> Placement {
        (r.server, r.version, r.scope, r.modes)
    }

    /// Every position the topology composes, in both modes.
    fn all_positions() -> Vec<Position> {
        let mut positions = Vec::new();
        for mode in SecurityMode::ALL.iter().copied() {
            for class in [AuthClass::Open, AuthClass::Internal, AuthClass::Bearer] {
                for (server, version, scope) in [
                    (Listener::Public, Some(Version::V1), Scope::Flat),
                    (Listener::Public, Some(Version::V1), Scope::Project),
                    (Listener::Public, None, Scope::Flat),
                    (Listener::Internal, Some(Version::V1), Scope::Flat),
                ] {
                    positions.push(Position {
                        server,
                        version,
                        scope,
                        mode,
                        class,
                    });
                }
            }
        }
        positions
    }

    /// The macro checks these rules at compile time; this covers hand-written
    /// registrations (ee's UI fallback).
    #[test]
    fn placements_are_valid() {
        for route in registered() {
            route.validate().unwrap();
        }
    }

    /// Every registered route is reachable from some composed position.
    #[test]
    fn every_route_is_composed() {
        let positions = all_positions();
        for route in registered() {
            assert!(
                positions.iter().any(|p| p.selects(route)),
                "{} is at a position the topology never mounts",
                route.name
            );
        }
    }

    /// A route absent from the OpenAPI doc still serves, so nothing else
    /// would notice it missing. Unversioned routes sit at the site root,
    /// outside the API.
    #[test]
    fn every_api_route_is_documented() {
        let documented: HashSet<String> = all_doc_paths().into_iter().collect();
        for route in registered()
            .into_iter()
            .filter(|route| route.version.is_some())
        {
            assert!(
                documented.contains(route.path),
                "{} ({}) is missing from the OpenAPI doc",
                route.name,
                route.path
            );
        }
    }

    fn all_doc_paths() -> Vec<String> {
        let states = RouterStates::default()
            .with(Services::default())
            .with(ExtendedServices::default());
        let mut paths: Vec<String> = all_positions()
            .into_iter()
            .flat_map(|p| {
                let (_, api) = mount_router(p, &states).split_for_parts();
                api.paths.paths.keys().cloned().collect::<Vec<_>>()
            })
            .collect();
        paths.sort();
        paths.dedup();
        paths
    }

    fn flat(mode: SecurityMode, states: &RouterStates) -> (Router, utoipa::openapi::OpenApi) {
        let position = Position {
            server: Listener::Public,
            version: Some(Version::V1),
            scope: Scope::Flat,
            mode,
            class: AuthClass::Bearer,
        };
        let (router, openapi) = mount_router(position, states).split_for_parts();
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

    /// The inactive mode's routes are absent (404, not 403) and out of the
    /// OpenAPI doc.
    #[td_test::test(db)]
    async fn test_each_mode_exposes_only_its_own_routes(db: TdPool) {
        let states = RouterStates::default()
            .with(Services::with_defaults(db))
            .with(ExtendedServices::default());

        let (router, openapi) = flat(SecurityMode::Simple, &states);
        assert!(!openapi.paths.paths.contains_key(LIST_ROLE_PATH));
        assert!(openapi.paths.paths.contains_key(LIST_BY_USER_PATH));
        assert_eq!(
            get_status(router.clone(), LIST_ROLE_PATH).await,
            StatusCode::NOT_FOUND
        );
        assert_ne!(
            get_status(router, LIST_BY_USER_PATH).await,
            StatusCode::NOT_FOUND
        );

        let (router, openapi) = flat(SecurityMode::Rbac, &states);
        assert!(openapi.paths.paths.contains_key(LIST_ROLE_PATH));
        assert!(!openapi.paths.paths.contains_key(LIST_BY_USER_PATH));
        assert_ne!(
            get_status(router.clone(), LIST_ROLE_PATH).await,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            get_status(router, LIST_BY_USER_PATH).await,
            StatusCode::NOT_FOUND
        );
    }

    /// Route names double as OpenAPI operation ids; a duplicate breaks
    /// generated clients. Rename the offending handler fn.
    #[test]
    fn route_names_are_unique() {
        let mut seen = HashSet::new();
        for r in registered() {
            assert!(seen.insert(r.name), "duplicate route name `{}`", r.name);
        }
    }

    /// Every route in this crate reaches the registry at its placement — the
    /// canary against linker dead-code elimination.
    #[test]
    fn expected_routes_registered() {
        let expected = [
            (FLAT, "cert_download"),
            (FLAT, "login"),
            (FLAT, "password_change"),
            (FLAT, "ping"),
            (FLAT, "cancel_execution"),
            (FLAT, "cancel_transaction"),
            (FLAT, "create_project"),
            (FLAT, "create_user"),
            (FLAT, "delete_project"),
            (FLAT, "delete_user"),
            (FLAT, "details"),
            (FLAT, "download_bundle"),
            (FLAT, "get_inflight_global"),
            (FLAT, "get_metrics_global"),
            (FLAT, "get_project"),
            (FLAT, "get_resource"),
            (FLAT, "get_snapshot_global"),
            (FLAT, "get_user"),
            (FLAT, "info"),
            (FLAT, "list_by_function_version"),
            (FLAT, "list_by_table_data_version"),
            (FLAT, "list_collections"),
            (FLAT, "list_function_runs"),
            (FLAT, "list_projects"),
            (FLAT, "list_transactions"),
            (FLAT, "list_users"),
            (FLAT, "list_workers"),
            (FLAT, "lists"),
            (FLAT, "logout"),
            (FLAT, "logs"),
            (FLAT, "logs_tail"),
            (FLAT, "read_execution"),
            (FLAT, "recover_execution"),
            (FLAT, "recover_transaction"),
            (FLAT, "refresh"),
            (FLAT, "replay"),
            (FLAT, "status"),
            (FLAT, "update_project"),
            (FLAT, "update_user"),
            (FLAT, "user_info"),
            (RBAC, "create_permission"),
            (RBAC, "create_role"),
            (RBAC, "create_user_role"),
            (RBAC, "delete_permission"),
            (RBAC, "delete_role"),
            (RBAC, "delete_user_role"),
            (RBAC, "list_permissions"),
            (RBAC, "list_role"),
            (RBAC, "list_user_roles"),
            (RBAC, "read_role"),
            (RBAC, "read_user_role"),
            (RBAC, "update_role"),
            (SIMPLE, "grant"),
            (SIMPLE, "list_by_project"),
            (SIMPLE, "list_by_user"),
            (SIMPLE, "revoke"),
            (PROJECT, "create_collection"),
            (PROJECT, "create_group"),
            (PROJECT, "delete_collection"),
            (PROJECT, "delete_function"),
            (PROJECT, "delete_group"),
            (PROJECT, "delete_table"),
            (PROJECT, "download"),
            (PROJECT, "enable"),
            (PROJECT, "execute"),
            (PROJECT, "get_collection"),
            (PROJECT, "get_collection_connection"),
            (PROJECT, "get_inflight"),
            (PROJECT, "get_metrics"),
            (PROJECT, "get_snapshot"),
            (PROJECT, "graph"),
            (PROJECT, "history"),
            (PROJECT, "list_data_versions"),
            (PROJECT, "list_functions"),
            (PROJECT, "list_functions_by_collection"),
            (PROJECT, "list_group"),
            (PROJECT, "list_patches"),
            (PROJECT, "list_tables"),
            (PROJECT, "list_tables_by_collection"),
            (PROJECT, "patch"),
            (PROJECT, "raw_data"),
            (PROJECT, "read_function"),
            (PROJECT, "read_group"),
            (PROJECT, "read_run"),
            (PROJECT, "register"),
            (PROJECT, "sample"),
            (PROJECT, "schema"),
            (PROJECT, "synchrotron"),
            (PROJECT, "update_collection"),
            (PROJECT, "update_function"),
            (PROJECT, "update_group"),
            (PROJECT, "upload"),
            (PROJECT, "upsert_collection_connection"),
            (FLAT, "create_service_token"),
            (FLAT, "list_service_tokens"),
            (FLAT, "read_own_service_token"),
            (FLAT, "revoke_service_token"),
            (FLAT, "list_user_service_tokens"),
            (FLAT, "revoke_user_service_token"),
            (INTERNAL, "download_state"),
            (INTERNAL, "function_run_callback"),
            (INTERNAL, "function_version_execute"),
            (INTERNAL, "function_version_execution_status"),
            (INTERNAL, "worker_callback"),
            (INTERNAL, "worker_message"),
        ];
        let actual: HashSet<_> = registered()
            .iter()
            .map(|r| (placement(r), r.name))
            .collect();
        for entry in expected {
            assert!(actual.contains(&entry), "missing registration: {entry:?}");
        }
    }

    /// Opening a route to tokens is a decision recorded here.
    #[test]
    fn service_token_routes_are_declared() {
        let expected: Vec<(&str, &str)> = vec![
            ("DELETE", "/projects/{project}/collections/{collection}"),
            (
                "DELETE",
                "/projects/{project}/collections/{collection}/functions/{function}",
            ),
            (
                "DELETE",
                "/projects/{project}/collections/{collection}/tables/{table}",
            ),
            ("GET", "/auth/info"),
            ("GET", "/auth/service-token"),
            ("GET", "/collections"),
            ("GET", "/executions"),
            ("GET", "/executions/{execution}"),
            ("GET", "/executions/{execution}/details"),
            ("GET", "/function-runs"),
            ("GET", "/function-versions/{function_version}/bundle"),
            ("GET", "/function-versions/{function_version}/executions"),
            ("GET", "/inflight"),
            ("GET", "/metrics"),
            ("GET", "/projects"),
            ("GET", "/projects/{project}"),
            ("GET", "/projects/{project}/collections/{collection}"),
            (
                "GET",
                "/projects/{project}/collections/{collection}/connection",
            ),
            (
                "GET",
                "/projects/{project}/collections/{collection}/function-runs/{function_run}/inputs/{input_idx}/raw-data",
            ),
            (
                "GET",
                "/projects/{project}/collections/{collection}/functions",
            ),
            (
                "GET",
                "/projects/{project}/collections/{collection}/functions/{function}",
            ),
            (
                "GET",
                "/projects/{project}/collections/{collection}/functions/{function}/executions/{execution}",
            ),
            (
                "GET",
                "/projects/{project}/collections/{collection}/functions/{function}/history",
            ),
            (
                "GET",
                "/projects/{project}/collections/{collection}/functions/{function}/patches",
            ),
            ("GET", "/projects/{project}/collections/{collection}/tables"),
            (
                "GET",
                "/projects/{project}/collections/{collection}/tables/{table}/data-versions",
            ),
            (
                "GET",
                "/projects/{project}/collections/{collection}/tables/{table}/download",
            ),
            (
                "GET",
                "/projects/{project}/collections/{collection}/tables/{table}/flow",
            ),
            (
                "GET",
                "/projects/{project}/collections/{collection}/tables/{table}/sample",
            ),
            (
                "GET",
                "/projects/{project}/collections/{collection}/tables/{table}/schema",
            ),
            ("GET", "/projects/{project}/functions"),
            ("GET", "/projects/{project}/groups"),
            ("GET", "/projects/{project}/groups/{group}"),
            ("GET", "/projects/{project}/inflight"),
            ("GET", "/projects/{project}/metrics"),
            ("GET", "/projects/{project}/snapshot"),
            ("GET", "/projects/{project}/synchrotron"),
            ("GET", "/projects/{project}/tables"),
            ("GET", "/runtime-info"),
            ("GET", "/snapshot"),
            (
                "GET",
                "/table-data-versions/{table_data_version}/executions",
            ),
            ("GET", "/transactions"),
            ("GET", "/workers"),
            ("GET", "/workers/{worker}/logs"),
            ("GET", "/workers/{worker}/logs/tail"),
            ("POST", "/executions/{execution}/cancel"),
            ("POST", "/executions/{execution}/recover"),
            (
                "POST",
                "/logical-function-runs/{logical_function_run}/replay",
            ),
            ("POST", "/projects/{project}/collections"),
            ("POST", "/projects/{project}/collections/{collection}"),
            (
                "POST",
                "/projects/{project}/collections/{collection}/function-bundle-upload",
            ),
            (
                "POST",
                "/projects/{project}/collections/{collection}/functions",
            ),
            (
                "POST",
                "/projects/{project}/collections/{collection}/functions/{function}",
            ),
            (
                "POST",
                "/projects/{project}/collections/{collection}/functions/{function}/enable",
            ),
            (
                "POST",
                "/projects/{project}/collections/{collection}/functions/{function}/execute",
            ),
            (
                "POST",
                "/projects/{project}/collections/{collection}/functions/{function}/patch",
            ),
            ("POST", "/transactions/{transaction}/cancel"),
            ("POST", "/transactions/{transaction}/recover"),
        ];
        let own: Vec<_> = routes_accepting(Auth::ServiceToken)
            .into_iter()
            .filter(|(_, path)| {
                !te_apiserver::EDITION_PATH_PREFIXES
                    .iter()
                    .any(|prefix| path.starts_with(prefix))
            })
            .collect();
        assert_eq!(own, expected);
    }

    /// A route's URL template and its documented parameters must agree: every
    /// `{segment}` in the path is a documented `Path` parameter and vice versa,
    /// and nothing a handler takes as `Query<_>` is documented as a path
    /// parameter. Nothing else enforces this — the URL is a literal on the
    /// handler and the params come from its extractors, so a renamed segment or
    /// a renamed struct field would otherwise surface as a runtime 400.
    #[test]
    fn path_params_match_url_placeholders() {
        let states = RouterStates::default()
            .with(Services::default())
            .with(ExtendedServices::default());
        let mut checked = 0;
        for position in all_positions() {
            let (_, api) = mount_router(position, &states).split_for_parts();
            for (path, item) in api.paths.paths.iter() {
                // `{*path}` is axum's greedy wildcard; the parameter it
                // documents is named without the star.
                let placeholders: HashSet<&str> = path
                    .split('/')
                    .filter_map(|seg| seg.strip_prefix('{')?.strip_suffix('}'))
                    .map(|name| name.trim_start_matches('*'))
                    .collect();
                let ops = [
                    ("get", &item.get),
                    ("put", &item.put),
                    ("post", &item.post),
                    ("delete", &item.delete),
                    ("patch", &item.patch),
                    ("head", &item.head),
                    ("options", &item.options),
                    ("trace", &item.trace),
                ];
                for (method, op) in ops
                    .into_iter()
                    .filter_map(|(m, o)| o.as_ref().map(|o| (m, o)))
                {
                    let params = op.parameters.clone().unwrap_or_default();
                    let documented: HashSet<&str> = params
                        .iter()
                        .filter(|p| matches!(p.parameter_in, ParameterIn::Path))
                        .map(|p| p.name.as_str())
                        .collect();
                    assert_eq!(
                        placeholders, documented,
                        "{method} {path}: URL placeholders and documented path parameters differ"
                    );
                    checked += 1;
                }
            }
        }
        assert!(
            checked > 90,
            "expected the full route surface, got {checked}"
        );
    }

    /// The assembled OpenAPI doc carries this crate's full route surface, and
    /// nothing undeclared: unknown paths may only come from the edition
    /// extension, under the prefixes it declares.
    #[test]
    fn doc_paths_complete() {
        let expected = [
            "/auth/info",
            "/auth/login",
            "/auth/logout",
            "/auth/password_change",
            "/auth/refresh",
            "/auth/service-token",
            "/auth/service-tokens",
            "/auth/service-tokens/{slot}",
            "/collections",
            "/executions",
            "/executions/{execution}",
            "/executions/{execution}/cancel",
            "/executions/{execution}/details",
            "/executions/{execution}/recover",
            "/function-runs",
            "/function-versions/{function_version}/bundle",
            "/function-versions/{function_version}/executions",
            "/inflight",
            "/internal/function-runs/{function_run}/workers/{worker}",
            "/internal/function-versions/{function_version}/download-state",
            "/internal/function-versions/{function_version}/execute",
            "/internal/function-versions/{function_version}/execution-status",
            "/internal/workers/{worker}",
            "/internal/workers/{worker}/message",
            "/logical-function-runs/{logical_function_run}/replay",
            "/metrics",
            "/ping",
            "/permission-grants",
            "/permission-grants/by-project",
            "/permission-grants/by-user",
            "/projects",
            "/projects/{project}",
            "/projects/{project}/collections",
            "/projects/{project}/collections/{collection}",
            "/projects/{project}/collections/{collection}/connection",
            "/projects/{project}/collections/{collection}/function-bundle-upload",
            "/projects/{project}/collections/{collection}/function-runs/{function_run}/inputs/{input_idx}/raw-data",
            "/projects/{project}/collections/{collection}/functions",
            "/projects/{project}/collections/{collection}/functions/{function}",
            "/projects/{project}/collections/{collection}/functions/{function}/enable",
            "/projects/{project}/collections/{collection}/functions/{function}/execute",
            "/projects/{project}/collections/{collection}/functions/{function}/executions/{execution}",
            "/projects/{project}/collections/{collection}/functions/{function}/history",
            "/projects/{project}/collections/{collection}/functions/{function}/patch",
            "/projects/{project}/collections/{collection}/functions/{function}/patches",
            "/projects/{project}/collections/{collection}/tables",
            "/projects/{project}/collections/{collection}/tables/{table}",
            "/projects/{project}/collections/{collection}/tables/{table}/data-versions",
            "/projects/{project}/collections/{collection}/tables/{table}/download",
            "/projects/{project}/collections/{collection}/tables/{table}/flow",
            "/projects/{project}/collections/{collection}/tables/{table}/sample",
            "/projects/{project}/collections/{collection}/tables/{table}/schema",
            "/projects/{project}/functions",
            "/projects/{project}/groups",
            "/projects/{project}/groups/{group}",
            "/projects/{project}/inflight",
            "/projects/{project}/metrics",
            "/projects/{project}/snapshot",
            "/projects/{project}/synchrotron",
            "/projects/{project}/tables",
            "/resources/{*path}",
            "/roles",
            "/roles/{role}",
            "/roles/{role}/permissions",
            "/roles/{role}/permissions/{permission}",
            "/roles/{role}/users",
            "/roles/{role}/users/{user}",
            "/runtime-info",
            "/snapshot",
            "/ssl-cert",
            "/status",
            "/table-data-versions/{table_data_version}/executions",
            "/transactions",
            "/transactions/{transaction}/cancel",
            "/transactions/{transaction}/recover",
            "/users",
            "/users/{user}",
            "/users/{user}/service-tokens",
            "/users/{user}/service-tokens/{slot}",
            "/workers",
            "/workers/{worker}/logs",
            "/workers/{worker}/logs/tail",
        ];
        let actual = all_doc_paths();
        let actual_set: HashSet<&str> = actual.iter().map(String::as_str).collect();
        for path in expected {
            assert!(actual_set.contains(path), "missing doc path: {path}");
        }
        let expected_set: HashSet<&str> = expected.into_iter().collect();
        for path in &actual {
            assert!(
                expected_set.contains(path.as_str())
                    || te_apiserver::EDITION_PATH_PREFIXES
                        .iter()
                        .any(|prefix| path.starts_with(prefix)),
                "undeclared doc path: {path}"
            );
        }
    }
}
