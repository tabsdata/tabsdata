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
pub(crate) mod transactions;
pub(crate) mod user_roles;
pub(crate) mod users;
pub(crate) mod workers;

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use ta_apiserver::router::{Mount, RouterStates, mount_router, registered};
    use td_services::Services;
    use te_services::ExtendedServices;
    use utoipa::openapi::path::ParameterIn;

    fn all_doc_paths() -> Vec<String> {
        let states = RouterStates::default()
            .with(Services::default())
            .with(ExtendedServices::default());
        let mut paths: Vec<String> = [
            Mount::Unauthenticated,
            Mount::Global,
            Mount::Rbac,
            Mount::Simple,
            Mount::Project,
            Mount::Internal,
            Mount::Root,
        ]
        .into_iter()
        .flat_map(|m| {
            let (_, api) = mount_router(m, &states).split_for_parts();
            api.paths.paths.keys().cloned().collect::<Vec<_>>()
        })
        .collect();
        paths.sort();
        paths
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

    /// Every route in this crate reaches the registry with its mount — the
    /// canary against linker dead-code elimination.
    #[test]
    fn expected_routes_registered() {
        let expected = [
            (Mount::Unauthenticated, "cert_download"),
            (Mount::Unauthenticated, "login"),
            (Mount::Unauthenticated, "password_change"),
            (Mount::Unauthenticated, "ping"),
            (Mount::Global, "cancel_execution"),
            (Mount::Global, "cancel_transaction"),
            (Mount::Global, "create_project"),
            (Mount::Global, "create_user"),
            (Mount::Global, "delete_project"),
            (Mount::Global, "delete_user"),
            (Mount::Global, "details"),
            (Mount::Global, "download_bundle"),
            (Mount::Global, "get_inflight_global"),
            (Mount::Global, "get_metrics_global"),
            (Mount::Global, "get_project"),
            (Mount::Global, "get_resource"),
            (Mount::Global, "get_snapshot_global"),
            (Mount::Global, "get_user"),
            (Mount::Global, "info"),
            (Mount::Global, "list_by_function_version"),
            (Mount::Global, "list_by_physical_table_data_version"),
            (Mount::Global, "list_collections"),
            (Mount::Global, "list_function_runs"),
            (Mount::Global, "list_projects"),
            (Mount::Global, "list_transactions"),
            (Mount::Global, "list_users"),
            (Mount::Global, "list_workers"),
            (Mount::Global, "lists"),
            (Mount::Global, "logout"),
            (Mount::Global, "logs"),
            (Mount::Global, "logs_tail"),
            (Mount::Global, "read_execution"),
            (Mount::Global, "recover_execution"),
            (Mount::Global, "recover_transaction"),
            (Mount::Global, "refresh"),
            (Mount::Global, "replay"),
            (Mount::Global, "status"),
            (Mount::Global, "update_project"),
            (Mount::Global, "update_user"),
            (Mount::Global, "user_info"),
            (Mount::Rbac, "create_permission"),
            (Mount::Rbac, "create_role"),
            (Mount::Rbac, "create_user_role"),
            (Mount::Rbac, "delete_permission"),
            (Mount::Rbac, "delete_role"),
            (Mount::Rbac, "delete_user_role"),
            (Mount::Rbac, "list_permissions"),
            (Mount::Rbac, "list_role"),
            (Mount::Rbac, "list_user_roles"),
            (Mount::Rbac, "read_role"),
            (Mount::Rbac, "read_user_role"),
            (Mount::Rbac, "update_role"),
            (Mount::Simple, "grant"),
            (Mount::Simple, "list_by_project"),
            (Mount::Simple, "list_by_user"),
            (Mount::Simple, "revoke"),
            (Mount::Project, "create_collection"),
            (Mount::Project, "create_group"),
            (Mount::Project, "delete_collection"),
            (Mount::Project, "delete_function"),
            (Mount::Project, "delete_group"),
            (Mount::Project, "delete_table"),
            (Mount::Project, "download"),
            (Mount::Project, "enable"),
            (Mount::Project, "execute"),
            (Mount::Project, "get_collection"),
            (Mount::Project, "get_collection_connection"),
            (Mount::Project, "get_inflight"),
            (Mount::Project, "get_metrics"),
            (Mount::Project, "get_snapshot"),
            (Mount::Project, "graph"),
            (Mount::Project, "history"),
            (Mount::Project, "list_data_versions"),
            (Mount::Project, "list_functions"),
            (Mount::Project, "list_functions_by_collection"),
            (Mount::Project, "list_group"),
            (Mount::Project, "list_patches"),
            (Mount::Project, "list_tables"),
            (Mount::Project, "list_tables_by_collection"),
            (Mount::Project, "patch"),
            (Mount::Project, "read_function"),
            (Mount::Project, "read_group"),
            (Mount::Project, "read_run"),
            (Mount::Project, "register"),
            (Mount::Project, "sample"),
            (Mount::Project, "schema"),
            (Mount::Project, "synchrotron"),
            (Mount::Project, "update_collection"),
            (Mount::Project, "update_function"),
            (Mount::Project, "update_group"),
            (Mount::Project, "upload"),
            (Mount::Project, "upsert_collection_connection"),
            (Mount::Internal, "download_state"),
            (Mount::Internal, "function_run_callback"),
            (Mount::Internal, "function_version_execute"),
            (Mount::Internal, "function_version_execution_status"),
            (Mount::Internal, "worker_callback"),
            (Mount::Internal, "worker_message"),
        ];
        let actual: HashSet<_> = registered().iter().map(|r| (r.mount, r.name)).collect();
        for entry in expected {
            assert!(actual.contains(&entry), "missing registration: {entry:?}");
        }
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
        for mount in [
            Mount::Unauthenticated,
            Mount::Global,
            Mount::Rbac,
            Mount::Simple,
            Mount::Project,
            Mount::Internal,
            Mount::Root,
        ] {
            let (_, api) = mount_router(mount, &states).split_for_parts();
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
            "/collections",
            "/executions",
            "/executions/{execution}",
            "/executions/{execution}/cancel",
            "/executions/{execution}/details",
            "/executions/{execution}/recover",
            "/function-runs",
            "/function-versions/{function_version_id}/bundle",
            "/function-versions/{function_version_id}/executions",
            "/inflight",
            "/internal/function-runs/{function_run_id}/workers/{worker_id}",
            "/internal/function-versions/{function_version_id}/download-state",
            "/internal/function-versions/{function_version_id}/execute",
            "/internal/function-versions/{function_version_id}/execution-status",
            "/internal/workers/{worker_id}",
            "/internal/workers/{worker_id}/message",
            "/logical-function-runs/{logical_function_run}/replay",
            "/metrics",
            "/permission-grants",
            "/permission-grants/by-project",
            "/permission-grants/by-user",
            "/projects",
            "/projects/{project}",
            "/projects/{project}/collections",
            "/projects/{project}/collections/{collection}",
            "/projects/{project}/collections/{collection}/connection",
            "/projects/{project}/collections/{collection}/function-bundle-upload",
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
                    || te_apiserver::DOC_PATH_PREFIXES
                        .iter()
                        .any(|prefix| path.starts_with(prefix)),
                "undeclared doc path: {path}"
            );
        }
    }
}
