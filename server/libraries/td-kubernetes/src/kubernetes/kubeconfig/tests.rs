//
// Copyright 2026 Tabs Data Inc.
//

use std::io::Write;

use tempfile::NamedTempFile;

use super::*;

const FULL_KUBECONFIG: &str = "\
apiVersion: v1
kind: Config
current-context: dev
clusters:
- name: dev-cluster
  cluster:
    server: https://dev.example.com
- name: prod-cluster
  cluster:
    server: https://prod.example.com
contexts:
- name: dev
  context:
    cluster: dev-cluster
    namespace: dev-ns
    user: dev-user
- name: prod
  context:
    cluster: prod-cluster
    user: prod-user
users:
- name: dev-user
  user:
    token: dev-token
- name: prod-user
  user:
    token: prod-token
";

const KUBECONFIG_WITHOUT_CURRENT_CONTEXT: &str = "\
apiVersion: v1
kind: Config
clusters:
- name: dev-cluster
  cluster:
    server: https://dev.example.com
contexts:
- name: dev
  context:
    cluster: dev-cluster
    namespace: dev-ns
    user: dev-user
users:
- name: dev-user
  user:
    token: dev-token
";

fn write_kubeconfig(content: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("create temp kubeconfig");
    file.write_all(content.as_bytes()).expect("write fixture");
    file.flush().expect("flush fixture");
    file
}

#[test]
fn resolve_kubefile_with_explicit_path_returns_that_path() {
    let path = PathBuf::from("/tmp/some-kubeconfig");
    assert_eq!(resolve_kubefile(Some(&path)), Some(path));
}

#[test]
fn resolve_current_context_returns_value_from_file() {
    let file = write_kubeconfig(FULL_KUBECONFIG);
    assert_eq!(
        resolve_current_context(Some(file.path())),
        Some("dev".to_owned())
    );
}

#[test]
fn resolve_current_context_returns_none_for_missing_file() {
    let path = PathBuf::from("/nonexistent/path/to/kubeconfig");
    assert_eq!(resolve_current_context(Some(&path)), None);
}

#[test]
fn resolve_current_context_returns_none_when_field_absent() {
    let file = write_kubeconfig(KUBECONFIG_WITHOUT_CURRENT_CONTEXT);
    assert_eq!(resolve_current_context(Some(file.path())), None);
}

#[test]
fn resolve_cluster_name_for_explicit_context() {
    let file = write_kubeconfig(FULL_KUBECONFIG);
    assert_eq!(
        resolve_cluster_name(Some(file.path()), Some("prod")),
        Some("prod-cluster".to_owned())
    );
}

#[test]
fn resolve_cluster_name_falls_back_to_current_context() {
    let file = write_kubeconfig(FULL_KUBECONFIG);
    assert_eq!(
        resolve_cluster_name(Some(file.path()), None),
        Some("dev-cluster".to_owned())
    );
}

#[test]
fn resolve_cluster_name_returns_none_for_unknown_context() {
    let file = write_kubeconfig(FULL_KUBECONFIG);
    assert_eq!(
        resolve_cluster_name(Some(file.path()), Some("missing")),
        None
    );
}

#[test]
fn resolve_cluster_name_returns_none_when_no_context_resolvable() {
    let file = write_kubeconfig(KUBECONFIG_WITHOUT_CURRENT_CONTEXT);
    assert_eq!(resolve_cluster_name(Some(file.path()), None), None);
}

#[test]
fn resolve_namespace_returns_value_from_context() {
    let file = write_kubeconfig(FULL_KUBECONFIG);
    assert_eq!(
        resolve_namespace(Some(file.path()), Some("dev")),
        Some("dev-ns".to_owned())
    );
}

#[test]
fn resolve_namespace_returns_none_when_context_omits_it() {
    let file = write_kubeconfig(FULL_KUBECONFIG);
    assert_eq!(resolve_namespace(Some(file.path()), Some("prod")), None);
}

#[test]
fn resolve_namespace_falls_back_to_current_context() {
    let file = write_kubeconfig(FULL_KUBECONFIG);
    assert_eq!(
        resolve_namespace(Some(file.path()), None),
        Some("dev-ns".to_owned())
    );
}

#[test]
fn resolve_namespace_returns_none_for_unknown_context() {
    let file = write_kubeconfig(FULL_KUBECONFIG);
    assert_eq!(resolve_namespace(Some(file.path()), Some("missing")), None);
}

#[test]
fn resolve_cluster_url_resolves_through_context_to_cluster_server() {
    let file = write_kubeconfig(FULL_KUBECONFIG);
    assert_eq!(
        resolve_cluster_url(Some(file.path()), Some("prod")),
        Some("https://prod.example.com".to_owned())
    );
}

#[test]
fn resolve_cluster_url_falls_back_to_current_context() {
    let file = write_kubeconfig(FULL_KUBECONFIG);
    assert_eq!(
        resolve_cluster_url(Some(file.path()), None),
        Some("https://dev.example.com".to_owned())
    );
}

#[test]
fn resolve_cluster_url_returns_none_for_unknown_context() {
    let file = write_kubeconfig(FULL_KUBECONFIG);
    assert_eq!(
        resolve_cluster_url(Some(file.path()), Some("missing")),
        None
    );
}

#[test]
fn resolve_cluster_url_returns_none_for_missing_file() {
    let path = PathBuf::from("/nonexistent/path/to/kubeconfig");
    assert_eq!(resolve_cluster_url(Some(&path), Some("dev")), None);
}
