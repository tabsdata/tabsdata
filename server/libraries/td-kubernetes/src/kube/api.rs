//
// Copyright 2026 Tabs Data Inc.
//

//! Canonical verbs for the Kubernetes API layer.
//!
//! Verb table (resource is singular: pod, job, configmap, secret, namespace, cluster):
//!
//!   `await_<resource>_condition`   wait on a caller-supplied condition
//!   `build_<resource>`              factory for a client or handle
//!   `create_<resource>`             create; error if it exists
//!   `delete_<resource>`             delete one named; error if missing
//!   `delete_<resource>s_by_labels`  selector-driven bulk delete
//!   `ensure_<resource>_<state>`     idempotent precondition
//!   `exists_<resource>`             predicate; returns bool; does not error on missing
//!   `get_<resource>`                fetch one; returns `Option<T>`
//!   `list_<resource>s_by_labels`    selector-driven bulk fetch
//!   `replace_<resource>`            replace by name; error if missing
//!   `require_<resource>`            fetch one; errors if missing
//!   `upsert_<resource>`             create if missing, replace if present (idempotent)

pub mod client;
pub mod cluster;
pub mod configmap;
pub mod job;
pub mod namespace;
pub mod pod;
pub mod secret;

pub use client::{build_incluster_kube_client, build_kube_client, install_crypto_provider};
pub use cluster::ensure_cluster_reachable;
pub use configmap::{
    create_configmap, delete_configmap, exists_configmap, get_configmap, require_configmap,
};
pub use namespace::ensure_namespace_exists;
pub use secret::{delete_secret, exists_secret, upsert_secret};
