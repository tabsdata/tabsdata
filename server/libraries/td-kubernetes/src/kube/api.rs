//
// Copyright 2026 Tabsdata Inc.
//

//! Canonical verbs for the Kubernetes API layer.
//!
//! Verb table (resource is singular: pod, job, configmap, secret, namespace, cluster):
//!
//!   `build_<resource>`              factory for a client or handle
//!   `create_<resource>`             create; error if it exists
//!   `replace_<resource>`            replace entirely; error if missing
//!   `upsert_<resource>`             create if missing, replace if present
//!   `delete_<resource>`             delete one named; error if missing
//!   `delete_<resource>s`            selector-driven bulk delete
//!   `get_<resource>`                fetch one with data; returns `Option<T>`
//!   `get_<resource>s`               selector-driven bulk fetch with data
//!   `require_<resource>`            fetch one with data; errors if missing
//!   `list_<resource>s`              selector-driven bulk fetch (names + key lists, no values)
//!   `exists_<resource>`             predicate; returns bool; does not error on missing
//!   `ensure_<resource>_<state>`     idempotent precondition (e.g.
//!                                     `ensure_<resource>_exists`,
//!                                     `ensure_<resource>_reachable`)
//!   `await_<resource>_condition`    wait on a caller-supplied condition
//!
//! Process-level helpers (not resource verbs) live in `support::client` and are exempt
//! from the table (e.g. `install_crypto_provider`).

pub mod domain;
pub mod entity;
pub mod support;
