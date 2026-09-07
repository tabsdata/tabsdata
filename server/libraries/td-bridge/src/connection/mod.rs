//
// Copyright 2026 Tabsdata Inc.
//

//! The `ConnectionStore` port: how the connection services persist/read a
//! collection's connection, keyed by `(project, connection)` — the eventual
//! consumer (a worker pod) is project-scoped, so the backend namespaces by
//! project. The concrete impl (the k8s bridge over HTTP, a k8s Secret store, …)
//! is vended by [`BridgeProvider::connection_store`](crate::BridgeProvider::connection_store),
//! so service code stays free of the transport. [`noop::NoopConnectionStore`]
//! is the store used when no backend is configured (local/tests).

use async_trait::async_trait;
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::connection::{ConnectionHydrated, ConnectionRead, ConnectionUpsert, KeyValue};
use td_objects::types::basic::{CollectionId, CollectionName, ConnectionId, ProjectId};

/// Writes are IDEMPOTENT — that is contract, not detail: the server calls the
/// store FIRST and persists the connection row only after the write succeeds,
/// so it may re-issue an `upsert` (same id, same or new content) or `delete`
/// (possibly never-stored id) at any time and expects the same outcome.
#[async_trait]
pub trait ConnectionStore: Send + Sync {
    /// Create or update the connection; returns what is now stored (names only).
    /// Idempotent: re-running with the same id overwrites.
    ///
    /// The document's `name` is exclusive within the project: the store MUST
    /// reject an upsert whose name is already stored under a different
    /// connection id — the worker artifact path is derived from the name, so
    /// a second owner would hijack the first one's artifact.
    ///
    /// Secret entries with `value: None` mean "keep the stored value": the
    /// store MUST substitute the value it already holds for that key, and MUST
    /// reject the write if it holds none. The server pre-validates kept keys
    /// against [`fetch`](ConnectionStore::fetch), but the store is the one
    /// merging — collection renames rely on this (the full pair is
    /// re-upserted with every value sent as kept).
    async fn upsert(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
        collection: &CollectionName,
        collection_id: &CollectionId,
        connection: &ConnectionUpsert,
    ) -> Result<ConnectionRead, TdError>;

    /// Read the stored connection (dehydrated + secret names, no values).
    async fn fetch(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
    ) -> Result<ConnectionRead, TdError>;

    /// Privileged read: the stored connection WITH secret values.
    async fn fetch_hydrated(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
    ) -> Result<ConnectionHydrated, TdError>;

    /// Drop the connection (e.g. on collection delete) so its secrets aren't
    /// orphaned. Idempotent: deleting a never-stored id succeeds.
    async fn delete(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
    ) -> Result<(), TdError>;
}

pub mod file;
#[cfg(feature = "test-utils")]
pub mod memory;
pub mod noop;

/// Names-only view of a stored pair (values never leave the store).
pub fn read_of(upsert: &ConnectionUpsert) -> ConnectionRead {
    ConnectionRead {
        id: None,
        connection: upsert.connection.clone().unwrap_or(serde_json::Value::Null),
        secret_names: keys(upsert.secrets.as_deref()),
    }
}

/// Full-values view of a stored pair — for privileged reads only.
pub fn hydrated_of(upsert: &ConnectionUpsert) -> ConnectionHydrated {
    ConnectionHydrated {
        connection: upsert.connection.clone().unwrap_or(serde_json::Value::Null),
        secrets: upsert.secrets.clone().unwrap_or_default(),
    }
}

fn keys(entries: Option<&[KeyValue]>) -> Vec<String> {
    entries
        .unwrap_or(&[])
        .iter()
        .map(|kv| kv.key.clone())
        .collect()
}

/// Resolve `value: None` entries ("keep the stored value") against the
/// previously stored pair. The server verifies this before calling; the
/// store re-checks because the kept value must actually be substituted here.
pub fn merge_kept(
    connection: &ConnectionUpsert,
    previous: Option<&ConnectionUpsert>,
) -> Result<ConnectionUpsert, TdError> {
    let mut merged = connection.clone();
    let stored = previous.and_then(|p| p.secrets.as_deref());
    if let Some(entries) = merged.secrets.as_mut() {
        for entry in entries.iter_mut().filter(|kv| kv.value.is_none()) {
            let kept = stored
                .unwrap_or(&[])
                .iter()
                .find(|kv| kv.key == entry.key)
                .and_then(|kv| kv.value.clone());
            match kept {
                Some(value) => entry.value = Some(value),
                None => {
                    return Err(api_error!(
                        ApiError::InputError,
                        "cannot keep the stored value of '{}': it is not stored",
                        entry.key
                    ));
                }
            }
        }
    }
    Ok(merged)
}
