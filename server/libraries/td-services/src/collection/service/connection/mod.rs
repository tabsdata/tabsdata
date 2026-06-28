//
// Copyright 2026 Tabs Data Inc.
//

//! Services for a source/destination collection's connection info. They resolve
//! the collection, gate on its flavour/authority, and persist/read through the
//! [`ConnectionStore`](td_bridge::connection::ConnectionStore) port.

mod layer;
mod read;
#[cfg(test)]
pub(crate) mod test_support;
mod upsert;

pub(crate) use layer::{
    CollectionCreatePlan, ensure_collection_name_free, escalate_for_connected,
    insert_collection_with_connection, plan_collection_create,
};
pub use read::ConnectionReadService;
pub use upsert::ConnectionUpsertService;

use td_bridge::connection::ConnectionStore;
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::connection::{ConnectionUpsert, KeyValue};
use td_objects::types::basic::{CollectionId, CollectionName, ConnectionId, ProjectId};
use tracing::warn;

/// Best-effort removal of a connection's store content (create rollback,
/// collection delete). Failure is logged, not propagated — the DB outcome is
/// already settled, and a leftover is invisible: an idempotent re-delete or
/// overwrite repairs it.
pub(crate) async fn drop_connection_best_effort(
    store: &dyn ConnectionStore,
    project: &ProjectId,
    connection_id: &ConnectionId,
) {
    if let Err(e) = store.delete(project, connection_id).await {
        warn!(error = %e, "could not drop the connection's store content");
    }
}

/// Rewrite the stored document's `name` — the connection is named by its
/// collection, so it follows a collection rename. Values never leave the
/// store: secrets/vars ride as kept (`value: None`) entries the store
/// resolves against what it holds. Idempotent like any store write.
pub(crate) async fn rename_stored_connection(
    store: &dyn ConnectionStore,
    project: &ProjectId,
    connection_id: &ConnectionId,
    collection_id: &CollectionId,
    new_name: &CollectionName,
) -> Result<(), TdError> {
    let stored = store.fetch(project, connection_id).await?;
    let mut document = stored.connection;
    let object = document.as_object_mut().ok_or_else(|| {
        api_error!(
            ApiError::InternalError,
            "the stored connection document is not a JSON object"
        )
    })?;
    object.insert(
        "name".to_string(),
        serde_json::Value::String(new_name.to_string()),
    );
    let payload = ConnectionUpsert {
        connection: Some(document),
        secrets: Some(kept(stored.secret_names)),
        vars: Some(kept(stored.var_names)),
    };
    store
        .upsert(project, connection_id, new_name, collection_id, &payload)
        .await?;
    Ok(())
}

fn kept(names: Vec<String>) -> Vec<KeyValue> {
    names
        .into_iter()
        .map(|key| KeyValue { key, value: None })
        .collect()
}
