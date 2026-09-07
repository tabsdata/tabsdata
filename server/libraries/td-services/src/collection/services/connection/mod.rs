//
// Copyright 2026 Tabsdata Inc.
//

//! Services for a source/destination collection's connection info. They resolve
//! the collection, gate on its flavour/authority, and persist/read through the
//! [`ConnectionStore`](td_bridge::connection::ConnectionStore) port.

mod read;
#[cfg(test)]
pub(crate) mod test_support;
mod upsert;

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

// Layers shared by the connection-carrying services. A connection is 1:1
// with its source/destination collection: it is created WITH the collection
// (a source/destination collection cannot exist without one), named BY the
// collection, and dies with it — there is no standalone connection
// create/delete. The [`ConnectionStore`](td_bridge::connection::ConnectionStore)
// calls themselves live in the orchestrating services — never in a layer,
// so no DB connection is ever held across them.

use serde::Deserialize;
use std::collections::HashSet;
use td_authz::rbac::{ManageProject, Project, SqlRbacSource};
use td_database::access::Gated;
use td_database::domain::{Apiserver, DomainConnection};
use td_objects::DaoCore;
use td_objects::FromDao;
use td_objects::dxo::collection::{
    ActiveCollectionDB, ActiveCollectionDBWithNames, CollectionCreate, CollectionCreateDB,
    CollectionRead,
};
use td_objects::dxo::connection::ConnectionDB;
use td_objects::dxo::crudl::RequestContext;
use td_objects::errors::SqlError;
use td_objects::types::basic::{CollectionType, ConnectionKind, GroupId};
use td_queries::LiveEntry;

/// Source/destination collections carry connection config — `ManageProject`
/// territory per the permission model ("CRUD sources and destinations");
/// plain `DataFactory` collections need only the `DevelopProject` floor the
/// pipeline already checked.
pub(crate) async fn escalate_for_connected(
    source: &SqlRbacSource,
    conn: &mut impl Gated<Apiserver>,
    request: &RequestContext,
    project: &ProjectId,
    collection_type: &CollectionType,
) -> Result<(), TdError> {
    match collection_type {
        CollectionType::DataFactory => Ok(()),
        CollectionType::Source | CollectionType::Destination => {
            let _grant = source
                .require::<Project, ManageProject>(conn, request, project)
                .await?;
            Ok(())
        }
    }
}

/// Only `Source`/`Destination` collections carry connection info; reject a
/// `DataFactory`.
pub fn ensure_connectable(collection: &ActiveCollectionDB) -> Result<(), TdError> {
    match &collection.collection_type {
        CollectionType::Source | CollectionType::Destination => Ok(()),
        CollectionType::DataFactory => Err(api_error!(
            ApiError::InputError,
            "collection '{}' is a DataFactory and does not carry connection info",
            collection.name
        )),
    }
}

/// Until subset updates (just-secrets / just-vars with fetch-and-align) are
/// supported, an upsert must carry the full triple — connection, secrets and
/// vars (the lists may be empty). Absent fields would leave "keep existing?"
/// semantics to the store backend; requiring them keeps the contract exact.
pub fn validate_upsert(connection: &ConnectionUpsert) -> Result<(), TdError> {
    if connection.connection.is_none() || connection.secrets.is_none() || connection.vars.is_none()
    {
        return Err(api_error!(
            ApiError::InputError,
            "a connection upsert must include the dehydrated connection, its secrets \
             and its vars (the lists may be empty)"
        ));
    }
    Ok(())
}

/// The validated dehydrated document — the fields the server reads (the rest
/// of the JSON stays opaque) plus what the usability check derived from it.
#[derive(Debug, Clone)]
pub struct ConnectionDocument {
    pub kind: ConnectionKind,
    /// Secret keys carried with `value: None` ("keep the stored value") — the
    /// orchestrator must verify the store actually holds them.
    pub kept_secrets: Vec<String>,
    /// Var keys carried with `value: None`, same rule.
    pub kept_vars: Vec<String>,
}

/// [`validate_connection_document`] over an upsert payload.
pub fn validate_document(connection: &ConnectionUpsert) -> Result<ConnectionDocument, TdError> {
    let document = connection.connection.as_ref().ok_or_else(|| {
        api_error!(
            ApiError::InputError,
            "a connection upsert must include the dehydrated connection"
        )
    })?;
    validate_connection_document(
        document,
        connection.secrets.as_deref().unwrap_or(&[]),
        connection.vars.as_deref().unwrap_or(&[]),
    )
}

/// The only field the server reads off a dehydrated document; the rest stays
/// opaque (no `deny_unknown_fields`) — forwarded to the store and resolved by
/// the worker. `type` is required — a missing one fails deserialization. It is
/// the connection's fully-qualified Conn class (`<module>:<qualname>`); the
/// collection is keyed by it and the function register/update check matches it
/// against the function's own Conn class. The connection carries no name: it is
/// 1:1 with its collection and named by it (the server already has the
/// collection name).
#[derive(Deserialize)]
#[serde(expecting = "a dehydrated connection object with a 'type'")]
struct RawConnectionDocument {
    #[serde(rename = "type")]
    type_: String,
}

/// Validates a dehydrated document and the payload's internal consistency:
///
/// - the document is an object with a `name` and a valid `kind`;
/// - secret/var keys are unique across both lists (a duplicate would make
///   "which value applies" backend-dependent).
///
/// Keys with `value: None` mean "keep the stored value"; whether the store
/// actually holds them is verified by the orchestrator (it owns store access).
pub fn validate_connection_document(
    document: &serde_json::Value,
    secrets: &[KeyValue],
    vars: &[KeyValue],
) -> Result<ConnectionDocument, TdError> {
    let raw = RawConnectionDocument::deserialize(document)
        .map_err(|e| api_error!(ApiError::InputError, "invalid dehydrated connection: {e}"))?;
    let kind = ConnectionKind::try_from(raw.type_.as_str())?;

    let mut seen: HashSet<&str> = HashSet::new();
    let mut duplicated: Vec<&str> = Vec::new();
    for entry in secrets.iter().chain(vars.iter()) {
        if !seen.insert(entry.key.as_str()) {
            duplicated.push(entry.key.as_str());
        }
    }
    if !duplicated.is_empty() {
        return Err(api_error!(
            ApiError::InputError,
            "duplicate secret/var keys (a key must appear once across secrets \
             and vars): {}",
            duplicated.join(", ")
        ));
    }

    Ok(ConnectionDocument {
        kind,
        kept_secrets: kept_keys(secrets),
        kept_vars: kept_keys(vars),
    })
}

fn kept_keys(entries: &[KeyValue]) -> Vec<String> {
    entries
        .iter()
        .filter(|kv| kv.value.is_none())
        .map(|kv| kv.key.clone())
        .collect()
}

/// What the read-only resolution hands the upsert orchestrator: the existing
/// connection row (upserts only UPDATE — the connection is created with its
/// collection) and the validated document.
#[derive(Debug, Clone)]
pub struct ConnectionResolution {
    pub row: ConnectionDB,
    pub document: ConnectionDocument,
    pub collection: CollectionName,
    pub collection_id: CollectionId,
}

/// Resolves the collection's connection ROW for an update. The connection
/// exists by construction (created with the collection); its connector kind
/// is immutable — content was stored under it.
pub async fn resolve_connection(
    conn: &mut DomainConnection<Apiserver>,
    collection: &ActiveCollectionDB,
    document: &ConnectionDocument,
) -> Result<ConnectionResolution, TdError> {
    let connection_id = collection.connection_id.ok_or_else(|| {
        api_error!(
            ApiError::NotFound,
            "the collection has no connection configured"
        )
    })?;
    let existing: ConnectionDB = conn.live().fetch_one(connection_id).await?;
    ensure_same_kind(&existing, &document.kind)?;
    Ok(ConnectionResolution {
        row: existing,
        document: document.clone(),
        collection: collection.name.clone(),
        collection_id: collection.id,
    })
}

fn ensure_same_kind(existing: &ConnectionDB, kind: &ConnectionKind) -> Result<(), TdError> {
    if existing.kind != *kind {
        return Err(api_error!(
            ApiError::InputError,
            "the connection's connector kind is '{}' and cannot change (got '{}'); \
             delete and recreate the collection to use a different connector",
            existing.kind,
            kind
        ));
    }
    Ok(())
}

/// The collection's connection id, or 404 — without a store round-trip.
pub fn require_connection_id(collection: &ActiveCollectionDB) -> Result<ConnectionId, TdError> {
    collection.connection_id.ok_or_else(|| {
        api_error!(
            ApiError::NotFound,
            "the collection has no connection configured"
        )
    })
}

/// Validates a creation payload and plans the whole insert (store-first: the
/// orchestrator persists nothing until the store write succeeds).
/// Source/Destination collections REQUIRE a connection — they are 1:1 with
/// it and cannot exist without one; a DataFactory cannot carry one.
pub async fn plan_collection_create(
    conn: &mut DomainConnection<Apiserver>,
    request_context: &RequestContext,
    create: &CollectionCreate,
    group_id: &GroupId,
    collection_type: &CollectionType,
) -> Result<CollectionCreatePlan, TdError> {
    // A duplicate name is rejected up front for every collection flavour (not
    // just connected ones) with a clean "already exists" error instead of the
    // raw unique-constraint violation from the insert. Connected collections
    // additionally need this before the store write (see the helper's note).
    ensure_collection_name_free(conn, &create.name).await?;
    let connection_row = match collection_type {
        CollectionType::DataFactory => {
            if create.connection.is_some() || create.secrets.is_some() || create.vars.is_some() {
                return Err(api_error!(
                    ApiError::InputError,
                    "collection '{}' resolves to a DataFactory and cannot carry \
                     connection info",
                    create.name
                ));
            }
            None
        }
        CollectionType::Source | CollectionType::Destination => {
            let (Some(document), Some(secrets), Some(vars)) =
                (&create.connection, &create.secrets, &create.vars)
            else {
                return Err(api_error!(
                    ApiError::InputError,
                    "a source/destination collection is created WITH its connection: \
                     the payload must include the dehydrated connection, its secrets \
                     and its vars (the lists may be empty)"
                ));
            };
            let document = validate_connection_document(document, secrets, vars)?;
            if !document.kept_secrets.is_empty() || !document.kept_vars.is_empty() {
                return Err(api_error!(
                    ApiError::InputError,
                    "secrets/vars without a value mean 'keep the stored value', but \
                     nothing is stored yet for a new collection"
                ));
            }
            Some(ConnectionDB {
                id: ConnectionId::default(),
                kind: document.kind.clone(),
                created_on: request_context.time.into(),
                created_by_id: request_context.user_id.into(),
                modified_on: request_context.time.into(),
                modified_by_id: request_context.user_id.into(),
            })
        }
    };
    Ok(CollectionCreatePlan {
        create: create.clone(),
        collection_id: CollectionId::default(),
        group_id: *group_id,
        collection_type: collection_type.clone(),
        connection_row,
    })
}

/// The store is written BEFORE the insert (store-first), so when a connection
/// rides along the name must be known free up front: the worker artifact path
/// is derived from the name, and a duplicate would overwrite the existing
/// collection's artifact before the TX uniqueness check fires (the failure
/// cleanup would then delete it). The store's own name-ownership guard covers
/// the remaining race.
pub(crate) async fn ensure_collection_name_free(
    conn: &mut DomainConnection<Apiserver>,
    name: &CollectionName,
) -> Result<(), TdError> {
    let existing: Vec<ActiveCollectionDB> = conn.live().fetch_all(name).await?;
    if existing.is_empty() {
        Ok(())
    } else {
        // Clean "already exists" (input-error/400), matching the
        // `By::<Name>::assert_not_exists` errors used for the other entities.
        Err(SqlError::EntityAlreadyExists(
            ActiveCollectionDB::display_name().to_string(),
            format!(" for 'name' = '{name}'"),
        ))?
    }
}

/// Everything the create transaction persists, resolved and validated up
/// front: the original payload, the resolved group/type, and the minted
/// connection row for source/destination collections.
#[derive(Debug, Clone)]
pub struct CollectionCreatePlan {
    pub create: CollectionCreate,
    pub collection_id: CollectionId,
    pub group_id: GroupId,
    pub collection_type: CollectionType,
    pub connection_row: Option<ConnectionDB>,
}

/// The create transaction: insert the (store-confirmed) connection row, then
/// the collection pointing at it, and read the result back.
pub async fn insert_collection_with_connection(
    conn: &mut DomainConnection<Apiserver>,
    request_context: &RequestContext,
    plan: &CollectionCreatePlan,
) -> Result<CollectionRead, TdError> {
    if let Some(row) = &plan.connection_row {
        conn.live().insert(row).await?;
    }
    let collection = CollectionCreateDB {
        id: plan.collection_id,
        name: plan.create.name.clone(),
        description: plan.create.description.clone(),
        created_on: request_context.time.into(),
        created_by_id: request_context.user_id.into(),
        modified_on: request_context.time.into(),
        modified_by_id: request_context.user_id.into(),
        name_when_deleted: None,
        group_id: plan.group_id,
        base_path: plan.create.base_path.clone(),
        collection_type: plan.collection_type.clone(),
        connection_id: plan.connection_row.as_ref().map(|row| row.id),
    };
    conn.live().insert(&collection).await?;
    let with_names: ActiveCollectionDBWithNames = conn.live().fetch_one(collection.id).await?;
    CollectionRead::from_dao(with_names)
}
