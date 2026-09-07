//
// Copyright 2026 Tabsdata Inc.
//

//! Transport DTOs for a collection's connection info: the dehydrated connection
//! plus the secret/var key-values exchanged between the apiserver, the
//! connection services, and (behind the store) the bridge. The connection JSON
//! itself is opaque (`serde_json::Value`).

use crate::types::basic::ConnectionId;

/// One secret/var as `(key, value)`. Hand-written (not a `Dto`) so its `Debug`
/// redacts `value`: these carry secret literals and the apiserver debug-logs
/// request bodies. `ConnectionUpsert`'s derived `Debug` formats its secrets
/// through this impl, so redaction holds.
#[derive(Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct KeyValue {
    pub key: String,
    pub value: Option<String>,
}

impl std::fmt::Debug for KeyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyValue")
            .field("key", &self.key)
            .field("value", &self.value.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// The 3 JSON objects an upsert carries to the store. All optional so an update
/// can carry just secrets or just vars; a create carries all three. Alignment of
/// the triple is validated CLI-side.
#[ta_objects::Dto]
pub struct ConnectionUpsert {
    /// On the wire it is a YAML string (the dehydrated connection document),
    /// matching the create endpoint; it is parsed into the connection object
    /// here so the rest of the pipeline keeps working on a value.
    #[schema(value_type = String)]
    #[serde(
        default,
        deserialize_with = "crate::dxo::collection::deserialize_yaml_connection"
    )]
    pub connection: Option<serde_json::Value>,
    pub secrets: Option<Vec<KeyValue>>,
    pub vars: Option<Vec<KeyValue>>,
}

/// What the store returns on read: the dehydrated connection plus the names
/// (never the values) of the stored secrets and vars. `id` is stamped by the
/// apiserver (the connection row's id), not by the store backend.
#[ta_objects::Dto]
pub struct ConnectionRead {
    #[serde(default)]
    pub id: Option<ConnectionId>,
    #[schema(value_type = Object)]
    pub connection: serde_json::Value,
    pub secret_names: Vec<String>,
    pub var_names: Vec<String>,
}

/// Privileged read: the dehydrated document plus the secret/var key-values
/// WITH values.
#[ta_objects::Dto]
pub struct ConnectionHydrated {
    #[schema(value_type = Object)]
    pub connection: serde_json::Value,
    pub secrets: Vec<KeyValue>,
    pub vars: Vec<KeyValue>,
}

use crate::types::basic::{AtTime, ConnectionKind, Created, Modified, ProjectId, UserId};

/// A connection row: stable identity + connector kind, 1:1 with its
/// source/destination collection (created with it, named by it, deleted
/// with it). The connection CONTENT (dehydrated JSON, secrets, vars)
/// lives in the store backend, keyed by this row's id; the row pins the
/// connector kind across updates. The row exists only once a store write
/// succeeded (store-first ordering), so its presence IS the confirmation.
/// `project_id` is owned by RLS (field-less, like groups).
#[ta_objects::Dao]
#[dao(table = "connections", ops(insert), project_scoped)]
pub struct ConnectionInsertDB {
    #[dao(pk)]
    pub id: ConnectionId,
    pub kind: ConnectionKind,
    pub created_on: Created<AtTime>,
    pub created_by_id: Created<UserId>,
    pub modified_on: Modified<AtTime>,
    pub modified_by_id: Modified<UserId>,
}

/// The row as the database holds it: `project_id` is the `project_isolation` policy's, defaulted from the connection the insert ran on.
#[ta_objects::Dao]
#[dao(table = "connections", ops(select), project_scoped)]
pub struct ConnectionDB {
    #[dao(flatten)]
    pub base: ConnectionInsertDB,
    pub project_id: ProjectId,
}
