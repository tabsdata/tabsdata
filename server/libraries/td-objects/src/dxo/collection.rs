//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::connection::KeyValue;
use crate::dxo::crudl::RequestContext;
use crate::types::basic::{
    AtTime, BasePath, CollectionId, CollectionIdName, CollectionName, CollectionType,
    CollectionWhenDeleted, ConnectionId, Description, GroupId, GroupName, ProjectId, ProjectName,
    UserId, UserName,
};
use ta_objects::FromDao;
use td_common::id::Id;
use td_error::TdError;

#[ta_objects::Dao]
#[dao(sql_table = "collections")]
pub struct CollectionCreateDB {
    pub id: CollectionId,
    pub name: CollectionName,
    pub description: Description,
    pub created_on: AtTime,
    pub created_by_id: UserId,
    pub modified_on: AtTime,
    pub modified_by_id: UserId,
    pub name_when_deleted: Option<CollectionName>,
    // Resolved from `CollectionCreate::group` (id-or-name) by the create service.
    pub group_id: GroupId,
    // Logical storage prefix, mapped from `CollectionCreate::base_path`.
    // `None` (omitted) inherits the owning project's base_path at stamp time.
    pub base_path: Option<BasePath>,
    // Inferred from the resolved group by the create service.
    pub collection_type: CollectionType,
    // The connection minted with the collection (source/destination only).
    pub connection_id: Option<ConnectionId>,
}

#[ta_objects::Dao]
#[dao(sql_table = "collections_active")]
pub struct ActiveCollectionDB {
    pub id: CollectionId,
    /// The project that owns this collection. Read from the DB column when
    /// SELECT'd so downstream constructions (functions, runs, workers) can
    /// propagate it. Set by RLS on INSERTs.
    pub project_id: ProjectId,
    pub name: CollectionName,
    pub description: Description,
    pub created_on: AtTime,
    pub created_by_id: UserId,
    pub modified_on: AtTime,
    pub modified_by_id: UserId,
    pub group_id: GroupId,
    // Raw override: `None` means inherit the owning project's base_path.
    pub base_path: Option<BasePath>,
    // Resolved prefix from the `collections_active` view (COALESCE of own
    // base_path, else project's). Read-only; stamp it at register/upload/download.
    pub effective_base_path: BasePath,
    pub collection_type: CollectionType,
    /// The connection minted with this collection (source/destination
    /// only — they are 1:1 with it).
    pub connection_id: Option<ConnectionId>,
}

impl ActiveCollectionDB {
    /// The soft-delete update: the row is renamed to `deleted_<id>`,
    /// freeing the name for reuse.
    pub fn to_delete_dao(&self, ctx: &RequestContext) -> CollectionDeleteDB {
        CollectionDeleteDB {
            name: generate_deleted_name(),
            modified_on: ctx.time,
            modified_by_id: ctx.user_id,
            name_when_deleted: self.name.clone(),
        }
    }
}

// Canonical read/join surface: the `collections_resolved` view — every row,
// including soft-deleted, with the real name resolved. Same columns as
// `ActiveCollectionDB` plus the `deleted` flag; resolve a collection
// referenced by an entity that can outlive it through this type.
#[ta_objects::Dao]
#[dao(sql_table = "collections_resolved")]
pub struct CollectionDB {
    #[dao(flatten)]
    pub base: ActiveCollectionDB,
    pub deleted: CollectionWhenDeleted,
}

#[ta_objects::Dao]
#[dao(sql_table = "collections")]
pub struct CollectionDeleteDB {
    pub name: CollectionName,

    pub modified_on: AtTime,
    pub modified_by_id: UserId,

    pub name_when_deleted: CollectionName,
}

pub fn generate_deleted_name() -> CollectionName {
    CollectionName::try_from(format!("deleted_{}", Id::default())).unwrap()
}

#[ta_objects::Dao]
#[dao(sql_table = "collections__with_names")]
pub struct ActiveCollectionDBWithNames {
    #[dao(flatten)]
    pub base: ActiveCollectionDB,
    // Owning project's name, from the view's join to projects_resolved.
    pub project: ProjectName,
    pub created_by: UserName,
    pub modified_by: UserName,
    // Resolved group display name from the joined groups row.
    pub group: GroupName,
}

#[ta_objects::Dto]
pub struct CollectionCreate {
    pub name: CollectionName,
    pub description: Description,
    // Name of the group to assign. Defaults to the default group.
    #[serde(default = "GroupName::default_group")]
    pub group: GroupName,
    // Logical storage prefix for the collection's artifacts.
    // Omitted (`None`) inherits the owning project's base_path.
    #[serde(default)]
    pub base_path: Option<BasePath>,
    /// The connection triple — REQUIRED for source/destination collections
    /// (they are 1:1 with their connection and cannot exist without it),
    /// forbidden for a DataFactory. On the wire it is a YAML string (the
    /// dehydrated connection document); it is parsed into the connection
    /// object here, so the rest of the pipeline keeps working on a value.
    #[schema(value_type = String)]
    #[serde(
        default,
        deserialize_with = "crate::dxo::collection::deserialize_yaml_connection"
    )]
    pub connection: Option<serde_json::Value>,
    #[serde(default)]
    pub secrets: Option<Vec<KeyValue>>,
    #[serde(default)]
    pub vars: Option<Vec<KeyValue>>,
}

#[ta_objects::Dto]
pub struct CollectionUpdate {
    pub name: Option<CollectionName>,
    pub description: Option<Description>,
    // Tri-state (the Dto macro injects the serde deserializer): absent -> None
    // (unchanged); `null` -> Some(None) (clear -> inherit project); value ->
    // Some(Some(x)) (set).
    pub base_path: Option<Option<BasePath>>,
}

impl CollectionUpdate {
    pub fn apply_to(&self, db: &mut CollectionUpdateDB) {
        if let Some(name) = &self.name {
            db.name = name.clone();
        }
        if let Some(description) = &self.description {
            db.description = description.clone();
        }
        // Re-wrap as `Some(inner)` so `update_values` writes the column; the
        // DTO's outer `None` falls through here (unchanged).
        if let Some(base_path) = &self.base_path {
            db.base_path = Some(base_path.clone());
        }
    }
}

#[ta_objects::Dao]
#[dao(update, sql_table = "collections")]
pub struct CollectionUpdateDB {
    pub name: CollectionName,
    pub description: Description,
    pub modified_on: AtTime,
    pub modified_by_id: UserId,
    // DAO side of `CollectionUpdate::base_path`'s tri-state (here the outer
    // `None` skips the column). Seeded from the current `ActiveCollectionDB`,
    // overridden by the update layer.
    pub base_path: Option<Option<BasePath>>,
}

#[ta_objects::Dto]
#[dto(list(on = ActiveCollectionDBWithNames))]
pub struct CollectionRead {
    #[dto(list(pagination_by = "+"))]
    pub id: CollectionId,
    pub project_id: ProjectId,
    #[dto(list(filter, filter_like, order_by))]
    pub name: CollectionName,
    #[dto(list(filter, filter_like, order_by))]
    pub description: Description,
    pub created_on: AtTime,
    pub created_by_id: UserId,
    pub modified_on: AtTime,
    pub modified_by_id: UserId,
    pub group_id: GroupId,
    pub base_path: Option<BasePath>,
    pub effective_base_path: BasePath,
    #[dto(list(filter, order_by))]
    pub collection_type: CollectionType,
    pub connection_id: Option<ConnectionId>,
    // Disambiguates same-named collections across projects in wildcard listings.
    #[dto(list(filter, filter_like, order_by))]
    pub project: ProjectName,
    pub created_by: UserName,
    pub modified_by: UserName,
    #[dto(list(filter, filter_like, order_by))]
    pub group: GroupName,
}

impl FromDao for CollectionRead {
    type Dao = ActiveCollectionDBWithNames;

    fn from_dao(dao: ActiveCollectionDBWithNames) -> Result<Self, TdError> {
        let ActiveCollectionDBWithNames {
            base,
            project,
            created_by,
            modified_by,
            group,
        } = dao;
        Ok(Self {
            id: base.id,
            project_id: base.project_id,
            name: base.name,
            description: base.description,
            created_on: base.created_on,
            created_by_id: base.created_by_id,
            modified_on: base.modified_on,
            modified_by_id: base.modified_by_id,
            group_id: base.group_id,
            base_path: base.base_path,
            effective_base_path: base.effective_base_path,
            collection_type: base.collection_type,
            connection_id: base.connection_id,
            project,
            created_by,
            modified_by,
            group,
        })
    }
}

impl From<&ActiveCollectionDB> for CollectionUpdateDB {
    fn from(src: &ActiveCollectionDB) -> Self {
        CollectionUpdateDB {
            name: src.name.clone(),
            description: src.description.clone(),
            modified_on: src.modified_on,
            modified_by_id: src.modified_by_id,
            base_path: Some(src.base_path.clone()),
        }
    }
}

/// Parse the `connection` field into the `serde_json::Value` the rest of the
/// pipeline consumes. It accepts either form:
///
/// - a YAML string — the dehydrated connection document the client sends on the
///   wire (YAML is a superset of JSON, so a JSON string parses too); or
/// - an already-parsed object — the form `ConnectionUpsert` round-trips through
///   the connection store's own serialize/deserialize (the stored record holds
///   the connection as a map, not a string).
///
/// `null`/absent -> `None`.
pub(crate) fn deserialize_yaml_connection<'de, D>(
    deserializer: D,
) -> Result<Option<serde_json::Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    match Option::<serde_json::Value>::deserialize(deserializer)? {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(text)) => serde_yaml::from_str(&text)
            .map(Some)
            .map_err(serde::de::Error::custom),
        Some(other) => Ok(Some(other)),
    }
}

#[ta_objects::UrlParam]
pub struct CollectionParam {
    pub collection: CollectionIdName,
}

#[cfg(test)]
mod tests {
    use super::CollectionCreate;

    #[test]
    fn connection_field_parses_a_yaml_string_into_a_value() {
        // On the wire `connection` is a YAML string inside the JSON body.
        let body = r#"{
            "name": "c",
            "description": "d",
            "connection": "kind: mysqlSrcConn\nspec:\n  uri: mysql://h/db\n"
        }"#;
        let create: CollectionCreate = serde_json::from_str(body).unwrap();
        let conn = create.connection.expect("connection parsed");
        assert_eq!(conn["kind"], "mysqlSrcConn");
        assert_eq!(conn["spec"]["uri"], "mysql://h/db");
    }

    #[test]
    fn connection_absent_is_none() {
        let create: CollectionCreate =
            serde_json::from_str(r#"{"name": "c", "description": "d"}"#).unwrap();
        assert!(create.connection.is_none());
    }
}
