//
// Copyright 2025 Tabsdata Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::connection::KeyValue;
    use crate::dxo::crudl::RequestContext;
    use crate::types::basic::{
        AtTime, BasePath, CollectionId, CollectionName, CollectionType, CollectionWhenDeleted,
        ConnectionId, Description, GroupId, GroupName, ProjectId, ProjectName, UserId, UserName,
    };
    use td_common::id::Id;

    #[ta_objects::Dao]
    #[dao(sql_table = "collections")]
    #[dxo(
        builder(try_from = CollectionCreate, skip_all),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct CollectionCreateDB {
        #[dxo(extractor)]
        #[builder(default)]
        pub id: CollectionId,
        #[dxo(builder(include), extractor)]
        pub name: CollectionName,
        #[dxo(builder(include))]
        pub description: Description,
        #[dxo(updater(include, field = "time"))]
        pub created_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub created_by_id: UserId,
        #[dxo(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
        #[builder(default)]
        pub name_when_deleted: Option<CollectionName>,
        // Resolved from `CollectionCreate::group` (id-or-name) by the create service.
        #[dxo(setter)]
        pub group_id: GroupId,
        // Logical storage prefix, mapped from `CollectionCreate::base_path`.
        // `None` (omitted) inherits the owning project's base_path at stamp time.
        #[dxo(builder(include))]
        #[builder(default)]
        pub base_path: Option<BasePath>,
        // Inferred from the resolved group by the create service.
        #[dxo(setter)]
        pub collection_type: CollectionType,
        // The connection minted with the collection (source/destination only).
        #[dxo(setter)]
        #[builder(default)]
        pub connection_id: Option<ConnectionId>,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "collections_active")]
    #[dxo(builder(try_from = CollectionCreateDB))]
    pub struct ActiveCollectionDB {
        #[dxo(extractor)]
        pub id: CollectionId,
        /// The project that owns this collection. Read from the DB column when
        /// SELECT'd so downstream constructions (functions, runs, workers) can
        /// propagate it. Set by RLS on INSERTs.
        #[dxo(extractor, builder(skip))]
        pub project_id: ProjectId,
        #[dxo(extractor)]
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
        #[dxo(extractor, builder(skip))]
        pub effective_base_path: BasePath,
        #[dxo(extractor)]
        pub collection_type: CollectionType,
        /// The connection minted with this collection (source/destination
        /// only — they are 1:1 with it).
        #[dxo(builder(skip))]
        #[builder(default)]
        pub connection_id: Option<ConnectionId>,
    }

    // Canonical read/join surface: the `collections_resolved` view — every row,
    // including soft-deleted, with the real name resolved. Same columns as
    // `ActiveCollectionDB` plus the `deleted` flag; resolve a collection
    // referenced by an entity that can outlive it through this type.
    #[ta_objects::Dao]
    #[dao(sql_table = "collections_resolved")]
    #[inherits(ActiveCollectionDB)]
    pub struct CollectionDB {
        pub deleted: CollectionWhenDeleted,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "collections")]
    #[dxo(
        builder(try_from = ActiveCollectionDB, skip_all),
        updater(try_from = RequestContext, skip_all),
    )]
    pub struct CollectionDeleteDB {
        #[builder(default = "generate_deleted_name()")]
        pub name: CollectionName,

        #[dxo(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,

        #[dxo(builder(include, field = "name"))]
        pub name_when_deleted: CollectionName,
    }

    fn generate_deleted_name() -> CollectionName {
        CollectionName::try_from(format!("deleted_{}", Id::default())).unwrap()
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "collections__with_names")]
    #[inherits(ActiveCollectionDB)]
    pub struct ActiveCollectionDBWithNames {
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
        #[dxo(extractor)]
        #[serde(default = "GroupName::default_group")]
        #[builder(default = "GroupName::default_group()")]
        pub group: GroupName,
        // Logical storage prefix for the collection's artifacts.
        // Omitted (`None`) inherits the owning project's base_path.
        #[serde(default)]
        #[builder(default)]
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
        #[builder(default)]
        pub connection: Option<serde_json::Value>,
        #[serde(default)]
        #[builder(default)]
        pub secrets: Option<Vec<KeyValue>>,
        #[serde(default)]
        #[builder(default)]
        pub vars: Option<Vec<KeyValue>>,
    }

    #[ta_objects::Dto]
    pub struct CollectionUpdate {
        pub name: Option<CollectionName>,
        pub description: Option<Description>,
        // Tri-state (the Dto macro injects the serde deserializer): absent -> None
        // (unchanged); `null` -> Some(None) (clear -> inherit project); value ->
        // Some(Some(x)) (set).
        #[builder(default)]
        pub base_path: Option<Option<BasePath>>,
    }

    #[ta_objects::Dao]
    #[dao(update, sql_table = "collections")]
    #[dxo(
        builder(try_from = ActiveCollectionDB),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct CollectionUpdateDB {
        pub name: CollectionName,
        pub description: Description,
        #[dxo(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
        // DAO side of `CollectionUpdate::base_path`'s tri-state (here the outer
        // `None` skips the column). Seeded from the current `ActiveCollectionDB`,
        // overridden by the update layer.
        pub base_path: Option<Option<BasePath>>,
    }

    #[ta_objects::Dto]
    #[dto(list(on = ActiveCollectionDBWithNames))]
    #[inherits(ActiveCollectionDBWithNames)]
    pub struct CollectionRead {
        // Disambiguates same-named collections across projects in wildcard listings.
        #[dto(list(filter, filter_like, order_by))]
        pub project: ProjectName,
        #[dto(list(pagination_by = "+"))]
        pub id: CollectionId,
        #[dto(list(filter, filter_like, order_by))]
        pub name: CollectionName,
        #[dto(list(filter, filter_like, order_by))]
        pub description: Description,
        #[dto(list(filter, filter_like, order_by))]
        pub group: GroupName,
        // `None` means the collection inherits its project's base_path.
        pub base_path: Option<BasePath>,
        #[dto(list(filter, order_by))]
        pub collection_type: CollectionType,
        pub connection_id: Option<ConnectionId>,
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
