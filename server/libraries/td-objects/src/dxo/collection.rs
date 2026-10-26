//
// Copyright 2025 Tabsdata Inc.
//

//! A collection is an SCD2 timeline of versions; its id is what every
//! function, table and run points at. The live collections are the open
//! versions; a delete closes the timeline.

use crate::dxo::connection::KeyValue;
use crate::dxo::crudl::RequestContext;
use crate::dxo::scd2_interval::Scd2Interval;
use crate::types::basic::{
    AtTime, BasePath, CollectionId, CollectionIdName, CollectionLabel, CollectionName,
    CollectionType, CollectionVersionId, ConnectionId, Description, GroupId, GroupLabel, GroupName,
    ProjectId, ProjectLabel, Scd2ValidFrom, UserId, UserLabel,
};
use crate::types::label::Label;
use ta_objects::FromDao;
use td_error::TdError;

#[ta_objects::Dao]
#[dao(table = "collections", ops(insert), scd2, project_scoped)]
pub struct CollectionInsertDB {
    #[dao(pk)]
    pub id: CollectionVersionId,
    #[dao(partition_by)]
    pub collection_id: CollectionId,
    pub name: CollectionName,
    pub description: Description,
    // Logical storage prefix. `None` inherits the owning project's base_path
    // at stamp time.
    pub base_path: Option<BasePath>,
    // Fixed for the collection's life, from here down. Resolved from
    // `CollectionCreate::group` (id-or-name) by the create service.
    pub group_id: GroupId,
    // Inferred from the resolved group by the create service.
    pub collection_type: CollectionType,
    // The connection minted with the collection (source/destination only).
    pub connection_id: Option<ConnectionId>,
    pub defined_by_id: UserId,
}

#[ta_objects::Dao]
#[dao(
    table = "collections",
    ops(select, close),
    scd2,
    project_scoped(unique_at(CollectionName), unique_at(CollectionIdName))
)]
pub struct CollectionDB {
    #[dao(flatten)]
    pub base: CollectionInsertDB,
    pub project_id: ProjectId,
    #[dao(flatten)]
    pub interval: Scd2Interval,
}

impl CollectionDB {
    /// How a row names this version's collection: its name while this
    /// version is open, `[<id>]` once it is closed.
    pub fn label(&self) -> CollectionLabel {
        if self.interval.valid_to.is_infinity() {
            CollectionLabel::new(self.base.name.clone())
        } else {
            CollectionLabel::new(Label::Deleted(self.base.collection_id))
        }
    }

    /// The version an update opens: this one's values with the update's
    /// changes on top.
    pub fn updated(&self, update: &CollectionUpdate, ctx: &RequestContext) -> CollectionInsertDB {
        CollectionInsertDB {
            id: CollectionVersionId::default(),
            name: update
                .name
                .clone()
                .unwrap_or_else(|| self.base.name.clone()),
            description: update
                .description
                .clone()
                .unwrap_or_else(|| self.base.description.clone()),
            base_path: update
                .base_path
                .clone()
                .unwrap_or_else(|| self.base.base_path.clone()),
            defined_by_id: ctx.user_id,
            ..self.base.clone()
        }
    }
}

#[ta_objects::Dao]
#[dao(
    view = "collections__with_names",
    scd2,
    project_scoped(unique_at(CollectionName), unique_at(CollectionIdName))
)]
pub struct CollectionDBWithNames {
    #[dao(flatten)]
    pub base: CollectionDB,
    // Resolved prefix (COALESCE of own base_path, else project's). Read-only;
    // stamp it at register/upload/download.
    pub effective_base_path: BasePath,
    pub project: ProjectLabel,
    pub defined_by: UserLabel,
    pub group: GroupLabel,
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
    /// The connection pair — REQUIRED for source/destination collections
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

#[ta_objects::Dto]
#[dto(list(on = CollectionDBWithNames))]
pub struct CollectionRead {
    #[dto(list(pagination_by = "+"))]
    pub id: CollectionVersionId,
    #[dto(list(filter))]
    pub collection_id: CollectionId,
    #[dto(list(filter))]
    pub project_id: ProjectId,
    #[dto(list(filter, filter_like, order_by))]
    pub name: CollectionName,
    #[dto(list(filter, filter_like, order_by))]
    pub description: Description,
    pub group_id: GroupId,
    pub base_path: Option<BasePath>,
    pub effective_base_path: BasePath,
    #[dto(list(filter, order_by))]
    pub collection_type: CollectionType,
    pub connection_id: Option<ConnectionId>,
    #[dto(list(filter, order_by, col = Scd2ValidFrom))]
    pub defined_on: AtTime,
    pub defined_by_id: UserId,
    // Disambiguates same-named collections across projects in wildcard listings.
    #[dto(list(filter, filter_like, order_by))]
    pub project: ProjectLabel,
    #[dto(list(filter, filter_like, order_by))]
    pub defined_by: UserLabel,
    #[dto(list(filter, filter_like, order_by))]
    pub group: GroupLabel,
}

impl FromDao for CollectionRead {
    type Dao = CollectionDBWithNames;

    fn from_dao(dao: CollectionDBWithNames) -> Result<Self, TdError> {
        let CollectionDBWithNames {
            base:
                CollectionDB {
                    base,
                    project_id,
                    interval,
                },
            effective_base_path,
            project,
            defined_by,
            group,
        } = dao;
        Ok(Self {
            id: base.id,
            collection_id: base.collection_id,
            project_id,
            name: base.name,
            description: base.description,
            group_id: base.group_id,
            base_path: base.base_path,
            effective_base_path,
            collection_type: base.collection_type,
            connection_id: base.connection_id,
            defined_on: interval.valid_from.into(),
            defined_by_id: base.defined_by_id,
            project,
            defined_by,
            group,
        })
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
