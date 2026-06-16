//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::connection::KeyValue;
    use crate::dxo::crudl::RequestContext;
    use crate::types::basic::{
        AtTime, BasePath, CollectionId, CollectionName, CollectionType, ConnectionId, Description,
        GroupId, GroupName, ProjectId, UserId, UserName,
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
        // Logical storage prefix, mapped from `CollectionCreate::base_path` (defaults `/`).
        #[dxo(builder(include))]
        #[builder(default)]
        pub base_path: BasePath,
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
    pub struct CollectionDB {
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
        #[dxo(extractor)]
        pub base_path: BasePath,
        #[dxo(extractor)]
        pub collection_type: CollectionType,
        /// The connection minted with this collection (source/destination
        /// only — they are 1:1 with it).
        #[dxo(builder(skip))]
        #[builder(default)]
        pub connection_id: Option<ConnectionId>,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "collections")]
    #[dxo(
        builder(try_from = CollectionDB, skip_all),
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
    #[inherits(CollectionDB)]
    pub struct CollectionDBWithNames {
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
        // Logical storage prefix for the collection's artifacts. Defaults to `/`.
        #[serde(default)]
        #[builder(default)]
        pub base_path: BasePath,
        /// The connection triple — REQUIRED for source/destination collections
        /// (they are 1:1 with their connection and cannot exist without it),
        /// forbidden for a DataFactory.
        #[schema(value_type = Object)]
        #[serde(default)]
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
        #[serde(default)]
        #[builder(default)]
        pub base_path: Option<BasePath>,
    }

    #[ta_objects::Dao]
    #[dao(update, sql_table = "collections")]
    #[dxo(
        builder(try_from = CollectionDB),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct CollectionUpdateDB {
        pub name: CollectionName,
        pub description: Description,
        #[dxo(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
        // Seeded from the current `CollectionDB`; overridden by the update layer when
        // `CollectionUpdate::base_path` is provided.
        pub base_path: BasePath,
    }

    #[ta_objects::Dto]
    #[dto(list(on = CollectionDBWithNames))]
    #[inherits(CollectionDBWithNames)]
    pub struct CollectionRead {
        #[dto(list(pagination_by = "+"))]
        pub id: CollectionId,
        #[dto(list(filter, filter_like, order_by))]
        pub name: CollectionName,
        #[dto(list(filter, filter_like, order_by))]
        pub description: Description,
        #[dto(list(filter, filter_like, order_by))]
        pub group: GroupName,
        #[dto(list(filter, filter_like, order_by))]
        pub base_path: BasePath,
        #[dto(list(filter, order_by))]
        pub collection_type: CollectionType,
        pub connection_id: Option<ConnectionId>,
    }
}
