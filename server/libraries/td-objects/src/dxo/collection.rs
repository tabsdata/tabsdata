//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::types::basic::{
        AtTime, CollectionId, CollectionName, Description, UserId, UserName,
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
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "collections_active")]
    #[dxo(builder(try_from = CollectionCreateDB))]
    pub struct CollectionDB {
        #[dxo(extractor)]
        pub id: CollectionId,
        #[dxo(extractor)]
        pub name: CollectionName,
        pub description: Description,
        pub created_on: AtTime,
        pub created_by_id: UserId,
        pub modified_on: AtTime,
        pub modified_by_id: UserId,
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
    }

    #[ta_objects::Dto]
    pub struct CollectionCreate {
        pub name: CollectionName,
        pub description: Description,
    }

    #[ta_objects::Dto]
    pub struct CollectionUpdate {
        pub name: Option<CollectionName>,
        pub description: Option<Description>,
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
    }
}
