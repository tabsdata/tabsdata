//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
pub mod defs {
    use crate::dxo::crudl::RequestContext;
    use crate::types::id::{CollectionId, UserId};
    use crate::types::string::{CollectionName, Description, UserName};
    use crate::types::timestamp::AtTime;
    use td_common::id::Id;

    #[td_type::Dao]
    #[dao(sql_table = "collections")]
    #[td_type(
        builder(try_from = CollectionCreate, skip_all),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct CollectionCreateDB {
        #[td_type(extractor)]
        #[builder(default)]
        pub id: CollectionId,
        #[td_type(builder(include), extractor)]
        pub name: CollectionName,
        #[td_type(builder(include))]
        pub description: Description,
        #[td_type(updater(include, field = "time"))]
        pub created_on: AtTime,
        #[td_type(updater(include, field = "user_id"))]
        pub created_by_id: UserId,
        #[td_type(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[td_type(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
        #[builder(default)]
        pub name_when_deleted: Option<CollectionName>,
    }

    #[td_type::Dao]
    #[dao(sql_table = "collections_active")]
    #[td_type(
        builder(try_from = CollectionCreateDB),
    )]
    pub struct CollectionDB {
        #[td_type(extractor)]
        pub id: CollectionId,
        #[td_type(extractor)]
        pub name: CollectionName,
        pub description: Description,
        pub created_on: AtTime,
        pub created_by_id: UserId,
        pub modified_on: AtTime,
        pub modified_by_id: UserId,
    }

    #[td_type::Dao]
    #[dao(sql_table = "collections")]
    #[td_type(
        builder(try_from = CollectionDB, skip_all),
        updater(try_from = RequestContext, skip_all),
    )]
    pub struct CollectionDeleteDB {
        #[builder(default = "generate_deleted_name()")]
        pub name: CollectionName,

        #[td_type(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[td_type(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,

        #[td_type(builder(include, field = "name"))]
        pub name_when_deleted: CollectionName,
    }

    fn generate_deleted_name() -> CollectionName {
        CollectionName::try_from(format!("deleted_{}", Id::default())).unwrap()
    }

    #[td_type::Dao]
    #[dao(sql_table = "collections__with_names")]
    #[inherits(CollectionDB)]
    pub struct CollectionDBWithNames {
        pub created_by: UserName,
        pub modified_by: UserName,
    }

    #[td_type::Dto]
    pub struct CollectionCreate {
        pub name: CollectionName,
        pub description: Description,
    }

    #[td_type::Dto]
    pub struct CollectionUpdate {
        pub name: Option<CollectionName>,
        pub description: Option<Description>,
    }

    #[td_type::Dao]
    #[dao(sql_table = "collections")]
    #[td_type(
        builder(try_from = CollectionDB),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct CollectionUpdateDB {
        pub name: CollectionName,
        pub description: Description,
        #[td_type(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[td_type(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
    }

    #[td_type::Dto]
    #[dto(list(on = CollectionDBWithNames))]
    #[td_type(builder(try_from = CollectionDBWithNames))]
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
