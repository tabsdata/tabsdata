//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{AtTime, CollectionId, CollectionName, Description, UserId, UserName};
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
    id: CollectionId,
    #[td_type(builder(include), extractor)]
    name: CollectionName,
    #[td_type(builder(include))]
    description: Description,
    #[td_type(updater(include, field = "time"))]
    created_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    created_by_id: UserId,
    #[td_type(updater(include, field = "time"))]
    modified_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    modified_by_id: UserId,
    #[builder(default)]
    name_when_deleted: Option<CollectionName>,
}

#[td_type::Dao]
#[dao(sql_table = "collections_active")]
#[td_type(
    builder(try_from = CollectionCreateDB),
)]
pub struct CollectionDB {
    #[td_type(extractor)]
    id: CollectionId,
    #[td_type(extractor)]
    name: CollectionName,
    description: Description,
    created_on: AtTime,
    created_by_id: UserId,
    modified_on: AtTime,
    modified_by_id: UserId,
}

fn generate_deleted_name() -> CollectionName {
    CollectionName::try_from(format!("deleted_{}", Id::default())).unwrap()
}

#[td_type::Dao]
#[dao(sql_table = "collections")]
#[td_type(
    updater(try_from = RequestContext, skip_all),
    updater(try_from = CollectionDB, skip_all)
)]
pub struct CollectionDeleteDB {
    #[builder(default = "generate_deleted_name()")]
    name: CollectionName,

    #[td_type(updater(try_from = RequestContext, include, field = "time"))]
    modified_on: AtTime,
    #[td_type(updater(try_from = RequestContext, include, field = "user_id"))]
    modified_by_id: UserId,

    #[td_type(updater(try_from = CollectionDB, include, field="name"))]
    name_when_deleted: CollectionName,
}

#[td_type::Dao]
#[dao(sql_table = "collections__with_names")]
pub struct CollectionDBWithNames {
    #[td_type(extractor)]
    #[builder(default)]
    id: CollectionId,
    #[td_type(extractor)]
    name: CollectionName,
    description: Description,
    created_on: AtTime,
    created_by_id: UserId,
    modified_on: AtTime,
    modified_by_id: UserId,

    created_by: UserName,
    modified_by: UserName,
}

#[td_type::Dto]
pub struct CollectionCreate {
    name: CollectionName,
    description: Description,
}

#[td_type::Dto]
pub struct CollectionUpdate {
    name: Option<CollectionName>,
    description: Option<Description>,
}

#[td_type::Dao]
#[dao(sql_table = "collections")]
#[td_type(
    builder(try_from = CollectionDB),
    updater(try_from = RequestContext, skip_all)
)]
pub struct CollectionUpdateDB {
    name: CollectionName,
    description: Description,
    #[td_type(updater(include, field = "time"))]
    modified_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    modified_by_id: UserId,
}

#[td_type::Dto]
#[dto(list(on = CollectionDBWithNames))]
#[td_type(builder(try_from = CollectionDBWithNames))]
pub struct CollectionRead {
    #[dto(list(pagination_by = "+"))]
    id: CollectionId,
    #[dto(list(filter, filter_like, order_by))]
    name: CollectionName,
    #[dto(list(filter, filter_like, order_by))]
    description: Description,
    created_on: AtTime,
    created_by_id: UserId,
    created_by: UserName,
    modified_on: AtTime,
    modified_by_id: UserId,
    modified_by: UserName,
}
