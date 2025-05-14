//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{AtTime, CollectionId, CollectionName, Description, UserId, UserName};

#[td_type::Dao(sql_table = "collections")]
#[td_type(builder(try_from = CollectionDB))]
#[td_type(builder(try_from = CollectionCreate, skip_all))]
#[td_type(updater(try_from = RequestContext, skip_all))]
pub struct CollectionDB {
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
}

#[td_type::Dao(sql_table = "collections__with_names")]
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
#[td_type(builder(try_from = CollectionDB))]
#[td_type(updater(try_from = RequestContext, skip_all))]
pub struct CollectionUpdateDB {
    name: CollectionName,
    description: Description,
    #[td_type(updater(include, field = "time"))]
    modified_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    modified_by_id: UserId,
}

#[td_type::Dto]
#[td_type(builder(try_from = CollectionDBWithNames))]
pub struct CollectionRead {
    id: CollectionId,
    name: CollectionName,
    description: Description,
    created_on: AtTime,
    created_by_id: UserId,
    created_by: UserName,
    modified_on: AtTime,
    modified_by_id: UserId,
    modified_by: UserName,
}
