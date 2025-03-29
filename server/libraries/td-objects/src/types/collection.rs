//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{AtTime, CollectionId, CollectionName, Description, UserId};
use td_type::Dao;

#[Dao(sql_table = "collections")]
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
