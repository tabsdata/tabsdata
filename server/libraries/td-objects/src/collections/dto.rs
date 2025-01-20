//
// Copyright 2025 Tabs Data Inc.
//

use crate::collections::dao::CollectionWithNames;
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_utoipa::api_server_schema;

/// API: Payload for collection create.
#[api_server_schema]
#[derive(Debug, Clone, PartialEq, Deserialize, Getters, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct CollectionCreate {
    name: String,
    description: String,
}

impl CollectionCreate {
    pub fn builder() -> CollectionCreateBuilder {
        CollectionCreateBuilder::default()
    }
}

/// API: Payload for collection update.
#[api_server_schema]
#[derive(Debug, Clone, PartialEq, Default, Deserialize, Getters, Builder)]
#[builder(setter(into, strip_option), default)]
#[getset(get = "pub")]
pub struct CollectionUpdate {
    name: Option<String>,
    description: Option<String>,
}

impl CollectionUpdate {
    pub fn builder() -> CollectionUpdateBuilder {
        CollectionUpdateBuilder::default()
    }
}

/// API: Payload for collection get.
#[api_server_schema]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct CollectionRead {
    id: String,
    name: String,
    description: String,
    created_on: i64,
    created_by_id: String,
    created_by: String,
    modified_on: i64,
    modified_by_id: String,
    modified_by: String,
}

/// API: Payload for collection list.
pub type CollectionList = CollectionRead;

impl From<&CollectionWithNames> for CollectionRead {
    fn from(collection: &CollectionWithNames) -> Self {
        CollectionRead {
            id: collection.id().clone(),
            name: collection.name().clone(),
            description: collection.description().clone(),
            created_on: collection.created_on().timestamp_millis(),
            created_by_id: collection.created_by_id().clone(),
            created_by: collection.created_by().clone(),
            modified_on: collection.modified_on().timestamp_millis(),
            modified_by_id: collection.modified_by_id().clone(),
            modified_by: collection.modified_by().clone(),
        }
    }
}
