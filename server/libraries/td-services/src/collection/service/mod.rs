//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::connection::{ConnectionReadService, ConnectionUpsertService};
use crate::collection::service::create::CreateCollectionService;
use crate::collection::service::delete::DeleteCollectionService;
use crate::collection::service::list::ListCollectionsService;
use crate::collection::service::read::ReadCollectionService;
use crate::collection::service::update::UpdateCollectionService;
use ta_services::factory::ServiceFactory;

mod connection;
mod create;
mod delete;
mod layer;
mod list;
mod read;
pub mod update;

#[cfg(test)]
mod test_errors;

#[derive(ServiceFactory)]
pub struct CollectionServices {
    pub create: CreateCollectionService,
    pub read: ReadCollectionService,
    pub update: UpdateCollectionService,
    pub delete: DeleteCollectionService,
    pub list: ListCollectionsService,
    /// Create/update a source/destination collection's connection (SysAdmin).
    pub connection_upsert: ConnectionUpsertService,
    /// Read a source/destination collection's connection (names only).
    pub connection_read: ConnectionReadService,
}
