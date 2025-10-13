//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::create::CreateCollectionService;
use crate::collection::service::delete::DeleteCollectionService;
use crate::collection::service::list::ListCollectionsService;
use crate::collection::service::read::ReadCollectionService;
use crate::collection::service::update::UpdateCollectionService;
use getset::Getters;
use ta_services::factory::ServiceFactory;

mod create;
mod delete;
mod layer;
mod list;
mod read;
mod update;

#[cfg(test)]
mod test_errors;

#[derive(ServiceFactory, Getters)]
#[getset(get = "pub")]
pub struct CollectionServices {
    create: CreateCollectionService,
    read: ReadCollectionService,
    update: UpdateCollectionService,
    delete: DeleteCollectionService,
    list: ListCollectionsService,
}
