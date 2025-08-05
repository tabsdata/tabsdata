//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::create::CreateCollectionService;
use crate::collection::service::delete::DeleteCollectionService;
use crate::collection::service::list::ListCollectionsService;
use crate::collection::service::read::ReadCollectionService;
use crate::collection::service::update::UpdateCollectionService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{
    CreateRequest, DeleteRequest, ListRequest, ListResponse, ReadRequest, UpdateRequest,
};
use td_objects::rest_urls::CollectionParam;
use td_objects::sql::DaoQueries;
use td_objects::types::collection::{CollectionCreate, CollectionRead, CollectionUpdate};
use td_tower::service_provider::TdBoxService;

mod create;
mod delete;
mod layer;
mod list;
mod read;
mod update;

#[cfg(test)]
mod test_errors;

pub struct CollectionServices {
    create_service_provider: CreateCollectionService,
    read_service_provider: ReadCollectionService,
    update_service_provider: UpdateCollectionService,
    delete_service_provider: DeleteCollectionService,
    list_service_provider: ListCollectionsService,
}

impl CollectionServices {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            create_service_provider: CreateCollectionService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
            read_service_provider: ReadCollectionService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
            update_service_provider: UpdateCollectionService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
            delete_service_provider: DeleteCollectionService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
            list_service_provider: ListCollectionsService::new(
                db.clone(),
                queries.clone(),
                authz_context.clone(),
            ),
        }
    }

    pub async fn create_collection(
        &self,
    ) -> TdBoxService<CreateRequest<(), CollectionCreate>, CollectionRead, TdError> {
        self.create_service_provider.service().await
    }

    pub async fn read_collection(
        &self,
    ) -> TdBoxService<ReadRequest<CollectionParam>, CollectionRead, TdError> {
        self.read_service_provider.service().await
    }

    pub async fn delete_collection(
        &self,
    ) -> TdBoxService<DeleteRequest<CollectionParam>, (), TdError> {
        self.delete_service_provider.service().await
    }

    pub async fn update_collection(
        &self,
    ) -> TdBoxService<UpdateRequest<CollectionParam, CollectionUpdate>, CollectionRead, TdError>
    {
        self.update_service_provider.service().await
    }

    pub async fn list_collections(
        &self,
    ) -> TdBoxService<ListRequest<()>, ListResponse<CollectionRead>, TdError> {
        self.list_service_provider.service().await
    }
}
