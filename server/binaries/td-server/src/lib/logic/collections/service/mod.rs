//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::collections::service::create_collection::CreateCollectionService;
use crate::logic::collections::service::delete_collection::DeleteCollectionService;
use crate::logic::collections::service::list_collections::ListCollectionsService;
use crate::logic::collections::service::read_collection::ReadCollectionService;
use crate::logic::collections::service::update_collection::UpdateCollectionService;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::collections::dto::{
    CollectionCreate, CollectionList, CollectionRead, CollectionUpdate,
};
use td_objects::crudl::{
    CreateRequest, DeleteRequest, ListRequest, ListResponse, ReadRequest, UpdateRequest,
};
use td_objects::rest_urls::CollectionParam;
use td_tower::service_provider::TdBoxService;

mod create_collection;
mod delete_collection;
mod list_collections;
mod read_collection;
mod update_collection;

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
    pub fn new(db: DbPool) -> Self {
        Self {
            create_service_provider: CreateCollectionService::new(db.clone()),
            read_service_provider: ReadCollectionService::new(db.clone()),
            update_service_provider: UpdateCollectionService::new(db.clone()),
            delete_service_provider: DeleteCollectionService::new(db.clone()),
            list_service_provider: ListCollectionsService::new(db.clone()),
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
    ) -> TdBoxService<ListRequest<()>, ListResponse<CollectionList>, TdError> {
        self.list_service_provider.service().await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::logic::collections::service::CollectionServices;
    use td_database::sql::DbPool;
    use td_objects::collections::dto::{CollectionCreate, CollectionRead};
    use td_objects::crudl::RequestContext;
    use td_tower::ctx_service::RawOneshot;

    /// Creates collections for tests.
    ///
    /// The collections are created with the name prefix and a number appended to it,
    /// full name is the name in uppercase, email is the name with `@test.com`,
    /// password is the name with `-password`.
    ///
    /// If creator_id is [`None`] the admin collection is used as the creator.
    pub async fn create_test_collections(
        db: &DbPool,
        creator_id: Option<String>,
        name_prefix: &str,
        count: usize,
    ) -> Vec<CollectionRead> {
        let (mut admin_user, admin_role) =
            td_database::test_utils::user_role_ids(db, td_security::ADMIN_USER).await;
        if let Some(creator_id) = creator_id {
            admin_user = creator_id;
        }
        let logic = CollectionServices::new(db.clone());
        let mut collections = Vec::new();
        for i in 0..count {
            let name = format!("{}{}", name_prefix, i);
            let request = RequestContext::with(&admin_user, &admin_role, true)
                .await
                .create(
                    (),
                    CollectionCreate::builder()
                        .name(&name)
                        .description(format!("{} description", name.to_uppercase()))
                        .build()
                        .unwrap(),
                );
            let service = logic.create_collection().await;
            let collection = service.raw_oneshot(request).await.unwrap();
            collections.push(collection);
        }
        collections
    }
}
