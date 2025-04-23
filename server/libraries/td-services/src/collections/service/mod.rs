//
// Copyright 2024 Tabs Data Inc.
//

use crate::collections::service::create_collection::CreateCollectionService;
use crate::collections::service::delete_collection::DeleteCollectionService;
use crate::collections::service::list_collections::ListCollectionsService;
use crate::collections::service::read_collection::ReadCollectionService;
use crate::collections::service::update_collection::UpdateCollectionService;
use std::sync::Arc;
use td_authz::AuthzContext;
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
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        Self {
            create_service_provider: CreateCollectionService::new(
                db.clone(),
                authz_context.clone(),
            ),
            read_service_provider: ReadCollectionService::new(db.clone(), authz_context.clone()),
            update_service_provider: UpdateCollectionService::new(
                db.clone(),
                authz_context.clone(),
            ),
            delete_service_provider: DeleteCollectionService::new(
                db.clone(),
                authz_context.clone(),
            ),
            list_service_provider: ListCollectionsService::new(db.clone(), authz_context),
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
    use crate::collections::service::CollectionServices;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_database::sql::DbPool;
    use td_objects::collections::dto::{CollectionCreate, CollectionRead};
    use td_objects::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
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
        let admin_user = creator_id
            .map(|id| UserId::try_from(id).unwrap())
            .unwrap_or(UserId::admin());
        let logic = CollectionServices::new(db.clone(), Arc::new(AuthzContext::default()));
        let mut collections = Vec::new();
        for i in 0..count {
            let name = format!("{}{}", name_prefix, i);
            let request = RequestContext::with(
                AccessTokenId::default(),
                admin_user,
                RoleId::sys_admin(),
                true,
            )
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
