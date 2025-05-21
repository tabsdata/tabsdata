//
// Copyright 2024 Tabs Data Inc.
//

use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse, RequestContext};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, NoPermissions, System};
use td_objects::tower_service::from::{ExtractService, With};
use td_objects::tower_service::sql::{By, SqlListService};
use td_objects::types::collection::CollectionRead;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ListCollectionsService {
    provider: ServiceProvider<ListRequest<()>, ListResponse<CollectionRead>, TdError>,
}

impl ListCollectionsService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        ListCollectionsService {
            provider: Self::provider(db, queries, authz_context),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>) {
            service_provider!(layers!(
                ConnectionProvider::new(db),
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(authz_context),
                from_fn(With::<ListRequest<()>>::extract::<RequestContext>),
                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<NoPermissions>::check), // no permission required

                from_fn(By::<()>::list::<(), DaoQueries, CollectionRead>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ListRequest<()>, ListResponse<CollectionRead>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_database::sql::DbPool;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::types::basic::{AccessTokenId, CollectionName, RoleId, UserId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_list_provider(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider =
            ListCollectionsService::provider(db, queries, Arc::new(AuthzContext::default()));
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ListRequest<()>, ListResponse<CollectionRead>>(&[
            type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<NoPermissions>::check), // no permission required
            type_of_val(&By::<()>::list::<(), DaoQueries, CollectionRead>),
        ]);
    }

    async fn test_list_collection(db: DbPool, admin: bool) {
        let name = CollectionName::try_from("ds0").unwrap();
        let _ = seed_collection(&db, &name, &UserId::admin()).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            admin,
        )
        .list((), ListParams::default());

        let service = ListCollectionsService::new(db, Arc::new(AuthzContext::default()))
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let list = response.unwrap();
        assert_eq!(list.len(), &1);
        assert_eq!(*list.data()[0].name(), name);
    }

    #[td_test::test(sqlx)]
    async fn test_list_collection_admin(db: DbPool) {
        test_list_collection(db, true).await;
    }

    #[td_test::test(sqlx)]
    async fn test_list_collection_non_admin(db: DbPool) {
        test_list_collection(db, false).await;
    }
}
