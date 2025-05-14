//
// Copyright 2024 Tabs Data Inc.
//

use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{DeleteRequest, RequestContext};
use td_objects::rest_urls::CollectionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SysAdmin, System};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlDeleteService, SqlSelectIdOrNameService};
use td_objects::types::basic::{CollectionId, CollectionIdName};
use td_objects::types::collection::CollectionDB;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct DeleteCollectionService {
    provider: ServiceProvider<DeleteRequest<CollectionParam>, (), TdError>,
}

impl DeleteCollectionService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        DeleteCollectionService {
            provider: Self::provider(db, queries, authz_context),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>) {
            service_provider!(layers!(
                TransactionProvider::new(db),
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(authz_context),
                from_fn(With::<DeleteRequest<CollectionParam>>::extract::<RequestContext>),
                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<SysAdmin>::check),

                from_fn(With::<DeleteRequest<CollectionParam>>::extract_name::<CollectionParam>),
                from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
                from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),

                from_fn(With::<CollectionDB>::extract::<CollectionId>),
                // TODO delete permissions with this collection
                // from_fn(By::<CollectionId>::delete::<DaoQueries, PermissionDB>),
                from_fn(By::<CollectionId>::delete::<DaoQueries, CollectionDB>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<DeleteRequest<CollectionParam>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::sql::{DaoQueries, SelectBy};
    use td_objects::test_utils::seed_collection2::seed_collection;
    use td_objects::types::basic::{AccessTokenId, CollectionName, RoleId, UserId};
    use td_objects::types::collection::CollectionDB;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_delete_service(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider =
            DeleteCollectionService::provider(db, queries, Arc::new(AuthzContext::default()));
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<DeleteRequest<CollectionParam>, ()>(&[
            type_of_val(&With::<DeleteRequest<CollectionParam>>::extract::<RequestContext>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SysAdmin>::check),
            type_of_val(&With::<DeleteRequest<CollectionParam>>::extract_name::<CollectionParam>),
            type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
            type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
            type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
            // TODO delete permissions with this collection
            // type_of_val(&By::<CollectionId>::delete::<DaoQueries, PermissionDB>),
            type_of_val(&By::<CollectionId>::delete::<DaoQueries, CollectionDB>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_delete_collection(db: DbPool) {
        let name = CollectionName::try_from("ds0").unwrap();
        let _ = seed_collection(&db, &name, &UserId::admin()).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
            false,
        )
        .delete(
            CollectionParam::builder()
                .try_collection(name.to_string())
                .unwrap()
                .build()
                .unwrap(),
        );

        let service = DeleteCollectionService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());

        let found: Vec<CollectionDB> = DaoQueries::default()
            .select_by::<CollectionDB>(&(&name))
            .unwrap()
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(found.len(), 0);
    }
}
