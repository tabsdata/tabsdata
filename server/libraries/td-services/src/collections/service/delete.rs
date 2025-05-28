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
use td_objects::tower_service::from::{
    builder, BuildService, ExtractNameService, ExtractService, UpdateService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService, SqlUpdateService};
use td_objects::types::basic::{CollectionId, CollectionIdName};
use td_objects::types::collection::{CollectionDB, CollectionDeleteDB, CollectionDeleteDBBuilder};
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

                from_fn(builder::<CollectionDeleteDBBuilder>),
                from_fn(With::<RequestContext>::update::<CollectionDeleteDBBuilder, _>),
                from_fn(With::<CollectionDB>::update::<CollectionDeleteDBBuilder, _>),
                from_fn(With::<CollectionDeleteDBBuilder>::build::<CollectionDeleteDB, _>),
                from_fn(By::<CollectionId>::update::<DaoQueries, CollectionDeleteDB, CollectionDB>),

                // TODO logic delete collection functions (freezing all their tables)
                // TODO logic delete collection tables (freezing all the functions that use those tables)
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
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::types::basic::{AccessTokenId, CollectionName, RoleId, UserId};
    use td_objects::types::collection::{CollectionCreateDB, CollectionDB};
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
            type_of_val(&builder::<CollectionDeleteDBBuilder>),
            type_of_val(&With::<RequestContext>::update::<CollectionDeleteDBBuilder, _>),
            type_of_val(&With::<CollectionDB>::update::<CollectionDeleteDBBuilder, _>),
            type_of_val(&With::<CollectionDeleteDBBuilder>::build::<CollectionDeleteDB, _>),
            type_of_val(
                &By::<CollectionId>::update::<DaoQueries, CollectionDeleteDB, CollectionDB>,
            ),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_delete_collection(db: DbPool) {
        let name = CollectionName::try_from("ds0").unwrap();
        let collection = seed_collection(&db, &name, &UserId::admin()).await;

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
            .select_by::<CollectionDB>(&())
            .unwrap()
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(found.len(), 0);

        let res: CollectionCreateDB = DaoQueries::default()
            .select_by::<CollectionCreateDB>(&(collection.id()))
            .unwrap()
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(res.name_when_deleted().as_ref().unwrap(), collection.name());
    }
}
