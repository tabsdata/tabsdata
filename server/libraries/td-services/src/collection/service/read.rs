//
// Copyright 2024 Tabs Data Inc.
//

use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::CollectionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, NoPermissions, System};
use td_objects::tower_service::from::{
    BuildService, ExtractNameService, ExtractService, TryIntoService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::CollectionIdName;
use td_objects::types::collection::{CollectionDBWithNames, CollectionRead, CollectionReadBuilder};
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ReadCollectionService {
    provider: ServiceProvider<ReadRequest<CollectionParam>, CollectionRead, TdError>,
}

impl ReadCollectionService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        ReadCollectionService {
            provider: Self::provider(db, queries, authz_context),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>) {
            service_provider!(layers!(
                ConnectionProvider::new(db),
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(authz_context),
                from_fn(With::<ReadRequest<CollectionParam>>::extract::<RequestContext>),
                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<NoPermissions>::check), // no permission required

                from_fn(With::<ReadRequest<CollectionParam>>::extract_name::<CollectionParam>),
                from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
                from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDBWithNames>),
                from_fn(With::<CollectionDBWithNames>::convert_to::<CollectionReadBuilder, _>),
                from_fn(With::<CollectionReadBuilder>::build::<CollectionRead, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ReadRequest<CollectionParam>, CollectionRead, TdError> {
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
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, CollectionName, Description, RoleId, UserId, UserName,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_read_provider(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider =
            ReadCollectionService::provider(db, queries, Arc::new(AuthzContext::default()));
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ReadRequest<CollectionParam>, CollectionRead>(&[
            type_of_val(&With::<ReadRequest<CollectionParam>>::extract::<RequestContext>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<NoPermissions>::check), // no permission required
            type_of_val(&With::<ReadRequest<CollectionParam>>::extract_name::<CollectionParam>),
            type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
            type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDBWithNames>),
            type_of_val(&With::<CollectionDBWithNames>::convert_to::<CollectionReadBuilder, _>),
            type_of_val(&With::<CollectionReadBuilder>::build::<CollectionRead, _>),
        ]);
    }

    async fn test_read_collection(db: DbPool, admin: bool) {
        let before = AtTime::now().await;
        let name = CollectionName::try_from("ds0").unwrap();
        let _ = seed_collection(&db, &name, &UserId::admin()).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            admin,
        )
        .read(
            CollectionParam::builder()
                .try_collection(name.to_string())
                .unwrap()
                .build()
                .unwrap(),
        );

        let service = ReadCollectionService::new(db, Arc::new(AuthzContext::default()))
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let created = response.unwrap();

        assert_eq!(*created.name(), name);
        assert_eq!(*created.description(), Description::default());
        assert!(*created.created_on() >= before);
        assert_eq!(*created.created_by_id(), UserId::admin());
        assert_eq!(*created.created_by(), UserName::admin());
        assert_eq!(created.modified_on(), created.created_on());
        assert_eq!(*created.modified_by_id(), UserId::admin());
        assert_eq!(*created.modified_by(), UserName::admin());
    }

    #[td_test::test(sqlx)]
    async fn test_read_collection_admin(db: DbPool) {
        test_read_collection(db, true).await;
    }

    #[td_test::test(sqlx)]
    async fn test_read_collection_non_admin(db: DbPool) {
        test_read_collection(db, false).await;
    }
}
