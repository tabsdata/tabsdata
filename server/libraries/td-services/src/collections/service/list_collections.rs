//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::collections::layers::list_collections_sql_select;
use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::collections::dao::CollectionWithNames;
use td_objects::collections::dto::{CollectionList, CollectionRead};
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::tower_service::authz::{AuthzOn, NoPermissions, System};
use td_objects::tower_service::extractor::extract_req_context;
use td_objects::tower_service::mapper::map_list;
use td_tower::default_services::{
    ConnectionProvider, ServiceEntry, ServiceReturn, Share, SrvCtxProvider,
};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use tower::ServiceBuilder;

pub struct ListCollectionsService {
    provider: ServiceProvider<ListRequest<()>, ListResponse<CollectionList>, TdError>,
}

impl ListCollectionsService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        ListCollectionsService {
            provider: Self::provider(db, authz_context),
        }
    }

    fn provider<Req: Share, Res: Share>(
        db: DbPool,
        authz_context: Arc<AuthzContext>,
    ) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(SrvCtxProvider::new(authz_context))
            .layer(from_fn(extract_req_context::<ListRequest<()>>))
            .layer(from_fn(AuthzOn::<System>::set))
            .layer(from_fn(Authz::<NoPermissions>::check)) // no permission required
            .layer(from_fn(list_collections_sql_select))
            .layer(from_fn(map_list::<(), CollectionWithNames, CollectionRead>))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ListRequest<()>, ListResponse<CollectionList>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::logic::collections::service::list_collections::ListCollectionsService;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_list_provider() {
        use crate::logic::collections::layers::list_collections_sql_select;
        use crate::logic::collections::service::list_collections::ListCollectionsService;
        use td_authz::Authz;
        use td_objects::collections::dao::CollectionWithNames;
        use td_objects::collections::dto::CollectionList;
        use td_objects::collections::dto::CollectionRead;
        use td_objects::crudl::{ListRequest, ListResponse};
        use td_objects::tower_service::authz::{AuthzOn, NoPermissions, System};
        use td_objects::tower_service::extractor::extract_req_context;
        use td_objects::tower_service::mapper::map_list;
        use td_tower::metadata::*;
        let db = td_database::test_utils::db().await.unwrap();
        let provider = ListCollectionsService::provider(db, Arc::new(AuthzContext::default()));
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ListRequest<()>, ListResponse<CollectionList>>(&[
            type_of_val(&extract_req_context::<ListRequest<()>>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<NoPermissions>::check),
            type_of_val(&list_collections_sql_select),
            type_of_val(&map_list::<(), CollectionWithNames, CollectionRead>),
        ]);
    }

    async fn test_list_collection(admin: bool) {
        let db = td_database::test_utils::db().await.unwrap();
        seed_collection(&db, None, "ds0").await;

        let service = ListCollectionsService::new(db, Arc::new(AuthzContext::default()))
            .service()
            .await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            admin,
        )
        .list((), ListParams::default());

        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let list = response.unwrap();
        assert_eq!(list.len(), &1);

        assert_eq!(list.data()[0].name(), "ds0");
    }

    #[tokio::test]
    async fn test_list_collection_admin() {
        test_list_collection(true).await;
    }

    #[tokio::test]
    async fn test_list_collection_non_admin() {
        test_list_collection(false).await;
    }
}
