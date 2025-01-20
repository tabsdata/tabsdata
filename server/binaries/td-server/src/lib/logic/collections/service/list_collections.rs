//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::collections::layers::{list_collections_authorize, list_collections_sql_select};
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::collections::dao::CollectionWithNames;
use td_objects::collections::dto::{CollectionList, CollectionRead};
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::tower_service::mapper::map_list;
use td_tower::default_services::{ConnectionProvider, ServiceEntry, ServiceReturn, Share};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tower::util::BoxService;
use tower::ServiceBuilder;

pub struct ListCollectionsService {
    provider: ServiceProvider<ListRequest<()>, ListResponse<CollectionList>, TdError>,
}

impl ListCollectionsService {
    pub fn new(db: DbPool) -> Self {
        ListCollectionsService {
            provider: Self::provider(db),
        }
    }

    fn provider<Req: Share, Res: Share>(db: DbPool) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(from_fn(list_collections_authorize))
            .layer(from_fn(list_collections_sql_select))
            .layer(from_fn(map_list::<(), CollectionWithNames, CollectionRead>))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(
        &self,
    ) -> BoxService<ListRequest<()>, ListResponse<CollectionList>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::logic::collections::service::list_collections::ListCollectionsService;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_user::admin_user;
    use tower::ServiceExt;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_list_provider() {
        use crate::logic::collections::layers::{
            list_collections_authorize, list_collections_sql_select,
        };
        use crate::logic::collections::service::list_collections::ListCollectionsService;
        use td_objects::collections::dao::CollectionWithNames;
        use td_objects::collections::dto::CollectionList;
        use td_objects::collections::dto::CollectionRead;
        use td_objects::crudl::{ListRequest, ListResponse};
        use td_objects::tower_service::mapper::map_list;
        use td_tower::metadata::*;
        use tower::ServiceExt;

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ListCollectionsService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ListRequest<()>, ListResponse<CollectionList>>(&[
            type_of_val(&list_collections_authorize),
            type_of_val(&list_collections_sql_select),
            type_of_val(&map_list::<(), CollectionWithNames, CollectionRead>),
        ]);
    }

    async fn test_list_collection(admin: bool) {
        let db = td_database::test_utils::db().await.unwrap();
        let admin_id = admin_user(&db).await;
        seed_collection(&db, None, "ds0").await;

        let service = ListCollectionsService::new(db).service().await;

        let request = RequestContext::with(&admin_id, "r", admin)
            .await
            .list((), ListParams::default());

        let response = service.oneshot(request).await;
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
