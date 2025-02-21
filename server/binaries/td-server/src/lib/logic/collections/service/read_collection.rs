//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::collections::layers::read_collection_authorize;
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::collections::dao::CollectionWithNames;
use td_objects::collections::dto::CollectionRead;
use td_objects::crudl::ReadRequest;
use td_objects::dlo::CollectionName;
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::extractor::extract_name;
use td_objects::tower_service::finder::find_by_name;
use td_objects::tower_service::mapper::map;
use td_tower::default_services::{ConnectionProvider, ServiceEntry, ServiceReturn, Share};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use tower::ServiceBuilder;

pub struct ReadCollectionService {
    provider: ServiceProvider<ReadRequest<CollectionParam>, CollectionRead, TdError>,
}

impl ReadCollectionService {
    pub fn new(db: DbPool) -> Self {
        ReadCollectionService {
            provider: Self::provider(db),
        }
    }

    fn provider<Req: Share, Res: Share>(db: DbPool) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(from_fn(
                extract_name::<ReadRequest<CollectionParam>, CollectionParam, CollectionName>,
            ))
            .layer(from_fn(find_by_name::<CollectionName, CollectionWithNames>))
            .layer(from_fn(read_collection_authorize))
            .layer(from_fn(map::<CollectionWithNames, CollectionRead>))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ReadRequest<CollectionParam>, CollectionRead, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::logic::collections::service::read_collection::ReadCollectionService;
    use td_common::id::Id;
    use td_common::time::UniqueUtc;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_user::admin_user;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_read_provider() {
        use crate::logic::collections::layers::read_collection_authorize;
        use crate::logic::collections::service::read_collection::ReadCollectionService;
        use td_objects::collections::dao::CollectionWithNames;
        use td_objects::collections::dto::CollectionRead;
        use td_objects::crudl::ReadRequest;
        use td_objects::dlo::CollectionName;
        use td_objects::tower_service::extractor::extract_name;
        use td_objects::tower_service::finder::find_by_name;
        use td_objects::tower_service::mapper::map;
        use td_tower::metadata::*;

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ReadCollectionService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ReadRequest<CollectionParam>, CollectionRead>(&[
            type_of_val(
                &extract_name::<ReadRequest<CollectionParam>, CollectionParam, CollectionName>,
            ),
            type_of_val(&find_by_name::<CollectionName, CollectionWithNames>),
            type_of_val(&read_collection_authorize),
            type_of_val(&map::<CollectionWithNames, CollectionRead>),
        ]);
    }

    async fn test_read_collection(admin: bool) {
        let before = UniqueUtc::now_millis()
            .await
            .naive_utc()
            .and_utc()
            .timestamp_millis();

        let db = td_database::test_utils::db().await.unwrap();
        let admin_id = admin_user(&db).await;
        seed_collection(&db, None, "ds0").await;

        let service = ReadCollectionService::new(db).service().await;

        let request = RequestContext::with(&admin_id, "r", admin)
            .await
            .read(CollectionParam::new("ds0"));

        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let created = response.unwrap();

        assert!(Id::try_from(created.id()).is_ok());
        assert_eq!(created.name(), "ds0");
        assert_eq!(created.description(), "Description: ds0");
        assert!(*created.created_on() > before);
        assert_eq!(created.created_by_id(), &admin_id);
        assert_eq!(created.created_by(), "admin");
        assert_eq!(created.modified_on(), created.created_on());
        assert_eq!(created.modified_by_id(), &admin_id);
        assert_eq!(created.modified_by(), "admin");
    }

    #[tokio::test]
    async fn test_read_collection_admin() {
        test_read_collection(true).await;
    }

    #[tokio::test]
    async fn test_read_collection_non_admin() {
        test_read_collection(false).await;
    }
}
