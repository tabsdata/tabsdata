//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::collections::layers::{
    create_collection_authorize, create_collection_build_dao, create_collection_sql_insert,
};
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::collections::dao::CollectionWithNames;
use td_objects::collections::dto::{CollectionCreate, CollectionRead};
use td_objects::crudl::CreateRequest;
use td_objects::dlo::CollectionId;
use td_objects::tower_service::creator::new_id;
use td_objects::tower_service::extractor::{
    extract_req_dto, extract_req_is_admin, extract_req_time, extract_req_user_id,
};
use td_objects::tower_service::finder::find_by_id;
use td_objects::tower_service::mapper::map;
use td_tower::default_services::{ServiceEntry, ServiceReturn, Share, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use tower::ServiceBuilder;

pub struct CreateCollectionService {
    provider: ServiceProvider<CreateRequest<(), CollectionCreate>, CollectionRead, TdError>,
}

impl CreateCollectionService {
    pub fn new(db: DbPool) -> Self {
        CreateCollectionService {
            provider: Self::provider(db),
        }
    }

    fn provider<Req: Share, Res: Share>(db: DbPool) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(TransactionProvider::new(db))
            .layer(from_fn(
                extract_req_is_admin::<CreateRequest<(), CollectionCreate>>,
            ))
            .layer(from_fn(create_collection_authorize))
            .layer(from_fn(
                extract_req_time::<CreateRequest<(), CollectionCreate>>,
            ))
            .layer(from_fn(
                extract_req_user_id::<CreateRequest<(), CollectionCreate>>,
            ))
            .layer(from_fn(
                extract_req_dto::<CreateRequest<(), CollectionCreate>, (), CollectionCreate>,
            ))
            .layer(from_fn(new_id::<CollectionId>))
            .layer(from_fn(create_collection_build_dao))
            .layer(from_fn(create_collection_sql_insert))
            .layer(from_fn(find_by_id::<CollectionId, CollectionWithNames>))
            .layer(from_fn(map::<CollectionWithNames, CollectionRead>))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<CreateRequest<(), CollectionCreate>, CollectionRead, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use crate::logic::collections::service::create_collection::CreateCollectionService;
    use td_common::id::Id;
    use td_common::time::UniqueUtc;
    use td_objects::collections::dto::CollectionCreateBuilder;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_user::admin_user;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_create_service() {
        use crate::logic::collections::layers::{
            create_collection_authorize, create_collection_build_dao, create_collection_sql_insert,
        };
        use crate::logic::collections::service::create_collection::CreateCollectionService;
        use td_objects::collections::dao::CollectionWithNames;
        use td_objects::collections::dto::{CollectionCreate, CollectionRead};
        use td_objects::crudl::CreateRequest;
        use td_objects::dlo::CollectionId;
        use td_objects::tower_service::creator::new_id;
        use td_objects::tower_service::extractor::{
            extract_req_dto, extract_req_is_admin, extract_req_time, extract_req_user_id,
        };
        use td_objects::tower_service::finder::find_by_id;
        use td_objects::tower_service::mapper::map;
        use td_tower::metadata::{type_of_val, Metadata};
        let db = td_database::test_utils::db().await.unwrap();
        let provider = CreateCollectionService::provider(db);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<CreateRequest<(), CollectionCreate>, CollectionRead>(&[
            type_of_val(&extract_req_is_admin::<CreateRequest<(), CollectionCreate>>),
            type_of_val(&create_collection_authorize),
            type_of_val(&extract_req_time::<CreateRequest<(), CollectionCreate>>),
            type_of_val(&extract_req_user_id::<CreateRequest<(), CollectionCreate>>),
            type_of_val(
                &extract_req_dto::<CreateRequest<(), CollectionCreate>, (), CollectionCreate>,
            ),
            type_of_val(&new_id::<CollectionId>),
            type_of_val(&create_collection_build_dao),
            type_of_val(&create_collection_sql_insert),
            type_of_val(&find_by_id::<CollectionId, CollectionWithNames>),
            type_of_val(&map::<CollectionWithNames, CollectionRead>),
        ]);
    }

    #[tokio::test]
    async fn test_create_dataset() {
        let db = td_database::test_utils::db().await.unwrap();
        let admin_id = admin_user(&db).await;

        let service = CreateCollectionService::new(db.clone()).service().await;

        let create = CollectionCreateBuilder::default()
            .name("ds0")
            .description("DS0")
            .build()
            .unwrap();

        let before = UniqueUtc::now_millis()
            .await
            .naive_utc()
            .and_utc()
            .timestamp_millis();

        let request = RequestContext::with(&admin_id, "r", true)
            .await
            .create((), create);

        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let created = response.unwrap();

        assert!(Id::try_from(created.id()).is_ok());
        assert_eq!(created.name(), "ds0");
        assert_eq!(created.description(), "DS0");
        assert!(*created.created_on() >= before);
        assert_eq!(created.created_by_id(), &admin_id);
        assert_eq!(created.created_by(), "admin");
        assert_eq!(created.modified_on(), created.created_on());
        assert_eq!(created.modified_by_id(), &admin_id);
        assert_eq!(created.modified_by(), "admin");

        const SELECT: &str = "SELECT count(*) FROM collections WHERE name = ?1";

        let found: i64 = sqlx::query_scalar(SELECT)
            .bind("ds0".to_string())
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(found, 1);
    }
}
