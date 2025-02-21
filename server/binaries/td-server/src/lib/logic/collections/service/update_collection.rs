//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::collections::layers::{
    update_collection_authorize, update_collection_build_dao, update_collection_sql_update,
    update_collection_validate,
};
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::collections::dao::{Collection, CollectionWithNames};
use td_objects::collections::dto::{CollectionRead, CollectionUpdate};
use td_objects::crudl::UpdateRequest;
use td_objects::dlo::{CollectionId, CollectionName};
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::extractor::{
    extract_collection_id, extract_name, extract_req_dto, extract_req_is_admin, extract_req_time,
    extract_req_user_id,
};
use td_objects::tower_service::finder::{find_by_id, find_by_name};
use td_objects::tower_service::mapper::map;
use td_tower::default_services::{ServiceEntry, ServiceReturn, Share, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::TdBoxService;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tower::ServiceBuilder;

pub struct UpdateCollectionService {
    provider:
        ServiceProvider<UpdateRequest<CollectionParam, CollectionUpdate>, CollectionRead, TdError>,
}

impl UpdateCollectionService {
    pub fn new(db: DbPool) -> Self {
        UpdateCollectionService {
            provider: Self::provider(db),
        }
    }

    fn provider<Req: Share, Res: Share>(db: DbPool) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(TransactionProvider::new(db))
            .layer(from_fn(
                extract_req_is_admin::<UpdateRequest<CollectionParam, CollectionUpdate>>,
            ))
            .layer(from_fn(update_collection_authorize))
            .layer(from_fn(
                extract_name::<
                    UpdateRequest<CollectionParam, CollectionUpdate>,
                    CollectionParam,
                    CollectionName,
                >,
            ))
            .layer(from_fn(
                extract_req_time::<UpdateRequest<CollectionParam, CollectionUpdate>>,
            ))
            .layer(from_fn(
                extract_req_user_id::<UpdateRequest<CollectionParam, CollectionUpdate>>,
            ))
            .layer(from_fn(
                extract_req_dto::<
                    UpdateRequest<CollectionParam, CollectionUpdate>,
                    CollectionParam,
                    CollectionUpdate,
                >,
            ))
            .layer(from_fn(update_collection_validate))
            .layer(from_fn(find_by_name::<CollectionName, Collection>))
            .layer(from_fn(update_collection_build_dao))
            .layer(from_fn(extract_collection_id::<Collection>))
            .layer(from_fn(update_collection_sql_update))
            .layer(from_fn(find_by_id::<CollectionId, CollectionWithNames>))
            .layer(from_fn(map::<CollectionWithNames, CollectionRead>))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<UpdateRequest<CollectionParam, CollectionUpdate>, CollectionRead, TdError>
    {
        self.provider.make().await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::logic::collections::service::update_collection::UpdateCollectionService;
    use td_common::id::Id;
    use td_common::time::UniqueUtc;
    use td_objects::collections::dto::CollectionUpdateBuilder;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_user::admin_user;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_update_provider() {
        use crate::logic::collections::layers::update_collection_authorize;
        use crate::logic::collections::layers::update_collection_validate;
        use crate::logic::collections::layers::{
            update_collection_build_dao, update_collection_sql_update,
        };
        use crate::logic::collections::service::update_collection::UpdateCollectionService;
        use td_objects::collections::dao::{Collection, CollectionWithNames};
        use td_objects::collections::dto::CollectionRead;
        use td_objects::collections::dto::CollectionUpdate;
        use td_objects::crudl::UpdateRequest;
        use td_objects::dlo::CollectionId;
        use td_objects::dlo::CollectionName;
        use td_objects::tower_service::extractor::{extract_collection_id, extract_req_dto};
        use td_objects::tower_service::extractor::{
            extract_name, extract_req_is_admin, extract_req_time, extract_req_user_id,
        };
        use td_objects::tower_service::finder::find_by_id;
        use td_objects::tower_service::finder::find_by_name;
        use td_objects::tower_service::mapper::map;
        use td_tower::metadata::type_of_val;
        use td_tower::metadata::*;

        let db = td_database::test_utils::db().await.unwrap();
        let provider = UpdateCollectionService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata
            .assert_service::<UpdateRequest<CollectionParam, CollectionUpdate>, CollectionRead>(&[
                type_of_val(
                    &extract_req_is_admin::<UpdateRequest<CollectionParam, CollectionUpdate>>,
                ),
                type_of_val(&update_collection_authorize),
                type_of_val(
                    &extract_name::<
                        UpdateRequest<CollectionParam, CollectionUpdate>,
                        CollectionParam,
                        CollectionName,
                    >,
                ),
                type_of_val(&extract_req_time::<UpdateRequest<CollectionParam, CollectionUpdate>>),
                type_of_val(
                    &extract_req_user_id::<UpdateRequest<CollectionParam, CollectionUpdate>>,
                ),
                type_of_val(
                    &extract_req_dto::<
                        UpdateRequest<CollectionParam, CollectionUpdate>,
                        CollectionParam,
                        CollectionUpdate,
                    >,
                ),
                type_of_val(&update_collection_validate),
                type_of_val(&find_by_name::<CollectionName, Collection>),
                type_of_val(&update_collection_build_dao),
                type_of_val(&extract_collection_id::<Collection>),
                type_of_val(&update_collection_sql_update),
                type_of_val(&find_by_id::<CollectionId, CollectionWithNames>),
                type_of_val(&map::<CollectionWithNames, CollectionRead>),
            ]);
    }

    #[tokio::test]
    async fn test_update_collection() {
        let db = td_database::test_utils::db().await.unwrap();
        let admin_id = admin_user(&db).await;
        seed_collection(&db, None, "ds0").await;

        let before_update = UniqueUtc::now_millis()
            .await
            .naive_utc()
            .and_utc()
            .timestamp_millis();

        let service = UpdateCollectionService::new(db).service().await;

        let update = CollectionUpdateBuilder::default()
            .name("ds1")
            .description("DS1")
            .build()
            .unwrap();

        let request = RequestContext::with(&admin_id, "r", true)
            .await
            .update(CollectionParam::new("ds0"), update);

        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let updated = response.unwrap();

        assert!(Id::try_from(updated.id()).is_ok());
        assert_eq!(updated.name(), "ds1");
        assert_eq!(updated.description(), "DS1");
        assert!(*updated.created_on() < before_update);
        assert_eq!(updated.created_by_id(), &admin_id);
        assert_eq!(updated.created_by(), "admin");
        assert!(*updated.modified_on() > before_update);
        assert_eq!(updated.modified_by_id(), &admin_id);
        assert_eq!(updated.modified_by(), "admin");
    }
}
