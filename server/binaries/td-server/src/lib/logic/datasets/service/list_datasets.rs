//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::find_collection_id::find_collection_id;
use crate::logic::datasets::layer::list_authorize::list_authorize;
use crate::logic::datasets::layer::list_datasets_sql::list_datasets_sql;
use crate::logic::datasets::layer::list_result_to_response::list_result_to_response;
use crate::logic::datasets::layer::list_to_collection_name::list_to_collection_name;
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::datasets::dto::*;
use td_objects::dlo::CollectionName;
use td_tower::default_services::{ConnectionProvider, ServiceEntry, ServiceReturn, Share};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider};
use tower::util::BoxService;
use tower::ServiceBuilder;

pub struct ListDatasetsService {
    provider: ServiceProvider<ListRequest<CollectionName>, ListResponse<DatasetList>, TdError>,
}

impl ListDatasetsService {
    /// Creates a new instance of [`ListDatasetsService`].
    pub fn new(db: DbPool) -> Self {
        Self {
            provider: Self::provider(db),
        }
    }

    fn provider<Req: Share, Res: Share>(db: DbPool) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(from_fn(list_authorize))
            .layer(from_fn(list_to_collection_name))
            .layer(from_fn(find_collection_id))
            .layer(from_fn(list_datasets_sql))
            .layer(from_fn(list_result_to_response))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(
        &self,
    ) -> BoxService<ListRequest<CollectionName>, ListResponse<DatasetList>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::{ListParams, ListResponse, RequestContext};
    use td_objects::dlo::CollectionName;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_user::seed_user;
    use tower::ServiceExt;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_read_provider() {
        use crate::logic::datasets::layer::find_collection_id::find_collection_id;
        use crate::logic::datasets::layer::list_authorize::list_authorize;
        use crate::logic::datasets::layer::list_datasets_sql::list_datasets_sql;
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ListDatasetsService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ListRequest<CollectionName>, ListResponse<DatasetList>>(&[
            type_of_val(&list_authorize),
            type_of_val(&list_to_collection_name),
            type_of_val(&find_collection_id),
            type_of_val(&list_datasets_sql),
            type_of_val(&list_result_to_response),
        ]);
    }

    #[tokio::test]
    async fn test_list() {
        let db = td_database::test_utils::db().await.unwrap();
        let creator_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (_dataset_id, _function_id) = seed_dataset(
            &db,
            Some(creator_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;
        let (_dataset_id, _function_id) = seed_dataset(
            &db,
            Some(creator_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[],
            &[],
            "hash",
        )
        .await;

        let service = ListDatasetsService::new(db).service().await;

        let request = RequestContext::with(&creator_id.to_string(), "r", false)
            .await
            .list(CollectionName::new("ds0"), ListParams::default());
        let response: ListResponse<DatasetList> = service.oneshot(request).await.unwrap();
        assert_eq!(*response.len(), 2);
    }
}
