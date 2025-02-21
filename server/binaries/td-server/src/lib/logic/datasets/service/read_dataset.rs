//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::dataset_with_names_to_api::dataset_with_names_to_api;
use crate::logic::datasets::layer::find_collection_id::find_collection_id;
use crate::logic::datasets::layer::read_dataset_authorize::read_dataset_authorize;
use crate::logic::datasets::layer::read_to_collection_name::read_to_collection_name;
use crate::logic::datasets::layer::read_to_dataset_name::read_to_dataset_name;
use crate::logic::datasets::layer::select_dataset_with_names::select_dataset_with_names;
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::crudl::ReadRequest;
use td_objects::datasets::dto::*;
use td_objects::rest_urls::FunctionParam;
use td_tower::default_services::{ConnectionProvider, ServiceEntry, ServiceReturn, Share};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use tower::ServiceBuilder;

pub struct ReadDatasetService {
    provider: ServiceProvider<ReadRequest<FunctionParam>, DatasetRead, TdError>,
}

impl ReadDatasetService {
    /// Creates a new instance of [`ReadDatasetService`].
    pub fn new(db: DbPool) -> Self {
        ReadDatasetService {
            provider: Self::provider(db),
        }
    }

    fn provider<Req: Share, Res: Share>(db: DbPool) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(from_fn(read_dataset_authorize))
            .layer(from_fn(read_to_collection_name))
            .layer(from_fn(read_to_dataset_name))
            .layer(from_fn(find_collection_id))
            .layer(from_fn(select_dataset_with_names))
            .layer(from_fn(dataset_with_names_to_api))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn service(&self) -> TdBoxService<ReadRequest<FunctionParam>, DatasetRead, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::collections::service::tests::create_test_collections;
    use crate::logic::datasets::service::create_dataset::CreateDatasetService;
    use crate::logic::datasets::service::read_dataset::ReadDatasetService;
    use crate::logic::users::service::create_user::tests::create_test_users;
    use td_objects::crudl::RequestContext;
    use td_objects::dlo::CollectionName;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_read_provider() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ReadDatasetService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ReadRequest<FunctionParam>, DatasetRead>(&[
            type_of_val(&read_dataset_authorize),
            type_of_val(&read_to_collection_name),
            type_of_val(&read_to_dataset_name),
            type_of_val(&find_collection_id),
            type_of_val(&select_dataset_with_names),
            type_of_val(&dataset_with_names_to_api),
        ]);
    }

    #[tokio::test]
    async fn test_read() {
        let db = td_database::test_utils::db().await.unwrap();
        let users = create_test_users(&db, None, "u", 1, true).await;
        let collection = create_test_collections(&db, None, "ds", 1).await;

        let request = RequestContext::with(users[0].id(), "r", false)
            .await
            .create(
                CollectionName::new(collection[0].name()),
                DatasetWrite {
                    name: "d0".to_string(),
                    description: "D0".to_string(),
                    data_location: None,
                    bundle_hash: "hash".to_string(),
                    tables: vec!["t0".to_string()],
                    dependencies: vec![],
                    trigger_by: Some(vec![]),
                    function_snippet: None,
                },
            );

        let service = CreateDatasetService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        println!("{:#?}", response);

        let request = RequestContext::with(users[0].id(), "r", false)
            .await
            .read(FunctionParam::new("ds0", "d0"));

        let service = ReadDatasetService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        println!("{:#?}", response);
    }
}
