//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::poll_execution_requirements::poll_execution_requirements;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::datasets::dao::DsReadyToExecute;
use td_objects::dlo::Limit;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct PollDatasetsService {
    provider: ServiceProvider<(), Vec<DsReadyToExecute>, TdError>,
}

impl PollDatasetsService {
    /// Creates a new instance of [`PollDatasetsService`].
    pub fn new(db: DbPool) -> Self {
        Self {
            provider: Self::provider(db.clone()),
        }
    }

    p! {
        provider(db: DbPool) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(Arc::new(Limit::new(10))),
                ConnectionProvider::new(db),
                from_fn(poll_execution_requirements),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<(), Vec<DsReadyToExecute>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::datasets::service::execution::create_plan::CreatePlanService;
    use crate::logic::datasets::service::execution::schedule::tests::td_uri;
    use td_objects::crudl::RequestContext;
    use td_objects::datasets::dto::ExecutionPlanWriteBuilder;
    use td_objects::rest_urls::FunctionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_user::seed_user;
    use td_storage::location::StorageLocation;
    use td_tower::ctx_service::RawOneshot;
    use td_transaction::TransactionBy;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_poll_service() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = PollDatasetsService::provider(db.clone());
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<(), Vec<DsReadyToExecute>>(&[type_of_val(
            &poll_execution_requirements,
        )]);
    }

    #[tokio::test]
    async fn test_single_dataset() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;

        let request = RequestContext::with(user_id, "r", false).await.create(
            FunctionParam::new("ds0", "d0"),
            ExecutionPlanWriteBuilder::default()
                .name("test".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();

        let provider = PollDatasetsService::provider(db.clone());
        let service = provider.make().await;

        let response: Vec<DsReadyToExecute> = service.raw_oneshot(()).await.unwrap();
        assert_eq!(response.len(), 1);

        let ds = response.first().unwrap();
        assert_eq!(ds.collection_id(), &collection_id.to_string());
        assert_eq!(ds.collection_name().as_str(), "ds0");
        assert_eq!(ds.dataset_id(), &d0.to_string());
        assert_eq!(ds.dataset_name().as_str(), "d0");

        assert_eq!(ds.collection_id(), &collection_id.to_string());
        assert_eq!(ds.collection_name().as_str(), "ds0");
        assert_eq!(ds.dataset_id(), &d0.to_string());
        assert_eq!(ds.dataset_name().as_str(), "d0");
        assert_eq!(ds.function_id(), &f0.to_string());

        assert_eq!(ds.storage_location_version(), &StorageLocation::current());
    }

    #[tokio::test]
    async fn test_multiple_datasets() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;

        let (_d1, _f1) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[td_uri(&collection_id, &d0, Some("t0"), Some("HEAD"))],
            &[td_uri(&collection_id, &d0, None, None)],
            "hash",
        )
        .await;

        let request = RequestContext::with(user_id, "r", false).await.create(
            FunctionParam::new("ds0", "d0"),
            ExecutionPlanWriteBuilder::default()
                .name("test".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();

        let (d2, f2) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d2",
            &["t2"],
            &[td_uri(&collection_id, &d0, Some("t0"), Some("HEAD"))],
            &[],
            "hash",
        )
        .await;

        let request = RequestContext::with(user_id, "r", false).await.create(
            FunctionParam::new("ds0", "d0"),
            ExecutionPlanWriteBuilder::default()
                .name("test".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();

        let provider = PollDatasetsService::provider(db.clone());
        let service = provider.make().await;

        let response: Vec<DsReadyToExecute> = service.raw_oneshot(()).await.unwrap();
        assert_eq!(response.len(), 2);

        response.iter().for_each(|ds| {
            let dataset_name = ds.dataset_name();
            let (dataset_id, function_id) = match dataset_name.as_str() {
                "d0" => (&d0, &f0),
                "d2" => (&d2, &f2),
                _ => panic!("Unexpected dataset name: {}", dataset_name),
            };

            assert_eq!(ds.collection_id(), &collection_id.to_string());
            assert_eq!(ds.collection_name().as_str(), "ds0");
            assert_eq!(ds.dataset_id(), &dataset_id.to_string());
            assert_eq!(ds.dataset_name().as_str(), dataset_name);

            assert_eq!(ds.collection_id(), &collection_id.to_string());
            assert_eq!(ds.collection_name().as_str(), "ds0");
            assert_eq!(ds.dataset_id(), &dataset_id.to_string());
            assert_eq!(ds.dataset_name().as_str(), dataset_name);
            assert_eq!(ds.function_id(), &function_id.to_string());

            assert_eq!(ds.storage_location_version(), &StorageLocation::current());
        });
    }

    #[tokio::test]
    async fn test_no_datasets() {
        let db = td_database::test_utils::db().await.unwrap();

        let provider = PollDatasetsService::provider(db.clone());
        let service = provider.make().await;

        let response: Vec<DsReadyToExecute> = service.raw_oneshot(()).await.unwrap();
        assert!(response.is_empty());
    }

    #[tokio::test]
    async fn test_with_multiple_users() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id1 = seed_user(&db, None, "u1", true).await;
        let user_id2 = seed_user(&db, None, "u2", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
            &db,
            Some(user_id1.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;

        let (d1, f1) = seed_dataset(
            &db,
            Some(user_id2.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[],
            &[],
            "hash",
        )
        .await;

        let request = RequestContext::with(user_id1, "r", false).await.create(
            FunctionParam::new("ds0", "d0"),
            ExecutionPlanWriteBuilder::default()
                .name("test".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();
        let request = RequestContext::with(user_id2, "r", false).await.create(
            FunctionParam::new("ds0", "d1"),
            ExecutionPlanWriteBuilder::default()
                .name("test".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();

        let provider = PollDatasetsService::provider(db.clone());
        let service = provider.make().await;

        let response: Vec<DsReadyToExecute> = service.raw_oneshot(()).await.unwrap();
        assert_eq!(response.len(), 2);

        response.iter().for_each(|ds| {
            let dataset_name = ds.dataset_name();
            let (dataset_id, function_id) = match dataset_name.as_str() {
                "d0" => (&d0, &f0),
                "d1" => (&d1, &f1),
                _ => panic!("Unexpected dataset name: {}", dataset_name),
            };

            assert_eq!(ds.collection_id(), &collection_id.to_string());
            assert_eq!(ds.collection_name().as_str(), "ds0");
            assert_eq!(ds.dataset_id(), &dataset_id.to_string());
            assert_eq!(ds.dataset_name().as_str(), dataset_name);

            assert_eq!(ds.collection_id(), &collection_id.to_string());
            assert_eq!(ds.collection_name().as_str(), "ds0");
            assert_eq!(ds.dataset_id(), &dataset_id.to_string());
            assert_eq!(ds.dataset_name().as_str(), dataset_name);
            assert_eq!(ds.function_id(), &function_id.to_string());

            assert_eq!(ds.storage_location_version(), &StorageLocation::current());
        });
    }

    #[tokio::test]
    async fn test_datasets_with_same_dependencies() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, f0) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;

        let (d1, f1) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[td_uri(&collection_id, &d0, Some("t0"), Some("HEAD"))],
            &[],
            "hash",
        )
        .await;

        let request = RequestContext::with(user_id, "r", false).await.create(
            FunctionParam::new("ds0", "d0"),
            ExecutionPlanWriteBuilder::default()
                .name("test".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();
        let request = RequestContext::with(user_id, "r", false).await.create(
            FunctionParam::new("ds0", "d0"),
            ExecutionPlanWriteBuilder::default()
                .name("test".to_string())
                .build()
                .unwrap(),
        );
        let _ep = CreatePlanService::new(db.clone(), Arc::new(TransactionBy::default()))
            .service()
            .await
            .raw_oneshot(request)
            .await
            .unwrap();

        let provider = PollDatasetsService::provider(db.clone());
        let service = provider.make().await;

        let response: Vec<DsReadyToExecute> = service.raw_oneshot(()).await.unwrap();
        assert_eq!(response.len(), 2);

        response.iter().for_each(|ds| {
            let dataset_name = ds.dataset_name();
            let (dataset_id, function_id) = match dataset_name.as_str() {
                "d0" => (&d0, &f0),
                "d1" => (&d1, &f1),
                _ => panic!("Unexpected dataset name: {}", dataset_name),
            };

            assert_eq!(ds.collection_id(), &collection_id.to_string());
            assert_eq!(ds.collection_name().as_str(), "ds0");
            assert_eq!(ds.dataset_id(), &dataset_id.to_string());
            assert_eq!(ds.dataset_name().as_str(), dataset_name);

            assert_eq!(ds.collection_id(), &collection_id.to_string());
            assert_eq!(ds.collection_name().as_str(), "ds0");
            assert_eq!(ds.dataset_id(), &dataset_id.to_string());
            assert_eq!(ds.dataset_name().as_str(), dataset_name);
            assert_eq!(ds.function_id(), &function_id.to_string());

            assert_eq!(ds.storage_location_version(), &StorageLocation::current());
        });
    }
}
