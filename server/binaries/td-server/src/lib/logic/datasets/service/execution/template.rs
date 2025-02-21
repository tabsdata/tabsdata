//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::layer::data_dependencies_graph_sql::data_dependencies_graph_sql;
use crate::logic::datasets::layer::deserialize_execution_template::deserialize_execution_template;
use crate::logic::datasets::layer::execution_template_exists::execution_template_exists;
use crate::logic::datasets::layer::execution_template_to_api::execution_template_to_api;
use crate::logic::datasets::layer::generate_execution_template::generate_execution_template;
use crate::logic::datasets::layer::insert_execution_template::update_execution_template;
use crate::logic::datasets::layer::read_dataset_authorize::read_dataset_authorize;
use crate::logic::datasets::layer::select_dataset_function::select_dataset_function;
use crate::logic::datasets::layer::triggers_graph_sql::triggers_graph_sql;
use crate::logic::datasets::layer::unwrap_execution_template::unwrap_execution_template;
use std::sync::Arc;
use td_common::error::TdError;
use td_database::sql::DbPool;
use td_objects::collections::dao::Collection;
use td_objects::crudl::ReadRequest;
use td_objects::datasets::dao::DatasetWithNames;
use td_objects::datasets::dto::*;
use td_objects::dlo::{CollectionId, CollectionName, DatasetName};
use td_objects::rest_urls::FunctionParam;
use td_objects::tower_service::extractor::{
    extract_collection_id, extract_dataset_id, extract_name,
};
use td_objects::tower_service::finder::{find_by_name, find_scoped_by_name};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{conditional, Do, Else, If, SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{l, layers, p, service, service_provider};
use td_transaction::TransactionBy;

pub struct TemplateService {
    provider: ServiceProvider<ReadRequest<FunctionParam>, ExecutionTemplateRead, TdError>,
}

impl TemplateService {
    /// Creates a new instance of [`TemplateService`].
    pub fn new(db: DbPool, transaction_by: Arc<TransactionBy>) -> Self {
        Self {
            provider: Self::provider(db, transaction_by),
        }
    }

    p! {
        provider(db: DbPool, transaction_by: Arc<TransactionBy>) -> TdError {
            service_provider!(layers!(
                TransactionProvider::new(db),
                from_fn(read_dataset_authorize),
                from_fn(extract_name::<ReadRequest<FunctionParam>, FunctionParam, CollectionName>),
                from_fn(find_by_name::<CollectionName, Collection>),
                from_fn(extract_collection_id::<Collection>),
                from_fn(extract_name::<ReadRequest<FunctionParam>, FunctionParam, DatasetName>),
                from_fn(find_scoped_by_name::<CollectionId, DatasetName, DatasetWithNames>),
                from_fn(extract_dataset_id::<DatasetWithNames>),
                Self::create_template(transaction_by),
                from_fn(execution_template_to_api),
            ))
        }
    }

    l! {
        create_template(transaction_by: Arc<TransactionBy>) -> TdError {
            layers!(
                SrvCtxProvider::new(transaction_by),
                conditional(
                    If(service!(layers!(
                        from_fn(select_dataset_function),
                        from_fn(deserialize_execution_template),
                        from_fn(execution_template_exists)
                    ))),
                    Do(service!(layers!(from_fn(unwrap_execution_template)))),
                    Else(service!(layers!(
                        from_fn(data_dependencies_graph_sql),
                        from_fn(triggers_graph_sql),
                        from_fn(generate_execution_template),
                        from_fn(update_execution_template),
                    )))
                ),
            )
        }
    }

    /// Returns a service that creates an execution template if it is absent and returns it
    /// for the given [`FunctionParam`].
    pub async fn service(
        &self,
    ) -> TdBoxService<ReadRequest<FunctionParam>, ExecutionTemplateRead, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::datasets::service::update_dataset::UpdateDatasetService;
    use itertools::Itertools;
    use td_common::uri::TdUri;
    use td_interceptor::execution::test_utils::TdUriFilter;
    use td_interceptor_api::execution::test_utils::FilterTriggered;
    use td_objects::crudl::RequestContext;
    use td_objects::datasets::dto::DatasetWrite;
    use td_objects::rest_urls::FunctionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_user::seed_user;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_read_provider() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = TemplateService::provider(db, Arc::new(TransactionBy::default()));
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ReadRequest<FunctionParam>, ExecutionTemplateRead>(&[
            type_of_val(&read_dataset_authorize),
            type_of_val(&extract_name::<ReadRequest<FunctionParam>, FunctionParam, CollectionName>),
            type_of_val(&find_by_name::<CollectionName, Collection>),
            type_of_val(&extract_collection_id::<Collection>),
            type_of_val(&extract_name::<ReadRequest<FunctionParam>, FunctionParam, DatasetName>),
            type_of_val(&find_scoped_by_name::<CollectionId, DatasetName, DatasetWithNames>),
            type_of_val(&extract_dataset_id::<DatasetWithNames>),
            // Create template service
            type_of_val(&select_dataset_function),
            type_of_val(&deserialize_execution_template),
            type_of_val(&execution_template_exists),
            type_of_val(&unwrap_execution_template),
            type_of_val(&data_dependencies_graph_sql),
            type_of_val(&triggers_graph_sql),
            type_of_val(&generate_execution_template),
            type_of_val(&update_execution_template),
            //
            type_of_val(&execution_template_to_api),
        ]);
    }

    async fn run_and_assert(
        db: &DbPool,
        user_id: &str,
        collection_name: &str,
        dataset_name: &str,
        triggered_datasets: &[TdUri],
    ) {
        let service_provider = TemplateService::new(db.clone(), Arc::new(TransactionBy::default()));

        let request = RequestContext::with(user_id, "r", false)
            .await
            .read(FunctionParam::new(collection_name, dataset_name));

        let service = service_provider.service().await;
        let response = service.raw_oneshot(request).await.unwrap();

        assert_eq!(response.collection_name(), collection_name);
        assert_eq!(response.dataset_name(), dataset_name);

        let triggered_datasets = TdUriFilter.filter(triggered_datasets.to_vec());
        assert_eq!(
            response
                .triggered_datasets()
                .iter()
                .sorted()
                .collect::<Vec<_>>(),
            triggered_datasets
                .iter()
                .map(|uri| { uri.to_string() })
                .sorted()
                .as_ref()
                .iter()
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn test_template_service() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, _function_id) = seed_dataset(
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

        let (d1, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[TdUri::new(
                &collection_id.to_string(),
                &d0.to_string(),
                Some("t0"),
                Some("HEAD"),
            )
            .unwrap()],
            &[TdUri::new(&collection_id.to_string(), &d0.to_string(), None, None).unwrap()],
            "hash",
        )
        .await;

        let (d2, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d2",
            &["t2"],
            &[TdUri::new(
                &collection_id.to_string(),
                &d0.to_string(),
                Some("t0"),
                Some("HEAD~1"),
            )
            .unwrap()],
            &[TdUri::new(&collection_id.to_string(), &d1.to_string(), None, None).unwrap()],
            "hash",
        )
        .await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d0",
            &[
                TdUri::new_with_ids(collection_id, d0, None, None),
                TdUri::new_with_ids(collection_id, d1, None, None),
                TdUri::new_with_ids(collection_id, d2, None, None),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_template_service_last_node() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, _function_id) = seed_dataset(
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

        let (d1, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[TdUri::new(
                &collection_id.to_string(),
                &d0.to_string(),
                Some("t0"),
                Some("HEAD"),
            )
            .unwrap()],
            &[TdUri::new(&collection_id.to_string(), &d0.to_string(), None, None).unwrap()],
            "hash",
        )
        .await;

        let (d2, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d2",
            &["t2"],
            &[TdUri::new(
                &collection_id.to_string(),
                &d0.to_string(),
                Some("t0"),
                Some("HEAD~1"),
            )
            .unwrap()],
            &[TdUri::new(&collection_id.to_string(), &d1.to_string(), None, None).unwrap()],
            "hash",
        )
        .await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d2",
            &[TdUri::new_with_ids(collection_id, d2, None, None)],
        )
        .await;
    }

    #[tokio::test]
    async fn test_template_service_disconnected_datasets() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, _function_id) = seed_dataset(
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

        let (d1, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[],
            &[],
            "hash",
        )
        .await;

        let (_d2, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d2",
            &["t2"],
            &[TdUri::new(
                &collection_id.to_string(),
                &d1.to_string(),
                Some("t0"),
                Some("HEAD~1"),
            )
            .unwrap()],
            &[TdUri::new(&collection_id.to_string(), &d1.to_string(), None, None).unwrap()],
            "hash",
        )
        .await;

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d0",
            &[TdUri::new_with_ids(collection_id, d0, None, None)],
        )
        .await;
    }

    #[tokio::test]
    async fn test_template_service_single_dataset() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, _function_id) = seed_dataset(
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

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d0",
            &[TdUri::new_with_ids(collection_id, d0, None, None)],
        )
        .await;
    }

    #[tokio::test]
    async fn test_template_service_cyclic_data_dependencies() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;

        let (d0, _function_id) = seed_dataset(
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

        let (d1, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[],
            &[TdUri::new(&collection_id.to_string(), &d0.to_string(), None, None).unwrap()],
            "hash",
        )
        .await;

        // TODO dont use update service to update, create seed method.
        let service = UpdateDatasetService::new(db.clone()).service().await;
        let update = DatasetWrite {
            name: "d0".to_string(),
            description: "D0".to_string(),
            data_location: None,
            bundle_hash: "hash".to_string(),
            tables: vec![],
            dependencies: vec!["t1@HEAD".to_string()],
            trigger_by: Some(vec![]),
            function_snippet: None,
        };
        let request = RequestContext::with(&user_id.to_string(), "r", false)
            .await
            .update(FunctionParam::new("ds0", "d0"), update);
        let _ = service.raw_oneshot(request).await.unwrap();

        run_and_assert(
            &db,
            &user_id.to_string(),
            "ds0",
            "d0",
            &[
                TdUri::new_with_ids(collection_id, d0, None, None),
                TdUri::new_with_ids(collection_id, d1, None, None),
            ],
        )
        .await;
    }
}
