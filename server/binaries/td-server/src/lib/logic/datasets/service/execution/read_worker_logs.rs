//
// Copyright 2025 Tabs Data Inc.
//

use crate::logic::datasets::layer::get_worker_logs::get_worker_logs;
use crate::logic::datasets::layer::resolve_worker_log_path::resolve_worker_log_path;
use crate::logic::datasets::layer::select_ds_worker_message::select_ds_worker_message;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::datasets::dlo::BoxedSyncStream;
use td_objects::dlo::WorkerMessageId;
use td_objects::rest_urls::WorkerMessageParam;
use td_objects::tower_service::extractor::extract_name;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ReadWorkerLogsService {
    provider: ServiceProvider<ReadRequest<WorkerMessageParam>, BoxedSyncStream, TdError>,
}

impl ReadWorkerLogsService {
    /// Creates a new instance of [`ReadWorkerLogsService`].
    pub fn new(db: DbPool) -> Self {
        Self {
            provider: Self::provider(db.clone()),
        }
    }

    p! {
        provider(db: DbPool) -> TdError {
            service_provider!(layers!(
                ConnectionProvider::new(db),
                from_fn(extract_name::<ReadRequest<WorkerMessageParam>, WorkerMessageParam, WorkerMessageId>),
                from_fn(select_ds_worker_message),
                from_fn(resolve_worker_log_path),
                from_fn(get_worker_logs),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ReadRequest<WorkerMessageParam>, BoxedSyncStream, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use td_common::execution_status::TransactionStatus;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_data_version::seed_data_version;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_execution_plan::seed_execution_plan;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::test_utils::seed_worker_message::seed_worker_message;
    use td_objects::types::basic::{AccessTokenId, RoleId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_read_worker_logs_service() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = ReadWorkerLogsService::provider(db);
        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<ReadRequest<WorkerMessageParam>, BoxedSyncStream>(&[
            type_of_val(
                &extract_name::<ReadRequest<WorkerMessageParam>, WorkerMessageParam, WorkerMessageId>,
            ),
            type_of_val(&select_ds_worker_message),
            type_of_val(&resolve_worker_log_path),
            type_of_val(&get_worker_logs),
        ]);
    }

    // TODO: add tests that create actual logs.
    // TODO: tests using crate::logic::platform::resource::instance::WORKSPACE_ENV won't work.
    #[ignore]
    #[tokio::test]
    async fn test_no_logs() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "collection").await;

        let (dataset_id, function_id) = seed_dataset(
            &db,
            None,
            &collection_id,
            "dataset",
            &["table"],
            &[],
            &[],
            "hash",
        )
        .await;

        let execution_plan_id = seed_execution_plan(
            &db,
            "exec_plan_0",
            &collection_id,
            &dataset_id,
            &function_id,
            None,
        )
        .await;

        let transaction_id =
            seed_transaction(&db, &execution_plan_id, None, TransactionStatus::Scheduled).await;

        let data_version_id = seed_data_version(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &transaction_id,
            &execution_plan_id,
            "M",
            "R",
        )
        .await;

        let message_id = seed_worker_message(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &transaction_id,
            &execution_plan_id,
            &data_version_id,
        )
        .await;

        let provider = ReadWorkerLogsService::provider(db);
        let service = provider.make().await;

        let request: ReadRequest<WorkerMessageParam> =
            RequestContext::with(AccessTokenId::default(), user_id, RoleId::user(), false)
                .read(WorkerMessageParam::new(message_id));

        let response: BoxedSyncStream = service.raw_oneshot(request).await.unwrap();
        let bytes = response.into_inner().next().await.unwrap().unwrap();
        let response = String::from_utf8_lossy(&bytes).to_string();
        assert_eq!(response, "");
    }
}
