//
// Copyright 2025 Tabs Data Inc.
//

use crate::common::layers::extractor::extract_req_dto;
use crate::execution::layers::update_status::{
    update_function_run_status, update_table_data_version_status,
};
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::rest_urls::FunctionRunParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::extractor::{extract_req_context, extract_req_name};
use td_objects::tower_service::from::{BuildService, ExtractService, TryIntoService, With};
use td_objects::tower_service::sql::{By, SqlSelectAllService};
use td_objects::types::basic::{AtTime, FunctionRunId};
use td_objects::types::execution::{
    CallbackRequest, FunctionRunDB, UpdateFunctionRun, UpdateFunctionRunDB,
    UpdateFunctionRunDBBuilder,
};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ExecutionCallbackService {
    provider: ServiceProvider<UpdateRequest<FunctionRunParam, CallbackRequest>, (), TdError>,
}

impl ExecutionCallbackService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>) -> TdError {
            service_provider!(layers!(
                // Set context
                SrvCtxProvider::new(queries),

                // Extract from request.
                from_fn(extract_req_context::<UpdateRequest<FunctionRunParam, CallbackRequest>>),
                from_fn(extract_req_dto::<UpdateRequest<FunctionRunParam, CallbackRequest>, _>),
                from_fn(extract_req_name::<UpdateRequest<FunctionRunParam, CallbackRequest>, _>),
                from_fn(With::<RequestContext>::extract::<AtTime>),

                // Convert callback request to status update request.
                from_fn(With::<CallbackRequest>::convert_to::<UpdateFunctionRun, _>),

                // Extract function_run_id. We assume it's correct as the callback is constructed by the server.
                from_fn(With::<FunctionRunParam>::extract::<FunctionRunId>),

                // DB Transaction start.
                TransactionProvider::new(db),

                // Find function run (we will always have 1).
                from_fn(By::<FunctionRunId>::select_all::<DaoQueries, FunctionRunDB>),

                // Update function requirements status.
                from_fn(With::<UpdateFunctionRun>::convert_to::<UpdateFunctionRunDBBuilder, _>),
                from_fn(With::<UpdateFunctionRunDBBuilder>::build::<UpdateFunctionRunDB, _>),
                from_fn(update_function_run_status::<DaoQueries>),

                // Update table data versions status.
                from_fn(update_table_data_version_status::<DaoQueries>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<UpdateRequest<FunctionRunParam, CallbackRequest>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::services::schedule_commit::ScheduleCommitService;
    use crate::execution::services::schedule_request::ScheduleRequestService;
    use crate::execution::services::ExecuteFunctionService;
    use std::net::SocketAddr;
    use td_common::execution_status::FunctionRunUpdateStatus;
    use td_common::server::{FileWorkerMessageQueue, MessageAction, WorkerClass};
    use td_common::server::{ResponseMessagePayloadBuilder, WorkerMessageQueue};
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::{handle_sql_err, RequestContext};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection2::seed_collection;
    use td_objects::test_utils::seed_function2::seed_function;
    use td_objects::types::basic::AccessTokenId;
    use td_objects::types::basic::RoleId;
    use td_objects::types::basic::{
        BundleId, CollectionName, ExecutionName, FunctionRuntimeValues, TableDependency, TableName,
        UserId,
    };
    use td_objects::types::execution::{ExecutionRequest, FunctionRunDB};
    use td_objects::types::function::FunctionRegister;
    use td_objects::types::worker::FunctionInput;
    use td_storage::{MountDef, Storage};
    use td_test::file::mount_uri;
    use td_tower::ctx_service::RawOneshot;
    use testdir::testdir;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_callback(_db: DbPool) {
        // use td_tower::metadata::{type_of_val, Metadata};
        //
        // let queries = Arc::new(DaoQueries::default());
        // let provider = ExecuteFunctionService::provider(db, queries, transaction_by);
        // let service = provider.make().await;
        //
        // let response: Metadata = service.raw_oneshot(()).await.unwrap();
        // let metadata = response.get();
        //
        // metadata
        //     .assert_service::<CreateRequest<FunctionParam, ExecutionRequest>, ExecutionResponse>(
        //         &[],
        //     );
    }

    #[td_test::test(sqlx)]
    async fn test_callback(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        // Setup
        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .dependencies(vec![TableDependency::try_from("table_1")?])
            .triggers(None)
            .tables(vec![
                TableName::try_from("table_1")?,
                TableName::try_from("table_2")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let _ = seed_function(&db, &collection, &create).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .create(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("function_1")?
                .build()?,
            ExecutionRequest::builder()
                .name(Some(ExecutionName::try_from("test_execution")?))
                .build()?,
        );

        let service = ExecuteFunctionService::new(db.clone()).service().await;
        let execution = service.raw_oneshot(request).await?;

        let test_dir = testdir!();
        let mount_def = MountDef::builder()
            .id("id")
            .mount_path("/")
            .uri(mount_uri(&test_dir))
            .build()?;
        let storage = Arc::new(Storage::from(vec![mount_def]).await?);
        let message_queue = Arc::new(FileWorkerMessageQueue::with_location(&test_dir)?);
        let server_url = Arc::new(SocketAddr::from(([127, 0, 0, 1], 8080)));
        ScheduleRequestService::new(
            db.clone(),
            storage.clone(),
            message_queue.clone(),
            server_url,
        )
        .service()
        .await
        .raw_oneshot(())
        .await?;

        let created_messages = message_queue.locked_messages::<FunctionInput>().await;
        assert_eq!(created_messages.len(), 1);

        // Actual test
        ScheduleCommitService::new(db.clone(), message_queue.clone())
            .service()
            .await
            .raw_oneshot(())
            .await?;

        let locked_messages = message_queue.locked_messages::<FunctionInput>().await;
        assert_eq!(locked_messages.len(), 0);

        // Get function run
        let queries = DaoQueries::default();
        let function_runs: Vec<FunctionRunDB> = queries
            .select_by::<FunctionRunDB>(&(execution.manual_trigger().function_version_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(function_runs.len(), 1);
        let function_run = &function_runs[0];

        // Actual test
        let response: CallbackRequest = ResponseMessagePayloadBuilder::default()
            .id("".to_string())
            .class(WorkerClass::EPHEMERAL)
            .worker("".to_string())
            .action(MessageAction::Notify)
            .start(0)
            .end(None)
            .status(FunctionRunUpdateStatus::Running)
            .execution(0)
            .limit(None)
            .error(None)
            .context(None)
            .build()
            .unwrap();

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            FunctionRunParam::builder()
                .function_run_id(function_run.id())
                .build()?,
            response,
        );

        let service = ExecutionCallbackService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        let response: CallbackRequest = ResponseMessagePayloadBuilder::default()
            .id("".to_string())
            .class(WorkerClass::EPHEMERAL)
            .worker("".to_string())
            .action(MessageAction::Notify)
            .start(0)
            .end(Some(1744818449913))
            .status(FunctionRunUpdateStatus::Done)
            .execution(0)
            .limit(None)
            .error(None)
            .context(None)
            .build()
            .unwrap();

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            FunctionRunParam::builder()
                .function_run_id(function_run.id())
                .build()?,
            response,
        );

        let service = ExecutionCallbackService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        Ok(())
    }
}
