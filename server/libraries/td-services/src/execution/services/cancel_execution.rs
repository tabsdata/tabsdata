//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::update_status::update_function_run_status;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::UpdateRequest;
use td_objects::rest_urls::ExecutionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlSelectAllService, SqlSelectIdOrNameService};
use td_objects::types::basic::{ExecutionId, ExecutionIdName};
use td_objects::types::execution::{ExecutionDB, FunctionRunDB, UpdateFunctionRunDB};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ExecutionCancelService {
    provider: ServiceProvider<UpdateRequest<ExecutionParam, ()>, (), TdError>,
}

impl ExecutionCancelService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>) {
            service_provider!(layers!(
                // Set context
                SrvCtxProvider::new(queries),

                // Extract from request.
                from_fn(With::<UpdateRequest<ExecutionParam, ()>>::extract_name::<ExecutionParam>),

                // Extract function_run_id. We assume it's correct as the callback is constructed by the server.
                from_fn(With::<ExecutionParam>::extract::<ExecutionIdName>),

                // DB Transaction start.
                TransactionProvider::new(db),

                // Find function run.
                from_fn(By::<ExecutionIdName>::select::<DaoQueries, ExecutionDB>),
                from_fn(With::<ExecutionDB>::extract::<ExecutionId>),
                from_fn(By::<ExecutionId>::select_all::<DaoQueries, FunctionRunDB>),

                // Set cancel status
                from_fn(UpdateFunctionRunDB::cancel),

                // Update function requirements status
                from_fn(update_function_run_status::<DaoQueries>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<UpdateRequest<ExecutionParam, ()>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::services::callback::ExecutionCallbackService;
    use td_common::datetime::IntoDateTimeUtc;
    use td_common::execution_status::FunctionRunUpdateStatus;
    use td_common::server::{MessageAction, ResponseMessagePayloadBuilder, WorkerClass};
    use td_common::status::ExitStatus;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::{handle_sql_err, RequestContext};
    use td_objects::rest_urls::FunctionRunIdParam;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_execution::seed_execution;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_function_run::seed_function_run;
    use td_objects::test_utils::seed_table_data_version::seed_table_data_version;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, FunctionRuntimeValues,
        TableDependencyDto, TableNameDto, TableTriggerDto, UserId,
    };
    use td_objects::types::basic::{RoleId, TransactionKey};
    use td_objects::types::execution::{
        CallbackRequest, ExecutionDBWithStatus, ExecutionStatus, FunctionRunDB, FunctionRunStatus,
        TableDataVersionDBWithStatus, TransactionDBWithStatus, TransactionStatus,
    };
    use td_objects::types::function::FunctionRegister;
    use td_objects::types::table::TableVersionDB;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_cancel_execution(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider = ExecutionCancelService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<UpdateRequest<ExecutionParam, ()>, ()>(&[
            // Extract from request.
            type_of_val(&With::<UpdateRequest<ExecutionParam, ()>>::extract_name::<ExecutionParam>),
            // Extract function_run_id. We assume it's correct as the callback is constructed by the server.
            type_of_val(&With::<ExecutionParam>::extract::<ExecutionIdName>),
            // Find function run.
            type_of_val(&By::<ExecutionIdName>::select::<DaoQueries, ExecutionDB>),
            type_of_val(&With::<ExecutionDB>::extract::<ExecutionId>),
            type_of_val(&By::<ExecutionId>::select_all::<DaoQueries, FunctionRunDB>),
            // Set cancel status
            type_of_val(&UpdateFunctionRunDB::cancel),
            // Update function requirements status
            type_of_val(&update_function_run_status::<DaoQueries>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_cancel_execution(db: DbPool) -> Result<(), TdError> {
        let queries = DaoQueries::default();

        // Set collection
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        // Setup
        // Create function_1
        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependencyDto::try_from("table_1")?])
            .triggers(None)
            .tables(vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (_, function_version_1) = seed_function(&db, &collection, &create).await;

        // Create function_10
        let create = FunctionRegister::builder()
            .try_name("function_10")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependencyDto::try_from("table_1")?])
            .triggers(vec![TableTriggerDto::try_from("table_1")?])
            .tables(vec![
                TableNameDto::try_from("table_10")?,
                TableNameDto::try_from("table_20")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (_, function_version_10) = seed_function(&db, &collection, &create).await;

        // Create function_100
        let create = FunctionRegister::builder()
            .try_name("function_100")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependencyDto::try_from("table_10")?])
            .triggers(vec![TableTriggerDto::try_from("table_10")?])
            .tables(vec![
                TableNameDto::try_from("table_100")?,
                TableNameDto::try_from("table_200")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;

        let (_, function_version_100) = seed_function(&db, &collection, &create).await;

        // Create execution
        let execution = seed_execution(&db, &collection, &function_version_1).await;
        let transaction = seed_transaction(&db, &execution, &TransactionKey::try_from("S")?).await;

        let function_versions = vec![
            function_version_1.clone(),
            function_version_10.clone(),
            function_version_100.clone(),
        ];

        let mut function_runs = vec![];
        for function_version in function_versions.iter() {
            let function_run = seed_function_run(
                &db,
                &collection,
                function_version,
                &execution,
                &transaction,
                &FunctionRunStatus::RunRequested,
            )
            .await;
            function_runs.push(function_run);
        }

        let tables: Vec<TableVersionDB> = queries
            .select_by::<TableVersionDB>(&())?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;

        for table in tables {
            let function_run = function_runs
                .iter()
                .find(|f| f.function_version_id() == table.function_version_id())
                .unwrap();
            let _ = seed_table_data_version(
                &db,
                &collection,
                &execution,
                &transaction,
                function_run,
                &table,
            )
            .await;
        }

        // Update status to failed (so we can assert start/end times)
        let response: CallbackRequest = ResponseMessagePayloadBuilder::default()
            .id("".to_string())
            .class(WorkerClass::EPHEMERAL)
            .worker("".to_string())
            .action(MessageAction::Notify)
            .start(123)
            .end(Some(456))
            .status(FunctionRunUpdateStatus::Failed)
            .execution(0)
            .limit(None)
            .error(None)
            .exception_kind(None)
            .exception_message(None)
            .exception_error_code(None)
            .exit_status(ExitStatus::GeneralError.code())
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
            FunctionRunIdParam::builder()
                .function_run_id(function_runs[0].id())
                .build()?,
            response,
        );

        let service = ExecutionCallbackService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        // Create another execution that won't be cancelled
        let uncancelled_execution = seed_execution(&db, &collection, &function_version_1).await;
        let uncancelled_transaction =
            seed_transaction(&db, &uncancelled_execution, &TransactionKey::try_from("S")?).await;

        let mut uncancelled_function_runs = vec![];
        for function_version in function_versions.iter() {
            let function_run = seed_function_run(
                &db,
                &collection,
                function_version,
                &uncancelled_execution,
                &uncancelled_transaction,
                &FunctionRunStatus::RunRequested,
            )
            .await;
            uncancelled_function_runs.push(function_run);
        }

        // Actual test
        let before = Some(AtTime::now().await);
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            ExecutionParam::builder()
                .try_execution(execution.id().to_string())?
                .build()?,
            (),
        );

        let service = ExecutionCancelService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        // Assertions
        // Assert execution is incomplete
        let executions: Vec<ExecutionDBWithStatus> = queries
            .select_by::<ExecutionDBWithStatus>(&(execution.id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(executions.len(), 1);
        let execution = &executions[0];
        assert_eq!(
            *execution.started_on(),
            Some(123.datetime_utc()?.try_into()?)
        );
        assert!(*execution.ended_on() > before);
        assert_eq!(*execution.status(), ExecutionStatus::Incomplete);

        // Assert transaction is cancelled
        let transactions: Vec<TransactionDBWithStatus> = queries
            .select_by::<TransactionDBWithStatus>(&(transaction.id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(transactions.len(), 1);
        let transaction = &transactions[0];
        assert_eq!(
            *transaction.started_on(),
            Some(123.datetime_utc()?.try_into()?)
        );
        assert!(*transaction.ended_on() > before);
        assert_eq!(*transaction.status(), TransactionStatus::Canceled);

        // First function started and ended
        let function_version = &function_versions[0];
        // Assert all function_runs are cancelled
        let function_runs: Vec<FunctionRunDB> = queries
            .select_by::<FunctionRunDB>(&(function_version.id(), transaction.id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(function_runs.len(), 1);
        let function_run = &function_runs[0];
        assert_eq!(
            *function_run.started_on(),
            Some(123.datetime_utc()?.try_into()?)
        );
        assert_eq!(
            *function_run.ended_on(),
            Some(456.datetime_utc()?.try_into()?)
        );
        assert_eq!(*function_run.status(), FunctionRunStatus::Canceled);

        // Assert all table_data_versions are cancelled
        let table_data_versions: Vec<TableDataVersionDBWithStatus> = queries
            .select_by::<TableDataVersionDBWithStatus>(&(function_version.id(), execution.id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        for table_data_version in &table_data_versions {
            assert_eq!(
                table_data_version.triggered_on(),
                function_run.triggered_on()
            );
            assert_eq!(
                table_data_version.triggered_by_id(),
                function_run.triggered_by_id()
            );
            assert_eq!(table_data_version.status(), function_run.status());
        }

        // The rest didnt get to start
        for function_version in function_versions[1..].iter() {
            // Assert all function_runs are cancelled
            let function_runs: Vec<FunctionRunDB> = queries
                .select_by::<FunctionRunDB>(&(function_version.id(), execution.id()))?
                .build_query_as()
                .fetch_all(&db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(function_runs.len(), 1);
            let function_run = &function_runs[0];
            assert_eq!(*function_run.started_on(), None);
            assert!(*function_run.ended_on() > before);
            assert_eq!(*function_run.status(), FunctionRunStatus::Canceled);

            // Assert all table_data_versions are cancelled
            let table_data_versions: Vec<TableDataVersionDBWithStatus> = queries
                .select_by::<TableDataVersionDBWithStatus>(&(function_version.id()))?
                .build_query_as()
                .fetch_all(&db)
                .await
                .map_err(handle_sql_err)?;
            for table_data_version in &table_data_versions {
                assert_eq!(
                    table_data_version.triggered_on(),
                    function_run.triggered_on()
                );
                assert_eq!(
                    table_data_version.triggered_by_id(),
                    function_run.triggered_by_id()
                );
                assert_eq!(table_data_version.status(), function_run.status());
            }
        }

        // Assert uncancelled_execution is still scheduled
        let executions: Vec<ExecutionDBWithStatus> = queries
            .select_by::<ExecutionDBWithStatus>(&(uncancelled_execution.id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(executions.len(), 1);
        let execution = &executions[0];
        assert_eq!(*execution.started_on(), None);
        assert_eq!(*execution.ended_on(), None);
        assert_eq!(*execution.status(), ExecutionStatus::Scheduled);

        // Assert uncancelled_transaction is still scheduled
        let transactions: Vec<TransactionDBWithStatus> = queries
            .select_by::<TransactionDBWithStatus>(&(uncancelled_execution.id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(transactions.len(), 1);
        let transaction = &transactions[0];
        assert_eq!(*transaction.started_on(), None);
        assert_eq!(*transaction.ended_on(), None);
        assert_eq!(*transaction.status(), TransactionStatus::Scheduled);

        // Assert all function_runs are still scheduled
        let function_runs: Vec<FunctionRunDB> = queries
            .select_by::<FunctionRunDB>(&(uncancelled_execution.id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(function_runs.len(), 3);

        for function_run in &function_runs {
            assert_eq!(*function_run.started_on(), None);
            assert_eq!(*function_run.ended_on(), None);
            assert_eq!(*function_run.status(), FunctionRunStatus::RunRequested);
        }

        Ok(())
    }
}
