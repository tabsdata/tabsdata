//
// Copyright 2025 Tabs Data Inc.
//

use crate::common::layers::extractor::extract_req_dto;
use crate::execution::layers::update_status::{
    update_function_run_status, update_table_data_version_status_v2,
};
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::UpdateRequest;
use td_objects::rest_urls::FunctionRunParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::extractor::extract_req_name;
use td_objects::tower_service::from::{BuildService, ExtractService, TryIntoService, With};
use td_objects::tower_service::sql::{By, SqlSelectAllService};
use td_objects::types::basic::FunctionRunId;
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
                from_fn(extract_req_dto::<UpdateRequest<FunctionRunParam, CallbackRequest>, _>),
                from_fn(extract_req_name::<UpdateRequest<FunctionRunParam, CallbackRequest>, _>),

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
                from_fn(update_table_data_version_status_v2::<DaoQueries>),
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
    use td_common::datetime::IntoDateTimeUtc;
    use td_common::execution_status::FunctionRunUpdateStatus;
    use td_common::server::ResponseMessagePayloadBuilder;
    use td_common::server::{MessageAction, WorkerClass};
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::{handle_sql_err, RequestContext};
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection2::seed_collection;
    use td_objects::test_utils::seed_execution::seed_execution;
    use td_objects::test_utils::seed_function2::seed_function;
    use td_objects::test_utils::seed_function_requirement::seed_function_requirement;
    use td_objects::test_utils::seed_function_run::seed_function_run;
    use td_objects::test_utils::seed_table_data_version::seed_table_data_version;
    use td_objects::test_utils::seed_transaction2::seed_transaction;
    use td_objects::types::basic::{AccessTokenId, Decorator, TransactionKey};
    use td_objects::types::basic::{
        BundleId, CollectionName, FunctionRuntimeValues, TableDependency, TableName, UserId,
    };
    use td_objects::types::basic::{DependencyPos, RoleId, VersionPos};
    use td_objects::types::collection::CollectionDB;
    use td_objects::types::execution::{
        ExecutionDB, ExecutionDBWithStatus, ExecutionStatus, FunctionRunDB, FunctionRunStatus,
        TableDataVersionDB, TableDataVersionDBWithNames, TableDataVersionDBWithStatus,
        TransactionDB, TransactionDBWithStatus, TransactionStatus,
    };
    use td_objects::types::function::FunctionRegister;
    use td_objects::types::table::{TableDB, TableVersionDB};
    use td_objects::types::worker::v2::{FunctionOutputV2, WrittenTableV2};
    use td_objects::types::worker::FunctionOutput;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_callback(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider = ExecutionCallbackService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<UpdateRequest<FunctionRunParam, CallbackRequest>, ()>(&[
            // Extract from request.
            type_of_val(&extract_req_dto::<UpdateRequest<FunctionRunParam, CallbackRequest>, _>),
            type_of_val(&extract_req_name::<UpdateRequest<FunctionRunParam, CallbackRequest>, _>),
            // Convert callback request to status update request.
            type_of_val(&With::<CallbackRequest>::convert_to::<UpdateFunctionRun, _>),
            // Extract function_run_id. We assume it's correct as the callback is constructed by the server.
            type_of_val(&With::<FunctionRunParam>::extract::<FunctionRunId>),
            // Find function run (we will always have 1).
            type_of_val(&By::<FunctionRunId>::select_all::<DaoQueries, FunctionRunDB>),
            // Update function requirements status.
            type_of_val(&With::<UpdateFunctionRun>::convert_to::<UpdateFunctionRunDBBuilder, _>),
            type_of_val(&With::<UpdateFunctionRunDBBuilder>::build::<UpdateFunctionRunDB, _>),
            type_of_val(&update_function_run_status::<DaoQueries>),
            // Update table data versions status.
            type_of_val(&update_table_data_version_status_v2::<DaoQueries>),
        ]);
    }

    async fn schedule_function_execution(
        db: &DbPool,
        collection: &CollectionDB,
        function_create: &FunctionRegister,
    ) -> Result<FunctionRunDB, TdError> {
        let queries = DaoQueries::default();

        // Create function
        let (_, function_version) = seed_function(db, collection, function_create).await;

        // Create execution
        let execution = seed_execution(db, collection, &function_version).await;
        let transaction = seed_transaction(db, &execution, &TransactionKey::try_from("S")?).await;
        let function_run = seed_function_run(
            db,
            collection,
            &function_version,
            &execution,
            &transaction,
            &FunctionRunStatus::RunRequested,
        )
        .await;

        let tables: Vec<TableVersionDB> = queries
            .select_by::<TableVersionDB>(&(function_version.id()))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;

        for table in tables {
            let _ = seed_table_data_version(
                db,
                collection,
                &execution,
                &transaction,
                &function_run,
                &table,
            )
            .await;
        }

        // Assert scheduled function run
        let function_runs: Vec<FunctionRunDB> = queries
            .select_by::<FunctionRunDB>(&(function_version.id()))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(function_runs.len(), 1);
        let function_run = &function_runs[0];
        assert_eq!(*function_run.started_on(), None);
        assert_eq!(*function_run.ended_on(), None);
        assert_eq!(*function_run.status(), FunctionRunStatus::RunRequested);

        let table_data_versions: Vec<TableDataVersionDBWithStatus> = queries
            .select_by::<TableDataVersionDBWithStatus>(&(function_version.id()))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(
            table_data_versions.len(),
            function_create
                .tables()
                .as_ref()
                .map(|c| c.len())
                .unwrap_or(0)
        );
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

        let transactions: Vec<TransactionDBWithStatus> = queries
            .select_by::<TransactionDBWithStatus>(&(function_run.transaction_id()))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(transactions.len(), 1);
        let transaction = &transactions[0];
        assert_eq!(*transaction.started_on(), None);
        assert_eq!(*transaction.ended_on(), None);
        assert_eq!(*transaction.status(), TransactionStatus::Scheduled);

        let executions: Vec<ExecutionDBWithStatus> = queries
            .select_by::<ExecutionDBWithStatus>(&(function_run.execution_id()))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(executions.len(), 1);
        let execution = &executions[0];
        assert_eq!(*execution.started_on(), None);
        assert_eq!(*execution.ended_on(), None);
        assert_eq!(*execution.status(), ExecutionStatus::Scheduled);

        Ok(function_run.clone())
    }

    #[td_test::test(sqlx)]
    async fn test_callback_running(db: DbPool) -> Result<(), TdError> {
        // Setup
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let register = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependency::try_from("table_1")?])
            .triggers(None)
            .tables(vec![
                TableName::try_from("table_1")?,
                TableName::try_from("table_2")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let function_run = schedule_function_execution(&db, &collection, &register).await?;

        // Actual test
        let response: CallbackRequest = ResponseMessagePayloadBuilder::default()
            .id("".to_string())
            .class(WorkerClass::EPHEMERAL)
            .worker("".to_string())
            .action(MessageAction::Notify)
            .start(123)
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

        // Assertions
        let queries = DaoQueries::default();
        let function_runs: Vec<FunctionRunDB> = queries
            .select_by::<FunctionRunDB>(&(function_run.function_version_id()))?
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
        assert_eq!(*function_run.ended_on(), None);
        assert_eq!(*function_run.status(), FunctionRunStatus::Running);

        let table_data_versions: Vec<TableDataVersionDBWithStatus> = queries
            .select_by::<TableDataVersionDBWithStatus>(&(function_run.function_version_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(table_data_versions.len(), 2);
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

        let transactions: Vec<TransactionDBWithStatus> = queries
            .select_by::<TransactionDBWithStatus>(&(function_run.transaction_id()))?
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
        assert_eq!(*transaction.ended_on(), None);
        assert_eq!(*transaction.status(), TransactionStatus::Running);

        let executions: Vec<ExecutionDBWithStatus> = queries
            .select_by::<ExecutionDBWithStatus>(&(function_run.execution_id()))?
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
        assert_eq!(*execution.ended_on(), None);
        assert_eq!(*execution.status(), ExecutionStatus::Running);

        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_callback_done(db: DbPool) -> Result<(), TdError> {
        // Setup
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let register = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependency::try_from("table_1")?])
            .triggers(None)
            .tables(vec![
                TableName::try_from("table_1")?,
                TableName::try_from("table_2")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let function_run = schedule_function_execution(&db, &collection, &register).await?;

        // First set to running
        let response: CallbackRequest = ResponseMessagePayloadBuilder::default()
            .id("".to_string())
            .class(WorkerClass::EPHEMERAL)
            .worker("".to_string())
            .action(MessageAction::Notify)
            .start(123)
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

        // Then actual test
        let response: CallbackRequest = ResponseMessagePayloadBuilder::default()
            .id("".to_string())
            .class(WorkerClass::EPHEMERAL)
            .worker("".to_string())
            .action(MessageAction::Notify)
            .start(123)
            .end(Some(456))
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

        // Assertions
        let queries = DaoQueries::default();
        let function_runs: Vec<FunctionRunDB> = queries
            .select_by::<FunctionRunDB>(&(function_run.function_version_id()))?
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
        assert_eq!(*function_run.status(), FunctionRunStatus::Done);

        let table_data_versions: Vec<TableDataVersionDBWithStatus> = queries
            .select_by::<TableDataVersionDBWithStatus>(&(function_run.function_version_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(table_data_versions.len(), 2);
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

        let transactions: Vec<TransactionDBWithStatus> = queries
            .select_by::<TransactionDBWithStatus>(&(function_run.transaction_id()))?
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
        assert_eq!(
            *transaction.ended_on(),
            Some(456.datetime_utc()?.try_into()?)
        );
        assert_eq!(*transaction.status(), TransactionStatus::Published);

        let executions: Vec<ExecutionDBWithStatus> = queries
            .select_by::<ExecutionDBWithStatus>(&(function_run.execution_id()))?
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
        assert_eq!(*execution.ended_on(), Some(456.datetime_utc()?.try_into()?));
        assert_eq!(*execution.status(), ExecutionStatus::Done);

        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_callback_failed(db: DbPool) -> Result<(), TdError> {
        // Setup
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let register = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependency::try_from("table_1")?])
            .triggers(None)
            .tables(vec![
                TableName::try_from("table_1")?,
                TableName::try_from("table_2")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let function_run = schedule_function_execution(&db, &collection, &register).await?;

        // First set to running
        let response: CallbackRequest = ResponseMessagePayloadBuilder::default()
            .id("".to_string())
            .class(WorkerClass::EPHEMERAL)
            .worker("".to_string())
            .action(MessageAction::Notify)
            .start(123)
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

        // Then to failed
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

        // Assertions
        let queries = DaoQueries::default();
        let function_runs: Vec<FunctionRunDB> = queries
            .select_by::<FunctionRunDB>(&(function_run.function_version_id()))?
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
        assert_eq!(*function_run.status(), FunctionRunStatus::Failed);

        let table_data_versions: Vec<TableDataVersionDBWithStatus> = queries
            .select_by::<TableDataVersionDBWithStatus>(&(function_run.function_version_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(table_data_versions.len(), 2);
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

        let transactions: Vec<TransactionDBWithStatus> = queries
            .select_by::<TransactionDBWithStatus>(&(function_run.transaction_id()))?
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
        assert_eq!(*transaction.ended_on(), None);
        assert_eq!(*transaction.status(), TransactionStatus::Failed);

        let executions: Vec<ExecutionDBWithStatus> = queries
            .select_by::<ExecutionDBWithStatus>(&(function_run.execution_id()))?
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
        assert_eq!(*execution.ended_on(), None);
        assert_eq!(*execution.status(), ExecutionStatus::Running);

        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_callback_failed_downstream(db: DbPool) -> Result<(), TdError> {
        let queries = DaoQueries::default();

        // Setup
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let register = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependency::try_from("table_1")?])
            .triggers(None)
            .tables(vec![TableName::try_from("table_1")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let requirement_function_run =
            schedule_function_execution(&db, &collection, &register).await?;

        let req_executions: Vec<ExecutionDB> = queries
            .select_by::<ExecutionDB>(&(requirement_function_run.execution_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(req_executions.len(), 1);
        let _req_execution = &req_executions[0];

        let req_transactions: Vec<TransactionDB> = queries
            .select_by::<TransactionDB>(&(requirement_function_run.transaction_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(req_transactions.len(), 1);
        let _req_transaction = &req_transactions[0];

        let requirement_tables: Vec<TableDB> = queries
            .select_by::<TableDB>(&(requirement_function_run.function_version_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(requirement_tables.len(), 1);
        let requirement_table = &requirement_tables[0];

        let requirement_table_versions: Vec<TableVersionDB> = queries
            .select_by::<TableVersionDB>(&(requirement_function_run.function_version_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(requirement_table_versions.len(), 1);
        let requirement_table_version = &requirement_table_versions[0];

        let requirement_table_data_versions: Vec<TableDataVersionDB> = queries
            .select_by::<TableDataVersionDB>(&(requirement_function_run.function_version_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(requirement_table_data_versions.len(), 1);
        let requirement_table_data_version = &requirement_table_data_versions[0];

        let register = FunctionRegister::builder()
            .try_name("function_2")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependency::try_from("table_1")?])
            .triggers(None)
            .tables(vec![TableName::try_from("table_2")?])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let function_run = schedule_function_execution(&db, &collection, &register).await?;

        let executions: Vec<ExecutionDB> = queries
            .select_by::<ExecutionDB>(&(function_run.execution_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(executions.len(), 1);
        let execution = &executions[0];

        let transactions: Vec<TransactionDB> = queries
            .select_by::<TransactionDB>(&(function_run.transaction_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(transactions.len(), 1);
        let transaction = &transactions[0];

        let _ = seed_function_requirement(
            &db,
            &collection,
            execution,
            transaction,
            &function_run,
            requirement_table,
            requirement_table_version,
            Some(&requirement_function_run),
            Some(requirement_table_data_version),
            Some(&DependencyPos::try_from(0)?),
            &VersionPos::try_from(0)?,
        )
        .await;

        // First set to running
        let response: CallbackRequest = ResponseMessagePayloadBuilder::default()
            .id("".to_string())
            .class(WorkerClass::EPHEMERAL)
            .worker("".to_string())
            .action(MessageAction::Notify)
            .start(123)
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
                .function_run_id(requirement_function_run.id())
                .build()?,
            response,
        );

        let service = ExecutionCallbackService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        // Then actual test
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
                .function_run_id(requirement_function_run.id())
                .build()?,
            response,
        );

        let service = ExecutionCallbackService::new(db.clone()).service().await;
        service.raw_oneshot(request).await?;

        // Assertions
        // First assert that requirement_function_run is failed
        let requirement_function_runs: Vec<FunctionRunDB> = queries
            .select_by::<FunctionRunDB>(&(requirement_function_run.function_version_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(requirement_function_runs.len(), 1);
        let requirement_function_run = &requirement_function_runs[0];
        assert_eq!(
            *requirement_function_run.started_on(),
            Some(123.datetime_utc()?.try_into()?)
        );
        assert_eq!(
            *requirement_function_run.ended_on(),
            Some(456.datetime_utc()?.try_into()?)
        );
        assert_eq!(
            *requirement_function_run.status(),
            FunctionRunStatus::Failed
        );

        let table_data_versions: Vec<TableDataVersionDBWithStatus> = queries
            .select_by::<TableDataVersionDBWithStatus>(
                &(requirement_function_run.function_version_id()),
            )?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(table_data_versions.len(), 1);
        for table_data_version in &table_data_versions {
            assert_eq!(
                table_data_version.triggered_on(),
                requirement_function_run.triggered_on()
            );
            assert_eq!(
                table_data_version.triggered_by_id(),
                requirement_function_run.triggered_by_id()
            );
            assert_eq!(
                table_data_version.status(),
                requirement_function_run.status()
            );
        }

        let transactions: Vec<TransactionDBWithStatus> = queries
            .select_by::<TransactionDBWithStatus>(&(requirement_function_run.transaction_id()))?
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
        assert_eq!(*transaction.ended_on(), None);
        assert_eq!(*transaction.status(), TransactionStatus::Failed);

        let executions: Vec<ExecutionDBWithStatus> = queries
            .select_by::<ExecutionDBWithStatus>(&(requirement_function_run.execution_id()))?
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
        assert_eq!(*execution.ended_on(), None);
        assert_eq!(*execution.status(), ExecutionStatus::Running);

        // And function_run is on_hold
        let function_runs: Vec<FunctionRunDB> = queries
            .select_by::<FunctionRunDB>(&(function_run.function_version_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(function_runs.len(), 1);
        let function_run = &function_runs[0];
        assert_eq!(*function_run.started_on(), None);
        assert_eq!(*function_run.ended_on(), None);
        assert_eq!(*function_run.status(), FunctionRunStatus::OnHold);

        let table_data_versions: Vec<TableDataVersionDBWithStatus> = queries
            .select_by::<TableDataVersionDBWithStatus>(&(function_run.function_version_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(table_data_versions.len(), 1);
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

        let transactions: Vec<TransactionDBWithStatus> = queries
            .select_by::<TransactionDBWithStatus>(&(function_run.transaction_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(transactions.len(), 1);
        let transaction = &transactions[0];
        assert_eq!(*transaction.started_on(), None);
        assert_eq!(*transaction.ended_on(), None);
        assert_eq!(*transaction.status(), TransactionStatus::OnHold);

        let executions: Vec<ExecutionDBWithStatus> = queries
            .select_by::<ExecutionDBWithStatus>(&(function_run.execution_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(executions.len(), 1);
        let execution = &executions[0];
        assert_eq!(*execution.started_on(), None);
        assert_eq!(*execution.ended_on(), None);
        assert_eq!(*execution.status(), ExecutionStatus::Running);

        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_callback_table_data_version_status(db: DbPool) -> Result<(), TdError> {
        // Setup
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        let register = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependency::try_from("table_1")?])
            .triggers(None)
            .tables(vec![
                TableName::try_from("table_1")?,
                TableName::try_from("table_2")?,
            ])
            .runtime_values(FunctionRuntimeValues::try_from("foo runtime values")?)
            .reuse_frozen_tables(false)
            .build()?;
        let function_run = schedule_function_execution(&db, &collection, &register).await?;

        // First set to running
        let response: CallbackRequest = ResponseMessagePayloadBuilder::default()
            .id("".to_string())
            .class(WorkerClass::EPHEMERAL)
            .worker("".to_string())
            .action(MessageAction::Notify)
            .start(123)
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

        // Actual test
        let response: CallbackRequest = ResponseMessagePayloadBuilder::default()
            .id("".to_string())
            .class(WorkerClass::EPHEMERAL)
            .worker("".to_string())
            .action(MessageAction::Notify)
            .start(123)
            .end(None)
            .status(FunctionRunUpdateStatus::Done)
            .execution(0)
            .limit(None)
            .error(None)
            .context(Some(FunctionOutput::V2(
                FunctionOutputV2::builder()
                    .output(vec![
                        WrittenTableV2::NoData {
                            table: TableName::try_from("table_1")?,
                        },
                        WrittenTableV2::Data {
                            table: TableName::try_from("table_2")?,
                        },
                    ])
                    .build()?,
            )))
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

        // Assertions
        let queries = DaoQueries::default();
        let function_runs: Vec<FunctionRunDB> = queries
            .select_by::<FunctionRunDB>(&(function_run.function_version_id()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(function_runs.len(), 1);
        let function_run = &function_runs[0];

        let table_data_versions: Vec<TableDataVersionDBWithNames> = queries
            .select_by::<TableDataVersionDBWithNames>(&(&TableName::try_from("table_1")?))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(table_data_versions.len(), 1);
        let table_data_version = &table_data_versions[0];
        assert_eq!(
            table_data_version.triggered_on(),
            function_run.triggered_on()
        );
        assert_eq!(
            table_data_version.triggered_by_id(),
            function_run.triggered_by_id()
        );
        assert_eq!(table_data_version.status(), function_run.status());
        assert_eq!(*table_data_version.has_data(), Some(false.into()));

        let queries = DaoQueries::default();
        let table_data_versions: Vec<TableDataVersionDBWithNames> = queries
            .select_by::<TableDataVersionDBWithNames>(&(&TableName::try_from("table_2")?))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(table_data_versions.len(), 1);
        let table_data_version = &table_data_versions[0];
        assert_eq!(
            table_data_version.triggered_on(),
            function_run.triggered_on()
        );
        assert_eq!(
            table_data_version.triggered_by_id(),
            function_run.triggered_by_id()
        );
        assert_eq!(table_data_version.status(), function_run.status());
        assert_eq!(*table_data_version.has_data(), Some(true.into()));

        Ok(())
    }
}
