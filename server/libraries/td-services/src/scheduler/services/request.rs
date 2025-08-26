//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::layers::schedule::create_locked_workers;
use std::net::SocketAddr;
use td_common::server::{FileWorkerMessageQueue, WorkerMessageQueue};
use td_error::TdError;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{ExtractVecService, With};
use td_objects::tower_service::sql::{By, SqlSelectAllService, SqlUpdateService, insert_vec};
use td_objects::types::basic::FunctionRunId;
use td_objects::types::execution::{
    FunctionRunDB, FunctionRunToExecuteDB, UpdateFunctionRunDB, WorkerDB,
};
use td_storage::Storage;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layer, layers, provider};

// TODO make provider accept generics so scheduler services can be used with different message queues.
#[provider(
    name = ScheduleRequestService,
    request = (),
    response = (),
    connection = TransactionProvider,
    context = DaoQueries,
    context = Storage,
    context = FileWorkerMessageQueue,
    context = SocketAddr,
)]
fn provider() {
    layers!(request::<_, FileWorkerMessageQueue>())
}

// Requires:
// - Transaction connection
// - DaoQueries
// - Storage
// - T(MessageQueue)
// - SocketAddr server URL
#[layer]
pub fn request<T>()
where
    T: WorkerMessageQueue,
{
    layers!(
        // Get all function runs that are ready to execute.
        // This is, with status scheduled and with all requirements done.
        from_fn(By::<()>::select_all::<FunctionRunToExecuteDB>),
        // Create a locked message for each function run.
        from_fn(create_locked_workers::<T>),
        // And insert generated messages.
        from_fn(insert_vec::<WorkerDB>),
        // Update statuses.
        from_fn(With::<FunctionRunToExecuteDB>::extract_vec::<FunctionRunId>),
        from_fn(UpdateFunctionRunDB::run_requested),
        from_fn(By::<FunctionRunId>::update_all::<UpdateFunctionRunDB, FunctionRunDB>)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::services::execute::ExecuteFunctionService;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_common::server::{FileWorkerMessageQueue, SupervisorMessagePayload};
    use td_database::sql::DbPool;
    use td_objects::crudl::{RequestContext, handle_sql_err};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::sql::SelectBy;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, ExecutionName, FunctionRuntimeValues,
        RoleId, TableDependencyDto, TableName, TableNameDto, UserId,
    };
    use td_objects::types::execution::ExecutionRequest;
    use td_objects::types::execution::TableDataVersionDBWithNames;
    use td_objects::types::execution::WorkerMessageStatus;
    use td_objects::types::function::FunctionRegister;
    use td_objects::types::worker::v2::{InputTable, OutputTable};
    use td_objects::types::worker::{EnvPrefix, FunctionInput};
    use td_storage::{MountDef, SPath};
    use td_test::file::mount_uri;
    use td_tower::ctx_service::RawOneshot;
    use te_execution::transaction::TransactionBy;
    use testdir::testdir;
    use tower::ServiceExt;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_schedule_request(db: DbPool) -> Result<(), TdError> {
        use td_tower::metadata::type_of_val;

        ScheduleRequestService::with_defaults(db)
            .await
            .metadata()
            .await
            .assert_service::<(), ()>(&[
                // Get all function runs that are ready to execute.
                // This is, with status scheduled and with all requirements done.
                type_of_val(&By::<()>::select_all::<FunctionRunToExecuteDB>),
                // Create a locked message for each function run.
                type_of_val(&create_locked_workers::<FileWorkerMessageQueue>),
                // And insert generated messages.
                type_of_val(&insert_vec::<WorkerDB>),
                // Update statuses.
                type_of_val(&With::<FunctionRunToExecuteDB>::extract_vec::<FunctionRunId>),
                type_of_val(&UpdateFunctionRunDB::run_requested),
                type_of_val(&By::<FunctionRunId>::update_all::<UpdateFunctionRunDB, FunctionRunDB>),
            ]);
        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_schedule_request(db: DbPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = seed_collection(&db, &collection_name, &UserId::admin()).await;

        // Setup
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

        let _ = seed_function(&db, &collection, &create).await;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                FunctionParam::builder()
                    .try_collection(format!("{}", collection.name()))?
                    .try_function("function_1")?
                    .build()?,
                ExecutionRequest::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .build()?,
            );

        let queries = Arc::new(DaoQueries::default());
        let authz_context = Arc::new(AuthzContext::default());
        let transaction_by = Arc::new(TransactionBy::default());
        let service =
            ExecuteFunctionService::new(db.clone(), queries.clone(), authz_context, transaction_by)
                .service()
                .await;
        let execution = service.raw_oneshot(request).await?;

        // Actual test
        let test_dir = testdir!();
        let mount_def = MountDef::builder()
            .id("id")
            .path("/")
            .uri(mount_uri(&test_dir))
            .build()?;
        let storage = Arc::new(Storage::from(vec![mount_def]).await?);
        let message_queue = Arc::new(FileWorkerMessageQueue::with_location(&test_dir)?);
        let server_url = Arc::new(SocketAddr::from(([127, 0, 0, 1], 8080)));
        ScheduleRequestService::new(
            db.clone(),
            queries.clone(),
            storage.clone(),
            message_queue.clone(),
            server_url,
        )
        .service()
        .await
        .oneshot(())
        .await?;

        let created_message = message_queue.locked_messages().await;
        assert_eq!(created_message.len(), 1);

        let created_message = created_message[0].payload();
        let created_message = match created_message {
            SupervisorMessagePayload::SupervisorRequestMessagePayload(message) => message.context(),
            SupervisorMessagePayload::SupervisorResponseMessagePayload(_)
            | SupervisorMessagePayload::SupervisorExceptionMessagePayload(_) => {
                panic!("Unexpected SupervisorMessagePayload")
            }
        };
        let Some(FunctionInput::V2(message)) = created_message else {
            panic!("Unexpected FunctionInput version")
        };

        // V2 assertions
        let queries = DaoQueries::default();
        // Info
        let info = message.info();
        let function_runs: Vec<FunctionRunDB> = queries
            .select_by::<FunctionRunDB>(&(execution.manual_trigger()))?
            .build_query_as()
            .fetch_all(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(function_runs.len(), 1);
        let function_run = &function_runs[0];

        assert_eq!(info.collection_id(), collection.id());
        assert_eq!(info.collection(), collection.name());
        assert_eq!(
            info.function_version_id(),
            function_run.function_version_id()
        );
        assert_eq!(info.function_version_id(), execution.manual_trigger());
        assert_eq!(info.function_run_id(), function_run.id());

        let function_path = SPath::parse(format!(
            "/bundles/c/{}/f/{}.tgz",
            collection.id(),
            create.bundle_id()
        ))?;
        let (uri, mount_def) = storage.to_external_uri(&function_path)?;
        assert_eq!(*info.function_bundle().uri(), uri);
        assert_eq!(
            *info.function_bundle().env_prefix(),
            Some(EnvPrefix::try_from(mount_def.id())?)
        );
        assert_eq!(
            **info.triggered_on(),
            function_run.triggered_on().timestamp_millis()
        );
        assert!(info.triggered_on() < info.scheduled_on());
        assert_eq!(info.execution_id(), function_run.execution_id());
        assert_eq!(
            *info.execution_name(),
            Some(ExecutionName::try_from("test_execution")?)
        );

        let function_data_path = SPath::parse(format!(
            "/c/{}/x/{}/f/{}",
            collection.id(),
            function_run.transaction_id(),
            function_run.function_version_id()
        ))?;
        let (uri, mount_def) = storage.to_external_uri(&function_data_path)?;
        assert_eq!(*info.function_data().uri(), uri);
        assert_eq!(
            *info.function_data().env_prefix(),
            Some(EnvPrefix::try_from(mount_def.id())?)
        );

        // Input
        assert_eq!(message.input().len(), 1);
        let input_table = &message.input()[0];
        match input_table {
            InputTable::Table(input) => {
                assert!(input.table_data_version_id().is_none());
                assert_eq!(*input.name(), TableName::try_from("table_1")?);
                assert_eq!(input.collection_id(), collection.id());
                assert_eq!(input.collection(), collection.name());
                assert!(input.location().is_none());
                assert_eq!(**input.table_pos(), 0);
                assert_eq!(**input.version_pos(), 0);
            }
            _ => panic!("Unexpected Input Table type"),
        }

        // Output
        assert_eq!(message.output().len(), 2);
        for (index, output_table) in message.output().iter().enumerate() {
            match output_table {
                OutputTable::Table(output) => {
                    let table_data_version: TableDataVersionDBWithNames = queries
                        .select_by::<TableDataVersionDBWithNames>(
                            &(output.table_data_version_id()),
                        )?
                        .build_query_as()
                        .fetch_one(&db)
                        .await
                        .map_err(handle_sql_err)?;

                    assert_eq!(output.name(), table_data_version.name());
                    assert_eq!(output.collection_id(), collection.id());
                    assert_eq!(output.collection(), collection.name());
                    assert_eq!(output.table_id(), table_data_version.table_id());
                    assert_eq!(
                        output.table_version_id(),
                        table_data_version.table_version_id()
                    );
                    assert_eq!(output.table_data_version_id(), table_data_version.id());

                    let table_path = SPath::parse(format!(
                        "/c/{}/d/{}/t/{}/{}.t",
                        collection.id(),
                        table_data_version.id(),
                        table_data_version.table_id(),
                        table_data_version.table_version_id(),
                    ))?;
                    let (uri, mount_def) = storage.to_external_uri(&table_path)?;

                    assert_eq!(*output.location().uri(), uri);
                    assert_eq!(
                        *output.location().env_prefix(),
                        Some(EnvPrefix::try_from(mount_def.id())?)
                    );
                    assert_eq!(**output.table_pos(), index as i32);
                }
                _ => panic!("Unexpected Output Table type"),
            }
        }

        let message: WorkerDB = queries
            .select_by::<WorkerDB>(&(function_run.id()))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(message.collection_id(), collection.id());
        assert_eq!(message.execution_id(), execution.id());
        assert_eq!(message.function_run_id(), function_run.id());
        assert_eq!(
            message.function_version_id(),
            function_run.function_version_id()
        );
        assert_eq!(*message.message_status(), WorkerMessageStatus::Locked);

        Ok(())
    }
}
