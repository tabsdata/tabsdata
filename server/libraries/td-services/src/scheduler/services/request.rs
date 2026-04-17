//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::layers::request::{
    create_locked_cron_trigger_workers_start_request,
    create_locked_cron_trigger_workers_stop_request,
    create_locked_function_runner_workers_start_request,
    create_locked_stage_trigger_workers_start_request,
    create_locked_stage_trigger_workers_stop_request,
};
use ta_services::factory::service_factory;
use td_common::server::{FileWorkerMessageQueue, WorkerMessageQueue};
use td_objects::dxo::function_run::{FunctionRunDB, FunctionRunToExecuteDB, UpdateFunctionRunDB};
use td_objects::dxo::worker::{
    UpdateWorkerDB, UpdateWorkerMessageStatusDB, WorkerDB, WorkerDBWithNames,
};
use td_objects::tower_service::with::With;
use td_objects::types::addresses::InternalServerAddresses;
use td_objects::types::basic::{FunctionRunId, WorkerId};
use td_queries::By;
use td_queries::insert_vec;
use td_storage::Storage;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::{layer, layers};

#[service_factory(
    name = RequestService,
    request = (),
    response = (),
    connection = TransactionProvider,
    context = Storage,
    context = FileWorkerMessageQueue,
    context = InternalServerAddresses,
)]
fn service() {
    layers!(
        // Request function runners that need to be started
        request_function_runners::<_, FileWorkerMessageQueue>(),
        // Stop cron trigger runners that need to be stopped
        stop_cron_trigger_runners::<_, FileWorkerMessageQueue>(),
        // Request cron trigger runners that need to be started
        request_cron_trigger_runners::<_, FileWorkerMessageQueue>(),
        // Stop stage trigger runners that need to be stopped
        stop_stage_trigger_runners::<_, FileWorkerMessageQueue>(),
        // Request stage trigger runners that need to be started
        request_stage_trigger_runners::<_, FileWorkerMessageQueue>(),
    )
}

// Requires:
// - Transaction connection
// - Storage
// - T(MessageQueue)
// - ServerUrl
#[layer]
pub fn request_function_runners<T>()
where
    T: WorkerMessageQueue,
{
    layers!(
        // Get all function runs that are ready to execute.
        // This is, with status scheduled and with all requirements done.
        from_fn(By::<()>::fetch_all::<FunctionRunToExecuteDB>),
        // Create a locked message for each function run.
        from_fn(create_locked_function_runner_workers_start_request::<T>),
        // And insert generated messages.
        from_fn(insert_vec::<WorkerDB>),
        // Update statuses.
        from_fn(With::<Vec<FunctionRunToExecuteDB>>::extract::<Vec<FunctionRunId>>),
        from_fn(UpdateFunctionRunDB::run_requested),
        from_fn(By::<Vec<FunctionRunId>>::update_chunked::<UpdateFunctionRunDB, FunctionRunDB>),
    )
}

// Requires:
// - Transaction connection
// - T(MessageQueue)
// - ServerUrl
#[layer]
pub fn request_cron_trigger_runners<T>()
where
    T: WorkerMessageQueue,
{
    layers!(
        from_fn(
            By::<()>::fetch_all_in::<
                { WorkerDBWithNames::CronTriggersToSchedule },
                WorkerDBWithNames,
            >
        ),
        from_fn(create_locked_cron_trigger_workers_start_request::<T>),
        // Update statuses.
        from_fn(With::<Vec<WorkerDBWithNames>>::extract::<Vec<WorkerId>>),
        from_fn(UpdateWorkerDB::run_requested),
        from_fn(By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerDB, WorkerDB>),
        from_fn(UpdateWorkerMessageStatusDB::locked),
        from_fn(By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerMessageStatusDB, WorkerDB>),
    )
}

// Requires:
// - Transaction connection
// - T(MessageQueue)
// - ServerUrl
#[layer]
pub fn stop_cron_trigger_runners<T>()
where
    T: WorkerMessageQueue,
{
    layers!(
        from_fn(
            By::<()>::fetch_all_in::<{ WorkerDBWithNames::CronTriggersToStop }, WorkerDBWithNames>
        ),
        from_fn(create_locked_cron_trigger_workers_stop_request::<T>),
        // Update statuses.
        from_fn(With::<Vec<WorkerDBWithNames>>::extract::<Vec<WorkerId>>),
        from_fn(UpdateWorkerDB::stop_requested),
        from_fn(By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerDB, WorkerDB>),
        from_fn(UpdateWorkerMessageStatusDB::locked),
        from_fn(By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerMessageStatusDB, WorkerDB>),
    )
}

// Requires:
// - Transaction connection
// - Storage
// - T(MessageQueue)
// - ServerUrl
#[layer]
pub fn request_stage_trigger_runners<T>()
where
    T: WorkerMessageQueue,
{
    layers!(
        from_fn(
            By::<()>::fetch_all_in::<
                { WorkerDBWithNames::StageTriggersToSchedule },
                WorkerDBWithNames,
            >
        ),
        from_fn(create_locked_stage_trigger_workers_start_request::<T>),
        // Update statuses.
        from_fn(With::<Vec<WorkerDBWithNames>>::extract::<Vec<WorkerId>>),
        from_fn(UpdateWorkerDB::run_requested),
        from_fn(By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerDB, WorkerDB>),
        from_fn(UpdateWorkerMessageStatusDB::locked),
        from_fn(By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerMessageStatusDB, WorkerDB>),
    )
}

// Requires:
// - Transaction connection
// - T(MessageQueue)
// - ServerUrl
#[layer]
pub fn stop_stage_trigger_runners<T>()
where
    T: WorkerMessageQueue,
{
    layers!(
        from_fn(
            By::<()>::fetch_all_in::<{ WorkerDBWithNames::StageTriggersToStop }, WorkerDBWithNames>
        ),
        from_fn(create_locked_stage_trigger_workers_stop_request::<T>),
        // Update statuses.
        from_fn(With::<Vec<WorkerDBWithNames>>::extract::<Vec<WorkerId>>),
        from_fn(UpdateWorkerDB::stop_requested),
        from_fn(By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerDB, WorkerDB>),
        from_fn(UpdateWorkerMessageStatusDB::locked),
        from_fn(By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerMessageStatusDB, WorkerDB>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::services::execute::ExecuteFunctionService;
    use crate::scheduler::services::SchedulerContext;
    use std::str::FromStr;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_common::server::SupervisorMessagePayload;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionRequest;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table_data_version::TableDataVersionDBWithNames;
    use td_objects::dxo::yaml::WorkerInput;
    use td_objects::dxo::yaml::function_runner::v2::{InputTable, OutputTable};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, EnvPrefix,
        ExecutionName, FunctionIdName, FunctionRuntimeValues, RoleId, TableName, TableNameDto,
        UserId, WorkerMessageStatus,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::BasicQueries;
    use td_queries::test_utils::SeedQueries;
    use td_storage::SPath;
    use td_tower::ctx_service::RawOneshot;
    use tower::ServiceExt;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_schedule_request(db: TdPool) -> Result<(), TdError> {
        use td_tower::metadata::type_of_val;

        RequestService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<(), ()>(&[
                // Function
                // Get all function runs that are ready to execute.
                // This is, with status scheduled and with all requirements done.
                type_of_val(&By::<()>::fetch_all::<FunctionRunToExecuteDB>),
                // Create a locked message for each function run.
                type_of_val(
                    &create_locked_function_runner_workers_start_request::<FileWorkerMessageQueue>,
                ),
                // And insert generated messages.
                type_of_val(&insert_vec::<WorkerDB>),
                // Update statuses.
                type_of_val(&With::<Vec<FunctionRunToExecuteDB>>::extract::<Vec<FunctionRunId>>),
                type_of_val(&UpdateFunctionRunDB::run_requested),
                type_of_val(
                    &By::<Vec<FunctionRunId>>::update_chunked::<UpdateFunctionRunDB, FunctionRunDB>,
                ),
                // CronTrigger
                // Stop
                type_of_val(
                    &By::<()>::fetch_all_in::<
                        { WorkerDBWithNames::CronTriggersToStop },
                        WorkerDBWithNames,
                    >,
                ),
                type_of_val(
                    &create_locked_cron_trigger_workers_stop_request::<FileWorkerMessageQueue>,
                ),
                // Update statuses.
                type_of_val(&With::<Vec<WorkerDBWithNames>>::extract::<Vec<WorkerId>>),
                type_of_val(&UpdateWorkerDB::stop_requested),
                type_of_val(&By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerDB, WorkerDB>),
                type_of_val(&UpdateWorkerMessageStatusDB::locked),
                type_of_val(
                    &By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerMessageStatusDB, WorkerDB>,
                ),
                // Start
                type_of_val(
                    &By::<()>::fetch_all_in::<
                        { WorkerDBWithNames::CronTriggersToSchedule },
                        WorkerDBWithNames,
                    >,
                ),
                type_of_val(
                    &create_locked_cron_trigger_workers_start_request::<FileWorkerMessageQueue>,
                ),
                // Update statuses.
                type_of_val(&With::<Vec<WorkerDBWithNames>>::extract::<Vec<WorkerId>>),
                type_of_val(&UpdateWorkerDB::run_requested),
                type_of_val(&By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerDB, WorkerDB>),
                type_of_val(&UpdateWorkerMessageStatusDB::locked),
                type_of_val(
                    &By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerMessageStatusDB, WorkerDB>,
                ),
                // StageTrigger
                // Stop
                type_of_val(
                    &By::<()>::fetch_all_in::<
                        { WorkerDBWithNames::StageTriggersToStop },
                        WorkerDBWithNames,
                    >,
                ),
                type_of_val(
                    &create_locked_stage_trigger_workers_stop_request::<FileWorkerMessageQueue>,
                ),
                // Update statuses.
                type_of_val(&With::<Vec<WorkerDBWithNames>>::extract::<Vec<WorkerId>>),
                type_of_val(&UpdateWorkerDB::stop_requested),
                type_of_val(&By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerDB, WorkerDB>),
                type_of_val(&UpdateWorkerMessageStatusDB::locked),
                type_of_val(
                    &By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerMessageStatusDB, WorkerDB>,
                ),
                // Start
                type_of_val(
                    &By::<()>::fetch_all_in::<
                        { WorkerDBWithNames::StageTriggersToSchedule },
                        WorkerDBWithNames,
                    >,
                ),
                type_of_val(
                    &create_locked_stage_trigger_workers_start_request::<FileWorkerMessageQueue>,
                ),
                // Update statuses.
                type_of_val(&With::<Vec<WorkerDBWithNames>>::extract::<Vec<WorkerId>>),
                type_of_val(&UpdateWorkerDB::run_requested),
                type_of_val(&By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerDB, WorkerDB>),
                type_of_val(&UpdateWorkerMessageStatusDB::locked),
                type_of_val(
                    &By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerMessageStatusDB, WorkerDB>,
                ),
            ]);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_schedule_request(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed_collection(&collection_name, &UserId::admin())
            .await?;

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

        let _ = db.seed_function(&collection, &create).await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                    .function(FunctionIdName::from_str("function_1")?)
                    .build()?,
                ExecutionRequest::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .build()?,
            );

        let service = ExecuteFunctionService::with_defaults(db.clone()).service();
        let execution = service.raw_oneshot(request).await?;

        // Actual test
        let context = SchedulerContext::with_defaults(db.clone());
        RequestService::build(&context)
            .service()
            .oneshot(())
            .await?;

        let created_message = context.worker_queue.locked_messages().await;
        assert_eq!(created_message.len(), 1);

        let created_message = &created_message[0].payload;
        let created_message = match created_message {
            SupervisorMessagePayload::SupervisorRequestMessagePayload(message) => &message.context,
            SupervisorMessagePayload::SupervisorResponseMessagePayload(_)
            | SupervisorMessagePayload::SupervisorExceptionMessagePayload(_) => {
                panic!("Unexpected SupervisorMessagePayload")
            }
        };
        let Some(WorkerInput::FunctionV2(message)) = created_message else {
            panic!("Unexpected WorkerInput version")
        };

        // V2 assertions
        // Info
        let info = &message.info;
        let function_runs: Vec<FunctionRunDB> = db
            .fetch_all::<FunctionRunDB>(&(&execution.manual_trigger))
            .await?;
        assert_eq!(function_runs.len(), 1);
        let function_run = &function_runs[0];

        assert_eq!(info.collection_id, collection.id);
        assert_eq!(info.collection, collection.name);
        assert_eq!(info.function_version_id, function_run.function_version_id);
        assert_eq!(info.function_version_id, execution.manual_trigger);
        assert_eq!(info.function_run_id, function_run.id);

        let function_path = SPath::parse(format!(
            "/bundles/c/{}/f/{}.tgz",
            collection.id, create.bundle_id
        ))?;
        let (uri, mount_def) = context.storage.to_external_uri(&function_path)?;
        assert_eq!(info.function_bundle.uri, uri);
        assert_eq!(
            info.function_bundle.env_prefix,
            Some(EnvPrefix::try_from(&mount_def.id)?)
        );
        assert_eq!(info.triggered_on, function_run.triggered_on);
        assert!(*info.triggered_on < *info.scheduled_on);
        assert_eq!(info.execution_id, function_run.execution_id);
        assert_eq!(
            info.execution_name,
            Some(ExecutionName::try_from("test_execution")?)
        );

        let function_data_path = SPath::parse(format!(
            "/c/{}/x/{}/f/{}",
            collection.id, function_run.transaction_id, function_run.function_version_id
        ))?;
        let (uri, mount_def) = context.storage.to_external_uri(&function_data_path)?;
        assert_eq!(info.function_data.uri, uri);
        assert_eq!(
            info.function_data.env_prefix,
            Some(EnvPrefix::try_from(&mount_def.id)?)
        );

        // Input
        assert_eq!(message.input.len(), 1);
        let input_table = &message.input[0];
        match input_table {
            InputTable::Table(input) => {
                assert!(input.table_data_version_id.is_none());
                assert_eq!(input.name, TableName::try_from("table_1")?);
                assert_eq!(input.collection_id, collection.id);
                assert_eq!(input.collection, collection.name);
                assert!(input.location.is_none());
                assert_eq!(*input.table_pos, 0);
                assert_eq!(*input.version_pos, -1);
            }
            _ => panic!("Unexpected Input Table type"),
        }

        // Output
        assert_eq!(message.output.len(), 2);
        for (index, output_table) in message.output.iter().enumerate() {
            match output_table {
                OutputTable::Table(output) => {
                    let table_data_version: TableDataVersionDBWithNames = db
                        .fetch_one::<TableDataVersionDBWithNames>(&(output.table_data_version_id))
                        .await?;

                    assert_eq!(output.name, table_data_version.name);
                    assert_eq!(output.collection_id, collection.id);
                    assert_eq!(output.collection, collection.name);
                    assert_eq!(output.table_id, table_data_version.table_id);
                    assert_eq!(output.table_version_id, table_data_version.table_version_id);
                    assert_eq!(output.table_data_version_id, table_data_version.id);

                    let table_path = SPath::parse(format!(
                        "/c/{}/d/{}/t/{}/{}.t",
                        collection.id,
                        table_data_version.id,
                        table_data_version.table_id,
                        table_data_version.table_version_id,
                    ))?;
                    let (uri, mount_def) = context.storage.to_external_uri(&table_path)?;

                    assert_eq!(output.location.uri, uri);
                    assert_eq!(
                        output.location.env_prefix,
                        Some(EnvPrefix::try_from(&mount_def.id)?)
                    );
                    assert_eq!(*output.table_pos, index as i32);
                }
                _ => panic!("Unexpected Output Table type"),
            }
        }

        let message: WorkerDB = db.fetch_one::<WorkerDB>(&Some(function_run.id)).await?;
        assert_eq!(message.collection_id, collection.id);
        assert_eq!(message.execution_id, Some(execution.id));
        assert_eq!(message.function_run_id, Some(function_run.id));
        assert_eq!(
            message.function_version_id,
            function_run.function_version_id
        );
        assert_eq!(message.message_status, WorkerMessageStatus::Locked);

        Ok(())
    }
}
