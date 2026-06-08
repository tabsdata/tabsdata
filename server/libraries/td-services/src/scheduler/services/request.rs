//
// Copyright 2025 Tabs Data Inc.
//

use crate::logical::layers::dispatch_mint::mint_function_runs_on_dispatch;
use crate::scheduler::layers::dispatch::notify_dispatcher;
use crate::scheduler::layers::request::{
    create_locked_cron_trigger_workers_start_request,
    create_locked_cron_trigger_workers_stop_request,
    create_locked_function_runner_workers_start_request,
    create_locked_stage_trigger_workers_start_request,
    create_locked_stage_trigger_workers_stop_request, dispatch_at_time, enrich_minted_dispatch,
    fetch_lfr_to_dispatch, fetch_pfrs_to_dispatch, lfr_ids_from_minted_dispatch,
    materialize_replay_dispatch, pfr_ids_from_minted_dispatch,
};
use ta_services::factory::service_factory;
use td_authz::jwt::JwtConfig;
use td_common::server::{Orchestrator, OrchestratorMessageQueueHandle};
use td_execution::SchedulerNotify;
use td_objects::dxo::logical::{LogicalFunctionRunDB, UpdateLogicalFunctionRunDB};
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, UpdatePhysicalFunctionRunDB};
use td_objects::dxo::worker::{
    UpdateWorkerDB, UpdateWorkerMessageStatusDB, WorkerDB, WorkerDBWithNames,
};
use td_objects::tower_service::with::With;
use td_objects::types::addresses::InternalServerAddresses;
use td_objects::types::basic::{LogicalFunctionRunId, PhysicalFunctionRunId, WorkerId};
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
    scope = "Global",
    context = Storage,
    context = OrchestratorMessageQueueHandle,
    context = InternalServerAddresses,
    context = SchedulerNotify,
    context = Orchestrator,
    context = JwtConfig,
)]
fn service() {
    layers!(
        // Capture the dispatch tick's AtTime once, before any sub-pipeline.
        // Every readiness/mint/bind/yaml lookup in the tick shares this
        // temporal point so the dispatch is consistent even if operator
        // events arrive mid-tick.
        from_fn(dispatch_at_time),
        // Request function runners that need to be started — initial
        // dispatch path (Scheduled/ReScheduled LFRs, mint + bind + flip
        // LFR.status to Requested).
        request_function_runners(),
        // Request function runners for replay attempts — sticky LFRs
        // whose pointer was flipped to Requested by the replay
        // endpoint. No mint, no bind, no LFR.status flip.
        request_replay_function_runners(),
        // Stop cron trigger runners that need to be stopped
        stop_cron_trigger_runners(),
        // Request cron trigger runners that need to be started
        request_cron_trigger_runners(),
        // Stop stage trigger runners that need to be stopped
        stop_stage_trigger_runners(),
        // Request stage trigger runners that need to be started
        request_stage_trigger_runners(),
        from_fn(notify_dispatcher),
    )
}

// Requires:
// - Transaction connection
// - Storage
// - T(MessageQueue)
// - ServerUrl
#[layer]
pub fn request_function_runners() {
    layers!(
        // LFR-rooted readiness — Pending/ReScheduled LFRs whose logical
        // chain has no blocked input. Patch resolution happens at mint,
        // not here.
        from_fn(fetch_lfr_to_dispatch),
        // Mint FR + output TDV placeholders for each ready LFR (resolving
        // latest patch or head per LFR), then bind the LFR pointer +
        // each output LTDV pointer to the freshly-minted ids in this tx.
        from_fn(mint_function_runs_on_dispatch),
        // Project the minted set into the denormalized view the worker-yaml
        // builder consumes (one round-trip; reuses the readiness projection).
        from_fn(enrich_minted_dispatch),
        // Create a locked message for each minted FR.
        from_fn(create_locked_function_runner_workers_start_request),
        // And insert generated messages.
        from_fn(insert_vec::<WorkerDB>),
        // Flip LFR.status Scheduled/ReScheduled → Requested.
        from_fn(lfr_ids_from_minted_dispatch),
        from_fn(UpdateLogicalFunctionRunDB::requested),
        from_fn(
            By::<Vec<LogicalFunctionRunId>>::update_chunked::<
                UpdateLogicalFunctionRunDB,
                LogicalFunctionRunDB,
            >,
        ),
        // Flip PFR.status Requested → Dispatched so the replay-side
        // readiness query (`Pfr::to_dispatch`) excludes these
        // PFRs from the next tick.
        from_fn(pfr_ids_from_minted_dispatch),
        from_fn(UpdatePhysicalFunctionRunDB::dispatched),
        from_fn(
            By::<Vec<PhysicalFunctionRunId>>::update_chunked::<
                UpdatePhysicalFunctionRunDB,
                PhysicalFunctionRunDB,
            >,
        ),
    )
}

// Replay-side request pipeline: PFRs in `Requested` status that the
// replay endpoint minted without a WorkerDB. Shares enrich + worker-
// message creation + the PFR.status → Dispatched flip with the
// initial pipeline; skips mint (PFR + output TDVs already exist) and
// skips the LFR.status flip (LFR is sticky-terminal).
//
// Requires:
// - Transaction connection
// - Storage
// - T(MessageQueue)
// - ServerUrl
#[layer]
pub fn request_replay_function_runners() {
    layers!(
        // Physical-side readiness — PFRs in `Requested` with no WorkerDB.
        from_fn(fetch_pfrs_to_dispatch),
        // Build the MintedDispatch shape from rows that already exist.
        from_fn(materialize_replay_dispatch),
        from_fn(enrich_minted_dispatch),
        from_fn(create_locked_function_runner_workers_start_request),
        from_fn(insert_vec::<WorkerDB>),
        // Flip PFR.status Requested → Dispatched. No LFR.status flip
        // (sticky terminal stays put).
        from_fn(pfr_ids_from_minted_dispatch),
        from_fn(UpdatePhysicalFunctionRunDB::dispatched),
        from_fn(
            By::<Vec<PhysicalFunctionRunId>>::update_chunked::<
                UpdatePhysicalFunctionRunDB,
                PhysicalFunctionRunDB,
            >,
        ),
    )
}

// Requires:
// - Transaction connection
// - T(MessageQueue)
// - ServerUrl
#[layer]
pub fn request_cron_trigger_runners() {
    layers!(
        from_fn(
            By::<()>::fetch_all_in::<
                { WorkerDBWithNames::CronTriggersToSchedule },
                WorkerDBWithNames,
            >
        ),
        from_fn(create_locked_cron_trigger_workers_start_request),
        // Update statuses.
        from_fn(With::<Vec<WorkerDBWithNames>>::extract::<Vec<WorkerId>>),
        from_fn(UpdateWorkerDB::requested),
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
pub fn stop_cron_trigger_runners() {
    layers!(
        from_fn(
            By::<()>::fetch_all_in::<{ WorkerDBWithNames::CronTriggersToStop }, WorkerDBWithNames>
        ),
        from_fn(create_locked_cron_trigger_workers_stop_request),
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
pub fn request_stage_trigger_runners() {
    layers!(
        from_fn(
            By::<()>::fetch_all_in::<
                { WorkerDBWithNames::StageTriggersToSchedule },
                WorkerDBWithNames,
            >
        ),
        from_fn(create_locked_stage_trigger_workers_start_request),
        // Update statuses.
        from_fn(With::<Vec<WorkerDBWithNames>>::extract::<Vec<WorkerId>>),
        from_fn(UpdateWorkerDB::requested),
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
pub fn stop_stage_trigger_runners() {
    layers!(
        from_fn(
            By::<()>::fetch_all_in::<{ WorkerDBWithNames::StageTriggersToStop }, WorkerDBWithNames>
        ),
        from_fn(create_locked_stage_trigger_workers_stop_request),
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
    use http::Method;
    use std::str::FromStr;
    use std::sync::Arc;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_authz::jwt::decode_internal_token;
    use td_common::server::OrchestratorMessagePayload;
    use td_common::server::{
        Callback, Decorator as MessageDecorator, FunctionRequest, InMemoryOrchestratorMessageQueue,
        OrchestratorMessageQueueHandle,
    };
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionRequest;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
    use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBWithNames;
    use td_objects::dxo::yaml::WorkerInput;
    use td_objects::dxo::yaml::function_runner::v2::{InputTable, OutputTable};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, EnvPrefix,
        ExecutionName, FunctionIdName, FunctionRuntimeValues, ProjectId, RoleId, TableName,
        TableNameDto, UserId, WorkerMessageStatus,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::test_utils::SeedEntry;
    use td_queries::{LiveEntry, VersionedEntry};
    use td_storage::SPath;
    use td_tower::ctx_service::RawOneshot;
    use td_tower::project_scope::InProject;
    use tower::ServiceExt;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_schedule_request(db: TdPool) -> Result<(), TdError> {
        use td_tower::metadata::type_of_val;

        RequestService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<(), ()>(&[
                type_of_val(&dispatch_at_time),
                // Function — initial
                type_of_val(&fetch_lfr_to_dispatch),
                type_of_val(&mint_function_runs_on_dispatch),
                type_of_val(&enrich_minted_dispatch),
                type_of_val(&create_locked_function_runner_workers_start_request),
                type_of_val(&insert_vec::<WorkerDB>),
                type_of_val(&lfr_ids_from_minted_dispatch),
                type_of_val(&UpdateLogicalFunctionRunDB::requested),
                type_of_val(
                    &By::<Vec<LogicalFunctionRunId>>::update_chunked::<
                        UpdateLogicalFunctionRunDB,
                        LogicalFunctionRunDB,
                    >,
                ),
                type_of_val(&pfr_ids_from_minted_dispatch),
                type_of_val(&UpdatePhysicalFunctionRunDB::dispatched),
                type_of_val(
                    &By::<Vec<PhysicalFunctionRunId>>::update_chunked::<
                        UpdatePhysicalFunctionRunDB,
                        PhysicalFunctionRunDB,
                    >,
                ),
                // Function — replay
                type_of_val(&fetch_pfrs_to_dispatch),
                type_of_val(&materialize_replay_dispatch),
                type_of_val(&enrich_minted_dispatch),
                type_of_val(&create_locked_function_runner_workers_start_request),
                type_of_val(&insert_vec::<WorkerDB>),
                type_of_val(&pfr_ids_from_minted_dispatch),
                type_of_val(&UpdatePhysicalFunctionRunDB::dispatched),
                type_of_val(
                    &By::<Vec<PhysicalFunctionRunId>>::update_chunked::<
                        UpdatePhysicalFunctionRunDB,
                        PhysicalFunctionRunDB,
                    >,
                ),
                // CronTrigger
                // Stop
                type_of_val(
                    &By::<()>::fetch_all_in::<
                        { WorkerDBWithNames::CronTriggersToStop },
                        WorkerDBWithNames,
                    >,
                ),
                type_of_val(&create_locked_cron_trigger_workers_stop_request),
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
                type_of_val(&create_locked_cron_trigger_workers_start_request),
                // Update statuses.
                type_of_val(&With::<Vec<WorkerDBWithNames>>::extract::<Vec<WorkerId>>),
                type_of_val(&UpdateWorkerDB::requested),
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
                type_of_val(&create_locked_stage_trigger_workers_stop_request),
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
                type_of_val(&create_locked_stage_trigger_workers_start_request),
                // Update statuses.
                type_of_val(&With::<Vec<WorkerDBWithNames>>::extract::<Vec<WorkerId>>),
                type_of_val(&UpdateWorkerDB::requested),
                type_of_val(&By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerDB, WorkerDB>),
                type_of_val(&UpdateWorkerMessageStatusDB::locked),
                type_of_val(
                    &By::<Vec<WorkerId>>::update_chunked::<UpdateWorkerMessageStatusDB, WorkerDB>,
                ),
                type_of_val(&notify_dispatcher),
            ]);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_schedule_request(db: TdPool) -> Result<(), TdError> {
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .seed()
            .collection(&collection_name, &UserId::admin())
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

        let _ = db.seed().function(&collection, &create).await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .create(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                        .function(FunctionIdName::from_str("function_1")?)
                        .build()?,
                    ExecutionRequest::builder()
                        .name(Some(ExecutionName::try_from("test_execution")?))
                        .build()?,
                )
                .in_project(ProjectId::sandbox());

        let service = ExecuteFunctionService::with_defaults(db.clone()).service();
        let execution = service.raw_oneshot(request).await?;

        // Actual test
        let context = SchedulerContext::with_defaults(db.clone());
        RequestService::build(&context)
            .service()
            .oneshot(())
            .await?;

        let created_message = context.orchestrator_queue.locked_messages().await;
        assert_eq!(created_message.len(), 1);

        let created_message = &created_message[0].payload;
        let created_message = match created_message {
            OrchestratorMessagePayload::OrchestratorRequestMessagePayload(message) => {
                &message.context
            }
            OrchestratorMessagePayload::OrchestratorResponseMessagePayload(_)
            | OrchestratorMessagePayload::OrchestratorExceptionMessagePayload(_)
            | OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(_) => {
                panic!("Unexpected OrchestratorMessagePayload")
            }
        };
        let created_message: Option<WorkerInput> = created_message
            .as_ref()
            .map(|value| serde_yaml::from_value(value.clone()).expect("WorkerInput"));
        let Some(WorkerInput::FunctionV2(message)) = created_message else {
            panic!("Unexpected WorkerInput version")
        };

        // V2 assertions
        // Info
        let info = &message.info;
        let physical_function_runs: Vec<PhysicalFunctionRunDB> = db
            .live()
            .fetch_all::<PhysicalFunctionRunDB>(&(&execution.manual_trigger))
            .await?;
        assert_eq!(physical_function_runs.len(), 1);
        let physical_function_run = &physical_function_runs[0];

        assert_eq!(info.collection_id, collection.id);
        assert_eq!(info.collection, collection.name);
        assert_eq!(
            info.function_version_id,
            physical_function_run.function_version_id
        );
        assert_eq!(info.function_version_id, execution.manual_trigger);
        assert_eq!(info.function_run_id, physical_function_run.id);

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
        assert_eq!(info.triggered_on, physical_function_run.triggered_on);
        assert!(*info.triggered_on < *info.scheduled_on);
        assert_eq!(info.execution_id, physical_function_run.execution_id);
        assert_eq!(
            info.execution_name,
            Some(ExecutionName::try_from("test_execution")?)
        );

        let function_data_path = SPath::parse(format!(
            "/c/{}/x/{}/f/{}",
            collection.id,
            physical_function_run.transaction_id,
            physical_function_run.function_version_id
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
                    let physical_table_data_version: PhysicalTableDataVersionDBWithNames = db
                        .live()
                        .fetch_one::<PhysicalTableDataVersionDBWithNames>(
                            &(output.table_data_version_id),
                        )
                        .await?;

                    assert_eq!(output.name, physical_table_data_version.name);
                    assert_eq!(output.collection_id, collection.id);
                    assert_eq!(output.collection, collection.name);
                    assert_eq!(output.table_id, physical_table_data_version.table_id);
                    assert_eq!(
                        output.table_version_id,
                        physical_table_data_version.table_version_id
                    );
                    assert_eq!(output.table_data_version_id, physical_table_data_version.id);

                    let table_path = SPath::parse(format!(
                        "/c/{}/d/{}/t/{}/{}.t",
                        collection.id,
                        physical_table_data_version.id,
                        physical_table_data_version.table_id,
                        physical_table_data_version.table_version_id,
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

        let message: WorkerDB = db
            .live()
            .fetch_one::<WorkerDB>(&Some(physical_function_run.id.clone()))
            .await?;
        assert_eq!(message.collection_id, collection.id);
        assert_eq!(message.execution_id, Some(execution.id));
        assert_eq!(
            message.physical_function_run_id,
            Some(physical_function_run.id.clone())
        );
        assert_eq!(
            message.function_version_id,
            physical_function_run.function_version_id
        );
        assert_eq!(message.message_status, WorkerMessageStatus::Locked);

        Ok(())
    }

    /// End-to-end: seed a sticky-Committed LFR + its initial FR/LTDV
    /// rows, call the replay service to mint a new PFR + Requested
    /// pointer event, then run `RequestService` and verify it picks
    /// the Requested pointer up and creates a worker message for the
    /// fresh PFR. LFR.status must remain Committed; PFR count must be
    /// two (the seeded one + the replay one).
    #[td_test::test(db)]
    async fn test_schedule_request_replay(db: TdPool) -> Result<(), TdError> {
        use crate::logical::services::replay::ReplayLogicalFunctionRunService;
        use td_objects::and;
        use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunPointerDB};
        use td_objects::rest_urls::LogicalFunctionRunParam;
        use td_objects::types::basic::{
            LogicalFunctionRunStatus, LogicalPointerStatus, PhysicalFunctionRunStatus,
            TransactionKey,
        };

        // Seed a Committed function run by hand (mirrors a previously-
        // dispatched, committed execution). seed_physical_function_run
        // also seeds the LTDV identity rows + InitialMint Pending
        // pointer events the append-Requested layer relies on.
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("c_replay")?, &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .try_name("fr_replay")?
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("t0")?])
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .reuse_frozen_tables(false)
            .build()?;
        let function = db.seed().function(&collection, &create).await?;
        let execution = db.seed().execution(&function).await?;
        let txn = db
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY")?)
            .await?;
        let fr_old = db
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &txn,
                &LogicalFunctionRunStatus::Committed,
            )
            .await?;
        let lfr: LogicalFunctionRunDB = db
            .live()
            .fetch_one::<LogicalFunctionRunDB>(and!(&fr_old.execution_id, &fr_old.function_id))
            .await?;
        let lfr_id = lfr.id.clone();

        // Call replay — mints FR_new + Requested(FR_new) pointer event.
        let replay_param = LogicalFunctionRunParam::builder()
            .collection(CollectionIdName::from_str("c_replay")?)
            .logical_function_run(lfr_id.clone())
            .build()?;
        let replay_request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .create(replay_param, ())
                .in_project(ProjectId::sandbox());
        let replay_response = ReplayLogicalFunctionRunService::with_defaults(db.clone())
            .service()
            .raw_oneshot(replay_request)
            .await?;
        assert_ne!(replay_response.new_function_run_id, fr_old.id);

        // After replay-mint, two FRs exist under the LFR; LFR.status
        // is still Committed; the latest pointer is Requested(FR_new).
        let frs: Vec<PhysicalFunctionRunDB> = db
            .live()
            .fetch_all::<PhysicalFunctionRunDB>(and!(&lfr.execution_id, &lfr.function_id))
            .await?;
        assert_eq!(frs.len(), 2);

        // Run the scheduler request service. It should pick up the
        // Requested pointer and create a locked worker message for
        // the new PFR. No status flip on the LFR.
        let context = SchedulerContext::with_defaults(db.clone());
        RequestService::build(&context)
            .service()
            .oneshot(())
            .await?;

        let messages = context.orchestrator_queue.locked_messages().await;
        assert_eq!(
            messages.len(),
            1,
            "expected exactly one worker message for the replay PFR"
        );

        // The worker message references the new PFR.
        let payload = &messages[0].payload;
        let context_yaml = match payload {
            OrchestratorMessagePayload::OrchestratorRequestMessagePayload(m) => &m.context,
            _ => panic!("unexpected payload kind"),
        };
        let context_yaml: Option<WorkerInput> = context_yaml
            .as_ref()
            .map(|value| serde_yaml::from_value(value.clone()).expect("WorkerInput"));
        let Some(WorkerInput::FunctionV2(msg)) = context_yaml else {
            panic!("unexpected WorkerInput version");
        };
        assert_eq!(
            msg.info.function_run_id,
            replay_response.new_function_run_id
        );

        // A WorkerDB row was created referencing the new PFR.
        let worker: WorkerDB = db
            .live()
            .fetch_one::<WorkerDB>(&Some(replay_response.new_function_run_id.clone()))
            .await?;
        assert_eq!(worker.message_status, WorkerMessageStatus::Locked);
        assert_eq!(
            worker.physical_function_run_id,
            Some(replay_response.new_function_run_id.clone())
        );

        // LFR.status untouched.
        let lfr_after = db.live().fetch_one::<LogicalFunctionRunDB>(&lfr_id).await?;
        assert_eq!(lfr_after.status, LogicalFunctionRunStatus::Committed);

        // Pointer log untouched by the replay endpoint — the only
        // events under this LFR are the seed's InitialMint + Bound,
        // both still pointing at the original PFR. Pointer redirect
        // only happens at worker finish via `redirect_at_finish`.
        let latest = db
            .versioned(&td_objects::types::basic::AtTime::now())
            .fetch_one::<LogicalFunctionRunPointerDB>(&lfr_id)
            .await?;
        assert!(matches!(latest.status, LogicalPointerStatus::Bound));
        assert_eq!(latest.physical_function_run_id, Some(fr_old.id.clone()));

        // PFR.status flipped to Dispatched by the request layer.
        let pfr_new = db
            .live()
            .fetch_one::<PhysicalFunctionRunDB>(&replay_response.new_function_run_id)
            .await?;
        assert_eq!(pfr_new.status, PhysicalFunctionRunStatus::Dispatched);

        // Idempotency: a second tick must not create another worker
        // message — `to_dispatch` filters on PFR.status=Requested,
        // and we just flipped to Dispatched.
        let prior_messages_count = messages.len();
        RequestService::build(&context)
            .service()
            .oneshot(())
            .await?;
        let messages_after = context.orchestrator_queue.locked_messages().await;
        assert_eq!(
            messages_after.len(),
            prior_messages_count,
            "second tick must not enqueue a duplicate worker for the same replay PFR"
        );

        Ok(())
    }

    /// In kubernetes mode the request path emits the per-action
    /// `FunctionRequest::Start` kind (not the flat request), carrying the
    /// decorator + resolver, with no pre-resolved context. WorkerDB stays Locked.
    #[td_test::test(db)]
    async fn test_schedule_request_kubernetes(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("c_k8s")?, &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .try_name("function_1")?
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(Decorator::Publisher)
            .dependencies(vec![TableDependencyDto::try_from("table_1")?])
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_1")?])
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .reuse_frozen_tables(false)
            .build()?;
        let _ = db.seed().function(&collection, &create).await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .create(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                        .function(FunctionIdName::from_str("function_1")?)
                        .build()?,
                    ExecutionRequest::builder()
                        .name(Some(ExecutionName::try_from("e_k8s")?))
                        .build()?,
                )
                .in_project(ProjectId::sandbox());
        ExecuteFunctionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await?;

        // Run the request tick with the kubernetes orchestrator. Use an
        // in-memory queue (the file queue only accepts request messages).
        let mut context = SchedulerContext::with_defaults(db.clone());
        context.orchestrator_queue = Arc::new(OrchestratorMessageQueueHandle::new(Arc::new(
            InMemoryOrchestratorMessageQueue::default(),
        )));
        context.orchestrator = Arc::new(Orchestrator::Kubernetes);
        RequestService::build(&context)
            .service()
            .oneshot(())
            .await?;

        let created = context.orchestrator_queue.locked_messages().await;
        assert_eq!(created.len(), 1);

        // Kubernetes emits the per-action FunctionRequest kind, carrying the
        // decorator + resolver, with no pre-resolved context.
        let OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
            FunctionRequest::Start(start),
        ) = &created[0].payload
        else {
            panic!("expected a FunctionRequest::Start payload");
        };
        assert_eq!(start.decorator, MessageDecorator::Publisher);

        // Resolver is a GET to the resolve endpoint; responder is a POST.
        let Callback::Http(resolver) = &start.resolver;
        assert_eq!(resolver.method, Method::GET);
        assert!(resolver.url.path().ends_with("/message"));
        let Callback::Http(responder) = &start.responder;
        assert_eq!(responder.method, Method::POST);

        // WorkerDB row still created Locked.
        let worker_id = WorkerId::try_from(created[0].id.as_str())?;
        let worker: WorkerDB = db.live().fetch_one::<WorkerDB>(&worker_id).await?;
        assert_eq!(worker.message_status, WorkerMessageStatus::Locked);
        let function_run_id = worker.physical_function_run_id.clone().unwrap();

        // The resolver carries a valid internal token bound to this exact
        // request: GET, the /message scope, and the worker's resource ids.
        let bearer = resolver
            .headers
            .get("authorization")
            .and_then(|h| h.strip_prefix("Bearer "))
            .expect("authorization bearer header");
        let claims = decode_internal_token(&context.jwt_config, bearer)?;
        assert_eq!(claims.method, "GET");
        assert!(claims.scope.ends_with("/message"));
        assert_eq!(
            claims.bindings.get("worker_id").unwrap(),
            &worker_id.to_string()
        );
        assert_eq!(
            claims.bindings.get("function_run_id").unwrap(),
            &function_run_id.to_string()
        );

        Ok(())
    }
}
