//
// Copyright 2025 Tabsdata Inc.
//

use crate::execution::layers::update_status::{
    abandon_requested_physical_runs, update_function_run_status,
};
use crate::logical::layers::dispatch_mint::mint_function_runs_on_dispatch;
use crate::scheduler::layers::dispatch::notify_dispatcher;
use crate::scheduler::layers::request::{
    create_locked_cron_trigger_workers_start_request,
    create_locked_cron_trigger_workers_stop_request,
    create_locked_function_runner_workers_start_request,
    create_locked_stage_trigger_workers_start_request,
    create_locked_stage_trigger_workers_stop_request, dispatch_at_time, enrich_minted_dispatch,
    fail_stale_abandoned_attempts, fetch_lfr_to_dispatch, fetch_pfrs_to_dispatch,
    fetch_stale_dispatched_runs, lfr_ids_from_minted_dispatch, materialize_replay_dispatch,
    pfr_ids_from_dispatched_workers, stop_worker_update, stuck_lfrs_from_stale,
};
use ta_services::factory::service_factory;
use td_bridge::BridgeProviderHandle;
use td_common::jwt::JwtConfig;
use td_execution::{SchedulerConfig, SchedulerNotify};
use td_objects::dxo::logical::{LogicalFunctionRunDB, UpdateLogicalFunctionRunDB};
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, UpdatePhysicalFunctionRunDB};
use td_objects::dxo::worker::{
    UpdateWorkerDB, UpdateWorkerMessageStatusDB, WorkerDB, WorkerDBWithNames,
};
use td_objects::tower_service::with::With;
use td_objects::types::addresses::InternalServerEndpoint;
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
    context = BridgeProviderHandle,
    context = InternalServerEndpoint,
    context = SchedulerNotify,
    context = JwtConfig,
    context = SchedulerConfig,
)]
fn service() {
    layers!(
        // Capture the dispatch tick's AtTime once, before any sub-pipeline.
        // Every readiness/mint/bind/yaml lookup in the tick shares this
        // temporal point so the dispatch is consistent even if operator
        // events arrive mid-tick.
        from_fn(dispatch_at_time),
        // Runs first so recovered LFRs re-dispatch in this same tick.
        auto_recover_stuck_function_runners(),
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

// Stale `Dispatched` PFRs (pod never provisioned) → `Failed`; those under
// a `Requested` LFR additionally get the operator-recover flow.
#[layer]
pub fn auto_recover_stuck_function_runners() {
    layers!(
        from_fn(fetch_stale_dispatched_runs),
        from_fn(fail_stale_abandoned_attempts),
        from_fn(stuck_lfrs_from_stale),
        from_fn(abandon_requested_physical_runs),
        from_fn(UpdateLogicalFunctionRunDB::recover),
        from_fn(update_function_run_status),
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
        from_fn(pfr_ids_from_dispatched_workers),
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
        from_fn(pfr_ids_from_dispatched_workers),
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
        from_fn(stop_worker_update),
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
        from_fn(stop_worker_update),
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
    use std::collections::HashSet;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_bridge::messaging::memory::InMemoryOrchestratorMessageQueue;
    use td_bridge::messaging::{
        CronRequest, FunctionRequest, OrchestratorMessage, OrchestratorMessagePayload,
        OrchestratorMessageQueue, QueueError, StageTriggerRequest,
    };
    use td_bridge::test::TestBridgeProvider;
    use td_bridge::{BridgeProviderHandle, Orchestrator};
    use td_common::jwt::decode_internal_token;
    use td_common::worker::{Callback, Decorator as MessageDecorator, WorkerName};
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_error::TdError;
    use td_execution::StuckRequestedTimeout;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionRequest;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
    use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBWithNames;
    use td_objects::dxo::yaml::WorkerInput;
    use td_objects::dxo::yaml::function::{InputTable, OutputTable};
    use td_objects::rest_urls::FunctionParam;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, CronMask, Decorator, EnvPrefix,
        ExecutionName, FunctionIdName, FunctionRuntimeValues, LogicalFunctionRunStatus,
        PhysicalFunctionRunStatus, ProjectId, TableName, TableNameDto, UserId, WorkerMessageStatus,
        WorkerStatus, WorkerType,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::test_utils::SeedEntry;
    use td_queries::{LiveEntry, VersionedEntry};
    use td_storage::SPath;
    use td_tower::ctx_service::RawOneshot;
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
                // Auto-recover sweep
                type_of_val(&fetch_stale_dispatched_runs),
                type_of_val(&fail_stale_abandoned_attempts),
                type_of_val(&stuck_lfrs_from_stale),
                type_of_val(&abandon_requested_physical_runs),
                type_of_val(&UpdateLogicalFunctionRunDB::recover),
                type_of_val(&update_function_run_status),
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
                type_of_val(&pfr_ids_from_dispatched_workers),
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
                type_of_val(&pfr_ids_from_dispatched_workers),
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
                type_of_val(&stop_worker_update),
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
                type_of_val(&stop_worker_update),
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
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
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

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                    .function(FunctionIdName::from_str("function_1")?)
                    .build()?,
                ExecutionRequest::builder()
                    .name(Some(ExecutionName::try_from("test_execution")?))
                    .build()?,
            )
            .in_project(p.id);

        let service = ExecuteFunctionService::with_defaults(db.clone()).service();
        let execution = service.raw_oneshot(request).await?;

        // Actual test
        let context = SchedulerContext::with_defaults(db.clone());
        RequestService::build(&context)
            .service()
            .oneshot(())
            .await?;

        let created_message = context.bridge.message_queue().locked_messages().await;
        assert_eq!(created_message.len(), 1);

        let created_message = &created_message[0].payload;
        let created_message = match created_message {
            OrchestratorMessagePayload::OrchestratorRequestMessagePayload(message) => {
                &message.context
            }
            OrchestratorMessagePayload::OrchestratorResponseMessagePayload(_)
            | OrchestratorMessagePayload::OrchestratorExceptionMessagePayload(_)
            | OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(_)
            | OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(_)
            | OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(_) => {
                panic!("Unexpected OrchestratorMessagePayload")
            }
        };
        let created_message: Option<WorkerInput> = created_message
            .as_ref()
            .map(|value| serde_yaml::from_value(value.clone()).expect("WorkerInput"));
        let Some(WorkerInput::Function(message)) = created_message else {
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
            "/bundles/c/{}/f/{}.tar.zst",
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

        let p = db.seed().test_project().await?;

        // Seed a Committed function run by hand (mirrors a previously-
        // dispatched, committed execution). seed_physical_function_run
        // also seeds the LTDV identity rows + InitialMint Pending
        // pointer events the append-Requested layer relies on.
        let collection = db
            .scoped(p.id)
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
        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let execution = db.scoped(p.id).seed().execution(&function).await?;
        let txn = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY")?)
            .await?;
        let fr_old = db
            .scoped(p.id)
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
            .logical_function_run(lfr_id.clone())
            .build()?;
        let replay_request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(replay_param, ());
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

        let messages = context.bridge.message_queue().locked_messages().await;
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
        let Some(WorkerInput::Function(msg)) = context_yaml else {
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
        let messages_after = context.bridge.message_queue().locked_messages().await;
        assert_eq!(
            messages_after.len(),
            prior_messages_count,
            "second tick must not enqueue a duplicate worker for the same replay PFR"
        );

        Ok(())
    }

    fn stuck_timeout_context(db: &TdPool, timeout: StuckRequestedTimeout) -> SchedulerContext {
        let mut context = SchedulerContext::with_defaults(db.clone());
        context.scheduler_config = Arc::new(SchedulerConfig {
            stuck_requested_timeout: timeout,
            ..SchedulerConfig::default()
        });
        context
    }

    #[td_test::test(db)]
    async fn test_auto_recover_stuck_requested(db: TdPool) -> Result<(), TdError> {
        use td_objects::and;
        use td_objects::dxo::logical::LogicalFunctionRunDB;

        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("c_stuck")?, &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .try_name("f_stuck")?
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
        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                    .function(FunctionIdName::from_str("f_stuck")?)
                    .build()?,
                ExecutionRequest::builder().name(None).build()?,
            )
            .in_project(p.id);
        let execution = ExecuteFunctionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await?;

        let context_1 = SchedulerContext::with_defaults(db.clone());
        RequestService::build(&context_1)
            .service()
            .oneshot(())
            .await?;
        let pfrs: Vec<PhysicalFunctionRunDB> = db
            .live()
            .fetch_all::<PhysicalFunctionRunDB>(&(&execution.manual_trigger))
            .await?;
        assert_eq!(pfrs.len(), 1);
        let pfr_old = pfrs[0].clone();
        assert_eq!(pfr_old.status, PhysicalFunctionRunStatus::Dispatched);
        assert!(pfr_old.dispatched_on.is_some());
        assert_eq!(
            context_1
                .bridge
                .message_queue()
                .locked_messages()
                .await
                .len(),
            1
        );

        // A freshly-dispatched run is well within the default timeout — the
        // sweep must not touch it.
        let context_fresh = stuck_timeout_context(&db, StuckRequestedTimeout::default());
        RequestService::build(&context_fresh)
            .service()
            .oneshot(())
            .await?;
        let pfr: PhysicalFunctionRunDB = db
            .live()
            .fetch_one::<PhysicalFunctionRunDB>(&pfr_old.id)
            .await?;
        assert_eq!(pfr.status, PhysicalFunctionRunStatus::Dispatched);
        assert!(
            context_fresh
                .bridge
                .message_queue()
                .locked_messages()
                .await
                .is_empty()
        );

        let context_2 = stuck_timeout_context(&db, StuckRequestedTimeout(Duration::ZERO));
        RequestService::build(&context_2)
            .service()
            .oneshot(())
            .await?;

        let pfr_failed: PhysicalFunctionRunDB = db
            .live()
            .fetch_one::<PhysicalFunctionRunDB>(&pfr_old.id)
            .await?;
        assert_eq!(pfr_failed.status, PhysicalFunctionRunStatus::Failed);
        assert!(pfr_failed.ended_on.is_some());
        assert!(pfr_failed.ended_on_hour_ms.is_some());
        assert!(pfr_failed.ended_on_day_ms.is_some());
        assert!(pfr_failed.ended_on_week_ms.is_some());
        assert!(pfr_failed.ended_on_month_ms.is_some());

        let pfrs: Vec<PhysicalFunctionRunDB> = db
            .live()
            .fetch_all::<PhysicalFunctionRunDB>(&(&execution.manual_trigger))
            .await?;
        assert_eq!(pfrs.len(), 2);
        let pfr_new = pfrs.iter().find(|pfr| pfr.id != pfr_old.id).unwrap();
        assert_eq!(pfr_new.status, PhysicalFunctionRunStatus::Dispatched);
        assert!(pfr_new.dispatched_on.is_some());

        let lfr: LogicalFunctionRunDB = db
            .live()
            .fetch_one::<LogicalFunctionRunDB>(and!(&pfr_old.execution_id, &pfr_old.function_id))
            .await?;
        assert_eq!(lfr.status, LogicalFunctionRunStatus::Requested);
        assert_eq!(
            context_2
                .bridge
                .message_queue()
                .locked_messages()
                .await
                .len(),
            1
        );

        let context_3 = SchedulerContext::with_defaults(db.clone());
        RequestService::build(&context_3)
            .service()
            .oneshot(())
            .await?;
        let pfrs: Vec<PhysicalFunctionRunDB> = db
            .live()
            .fetch_all::<PhysicalFunctionRunDB>(&(&execution.manual_trigger))
            .await?;
        assert_eq!(pfrs.len(), 2);
        let pfr: PhysicalFunctionRunDB = db
            .live()
            .fetch_one::<PhysicalFunctionRunDB>(&pfr_new.id)
            .await?;
        assert_eq!(pfr.status, PhysicalFunctionRunStatus::Dispatched);
        assert!(
            context_3
                .bridge
                .message_queue()
                .locked_messages()
                .await
                .is_empty()
        );

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_auto_recover_fails_stale_replay(db: TdPool) -> Result<(), TdError> {
        use crate::logical::services::replay::ReplayLogicalFunctionRunService;
        use td_objects::and;
        use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunPointerDB};
        use td_objects::rest_urls::LogicalFunctionRunParam;
        use td_objects::types::basic::{AtTime, LogicalPointerStatus, TransactionKey};

        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("c_stuck_rp")?, &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .try_name("f_stuck_rp")?
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
        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let execution = db.scoped(p.id).seed().execution(&function).await?;
        let txn = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY")?)
            .await?;
        let fr_old = db
            .scoped(p.id)
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

        let replay_request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                LogicalFunctionRunParam::builder()
                    .logical_function_run(lfr.id.clone())
                    .build()?,
                (),
            );
        let replay_response = ReplayLogicalFunctionRunService::with_defaults(db.clone())
            .service()
            .raw_oneshot(replay_request)
            .await?;

        let context_1 = SchedulerContext::with_defaults(db.clone());
        RequestService::build(&context_1)
            .service()
            .oneshot(())
            .await?;
        let pfr: PhysicalFunctionRunDB = db
            .live()
            .fetch_one::<PhysicalFunctionRunDB>(&replay_response.new_function_run_id)
            .await?;
        assert_eq!(pfr.status, PhysicalFunctionRunStatus::Dispatched);
        assert!(pfr.dispatched_on.is_some());

        let context_2 = stuck_timeout_context(&db, StuckRequestedTimeout(Duration::ZERO));
        RequestService::build(&context_2)
            .service()
            .oneshot(())
            .await?;

        let pfr: PhysicalFunctionRunDB = db
            .live()
            .fetch_one::<PhysicalFunctionRunDB>(&replay_response.new_function_run_id)
            .await?;
        assert_eq!(pfr.status, PhysicalFunctionRunStatus::Failed);
        assert!(pfr.ended_on.is_some());

        let lfr_after: LogicalFunctionRunDB =
            db.live().fetch_one::<LogicalFunctionRunDB>(&lfr.id).await?;
        assert_eq!(lfr_after.status, LogicalFunctionRunStatus::Committed);
        let latest = db
            .versioned(&AtTime::now())
            .fetch_one::<LogicalFunctionRunPointerDB>(&lfr.id)
            .await?;
        assert!(matches!(latest.status, LogicalPointerStatus::Bound));
        assert_eq!(latest.physical_function_run_id, Some(fr_old.id.clone()));
        assert!(
            context_2
                .bridge
                .message_queue()
                .locked_messages()
                .await
                .is_empty()
        );

        Ok(())
    }

    /// In kubernetes mode the request path emits the per-action
    /// `FunctionRequest::Start` kind (not the flat request), carrying the
    /// decorator + resolver, with no pre-resolved context. WorkerDB stays Locked.
    #[td_test::test(db)]
    async fn test_schedule_request_kubernetes(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
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
        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                FunctionParam::builder()
                    .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                    .function(FunctionIdName::from_str("function_1")?)
                    .build()?,
                ExecutionRequest::builder()
                    .name(Some(ExecutionName::try_from("e_k8s")?))
                    .build()?,
            )
            .in_project(p.id);
        ExecuteFunctionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await?;

        // Run the request tick with the kubernetes orchestrator.
        let context = kubernetes_scheduler_context(&db);
        RequestService::build(&context)
            .service()
            .oneshot(())
            .await?;

        let created = context.bridge.message_queue().locked_messages().await;
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

        // The resolver carries a valid internal token bound to this exact
        // request: GET, the /message scope, and the worker id (the
        // function_run_id is fetched from the worker row).
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

        Ok(())
    }

    /// A `SchedulerContext` wired to the kubernetes orchestrator with an
    /// in-memory queue (the file queue only accepts flat request messages).
    fn kubernetes_scheduler_context(db: &TdPool) -> SchedulerContext {
        kubernetes_scheduler_context_with_queue(db, InMemoryOrchestratorMessageQueue::default())
    }

    fn kubernetes_scheduler_context_with_queue(
        db: &TdPool,
        queue: impl OrchestratorMessageQueue + 'static,
    ) -> SchedulerContext {
        let mut context = SchedulerContext::with_defaults(db.clone());
        context.bridge = Arc::new(BridgeProviderHandle::new(Arc::new(
            TestBridgeProvider::default()
                .with_orchestrator(Orchestrator::Kubernetes)
                .with_message_queue(queue),
        )));
        context
    }

    /// An in-memory queue that fails `put` for selected message ids or for
    /// the first N puts — drives the per-item isolation paths in the tick.
    struct FailPutQueue {
        inner: InMemoryOrchestratorMessageQueue,
        fail_ids: HashSet<String>,
        fail_first: AtomicUsize,
    }

    impl FailPutQueue {
        fn failing_ids(ids: impl IntoIterator<Item = String>) -> Self {
            Self {
                inner: InMemoryOrchestratorMessageQueue::default(),
                fail_ids: ids.into_iter().collect(),
                fail_first: AtomicUsize::new(0),
            }
        }

        fn failing_first(n: usize) -> Self {
            Self {
                inner: InMemoryOrchestratorMessageQueue::default(),
                fail_ids: HashSet::new(),
                fail_first: AtomicUsize::new(n),
            }
        }
    }

    #[async_trait::async_trait]
    impl OrchestratorMessageQueue for FailPutQueue {
        async fn put(
            &self,
            id: String,
            payload: OrchestratorMessagePayload<serde_yaml::Value>,
        ) -> Result<OrchestratorMessage<serde_yaml::Value>, QueueError> {
            let fail_burst = self
                .fail_first
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
            if fail_burst || self.fail_ids.contains(&id) {
                return Err(QueueError::BackendError {
                    cause: format!("injected put failure for '{id}'"),
                });
            }
            self.inner.put(id, payload).await
        }

        async fn commit(
            &self,
            message: &OrchestratorMessage<serde_yaml::Value>,
        ) -> Result<(), QueueError> {
            self.inner.commit(message).await
        }

        async fn rollback(
            &self,
            message: &OrchestratorMessage<serde_yaml::Value>,
        ) -> Result<(), QueueError> {
            self.inner.rollback(message).await
        }

        async fn locked_messages(&self) -> Vec<OrchestratorMessage<serde_yaml::Value>> {
            self.inner.locked_messages().await
        }
    }

    /// Assert a callback is a tokenized HTTP POST.
    fn assert_http_post(callback: &Callback) {
        let Callback::Http(http) = callback;
        assert_eq!(http.method, Method::POST);
    }

    /// Seed a collection + function and a worker of `worker_type` in
    /// `worker_status`/`message_status`.
    async fn seed_trigger_worker(
        db: &TdPool,
        project_id: ProjectId,
        collection_name: &str,
        worker_type: WorkerType,
        worker_status: WorkerStatus,
        message_status: WorkerMessageStatus,
    ) -> Result<WorkerDB, TdError> {
        let collection = db
            .scoped(project_id)
            .seed()
            .collection(
                &CollectionName::try_from(collection_name)?,
                &UserId::admin(),
            )
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
        let function = db
            .scoped(project_id)
            .seed()
            .function(&collection, &create)
            .await?;
        db.scoped(project_id)
            .seed()
            .worker(
                &collection,
                &function,
                &worker_type,
                &worker_status,
                &message_status,
            )
            .await
    }

    /// Seed a collection + function and a worker of `worker_type` in
    /// `worker_status`, then run the request tick against the kubernetes
    /// orchestrator with an in-memory queue. Returns the single enqueued
    /// message plus the seeded worker.
    async fn schedule_trigger_kubernetes(
        db: &TdPool,
        project_id: ProjectId,
        collection_name: &str,
        worker_type: WorkerType,
        worker_status: WorkerStatus,
    ) -> Result<
        (
            OrchestratorMessagePayload<serde_yaml::Value>,
            String,
            WorkerDB,
        ),
        TdError,
    > {
        let worker = seed_trigger_worker(
            db,
            project_id,
            collection_name,
            worker_type,
            worker_status,
            WorkerMessageStatus::Locked,
        )
        .await?;

        let context = kubernetes_scheduler_context(db);
        RequestService::build(&context)
            .service()
            .oneshot(())
            .await?;

        let mut created = context.bridge.message_queue().locked_messages().await;
        assert_eq!(created.len(), 1);
        let message = created.remove(0);
        Ok((message.payload, message.id, worker))
    }

    /// One poisoned worker must not abort the tick: the failing stop is
    /// skipped (stays `StopScheduled`, re-pickable next tick) while the
    /// healthy one completes — and the kubernetes stop is synchronous, so
    /// its row lands directly in `Stopped`.
    #[td_test::test(db)]
    async fn test_stop_stage_trigger_isolation_kubernetes(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let poisoned = seed_trigger_worker(
            &db,
            p.id,
            "c_stop_iso_1",
            WorkerType::StageTrigger,
            WorkerStatus::StopScheduled,
            WorkerMessageStatus::Unlocked,
        )
        .await?;
        let healthy = seed_trigger_worker(
            &db,
            p.id,
            "c_stop_iso_2",
            WorkerType::StageTrigger,
            WorkerStatus::StopScheduled,
            WorkerMessageStatus::Unlocked,
        )
        .await?;

        let queue = FailPutQueue::failing_ids([poisoned.id.to_string()]);
        let context = kubernetes_scheduler_context_with_queue(&db, queue);
        RequestService::build(&context)
            .service()
            .oneshot(())
            .await?;

        let skipped: WorkerDB = db.live().fetch_one::<WorkerDB>(&poisoned.id).await?;
        assert_eq!(skipped.status, WorkerStatus::StopScheduled);
        assert_eq!(skipped.message_status, WorkerMessageStatus::Unlocked);

        let stopped: WorkerDB = db.live().fetch_one::<WorkerDB>(&healthy.id).await?;
        assert_eq!(stopped.status, WorkerStatus::Stopped);
        assert!(stopped.ended_on.is_some());
        assert_eq!(stopped.message_status, WorkerMessageStatus::Locked);

        let created = context.bridge.message_queue().locked_messages().await;
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].id, healthy.id.to_string());
        Ok(())
    }

    /// The supervisor stop is asynchronous: the row waits in `StopRequested`
    /// for the exiting worker's callback (unlike the kubernetes synchronous
    /// stop, which lands in `Stopped`).
    #[td_test::test(db)]
    async fn test_stop_stage_trigger_supervisor_awaits_callback(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let worker = seed_trigger_worker(
            &db,
            p.id,
            "c_stop_sup",
            WorkerType::StageTrigger,
            WorkerStatus::StopScheduled,
            WorkerMessageStatus::Unlocked,
        )
        .await?;

        let context = SchedulerContext::with_defaults(db.clone());
        RequestService::build(&context)
            .service()
            .oneshot(())
            .await?;

        let updated: WorkerDB = db.live().fetch_one::<WorkerDB>(&worker.id).await?;
        assert_eq!(updated.status, WorkerStatus::StopRequested);
        assert!(updated.ended_on.is_none());
        Ok(())
    }

    /// A failing function-run dispatch is skipped without aborting the tick;
    /// its PFR stays `Requested` with no WorkerDB and is re-dispatched
    /// through the replay-side readiness — which runs right after the
    /// initial pipeline, so the retry already lands in the same tick (under
    /// a fresh worker id).
    #[td_test::test(db)]
    async fn test_function_dispatch_isolation_and_retry_kubernetes(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(
                &CollectionName::try_from("c_dispatch_iso")?,
                &UserId::admin(),
            )
            .await?;
        for (name, table) in [("function_1", "table_1"), ("function_2", "table_2")] {
            let create = FunctionRegister::builder()
                .try_name(name)?
                .try_description("d")?
                .bundle_id(BundleId::default())
                .try_snippet("s")?
                .decorator(Decorator::Publisher)
                .dependencies(vec![])
                .triggers(None)
                .tables(vec![TableNameDto::try_from(table)?])
                .runtime_values(FunctionRuntimeValues::try_from("rv")?)
                .reuse_frozen_tables(false)
                .build()?;
            db.scoped(p.id)
                .seed()
                .function(&collection, &create)
                .await?;
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .create(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                        .function(FunctionIdName::from_str(name)?)
                        .build()?,
                    ExecutionRequest::builder().name(None).build()?,
                )
                .in_project(p.id);
            ExecuteFunctionService::with_defaults(db.clone())
                .service()
                .raw_oneshot(request)
                .await?;
        }

        // The first put fails; the tick still succeeds, the other run
        // dispatches, and the failed run is retried by the replay-side
        // pipeline within the same tick.
        let context = kubernetes_scheduler_context_with_queue(&db, FailPutQueue::failing_first(1));
        RequestService::build(&context)
            .service()
            .oneshot(())
            .await?;
        let created = context.bridge.message_queue().locked_messages().await;
        assert_eq!(created.len(), 2);
        for message in created.iter() {
            let worker_id = WorkerId::try_from(message.id.as_str())?;
            let worker: WorkerDB = db.live().fetch_one::<WorkerDB>(&worker_id).await?;
            assert_eq!(worker.status, WorkerStatus::Requested);
        }

        // Nothing left for the next tick: both runs are Dispatched.
        let context = kubernetes_scheduler_context(&db);
        RequestService::build(&context)
            .service()
            .oneshot(())
            .await?;
        assert!(
            context
                .bridge
                .message_queue()
                .locked_messages()
                .await
                .is_empty()
        );
        Ok(())
    }

    /// Kubernetes stage-trigger start: lean `StageTriggerRequest::Start`
    /// carrying the decorator, the resolver it fetches its `request.yaml`
    /// from, and the POST trigger + responder callbacks.
    #[td_test::test(db)]
    async fn test_schedule_stage_trigger_start_kubernetes(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (payload, _id, worker) = schedule_trigger_kubernetes(
            &db,
            p.id,
            "c_stage_start",
            WorkerType::StageTrigger,
            WorkerStatus::Scheduled,
        )
        .await?;

        let OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
            StageTriggerRequest::Start(start),
        ) = &payload
        else {
            panic!("expected a StageTriggerRequest::Start payload");
        };
        assert_eq!(start.worker, WorkerName::StageTrigger);
        assert_eq!(start.decorator, MessageDecorator::Publisher);
        assert_eq!(start.function_version_id, worker.function_version_id);
        assert_eq!(start.collection_id, worker.collection_id);

        assert_http_post(&start.trigger);
        assert_http_post(&start.responder);

        // The lean k8s message no longer embeds external locations; the pod
        // fetches its request.yaml from the resolver (a tokenized HTTP GET).
        let Callback::Http(resolver) = &start.resolver;
        assert_eq!(resolver.method, Method::GET);

        Ok(())
    }

    /// Kubernetes stage-trigger stop: lean `StageTriggerRequest::Stop` keyed by
    /// the worker id (so the gateway can derive the pod to delete).
    #[td_test::test(db)]
    async fn test_stop_stage_trigger_kubernetes(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (payload, id, worker) = schedule_trigger_kubernetes(
            &db,
            p.id,
            "c_stage_stop",
            WorkerType::StageTrigger,
            WorkerStatus::StopScheduled,
        )
        .await?;

        let OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
            StageTriggerRequest::Stop(stop),
        ) = &payload
        else {
            panic!("expected a StageTriggerRequest::Stop payload");
        };
        assert_eq!(stop.worker, WorkerName::StageTrigger);
        assert_http_post(&stop.responder);
        // The message id is the worker id.
        assert_eq!(id, worker.id.to_string());

        Ok(())
    }

    /// Kubernetes cron-trigger start: lean `CronRequest::Start` carrying the
    /// POST trigger + responder and the worker-row identity (no fetch, no
    /// external locations).
    #[td_test::test(db)]
    async fn test_schedule_cron_trigger_start_kubernetes(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (payload, _id, worker) = schedule_trigger_kubernetes(
            &db,
            p.id,
            "c_cron_start",
            WorkerType::CronTrigger,
            WorkerStatus::Scheduled,
        )
        .await?;

        let OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Start(
            start,
        )) = &payload
        else {
            panic!("expected a CronRequest::Start payload");
        };
        assert_eq!(start.worker, WorkerName::CronTrigger);
        assert_eq!(start.function_version_id, worker.function_version_id);
        assert_eq!(start.collection_id, worker.collection_id);
        // The CronJob schedule mask travels in the start request, read from
        // the worker's serialized config.
        assert_eq!(start.schedule, CronMask::try_from("* * * * *")?);

        assert_http_post(&start.trigger);
        assert_http_post(&start.responder);

        Ok(())
    }

    /// Kubernetes cron-trigger stop: lean `CronRequest::Stop` keyed by the
    /// worker id.
    #[td_test::test(db)]
    async fn test_stop_cron_trigger_kubernetes(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (payload, id, worker) = schedule_trigger_kubernetes(
            &db,
            p.id,
            "c_cron_stop",
            WorkerType::CronTrigger,
            WorkerStatus::StopScheduled,
        )
        .await?;

        let OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Stop(
            stop,
        )) = &payload
        else {
            panic!("expected a CronRequest::Stop payload");
        };
        assert_eq!(stop.worker, WorkerName::CronTrigger);
        assert_http_post(&stop.responder);
        assert_eq!(id, worker.id.to_string());

        Ok(())
    }
}
