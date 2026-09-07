//
// Copyright 2025 Tabsdata Inc.
//

use crate::internal_api::InternalApi;
use crate::logical::dispatch_mint::mint_function_runs_on_dispatch;
use crate::project::active::active_project_ids;
use crate::scheduler::request::{
    WorkerMessages, create_locked_cron_trigger_workers_start_request,
    create_locked_cron_trigger_workers_stop_request,
    create_locked_function_runner_workers_start_request,
    create_locked_stage_trigger_workers_start_request,
    create_locked_stage_trigger_workers_stop_request, enrich_minted_dispatch,
    fetch_lfr_to_dispatch, fetch_pfrs_to_dispatch, lfr_ids_from_minted_dispatch,
    materialize_replay_dispatch, pfr_ids_from_dispatched_workers, put_worker_messages,
    stop_worker_update,
};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_database::access::{DbPool, Global, Grant};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_execution::{SchedulerConfig, SchedulerNotify};
use td_objects::StateFilter;
use td_objects::dxo::logical::{LogicalFunctionRunDB, UpdateLogicalFunctionRunDB};
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, UpdatePhysicalFunctionRunDB};
use td_objects::dxo::worker::{
    UpdateWorkerDB, UpdateWorkerMessageStatusDB, WorkerDB, WorkerDBWithNames,
};
use td_objects::types::basic::{AtTime, ProjectId, WorkerId, WorkerType};
use td_queries::{ChunkedQueries, LiveEntry};
use td_storage::Storage;
use tracing::{debug, error};

#[service_factory]
async fn request(
    storage: Ctx<Storage>,
    bridge: Ctx<BridgeProviderHandle>,
    internal_api: Ctx<InternalApi>,
    scheduler_notify: Ctx<SchedulerNotify>,
    config: Ctx<SchedulerConfig>,
    db: DbPool<Global>,
    _request: (),
) -> Result<(), TdError> {
    let grant = Grant::asserted("scheduler dispatch tick");
    // Capture the dispatch tick's AtTime once, before any sub-pipeline.
    // Every readiness/mint/bind/yaml lookup in the tick shares this
    // temporal point so the dispatch is consistent even if operator
    // events arrive mid-tick — and every project of this tick shares it too.
    let at = AtTime::now();
    // The saturation flag; the capped function-runner readiness fetches raise
    // it when they return a full batch, in any project.
    let mut saturated = false;

    // One transaction per project, each holding only that project's write
    // lock. A project whose dispatch fails — a rejected job, a poisoned
    // worker — no longer aborts every other project's tick with it.
    for project in projects_to_tick(&db, grant).await? {
        if let Err(e) = tick_project(
            &project,
            &at,
            &storage,
            &bridge,
            &internal_api,
            &config,
            &db,
            grant,
            &mut saturated,
        )
        .await
        {
            error!(
                project_id = %project,
                "scheduler.dispatch_tick: project failed, continuing with the rest: {e:?}"
            );
        }
    }

    scheduler_notify.notify_dispatcher();
    if saturated {
        debug!("scheduler.dispatch_tick: capped; re-notifying the scheduler");
        scheduler_notify.notify_scheduler();
    }
    Ok(())
}

/// Every live project gets a tick. Deliberately not "projects with pending
/// work": that predicate would have to union every sub-pipeline's readiness
/// condition, and missing one would silently stall a project forever, whereas
/// an idle project's tick is a handful of indexed lookups that return nothing.
async fn projects_to_tick(db: &DbPool<Global>, grant: Grant) -> Result<Vec<ProjectId>, TdError> {
    active_project_ids(db, grant).await
}

/// One project's dispatch, in its own transaction.
#[allow(clippy::too_many_arguments)]
async fn tick_project(
    project: &ProjectId,
    at: &AtTime,
    storage: &Storage,
    bridge: &BridgeProviderHandle,
    internal_api: &InternalApi,
    config: &SchedulerConfig,
    db: &DbPool<Global>,
    grant: Grant,
    saturated: &mut bool,
) -> Result<(), TdError> {
    let mut trx = db.in_project(project).begin_exclusive().await?;
    let conn = trx.get(grant);
    let mut messages = WorkerMessages::default();
    let orchestrator = bridge.orchestrator();

    // Request function runners that need to be started — initial dispatch
    // path (Scheduled/ReScheduled LFRs, mint + bind + flip LFR.status to
    // Requested).
    let ready = fetch_lfr_to_dispatch(conn, at, config, saturated).await?;
    let minted = mint_function_runs_on_dispatch(conn, &ready, at).await?;
    let enriched = enrich_minted_dispatch(conn, &minted).await?;
    let workers = create_locked_function_runner_workers_start_request(
        orchestrator,
        storage,
        internal_api,
        conn,
        at,
        &enriched,
        &mut messages,
    )
    .await?;
    conn.insert_vec::<WorkerDB>(&workers).await?;
    // Flip LFR.status Scheduled/ReScheduled -> Requested.
    let lfr_ids = lfr_ids_from_minted_dispatch(&minted)?;
    let requested = UpdateLogicalFunctionRunDB::requested();
    conn.update_chunked::<_, LogicalFunctionRunDB, _>(&requested, &lfr_ids)
        .await?;
    // Flip PFR.status Requested -> Dispatched so the replay-side readiness
    // query excludes these PFRs from the next tick.
    let pfr_ids = pfr_ids_from_dispatched_workers(&workers)?;
    let dispatched = UpdatePhysicalFunctionRunDB::dispatched(at);
    conn.update_chunked::<_, PhysicalFunctionRunDB, _>(&dispatched, &pfr_ids)
        .await?;

    // Request function runners for replay attempts — sticky LFRs whose
    // pointer was flipped to Requested by the replay endpoint. No mint, no
    // bind, no LFR.status flip.
    let replay_pfrs = fetch_pfrs_to_dispatch(conn, config, saturated).await?;
    let minted = materialize_replay_dispatch(conn, &replay_pfrs).await?;
    let enriched = enrich_minted_dispatch(conn, &minted).await?;
    let workers = create_locked_function_runner_workers_start_request(
        orchestrator,
        storage,
        internal_api,
        conn,
        at,
        &enriched,
        &mut messages,
    )
    .await?;
    conn.insert_vec::<WorkerDB>(&workers).await?;
    let pfr_ids = pfr_ids_from_dispatched_workers(&workers)?;
    conn.update_chunked::<_, PhysicalFunctionRunDB, _>(&dispatched, &pfr_ids)
        .await?;

    let to_stop =
        claim_trigger_workers(conn, WorkerDB::triggers_to_stop(WorkerType::CronTrigger)).await?;
    let to_stop = create_locked_cron_trigger_workers_stop_request(
        orchestrator,
        internal_api,
        to_stop,
        &mut messages,
    )
    .await?;
    update_worker_message_statuses(conn, orchestrator, at, &to_stop, true).await?;

    let to_start = claim_trigger_workers(
        conn,
        WorkerDB::triggers_to_schedule(WorkerType::CronTrigger),
    )
    .await?;
    let to_start = create_locked_cron_trigger_workers_start_request(
        orchestrator,
        internal_api,
        to_start,
        &mut messages,
    )
    .await?;
    update_worker_message_statuses(conn, orchestrator, at, &to_start, false).await?;

    let to_stop =
        claim_trigger_workers(conn, WorkerDB::triggers_to_stop(WorkerType::StageTrigger)).await?;
    let to_stop = create_locked_stage_trigger_workers_stop_request(
        orchestrator,
        internal_api,
        to_stop,
        &mut messages,
    )
    .await?;
    update_worker_message_statuses(conn, orchestrator, at, &to_stop, true).await?;

    let to_start = claim_trigger_workers(
        conn,
        WorkerDB::triggers_to_schedule(WorkerType::StageTrigger),
    )
    .await?;
    let to_start = create_locked_stage_trigger_workers_start_request(
        orchestrator,
        storage,
        internal_api,
        conn,
        to_start,
        &mut messages,
    )
    .await?;
    update_worker_message_statuses(conn, orchestrator, at, &to_start, false).await?;

    trx.commit().await?;

    put_worker_messages(bridge, messages).await;
    Ok(())
}

/// Two steps because only a base table is lockable: PostgreSQL refuses `FOR
/// UPDATE` through `workers__with_names`, which outer-joins. Worker rows are
/// written outside the project's lock too — the worker callback, `resolve`,
/// collection and function deletes — so the claim skips a worker one of them
/// is mutating rather than enqueuing for it, and keeps the tick from blocking
/// on it while holding the lock.
async fn claim_trigger_workers(
    conn: &mut DomainConnection<Apiserver>,
    readiness: StateFilter<WorkerDB>,
) -> Result<Vec<WorkerDBWithNames>, TdError> {
    let claimed: Vec<WorkerDB> = conn.live().claim_all_in::<WorkerDB>(readiness, &()).await?;
    if claimed.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<WorkerId> = claimed.into_iter().map(|w| w.id).collect();
    let workers = conn
        .fetch_all_chunked::<WorkerDBWithNames, WorkerId>(&ids)
        .await?;
    Ok(workers)
}

/// Flip the trigger workers' status (Requested, or the orchestrator-specific
/// stop status) and mark their messages Locked.
async fn update_worker_message_statuses(
    conn: &mut DomainConnection<Apiserver>,
    orchestrator: Orchestrator,
    at: &AtTime,
    workers: &[WorkerDBWithNames],
    stopping: bool,
) -> Result<(), TdError> {
    let worker_ids: Vec<WorkerId> = workers.iter().map(|x| x.base.id.clone()).collect();
    let status_update: UpdateWorkerDB = if stopping {
        stop_worker_update(orchestrator, at).await?
    } else {
        UpdateWorkerDB::requested()
    };
    conn.update_chunked::<_, WorkerDB, _>(&status_update, &worker_ids)
        .await?;
    let locked = UpdateWorkerMessageStatusDB::locked();
    conn.update_chunked::<_, WorkerDB, _>(&locked, &worker_ids)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::scheduler::test_wiring::dispatch_services;
    use http::Method;
    use std::collections::HashSet;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
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
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_execution::{MaxFunctionsPerTick, RecoveryConfig, StuckRequestedTimeout};
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionRequest;
    use td_objects::dxo::function::FunctionParam;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
    use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBWithNames;
    use td_objects::dxo::yaml::WorkerInput;
    use td_objects::dxo::yaml::function::{InputTable, OutputTable};
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, CronMask, Decorator, EnvPrefix,
        ExecutionName, FunctionIdName, FunctionOnly, FunctionRuntimeValues,
        OverrideDisabledTrigger, PhysicalFunctionRunStatus, ProjectId, ProjectName, SkipIfRunning,
        TableName, TableNameDto, UserId, WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::test_utils::SeedEntry;
    use td_queries::{LiveEntry, VersionedEntry};
    use td_storage::SPath;

    #[td_test::test(db)]
    async fn test_schedule_request(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            name: "function_1".try_into()?,
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: vec![TableDependencyDto::try_from("table_1")?].into(),
            triggers: None,
            tables: vec![
                TableNameDto::try_from("table_1")?,
                TableNameDto::try_from("table_2")?,
            ]
            .into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_1")?,
                    },
                ),
                ExecutionRequest {
                    name: Some(ExecutionName::try_from("test_execution")?),
                    function_only: FunctionOnly::default(),
                    override_disabled_trigger: OverrideDisabledTrigger::default(),
                    skip_if_running: SkipIfRunning::default(),
                    external_uid: None,
                },
            )
            .in_project(p.id);

        let service = dispatch_services(&db);
        let execution = service.execute_function(request).await?.data;

        let context = dispatch_services(&db);
        context.clone().request(()).await?;

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
            Some(EnvPrefix::try_from(mount_def.id.as_str())?)
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
            Some(EnvPrefix::try_from(mount_def.id.as_str())?)
        );

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

                    assert_eq!(output.name, physical_table_data_version.base.base.name);
                    assert_eq!(output.collection_id, collection.id);
                    assert_eq!(output.collection, collection.name);
                    assert_eq!(
                        output.table_id,
                        physical_table_data_version.base.base.table_id
                    );
                    assert_eq!(
                        output.table_version_id,
                        physical_table_data_version.base.base.table_version_id
                    );
                    assert_eq!(
                        output.table_data_version_id,
                        physical_table_data_version.base.base.id
                    );

                    let table_path = SPath::parse(format!(
                        "/c/{}/d/{}/t/{}/{}.t",
                        collection.id,
                        physical_table_data_version.base.base.id,
                        physical_table_data_version.base.base.table_id,
                        physical_table_data_version.base.base.table_version_id,
                    ))?;
                    let (uri, mount_def) = context.storage.to_external_uri(&table_path)?;

                    assert_eq!(output.location.uri, uri);
                    assert_eq!(
                        output.location.env_prefix,
                        Some(EnvPrefix::try_from(mount_def.id.as_str())?)
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
        use td_objects::and;
        use td_objects::dxo::logical::{
            LogicalFunctionRunDB, LogicalFunctionRunParam, LogicalFunctionRunPointerDB,
        };
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
        let create = FunctionRegister {
            name: "fr_replay".try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("t0")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let execution = db.scoped(p.id).seed().execution(&function).await?;
        let trx = db
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
                &trx,
                &LogicalFunctionRunStatus::Committed,
            )
            .await?;
        let lfr: LogicalFunctionRunDB = db
            .live()
            .fetch_one::<LogicalFunctionRunDB>(and!(&fr_old.execution_id, &fr_old.function_id))
            .await?;
        let lfr_id = lfr.id.clone();

        // Call replay — mints FR_new + Requested(FR_new) pointer event.
        let replay_param = LogicalFunctionRunParam {
            logical_function_run: lfr_id.clone(),
        };
        let replay_request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(replay_param, ());
        let replay_response = dispatch_services(&db)
            .replay_logical_function_run(replay_request)
            .await?
            .data;
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
        let context = dispatch_services(&db);
        context.clone().request(()).await?;

        let messages = context.bridge.message_queue().locked_messages().await;
        assert_eq!(
            messages.len(),
            1,
            "expected exactly one worker message for the replay PFR"
        );

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

        let pfr_new = db
            .live()
            .fetch_one::<PhysicalFunctionRunDB>(&replay_response.new_function_run_id)
            .await?;
        assert_eq!(pfr_new.status, PhysicalFunctionRunStatus::Dispatched);

        // Idempotency: a second tick must not create another worker
        // message — `to_dispatch` filters on PFR.status=Requested,
        // and we just flipped to Dispatched.
        let prior_messages_count = messages.len();
        context.clone().request(()).await?;
        let messages_after = context.bridge.message_queue().locked_messages().await;
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
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("c_k8s")?, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            name: "function_1".try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: vec![TableDependencyDto::try_from("table_1")?].into(),
            triggers: None,
            tables: vec![TableNameDto::try_from("table_1")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_1")?,
                    },
                ),
                ExecutionRequest {
                    name: Some(ExecutionName::try_from("e_k8s")?),
                    function_only: FunctionOnly::default(),
                    override_disabled_trigger: OverrideDisabledTrigger::default(),
                    skip_if_running: SkipIfRunning::default(),
                    external_uid: None,
                },
            )
            .in_project(p.id);
        dispatch_services(&db).execute_function(request).await?;

        let context = kubernetes_scheduler_context(&db);
        context.clone().request(()).await?;

        let created = context.bridge.message_queue().locked_messages().await;
        assert_eq!(created.len(), 1);

        let OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
            FunctionRequest::Start(start),
        ) = &created[0].payload
        else {
            panic!("expected a FunctionRequest::Start payload");
        };
        assert_eq!(start.decorator, MessageDecorator::Publisher);

        let Callback::Http(resolver) = &start.resolver;
        assert_eq!(resolver.method, Method::GET);
        assert!(resolver.url.path().ends_with("/message"));
        let Callback::Http(responder) = &start.responder;
        assert_eq!(responder.method, Method::POST);

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

    /// Work ready in two projects dispatches in the same tick. The tick is a
    /// loop over projects now — each with its own transaction and its own write
    /// lock — so a bug that ticked only the first would strand the second
    /// forever, and every single-project test above would still pass.
    #[td_test::test(db)]
    async fn test_tick_dispatches_every_project(db: TdPool) -> Result<(), TdError> {
        let mut executions = Vec::new();
        for (project, collection_name, function_name, table) in [
            ("p_one", "c_one", "f_one", "t_one"),
            ("p_two", "c_two", "f_two", "t_two"),
        ] {
            let p = db.seed().project(ProjectName::try_from(project)?).await?;
            let collection = db
                .scoped(p.id)
                .seed()
                .collection(
                    &CollectionName::try_from(collection_name)?,
                    &UserId::admin(),
                )
                .await?;
            let create = FunctionRegister {
                name: function_name.try_into()?,
                description: "d".try_into()?,
                bundle_id: BundleId::default(),
                snippet: "s".try_into()?,
                decorator: Decorator::Publisher,
                connector: None,
                connection: None,
                base_path: None,
                dependencies: vec![].into(),
                triggers: None,
                tables: vec![TableNameDto::try_from(table)?].into(),
                table_types: None,
                runtime_values: FunctionRuntimeValues::try_from("rv")?,
                reuse_frozen_tables: ReuseFrozen::FALSE,
                enabled: None,
                workers: None,
            };
            db.scoped(p.id)
                .seed()
                .function(&collection, &create)
                .await?;
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .create(
                    (
                        CollectionParam {
                            collection: CollectionIdName::from_str(collection_name)?,
                        },
                        FunctionParam {
                            function: FunctionIdName::from_str(function_name)?,
                        },
                    ),
                    ExecutionRequest {
                        name: None,
                        function_only: FunctionOnly::default(),
                        override_disabled_trigger: OverrideDisabledTrigger::default(),
                        skip_if_running: SkipIfRunning::default(),
                        external_uid: None,
                    },
                )
                .in_project(p.id);
            executions.push(dispatch_services(&db).execute_function(request).await?.data);
        }

        let context = kubernetes_scheduler_context(&db);
        context.clone().request(()).await?;

        assert_eq!(
            context.bridge.message_queue().locked_messages().await.len(),
            2,
            "both projects must dispatch in one tick"
        );
        for execution in &executions {
            let pfrs: Vec<PhysicalFunctionRunDB> = db
                .live()
                .fetch_all::<PhysicalFunctionRunDB>(&(&execution.manual_trigger))
                .await?;
            assert_eq!(pfrs.len(), 1, "each project must dispatch its own run");
        }
        Ok(())
    }

    /// `Services` wired to the kubernetes orchestrator with an
    /// in-memory queue (the file queue only accepts flat request messages).
    fn kubernetes_scheduler_context(db: &TdPool) -> Services {
        kubernetes_scheduler_context_with_queue(db, InMemoryOrchestratorMessageQueue::default())
    }

    fn kubernetes_scheduler_context_with_queue(
        db: &TdPool,
        queue: impl OrchestratorMessageQueue + 'static,
    ) -> Services {
        let mut context = dispatch_services(db);
        context.bridge = Ctx::new(BridgeProviderHandle::new(Arc::new(
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
        let create = FunctionRegister {
            name: "function_1".try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: vec![TableDependencyDto::try_from("table_1")?].into(),
            triggers: None,
            tables: vec![TableNameDto::try_from("table_1")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
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
        context.clone().request(()).await?;

        let mut created = context.bridge.message_queue().locked_messages().await;
        assert_eq!(created.len(), 1);
        let message = created.remove(0);
        Ok((message.payload, message.id, worker))
    }

    /// Stop messages are enqueued post-commit: a poisoned put neither aborts
    /// the tick nor holds back the remaining puts, so both rows commit
    /// `Stopped` (the kubernetes stop is synchronous) while only the healthy
    /// message reaches the queue. The pod the lost put leaves is recovery's job.
    #[td_test::test(db)]
    async fn test_stop_stage_trigger_put_failure_does_not_abort_tick_kubernetes(
        db: TdPool,
    ) -> Result<(), TdError> {
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
        context.clone().request(()).await?;

        for id in [&poisoned.id, &healthy.id] {
            let stopped: WorkerDB = db.live().fetch_one::<WorkerDB>(id).await?;
            assert_eq!(stopped.status, WorkerStatus::Stopped);
            assert!(stopped.ended_on.is_some());
            assert_eq!(stopped.message_status, WorkerMessageStatus::Locked);
        }

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

        let context = dispatch_services(&db);
        context.clone().request(()).await?;

        let updated: WorkerDB = db.live().fetch_one::<WorkerDB>(&worker.id).await?;
        assert_eq!(updated.status, WorkerStatus::StopRequested);
        assert!(updated.ended_on.is_none());
        Ok(())
    }

    /// Worker-message puts run post-commit, so a failing put neither aborts
    /// the tick nor loses the run: its rows are already committed (PFR
    /// `Dispatched`, WorkerDB `Locked`) — there is just no pod, so no
    /// callback ever arrives and the stuck-dispatched sweep re-dispatches
    /// the run under a fresh worker id. A failing put must not stop the
    /// remaining puts either.
    #[td_test::test(db)]
    async fn test_function_dispatch_put_failure_recovered_by_sweep_kubernetes(
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
        let mut executions = Vec::new();
        for (name, table) in [("function_1", "table_1"), ("function_2", "table_2")] {
            let create = FunctionRegister {
                name: name.try_into()?,
                description: "d".try_into()?,
                bundle_id: BundleId::default(),
                snippet: "s".try_into()?,
                decorator: Decorator::Publisher,
                connector: None,
                connection: None,
                base_path: None,
                dependencies: vec![].into(),
                triggers: None,
                tables: vec![TableNameDto::try_from(table)?].into(),
                table_types: None,
                runtime_values: FunctionRuntimeValues::try_from("rv")?,
                reuse_frozen_tables: ReuseFrozen::FALSE,
                enabled: None,
                workers: None,
            };
            db.scoped(p.id)
                .seed()
                .function(&collection, &create)
                .await?;
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .create(
                    (
                        CollectionParam {
                            collection: CollectionIdName::from_str(&format!(
                                "{}",
                                collection.name
                            ))?,
                        },
                        FunctionParam {
                            function: FunctionIdName::from_str(name)?,
                        },
                    ),
                    ExecutionRequest {
                        name: None,
                        function_only: FunctionOnly::default(),
                        override_disabled_trigger: OverrideDisabledTrigger::default(),
                        skip_if_running: SkipIfRunning::default(),
                        external_uid: None,
                    },
                )
                .in_project(p.id);
            let execution = dispatch_services(&db).execute_function(request).await?.data;
            executions.push(execution);
        }

        // The first put fails post-commit: the tick still succeeds, the
        // other run's put goes through, and BOTH runs' rows are committed.
        let context = kubernetes_scheduler_context_with_queue(&db, FailPutQueue::failing_first(1));
        context.clone().request(()).await?;
        let created = context.bridge.message_queue().locked_messages().await;
        assert_eq!(created.len(), 1);
        for execution in &executions {
            let pfrs: Vec<PhysicalFunctionRunDB> = db
                .live()
                .fetch_all::<PhysicalFunctionRunDB>(&(&execution.manual_trigger))
                .await?;
            assert_eq!(pfrs.len(), 1);
            assert_eq!(pfrs[0].status, PhysicalFunctionRunStatus::Dispatched);
            let worker: WorkerDB = db
                .live()
                .fetch_one::<WorkerDB>(&Some(pfrs[0].id.clone()))
                .await?;
            assert_eq!(worker.message_status, WorkerMessageStatus::Locked);
        }

        // The run with the lost put is NOT re-picked by the readiness
        // queries (its WorkerDB exists): a normal tick dispatches nothing.
        let context = kubernetes_scheduler_context(&db);
        context.clone().request(()).await?;
        assert!(
            context
                .bridge
                .message_queue()
                .locked_messages()
                .await
                .is_empty()
        );

        // The stuck-dispatched sweep (recovery tick) is the recovery path:
        // it fails the pod-less attempts; the next request tick
        // re-dispatches fresh ones.
        let mut recovery = Services::with_defaults(db.clone());
        recovery.recovery_config = Ctx::new(RecoveryConfig {
            stuck_requested_timeout: StuckRequestedTimeout(Duration::ZERO),
            ..RecoveryConfig::default()
        });
        recovery.recovery(()).await?;
        let context = kubernetes_scheduler_context(&db);
        context.clone().request(()).await?;
        for execution in &executions {
            let pfrs: Vec<PhysicalFunctionRunDB> = db
                .live()
                .fetch_all::<PhysicalFunctionRunDB>(&(&execution.manual_trigger))
                .await?;
            assert_eq!(pfrs.len(), 2);
            let failed = pfrs
                .iter()
                .filter(|pfr| pfr.status == PhysicalFunctionRunStatus::Failed)
                .count();
            let dispatched = pfrs
                .iter()
                .filter(|pfr| pfr.status == PhysicalFunctionRunStatus::Dispatched)
                .count();
            assert_eq!((failed, dispatched), (1, 1));
        }
        assert_eq!(
            context.bridge.message_queue().locked_messages().await.len(),
            2
        );
        Ok(())
    }

    /// With `max_functions_per_tick` = 1, a tick dispatches only the oldest
    /// ready run and re-notifies the scheduler post-commit; the backlog
    /// drains across back-to-back ticks and the re-notify stops once a tick
    /// comes in under the cap.
    #[td_test::test(db)]
    async fn test_dispatch_cap_oldest_first_and_self_notify(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("c_cap")?, &UserId::admin())
            .await?;
        let mut executions = Vec::new();
        for (name, table) in [("function_1", "table_1"), ("function_2", "table_2")] {
            let create = FunctionRegister {
                name: name.try_into()?,
                description: "d".try_into()?,
                bundle_id: BundleId::default(),
                snippet: "s".try_into()?,
                decorator: Decorator::Publisher,
                connector: None,
                connection: None,
                base_path: None,
                dependencies: vec![].into(),
                triggers: None,
                tables: vec![TableNameDto::try_from(table)?].into(),
                table_types: None,
                runtime_values: FunctionRuntimeValues::try_from("rv")?,
                reuse_frozen_tables: ReuseFrozen::FALSE,
                enabled: None,
                workers: None,
            };
            db.scoped(p.id)
                .seed()
                .function(&collection, &create)
                .await?;
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .create(
                    (
                        CollectionParam {
                            collection: CollectionIdName::from_str(&format!(
                                "{}",
                                collection.name
                            ))?,
                        },
                        FunctionParam {
                            function: FunctionIdName::from_str(name)?,
                        },
                    ),
                    ExecutionRequest {
                        name: None,
                        function_only: FunctionOnly::default(),
                        override_disabled_trigger: OverrideDisabledTrigger::default(),
                        skip_if_running: SkipIfRunning::default(),
                        external_uid: None,
                    },
                )
                .in_project(p.id);
            let execution = dispatch_services(&db).execute_function(request).await?.data;
            executions.push(execution);
        }

        let mut context = kubernetes_scheduler_context(&db);
        context.scheduler_config = Ctx::new(SchedulerConfig {
            max_functions_per_tick: MaxFunctionsPerTick(1),
            ..SchedulerConfig::default()
        });
        // Drain any notify permit stored by the execute calls above.
        let _ = tokio::time::timeout(
            Duration::from_millis(10),
            context.scheduler_notify.notified_scheduler(),
        )
        .await;

        // Tick 1: only the oldest ready run dispatches; the full batch
        // stores a scheduler re-notify.
        context.clone().request(()).await?;
        assert_eq!(
            context.bridge.message_queue().locked_messages().await.len(),
            1
        );
        let pfrs_oldest: Vec<PhysicalFunctionRunDB> = db
            .live()
            .fetch_all::<PhysicalFunctionRunDB>(&(&executions[0].manual_trigger))
            .await?;
        assert_eq!(pfrs_oldest.len(), 1);
        let pfrs_newest: Vec<PhysicalFunctionRunDB> = db
            .live()
            .fetch_all::<PhysicalFunctionRunDB>(&(&executions[1].manual_trigger))
            .await?;
        assert!(pfrs_newest.is_empty());
        tokio::time::timeout(
            Duration::from_millis(100),
            context.scheduler_notify.notified_scheduler(),
        )
        .await
        .expect("capped tick must re-notify the scheduler");

        // Tick 2: the remaining run dispatches — still a full batch, so
        // the re-notify fires again.
        context.clone().request(()).await?;
        assert_eq!(
            context.bridge.message_queue().locked_messages().await.len(),
            2
        );
        let pfrs_newest: Vec<PhysicalFunctionRunDB> = db
            .live()
            .fetch_all::<PhysicalFunctionRunDB>(&(&executions[1].manual_trigger))
            .await?;
        assert_eq!(pfrs_newest.len(), 1);
        tokio::time::timeout(
            Duration::from_millis(100),
            context.scheduler_notify.notified_scheduler(),
        )
        .await
        .expect("second full-batch tick must re-notify the scheduler");

        // Tick 3: nothing ready — under the cap, no re-notify.
        context.clone().request(()).await?;
        assert_eq!(
            context.bridge.message_queue().locked_messages().await.len(),
            2
        );
        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                context.scheduler_notify.notified_scheduler(),
            )
            .await
            .is_err(),
            "an under-cap tick must not re-notify the scheduler"
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
