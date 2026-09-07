//
// Copyright 2026 Tabsdata Inc.
//

use crate::internal_api::InternalApi;
use crate::project::active::active_project_ids;
use crate::recovery::{
    CronCandidates, RecoverySnapshot, StageCandidates, build_recovery_snapshot,
    current_trigger_versions, fn_candidates, stage_trigger_functions,
};
use crate::recovery::{
    apply_recovery_actions, classify_divergence, execute_workload_removals,
    recreate_trigger_workloads, sweep_stale_runs,
};
use ta_services::ctx_service::ReqCtx;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_bridge::BridgeProviderHandle;
use td_database::access::SeesProjectRows;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_execution::{RecoveryConfig, SchedulerNotify};
use td_objects::dxo::worker::WorkerDBWithNames;
use td_objects::types::basic::AtTime;
use td_queries::Queries;
use td_storage::Storage;
use tracing::instrument;
use tracing::{error, warn};

/// The recovery tick, over short DB phases so no connection is held during
/// orchestrator calls: observe (no DB) → snapshot (read-only) → classify
/// (pure) → workload removals (no DB) → apply (short TX) → recreates (no DB)
/// → notify (post-commit). Unobservable workloads degrade the tick to the
/// time-based sweep (short TX). Every orchestrator action is idempotent, so an
/// aborted tick converges on the next one.
#[allow(clippy::too_many_arguments)]
#[service_factory]
async fn recovery(
    storage: Ctx<Storage>,
    bridge: Ctx<BridgeProviderHandle>,
    internal_api: Ctx<InternalApi>,
    scheduler_notify: Ctx<SchedulerNotify>,
    config: Ctx<RecoveryConfig>,
    msgs: &mut ReqCtx,
    db: DbPool<AcrossProjects>,
    _request: (),
) -> Result<(), TdError> {
    let grant = Grant::asserted("recovery tick");
    let at = AtTime::now();

    let observer = bridge.workload_observer();
    let observation = match observer {
        None => None,
        Some(observer) => match observer.observe().await {
            Ok(observation) => Some(observation),
            Err(e) => {
                warn!(reason = ?e, "recovery.observe: workload observation failed");
                None
            }
        },
    };

    match (observer, observation) {
        (Some(observer), Some(observation)) => {
            let snapshot = {
                let mut conn = db.read().await?;
                take_recovery_snapshot(conn.get(grant), &at).await?
            };
            let decisions = classify_divergence(&config, &observation, &snapshot);
            let actions = execute_workload_removals(observer, &decisions).await;
            for (project, actions) in actions.by_project() {
                let mut trx = db.in_project(&project).begin_exclusive().await?;
                match apply_recovery_actions(msgs, trx.get(grant), &at, &actions).await {
                    Ok(()) => trx.commit().await?,
                    Err(e) => error!(
                        project_id = %project,
                        "recovery.apply: project failed, continuing with the rest: {e:?}"
                    ),
                }
            }
            recreate_trigger_workloads(
                &bridge,
                &storage,
                &internal_api,
                &decisions,
                &snapshot.stage_functions,
            )
            .await;
        }
        _ => {
            for project in active_project_ids(&db, grant).await? {
                let mut trx = db.in_project(&project).begin_exclusive().await?;
                match sweep_stale_runs(msgs, trx.get(grant), &at, &config).await {
                    Ok(()) => trx.commit().await?,
                    Err(e) => error!(
                        project_id = %project,
                        "recovery.sweep: project failed, continuing with the rest: {e:?}"
                    ),
                }
            }
        }
    }

    // Recovered LFRs re-dispatch on the next request tick; the apply/sweep
    // transaction has already committed here.
    scheduler_notify.notify_scheduler();
    Ok(())
}

/// One read-only pass over the DB: reconciliation candidates plus the stage
/// function/bundle locations the recreates need, so the orchestrator then runs
/// every Kubernetes call with no connection held.
#[instrument(name = "recovery.snapshot", skip_all, level = "debug", err(Debug))]
pub async fn take_recovery_snapshot<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    at: &AtTime,
) -> Result<RecoverySnapshot, TdError> {
    let cron_workers: Vec<WorkerDBWithNames> = conn
        .fetch_all(WorkerDBWithNames::cron_triggers_in_flight())
        .await?;
    let cron = CronCandidates(cron_workers);
    let stage_workers: Vec<WorkerDBWithNames> = conn
        .fetch_all(WorkerDBWithNames::stage_triggers_in_flight())
        .await?;
    let stage = StageCandidates(stage_workers);
    let current = current_trigger_versions(conn, &cron, &stage).await?;
    let fn_attempts = fn_candidates(conn).await?;
    let stage_functions = stage_trigger_functions(conn, &stage).await?;
    build_recovery_snapshot(at, &cron, &stage, &fn_attempts, &stage_functions, &current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::scheduler::test_wiring::dispatch_services;
    use std::str::FromStr;
    use std::time::Duration;
    use td_bridge::Orchestrator;
    use td_bridge::messaging::{CronRequest, OrchestratorMessagePayload, StageTriggerRequest};
    use td_bridge::test::{StaticWorkloadObserver, TestBridgeProvider};
    use td_database::access::LockInstant;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt;
    use td_execution::{DispatchGrace, StuckRequestedTimeout};
    use td_objects::dxo::collection::ActiveCollectionDB;
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionRequest;
    use td_objects::dxo::function::{
        FunctionDB, FunctionInsertDB, FunctionParam, FunctionRegister,
    };
    use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunStatusUpdateDB};
    use td_objects::dxo::physical_function_run::{
        PhysicalFunctionRunDB, PhysicalFunctionRunUpdateDB,
    };
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, FunctionIdName,
        FunctionOnly, FunctionRuntimeValues, FunctionVersionId, LogicalFunctionRunStatus,
        OverrideDisabledTrigger, PhysicalFunctionRunStatus, ProjectId, ReuseFrozen, SkipIfRunning,
        TableNameDto, UserId, WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    /// A recovery tick over an empty cluster observation, so every in-flight
    /// workload reads as missing.
    fn kubernetes_recovery_services(db: &TdPool, config: RecoveryConfig) -> Services {
        let mut services = dispatch_services(db);
        services.recovery_config = Ctx::new(config);
        services.bridge = Ctx::new(BridgeProviderHandle::new(std::sync::Arc::new(
            TestBridgeProvider::default()
                .with_orchestrator(Orchestrator::Kubernetes)
                .with_workload_observer(StaticWorkloadObserver::default()),
        )));
        services
    }

    /// The recovery tick sweeps, then a request tick re-dispatches what it
    /// recovered. Returns the request tick's services so tests can inspect
    /// its queue.
    async fn recovery_and_request_tick(
        db: &TdPool,
        timeout: StuckRequestedTimeout,
    ) -> Result<Services, TdError> {
        let mut recovery = dispatch_services(db);
        recovery.recovery_config = Ctx::new(RecoveryConfig {
            stuck_requested_timeout: timeout,
            ..RecoveryConfig::default()
        });
        recovery.recovery(()).await?;
        let request = dispatch_services(db);
        request.clone().request(()).await?;
        Ok(request)
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
        let create = FunctionRegister {
            name: "f_stuck".try_into()?,
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
                        function: FunctionIdName::from_str("f_stuck")?,
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

        let context_1 = dispatch_services(&db);
        context_1.clone().request(()).await?;
        let pfrs: Vec<PhysicalFunctionRunDB> = db
            .timeline()
            .fetch_all::<PhysicalFunctionRunDB>(&(&execution.manual_trigger))
            .await?;
        assert_eq!(pfrs.len(), 1);
        let pfr_old = pfrs[0].clone();
        assert_eq!(pfr_old.base.status, PhysicalFunctionRunStatus::Dispatched);
        assert!(pfr_old.base.dispatched_on.is_some());
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
        let context_fresh =
            recovery_and_request_tick(&db, StuckRequestedTimeout::default()).await?;
        let pfr: PhysicalFunctionRunDB = db
            .timeline()
            .fetch_one::<PhysicalFunctionRunDB>(&pfr_old.base.id)
            .await?;
        assert_eq!(pfr.base.status, PhysicalFunctionRunStatus::Dispatched);
        assert!(
            context_fresh
                .bridge
                .message_queue()
                .locked_messages()
                .await
                .is_empty()
        );

        let context_2 =
            recovery_and_request_tick(&db, StuckRequestedTimeout(Duration::ZERO)).await?;

        let pfr_failed: PhysicalFunctionRunDB = db
            .timeline()
            .fetch_one::<PhysicalFunctionRunDB>(&pfr_old.base.id)
            .await?;
        assert_eq!(pfr_failed.base.status, PhysicalFunctionRunStatus::Failed);
        assert!(pfr_failed.base.ended_on.is_some());
        assert!(pfr_failed.base.ended_on_hour_ms.is_some());
        assert!(pfr_failed.base.ended_on_day_ms.is_some());
        assert!(pfr_failed.base.ended_on_week_ms.is_some());
        assert!(pfr_failed.base.ended_on_month_ms.is_some());

        let pfrs: Vec<PhysicalFunctionRunDB> = db
            .timeline()
            .fetch_all::<PhysicalFunctionRunDB>(&(&execution.manual_trigger))
            .await?;
        assert_eq!(pfrs.len(), 2);
        let pfr_new = pfrs
            .iter()
            .find(|pfr| pfr.base.id != pfr_old.base.id)
            .unwrap();
        assert_eq!(pfr_new.base.status, PhysicalFunctionRunStatus::Dispatched);
        assert!(pfr_new.base.dispatched_on.is_some());

        let lfr: LogicalFunctionRunDB = db
            .fetch_one::<LogicalFunctionRunDB>(and!(
                &pfr_old.base.execution_id,
                &pfr_old.base.function_id
            ))
            .await?;
        assert_eq!(lfr.base.status, LogicalFunctionRunStatus::Requested);
        assert_eq!(
            context_2
                .bridge
                .message_queue()
                .locked_messages()
                .await
                .len(),
            1
        );

        let context_3 = dispatch_services(&db);
        context_3.clone().request(()).await?;
        let pfrs: Vec<PhysicalFunctionRunDB> = db
            .timeline()
            .fetch_all::<PhysicalFunctionRunDB>(&(&execution.manual_trigger))
            .await?;
        assert_eq!(pfrs.len(), 2);
        let pfr: PhysicalFunctionRunDB = db
            .timeline()
            .fetch_one::<PhysicalFunctionRunDB>(&pfr_new.base.id)
            .await?;
        assert_eq!(pfr.base.status, PhysicalFunctionRunStatus::Dispatched);
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
        use td_objects::and;
        use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunParam};
        use td_objects::types::basic::TransactionKey;

        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("c_stuck_rp")?, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            name: "f_stuck_rp".try_into()?,
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
            .fetch_one::<LogicalFunctionRunDB>(and!(
                &fr_old.base.execution_id,
                &fr_old.base.function_id
            ))
            .await?;

        let replay_request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                LogicalFunctionRunParam {
                    logical_function_run: lfr.base.id.clone(),
                },
                (),
            );
        let replay_response = dispatch_services(&db)
            .replay_logical_function_run(replay_request)
            .await?
            .data;

        let context_1 = dispatch_services(&db);
        context_1.clone().request(()).await?;
        let pfr: PhysicalFunctionRunDB = db
            .timeline()
            .fetch_one::<PhysicalFunctionRunDB>(&replay_response.new_function_run_id)
            .await?;
        assert_eq!(pfr.base.status, PhysicalFunctionRunStatus::Dispatched);
        assert!(pfr.base.dispatched_on.is_some());

        let context_2 =
            recovery_and_request_tick(&db, StuckRequestedTimeout(Duration::ZERO)).await?;

        let pfr: PhysicalFunctionRunDB = db
            .timeline()
            .fetch_one::<PhysicalFunctionRunDB>(&replay_response.new_function_run_id)
            .await?;
        assert_eq!(pfr.base.status, PhysicalFunctionRunStatus::Failed);
        assert!(pfr.base.ended_on.is_some());

        let lfr_after: LogicalFunctionRunDB =
            db.fetch_one::<LogicalFunctionRunDB>(&lfr.base.id).await?;
        assert_eq!(lfr_after.base.status, LogicalFunctionRunStatus::Committed);
        let current = db
            .scd2()
            .fetch_one::<PhysicalFunctionRunDB>(&lfr.base.id)
            .await?;
        assert_eq!(current.base.id, replay_response.new_function_run_id);
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

    /// Seed a collection + function and an enabled trigger worker in
    /// `worker_status`.
    async fn seed_trigger_worker(
        db: &TdPool,
        project_id: ProjectId,
        collection_name: &str,
        worker_type: WorkerType,
        worker_status: WorkerStatus,
    ) -> Result<(ActiveCollectionDB, FunctionDB, WorkerDB), TdError> {
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
            .scoped(project_id)
            .seed()
            .function(&collection, &create)
            .await?;
        let worker = db
            .scoped(project_id)
            .seed()
            .worker(
                &collection,
                &function,
                &worker_type,
                &worker_status,
                &WorkerMessageStatus::Unlocked,
            )
            .await?;
        Ok((collection, function, worker))
    }

    /// Insert a newer version of `function`, making it non-current.
    async fn outdate_function_version(
        db: &TdPool,
        project_id: ProjectId,
        function: &FunctionDB,
    ) -> Result<(), TdError> {
        let newer = FunctionInsertDB {
            id: FunctionVersionId::new(),
            defined_by_id: UserId::admin(),
            ..function.base.clone()
        };
        db.scoped(project_id)
            .scd2()
            .open(&newer, LockInstant::asserted(*AtTime::now()))
            .await?;
        Ok(())
    }

    /// Full-restart revival: in-flight cron + streamer workers with an empty
    /// cluster observation — the recovery tick recreates both workloads and
    /// leaves the worker rows untouched.
    #[td_test::test(db)]
    async fn test_restart_recreates_trigger_workloads(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (_, _, cron) = seed_trigger_worker(
            &db,
            p.id,
            "c_revive_cron",
            WorkerType::CronTrigger,
            WorkerStatus::Requested,
        )
        .await?;
        let (_, _, stage) = seed_trigger_worker(
            &db,
            p.id,
            "c_revive_stage",
            WorkerType::StageTrigger,
            WorkerStatus::Requested,
        )
        .await?;

        let services = kubernetes_recovery_services(&db, RecoveryConfig::default());
        services.clone().recovery(()).await?;

        let messages = services.bridge.message_queue().locked_messages().await;
        assert_eq!(messages.len(), 2);
        let cron_message = messages
            .iter()
            .find(|m| m.id == cron.base.id.to_string())
            .expect("cron workload must be recreated");
        assert!(matches!(
            &cron_message.payload,
            OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Start(
                _
            ))
        ));
        let stage_message = messages
            .iter()
            .find(|m| m.id == stage.base.id.to_string())
            .expect("streamer workload must be recreated");
        assert!(matches!(
            &stage_message.payload,
            OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
                StageTriggerRequest::Start(_)
            )
        ));

        let cron_after: WorkerDB = db.fetch_one(&cron.base.id).await?;
        assert_eq!(cron_after.base.status, WorkerStatus::Requested);
        let stage_after: WorkerDB = db.fetch_one(&stage.base.id).await?;
        assert_eq!(stage_after.base.status, WorkerStatus::Requested);

        Ok(())
    }

    /// A Running run whose workload vanished fails AND auto-recovers: the
    /// LFR goes Failed → ReScheduled in one tick and the next request tick
    /// dispatches a fresh attempt.
    #[td_test::test(db)]
    async fn test_auto_recover_running_missing_workload(db: TdPool) -> Result<(), TdError> {
        use td_objects::and;

        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(
                &CollectionName::try_from("c_run_missing")?,
                &UserId::admin(),
            )
            .await?;
        let create = FunctionRegister {
            name: "f_run_missing".try_into()?,
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
                        function: FunctionIdName::from_str("f_run_missing")?,
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
        dispatch_services(&db).request(()).await?;

        let pfrs: Vec<PhysicalFunctionRunDB> = db
            .timeline()
            .fetch_all::<PhysicalFunctionRunDB>(&(&execution.manual_trigger))
            .await?;
        assert_eq!(pfrs.len(), 1);
        let pfr_old = pfrs[0].clone();
        assert_eq!(pfr_old.base.status, PhysicalFunctionRunStatus::Dispatched);

        // The fn's begin-execution report: attempt and run go Running.
        let to_running = PhysicalFunctionRunUpdateDB {
            dispatched_on: pfr_old.base.dispatched_on,
            started_on: None,
            ended_on: None,
            status: PhysicalFunctionRunStatus::Running,
            ended_on_hour_ms: None,
            ended_on_day_ms: None,
            ended_on_week_ms: None,
            ended_on_month_ms: None,
            metric_table_versions: None,
            metric_rows: None,
            metric_data_bytes: None,
        };
        db.scd2().update(&to_running, &pfr_old.base.id).await?;
        let lfr: LogicalFunctionRunDB = db
            .fetch_one::<LogicalFunctionRunDB>(and!(
                &pfr_old.base.execution_id,
                &pfr_old.base.function_id
            ))
            .await?;
        let lfr_to_running = LogicalFunctionRunStatusUpdateDB {
            status: LogicalFunctionRunStatus::Running,
        };
        db.update(&lfr_to_running, &lfr.base.id).await?;

        // Empty cluster observation, no dispatch grace: the workload is
        // judged missing on the first tick.
        let services = kubernetes_recovery_services(
            &db,
            RecoveryConfig {
                dispatch_grace: DispatchGrace(Duration::ZERO),
                ..RecoveryConfig::default()
            },
        );
        services.recovery(()).await?;

        let pfr_failed: PhysicalFunctionRunDB = db
            .timeline()
            .fetch_one::<PhysicalFunctionRunDB>(&pfr_old.base.id)
            .await?;
        assert_eq!(pfr_failed.base.status, PhysicalFunctionRunStatus::Failed);
        assert!(pfr_failed.base.ended_on.is_some());
        let lfr_recovered: LogicalFunctionRunDB =
            db.fetch_one::<LogicalFunctionRunDB>(&lfr.base.id).await?;
        assert_eq!(
            lfr_recovered.base.status,
            LogicalFunctionRunStatus::ReScheduled
        );

        dispatch_services(&db).request(()).await?;
        let pfrs: Vec<PhysicalFunctionRunDB> = db
            .timeline()
            .fetch_all::<PhysicalFunctionRunDB>(&(&execution.manual_trigger))
            .await?;
        assert_eq!(pfrs.len(), 2);
        let pfr_new = pfrs
            .iter()
            .find(|pfr| pfr.base.id != pfr_old.base.id)
            .unwrap();
        assert_eq!(pfr_new.base.status, PhysicalFunctionRunStatus::Dispatched);
        let lfr_after: LogicalFunctionRunDB =
            db.fetch_one::<LogicalFunctionRunDB>(&lfr.base.id).await?;
        assert_eq!(lfr_after.base.status, LogicalFunctionRunStatus::Requested);

        Ok(())
    }

    /// Trigger workers of a superseded function version are not recovery's
    /// to maintain: no recreation, even with an empty observation.
    #[td_test::test(db)]
    async fn test_outdated_trigger_workers_not_recreated(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (_, cron_fn, _) = seed_trigger_worker(
            &db,
            p.id,
            "c_outdated_cron",
            WorkerType::CronTrigger,
            WorkerStatus::Requested,
        )
        .await?;
        let (_, stage_fn, _) = seed_trigger_worker(
            &db,
            p.id,
            "c_outdated_stage",
            WorkerType::StageTrigger,
            WorkerStatus::Requested,
        )
        .await?;
        outdate_function_version(&db, p.id, &cron_fn).await?;
        outdate_function_version(&db, p.id, &stage_fn).await?;

        let services = kubernetes_recovery_services(&db, RecoveryConfig::default());
        services.clone().recovery(()).await?;

        let messages = services.bridge.message_queue().locked_messages().await;
        assert!(messages.is_empty());

        Ok(())
    }

    /// A streamer's own failure callback is terminal: no recreation. A cron
    /// worker at Failed stays owned and is recreated.
    #[td_test::test(db)]
    async fn test_failed_streamer_not_recreated(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (_, _, cron) = seed_trigger_worker(
            &db,
            p.id,
            "c_failed_cron",
            WorkerType::CronTrigger,
            WorkerStatus::Failed,
        )
        .await?;
        let (_, _, stage) = seed_trigger_worker(
            &db,
            p.id,
            "c_failed_stage",
            WorkerType::StageTrigger,
            WorkerStatus::Failed,
        )
        .await?;

        let services = kubernetes_recovery_services(&db, RecoveryConfig::default());
        services.clone().recovery(()).await?;

        let messages = services.bridge.message_queue().locked_messages().await;
        assert_eq!(messages.len(), 1);
        let cron_message = &messages[0];
        assert_eq!(cron_message.id, cron.base.id.to_string());
        assert!(matches!(
            &cron_message.payload,
            OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Start(
                _
            ))
        ));

        let stage_after: WorkerDB = db.fetch_one(&stage.base.id).await?;
        assert_eq!(stage_after.base.status, WorkerStatus::Failed);

        Ok(())
    }
}
