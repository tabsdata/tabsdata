//
// Copyright 2026 Tabsdata Inc.
//

use crate::recovery::layers::{
    classify_divergence, execute_workload_removals, recreate_trigger_workloads,
};
use crate::recovery::services::apply::RecoveryApplyService;
use crate::recovery::services::snapshot::RecoverySnapshotService;
use crate::recovery::services::sweep::RecoverySweepService;
use std::sync::Arc;
use ta_services::factory::{FieldAccessor, ServiceFactory};
use ta_services::service::TdService;
use td_bridge::BridgeProviderHandle;
use td_common::jwt::JwtConfig;
use td_database::pool::TdPool;
use td_error::TdError;
use td_execution::{RecoveryConfig, SchedulerNotify};
use td_objects::types::addresses::InternalServerEndpoint;
use td_storage::Storage;
use td_tower::ctx_service::{CtxResponse, RawOneshot, ReqCtx};
use td_tower::service_provider::TdBoxService;
use tower::service_fn;
use tower::util::BoxCloneSyncService;
use tracing::warn;

/// The recovery tick, orchestrated over short DB phases so no connection is
/// held during orchestrator calls: observe (no DB) → snapshot (read-only) →
/// classify (pure) → workload removals (no DB) → apply (short TX) →
/// recreates (no DB) → notify (post-commit by construction). Unobservable
/// workloads degrade the tick to the time-based sweep (short TX). Every
/// orchestrator action is idempotent, so an aborted tick converges on the
/// next one.
pub struct RecoveryService {
    sweep: RecoverySweepService,
    snapshot: RecoverySnapshotService,
    apply: RecoveryApplyService,
    storage: Arc<Storage>,
    bridge: Arc<BridgeProviderHandle>,
    internal_endpoint: Arc<InternalServerEndpoint>,
    scheduler_notify: Arc<SchedulerNotify>,
    jwt_config: Arc<JwtConfig>,
    recovery_config: Arc<RecoveryConfig>,
}

impl RecoveryService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: TdPool,
        storage: Arc<Storage>,
        bridge: Arc<BridgeProviderHandle>,
        internal_endpoint: Arc<InternalServerEndpoint>,
        scheduler_notify: Arc<SchedulerNotify>,
        jwt_config: Arc<JwtConfig>,
        recovery_config: Arc<RecoveryConfig>,
    ) -> Self {
        Self {
            sweep: RecoverySweepService::new(db.clone(), recovery_config.clone()),
            snapshot: RecoverySnapshotService::new(db.clone()),
            apply: RecoveryApplyService::new(db),
            storage,
            bridge,
            internal_endpoint,
            scheduler_notify,
            jwt_config,
            recovery_config,
        }
    }
}

impl<C> ServiceFactory<C> for RecoveryService
where
    TdPool: FieldAccessor<C>,
    Arc<Storage>: FieldAccessor<C>,
    Arc<BridgeProviderHandle>: FieldAccessor<C>,
    Arc<InternalServerEndpoint>: FieldAccessor<C>,
    Arc<SchedulerNotify>: FieldAccessor<C>,
    Arc<JwtConfig>: FieldAccessor<C>,
    Arc<RecoveryConfig>: FieldAccessor<C>,
{
    type Service = Self;

    fn build(ctx: &C) -> Self {
        Self::new(
            TdPool::get_field(ctx),
            Arc::<Storage>::get_field(ctx),
            Arc::<BridgeProviderHandle>::get_field(ctx),
            Arc::<InternalServerEndpoint>::get_field(ctx),
            Arc::<SchedulerNotify>::get_field(ctx),
            Arc::<JwtConfig>::get_field(ctx),
            Arc::<RecoveryConfig>::get_field(ctx),
        )
    }
}

impl TdService for RecoveryService {
    type Request = ();
    type Response = ();
    type Error = TdError;

    fn service(&self) -> TdBoxService<Self::Request, Self::Response, Self::Error> {
        let sweep = self.sweep.service();
        let snapshot = self.snapshot.service();
        let apply = self.apply.service();
        let storage = self.storage.clone();
        let bridge = self.bridge.clone();
        let internal_endpoint = self.internal_endpoint.clone();
        let notify = self.scheduler_notify.clone();
        let jwt = self.jwt_config.clone();
        let config = self.recovery_config.clone();
        BoxCloneSyncService::new(service_fn(move |_: ()| {
            let sweep = sweep.clone();
            let snapshot = snapshot.clone();
            let apply = apply.clone();
            let storage = storage.clone();
            let bridge = bridge.clone();
            let internal_endpoint = internal_endpoint.clone();
            let notify = notify.clone();
            let jwt = jwt.clone();
            let config = config.clone();
            async move {
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
                        let snapshot = snapshot.raw_oneshot(()).await?;
                        let decisions = classify_divergence(&config, &observation, &snapshot);
                        let actions = execute_workload_removals(observer, &decisions).await;
                        apply.raw_oneshot(actions).await?;
                        recreate_trigger_workloads(
                            &bridge,
                            &storage,
                            &internal_endpoint,
                            &jwt,
                            &decisions,
                            &snapshot.stage_functions,
                        )
                        .await;
                    }
                    _ => {
                        sweep.raw_oneshot(()).await?;
                    }
                }
                // Recovered LFRs re-dispatch on the next request tick; the
                // apply/sweep transaction has already committed here.
                notify.notify_scheduler();
                Ok(CtxResponse::new((), ReqCtx::default()))
            }
        }))
    }

    #[cfg(feature = "test_tower_metadata")]
    async fn metadata(&self) -> td_tower::metadata::Metadata {
        self.snapshot.metadata().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::services::execute::ExecuteFunctionService;
    use crate::recovery::services::RecoveryContext;
    use crate::scheduler::services::FunctionSchedulerServices;
    use crate::scheduler::services::SchedulerContext;
    use std::str::FromStr;
    use std::time::Duration;
    use td_bridge::Orchestrator;
    use td_bridge::messaging::{CronRequest, OrchestratorMessagePayload, StageTriggerRequest};
    use td_bridge::test::{StaticWorkloadObserver, TestBridgeProvider};
    use td_database::pool::scoped::InProject;
    use td_execution::{DispatchGrace, StuckRequestedTimeout};
    use td_objects::dxo::collection::ActiveCollectionDB;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionRequest;
    use td_objects::dxo::function::{FunctionDB, FunctionDBBuilder, FunctionRegister};
    use td_objects::dxo::logical::{LogicalFunctionRunDB, UpdateLogicalFunctionRunDB};
    use td_objects::dxo::physical_function_run::{
        PhysicalFunctionRunDB, UpdatePhysicalFunctionRunDB,
    };
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::rest_urls::FunctionParam;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, FunctionIdName,
        FunctionRuntimeValues, FunctionVersionId, LogicalFunctionRunStatus,
        PhysicalFunctionRunStatus, TableNameDto, UserId, WorkerMessageStatus, WorkerStatus,
        WorkerType,
    };
    use td_objects::{Buildable, TryUpdater};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use tower::ServiceExt;

    fn kubernetes_recovery_context(db: &TdPool, config: RecoveryConfig) -> RecoveryContext {
        let mut context = RecoveryContext::with_defaults(db.clone());
        context.recovery_config = Arc::new(config);
        context.bridge = Arc::new(BridgeProviderHandle::new(Arc::new(
            TestBridgeProvider::default()
                .with_orchestrator(Orchestrator::Kubernetes)
                .with_workload_observer(StaticWorkloadObserver::default()),
        )));
        context
    }

    fn recovery_context(db: &TdPool, timeout: StuckRequestedTimeout) -> RecoveryContext {
        let mut context = RecoveryContext::with_defaults(db.clone());
        context.recovery_config = Arc::new(RecoveryConfig {
            stuck_requested_timeout: timeout,
            ..RecoveryConfig::default()
        });
        context
    }

    /// Returns the request tick's context so tests can inspect its queue.
    async fn recovery_and_request_tick(
        db: &TdPool,
        timeout: StuckRequestedTimeout,
    ) -> Result<SchedulerContext, TdError> {
        let recovery = recovery_context(db, timeout);
        RecoveryService::build(&recovery)
            .service()
            .oneshot(())
            .await?;
        let request = SchedulerContext::with_defaults(db.clone());
        FunctionSchedulerServices::build(&request)
            .request
            .service()
            .oneshot(())
            .await?;
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
        FunctionSchedulerServices::build(&context_1)
            .request
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
        let context_fresh =
            recovery_and_request_tick(&db, StuckRequestedTimeout::default()).await?;
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

        // Past the timeout: the recovery tick fails the attempt and
        // recovers the LFR; the next request tick re-dispatches it fresh.
        let context_2 =
            recovery_and_request_tick(&db, StuckRequestedTimeout(Duration::ZERO)).await?;

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

        let context_3 = recovery_and_request_tick(&db, StuckRequestedTimeout::default()).await?;
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
        use td_queries::VersionedEntry;

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
        FunctionSchedulerServices::build(&context_1)
            .request
            .service()
            .oneshot(())
            .await?;
        let pfr: PhysicalFunctionRunDB = db
            .live()
            .fetch_one::<PhysicalFunctionRunDB>(&replay_response.new_function_run_id)
            .await?;
        assert_eq!(pfr.status, PhysicalFunctionRunStatus::Dispatched);
        assert!(pfr.dispatched_on.is_some());

        // The stale replay attempt is only terminalized (the LFR is
        // sticky-Committed) and the next request tick dispatches nothing.
        let context_2 =
            recovery_and_request_tick(&db, StuckRequestedTimeout(Duration::ZERO)).await?;

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

    /// Seed a collection + function and an enabled trigger worker in
    /// `worker_status`.
    async fn seed_trigger_worker(
        db: &TdPool,
        project_id: td_objects::types::basic::ProjectId,
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
        let create = FunctionRegister::builder()
            .try_name("function_1")?
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
        project_id: td_objects::types::basic::ProjectId,
        function: &FunctionDB,
    ) -> Result<(), TdError> {
        let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin());
        let mut builder = FunctionDBBuilder::try_from(function)?;
        builder.id(FunctionVersionId::new());
        let newer = builder.try_update_from(&request_context)?.build()?;
        db.scoped(project_id)
            .live()
            .insert::<FunctionDB>(&newer)
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

        let context = kubernetes_recovery_context(&db, RecoveryConfig::default());
        RecoveryService::build(&context)
            .service()
            .oneshot(())
            .await?;

        let messages = context.bridge.message_queue().locked_messages().await;
        assert_eq!(messages.len(), 2);
        let cron_message = messages
            .iter()
            .find(|m| m.id == cron.id.to_string())
            .expect("cron workload must be recreated");
        assert!(matches!(
            &cron_message.payload,
            OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Start(
                _
            ))
        ));
        let stage_message = messages
            .iter()
            .find(|m| m.id == stage.id.to_string())
            .expect("streamer workload must be recreated");
        assert!(matches!(
            &stage_message.payload,
            OrchestratorMessagePayload::OrchestratorStageTriggerRequestMessagePayload(
                StageTriggerRequest::Start(_)
            )
        ));

        let cron_after: td_objects::dxo::worker::WorkerDB = db.live().fetch_one(&cron.id).await?;
        assert_eq!(cron_after.status, WorkerStatus::Requested);
        let stage_after: td_objects::dxo::worker::WorkerDB = db.live().fetch_one(&stage.id).await?;
        assert_eq!(stage_after.status, WorkerStatus::Requested);

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
        let create = FunctionRegister::builder()
            .try_name("f_run_missing")?
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
                    .function(FunctionIdName::from_str("f_run_missing")?)
                    .build()?,
                ExecutionRequest::builder().name(None).build()?,
            )
            .in_project(p.id);
        let execution = ExecuteFunctionService::with_defaults(db.clone())
            .service()
            .raw_oneshot(request)
            .await?;
        let dispatch = SchedulerContext::with_defaults(db.clone());
        FunctionSchedulerServices::build(&dispatch)
            .request
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

        // The fn's begin-execution report: attempt and run go Running.
        let to_running = UpdatePhysicalFunctionRunDB {
            dispatched_on: pfr_old.dispatched_on,
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
        db.live()
            .update::<_, PhysicalFunctionRunDB>(&to_running, &pfr_old.id)
            .await?;
        let lfr: LogicalFunctionRunDB = db
            .live()
            .fetch_one::<LogicalFunctionRunDB>(and!(&pfr_old.execution_id, &pfr_old.function_id))
            .await?;
        let lfr_to_running = UpdateLogicalFunctionRunDB {
            status: LogicalFunctionRunStatus::Running,
        };
        db.live()
            .update::<_, LogicalFunctionRunDB>(&lfr_to_running, &lfr.id)
            .await?;

        // Empty cluster observation, no dispatch grace: the workload is
        // judged missing on the first tick.
        let context = kubernetes_recovery_context(
            &db,
            RecoveryConfig {
                dispatch_grace: DispatchGrace(Duration::ZERO),
                ..RecoveryConfig::default()
            },
        );
        RecoveryService::build(&context)
            .service()
            .oneshot(())
            .await?;

        let pfr_failed: PhysicalFunctionRunDB = db
            .live()
            .fetch_one::<PhysicalFunctionRunDB>(&pfr_old.id)
            .await?;
        assert_eq!(pfr_failed.status, PhysicalFunctionRunStatus::Failed);
        assert!(pfr_failed.ended_on.is_some());
        let lfr_recovered: LogicalFunctionRunDB =
            db.live().fetch_one::<LogicalFunctionRunDB>(&lfr.id).await?;
        assert_eq!(lfr_recovered.status, LogicalFunctionRunStatus::ReScheduled);

        let request_tick = SchedulerContext::with_defaults(db.clone());
        FunctionSchedulerServices::build(&request_tick)
            .request
            .service()
            .oneshot(())
            .await?;
        let pfrs: Vec<PhysicalFunctionRunDB> = db
            .live()
            .fetch_all::<PhysicalFunctionRunDB>(&(&execution.manual_trigger))
            .await?;
        assert_eq!(pfrs.len(), 2);
        let pfr_new = pfrs.iter().find(|pfr| pfr.id != pfr_old.id).unwrap();
        assert_eq!(pfr_new.status, PhysicalFunctionRunStatus::Dispatched);
        let lfr_after: LogicalFunctionRunDB =
            db.live().fetch_one::<LogicalFunctionRunDB>(&lfr.id).await?;
        assert_eq!(lfr_after.status, LogicalFunctionRunStatus::Requested);

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

        let context = kubernetes_recovery_context(&db, RecoveryConfig::default());
        RecoveryService::build(&context)
            .service()
            .oneshot(())
            .await?;

        let messages = context.bridge.message_queue().locked_messages().await;
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

        let context = kubernetes_recovery_context(&db, RecoveryConfig::default());
        RecoveryService::build(&context)
            .service()
            .oneshot(())
            .await?;

        let messages = context.bridge.message_queue().locked_messages().await;
        assert_eq!(messages.len(), 1);
        let cron_message = &messages[0];
        assert_eq!(cron_message.id, cron.id.to_string());
        assert!(matches!(
            &cron_message.payload,
            OrchestratorMessagePayload::OrchestratorCronRequestMessagePayload(CronRequest::Start(
                _
            ))
        ));

        let stage_after: WorkerDB = db.live().fetch_one(&stage.id).await?;
        assert_eq!(stage_after.status, WorkerStatus::Failed);

        Ok(())
    }
}
