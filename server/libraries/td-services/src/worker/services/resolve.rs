//
// Copyright 2026 Tabsdata Inc.
//

use crate::scheduler::message_builder::{
    build_cron_input, build_function_v2, build_streamer_input, prefetch_dispatch,
};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_database::access::{AcrossProjects, DbPool, SeesProjectRows, WriteMode, retrying};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::worker::WorkerParam;
use td_objects::dxo::worker::{WorkerDB, WorkerDBWithNames, WorkerMessageStatusUpdateDB};
use td_objects::dxo::yaml::WorkerInput;
use td_objects::types::basic::{
    AtTime, PhysicalFunctionRunStatus, WorkerId, WorkerStatus, WorkerType,
};
use td_queries::Queries;
use td_storage::Storage;
use tracing::instrument;

#[service_factory]
async fn resolve_worker_message(
    storage: Ctx<Storage>,
    authz: Ctx<AuthzContext>,
    db: DbPool<AcrossProjects>,
    request: ReadRequest<WorkerParam>,
) -> Result<WorkerInput, TdError> {
    let (ctx, param) = request.into_parts();
    let worker_id = param.worker;
    let at = ctx.time;
    retrying(|attempt| async {
        let mut trx = db.begin(attempt).await?;
        let worker: WorkerDB = trx.get(authz.none()).fetch_one(&worker_id).await?;
        let mut trx = trx.enter_project(&worker.project_id).await?;
        let input =
            resolve_worker_message(trx.get(authz.none()), &storage, &worker_id, &at).await?;
        trx.commit().await?;
        Ok(input)
    })
    .await
}

/// Resolve any worker's message on demand: the single yaml generator the
/// kubernetes pull path uses (the replacement for the supervisor dispatch
/// tick — the pod pulls its message here instead of the scheduler renaming
/// a queue file). Dispatches on the worker's type to build the matching
/// `WorkerInput`, then flips the worker message status `Locked → Unlocked`.
#[instrument(
    name = "scheduler.resolve_worker_message",
    skip_all,
    level = "debug",
    err(Debug, level = "debug")
)]
pub async fn resolve_worker_message<S: SeesProjectRows, M: WriteMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    storage: &Storage,
    worker_id: &WorkerId,
    at: &AtTime,
) -> Result<WorkerInput, TdError> {
    // 404 guard + type dispatch: a rolled-back request leaves no worker
    // row, so an orphan pod resolving its message gets a clean not-found.
    let worker: WorkerDBWithNames = conn.fetch_one::<WorkerDBWithNames>(worker_id).await?;

    let input = match worker.base.base.worker_type {
        WorkerType::FunctionRunner => {
            let function_run_id = worker
                .base
                .base
                .physical_function_run_id
                .clone()
                .ok_or_else(|| {
                    api_error!(
                        ApiError::InternalError,
                        "function-runner worker [{}] has no physical_function_run_id",
                        worker_id,
                    )
                })?;

            // A pod resolving a terminalized attempt gets a clean
            // not-found instead of work.
            let pfr: PhysicalFunctionRunDB = conn
                .timeline()
                .fetch_one::<PhysicalFunctionRunDB>(&function_run_id)
                .await?;
            if !PhysicalFunctionRunStatus::IN_FLIGHT.contains(&pfr.base.status) {
                return Err(api_error!(
                    ApiError::NotFound,
                    "function run [{}] for worker [{}] is not in flight",
                    function_run_id,
                    worker_id,
                ));
            }

            // Dispatch projection for this FR (patch-aware fvid COALESCE).
            let fr_ids = [function_run_id.clone()];
            let frs = conn.logical_runs().for_dispatch(&fr_ids).await?;
            let f = frs.into_iter().next().ok_or_else(|| {
                api_error!(
                    ApiError::NotFound,
                    "no dispatchable function run [{}] for worker [{}]",
                    function_run_id,
                    worker_id,
                )
            })?;

            // Resolve as of the request time — late binding against the
            // upstream versions committed by the time the pod asked.
            let prefetch = prefetch_dispatch(&mut *conn, std::slice::from_ref(&f), at).await?;
            let function_input = build_function_v2(&mut *conn, storage, &f, &prefetch).await?;
            WorkerInput::Function(Box::new(function_input))
        }
        WorkerType::StageTrigger | WorkerType::CronTrigger => {
            // Only stopping/stopped is fenced: pod callbacks can park the
            // row at Failed/Done while recovery restarts the workload, and
            // the restarted pod's re-pull must succeed.
            if matches!(
                worker.base.base.status,
                WorkerStatus::StopScheduled | WorkerStatus::StopRequested | WorkerStatus::Stopped
            ) {
                return Err(api_error!(
                    ApiError::NotFound,
                    "trigger worker [{}] is stopping or stopped",
                    worker_id,
                ));
            }
            match worker.base.base.worker_type {
                WorkerType::StageTrigger => {
                    let streamer_input = build_streamer_input(&mut *conn, storage, worker).await?;
                    WorkerInput::Streamer(Box::new(streamer_input))
                }
                _ => {
                    let cron_input = build_cron_input(worker)?;
                    WorkerInput::Cron(Box::new(cron_input))
                }
            }
        }
    };

    // Flip Locked → Unlocked — the "make ready" the dispatch tick did for
    // the supervisor path. Idempotent: a pod restart can re-pull.
    let update = WorkerMessageStatusUpdateDB::unlocked();
    conn.update(&update, worker_id).await?;

    Ok(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRuntimeValues,
        LogicalFunctionRunStatus, TableNameDto, TransactionKey, UserId, WorkerMessageStatus,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    /// Seed a publisher + a Locked function-runner worker, then resolve its
    /// message via the endpoint service: it returns a resolved Function
    /// for the FR and flips the worker status Locked -> Unlocked.
    #[td_test::test(db)]
    async fn test_resolve_worker_message(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("c_resolve")?, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            child_functions: None,
            name: "fr_resolve".try_into()?,
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
            parent_tables: None,
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
        let transaction = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY")?)
            .await?;
        let pfr = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;
        let worker = db
            .scoped(p.id)
            .seed()
            .function_runner_worker(&execution, &transaction, &pfr, &WorkerMessageStatus::Locked)
            .await?;

        let param = WorkerParam {
            worker: worker.base.id.clone(),
        };
        let read = RequestContext::with(AccessTokenId::default(), UserId::admin()).read(param);
        let response = Services::with_defaults(db.clone())
            .resolve_worker_message(read)
            .await?
            .data;

        let WorkerInput::Function(message) = response else {
            panic!("expected Function");
        };
        assert_eq!(message.info.function_run_id, pfr.base.id);

        let worker_after: WorkerDB = db.fetch_one::<WorkerDB>(&worker.base.id).await?;
        assert_eq!(
            worker_after.base.message_status,
            WorkerMessageStatus::Unlocked
        );

        Ok(())
    }
}
