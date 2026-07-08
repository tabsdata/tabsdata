//
// Copyright 2026 Tabs Data Inc.
//

use crate::scheduler::message_builder::{
    build_cron_input, build_function_v2, build_streamer_input, prefetch_dispatch,
};
use ta_services::factory::service_factory;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::worker::{UpdateWorkerMessageStatusDB, WorkerDB, WorkerDBWithNames};
use td_objects::dxo::yaml::WorkerInput;
use td_objects::rest_urls::internal::WorkerIdParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{AtTime, WorkerId, WorkerType};
use td_queries::{LfrEntry, LiveEntry};
use td_storage::Storage;
use td_tower::default_services::TransactionProvider;
use td_tower::extractors::{MutRef, Ref};
use td_tower::from_fn::from_fn;
use td_tower::layers;
use tracing::instrument;

#[service_factory(
    name = ResolveWorkerMessageService,
    request = ReadRequest<WorkerIdParam>,
    response = WorkerInput,
    connection = TransactionProvider,
    // Internal worker-callback handler; the apiserver's project_resolver sets
    // the request's project from the URL, so it runs project-scoped like its
    // siblings (worker callback/list/logs).
    scope = "Project",
    context = Storage,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<WorkerIdParam>>::extract_name::<WorkerIdParam>),
        from_fn(With::<WorkerIdParam>::extract::<WorkerId>),
        from_fn(With::<ReadRequest<WorkerIdParam>>::extract::<RequestContext>),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(resolve_worker_message),
    )
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
    err(Debug)
)]
pub async fn resolve_worker_message(
    mut conn: MutRef<DomainConnection<Apiserver>>,
    storage: Ref<Storage>,
    worker_id: Ref<WorkerId>,
    at: Ref<AtTime>,
) -> Result<WorkerInput, TdError> {
    let worker_id = &*worker_id;

    // 404 guard + type dispatch: a rolled-back request leaves no worker
    // row, so an orphan pod resolving its message gets a clean not-found.
    let worker: WorkerDBWithNames = conn
        .live()
        .fetch_one::<WorkerDBWithNames>(worker_id)
        .await?;

    let input = match worker.worker_type {
        WorkerType::FunctionRunner => {
            let function_run_id = worker.physical_function_run_id.clone().ok_or_else(|| {
                api_error!(
                    ApiError::InternalError,
                    "function-runner worker [{}] has no physical_function_run_id",
                    worker_id,
                )
            })?;

            // Dispatch projection for this FR (patch-aware fvid COALESCE).
            let fr_ids = [function_run_id.clone()];
            let frs = conn.lfr().for_dispatch(&fr_ids).await?;
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
            let prefetch = prefetch_dispatch(&mut conn, std::slice::from_ref(&f), &at).await?;
            let function_input = build_function_v2(&mut conn, &storage, &f, &prefetch).await?;
            WorkerInput::Function(Box::new(function_input))
        }
        WorkerType::StageTrigger => {
            let streamer_input = build_streamer_input(&mut conn, &storage, &worker).await?;
            WorkerInput::Streamer(Box::new(streamer_input))
        }
        WorkerType::CronTrigger => {
            let cron_input = build_cron_input(&worker)?;
            WorkerInput::Cron(Box::new(cron_input))
        }
    };

    // Flip Locked → Unlocked — the "make ready" the dispatch tick did for
    // the supervisor path. Idempotent: a pod restart can re-pull.
    let update = UpdateWorkerMessageStatusDB::unlocked().await?;
    conn.live()
        .update::<_, WorkerDB>(&update, worker_id)
        .await?;

    Ok(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRuntimeValues,
        LogicalFunctionRunStatus, ProjectId, TableNameDto, TransactionKey, UserId,
        WorkerMessageStatus,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    /// Seed a publisher + a Locked function-runner worker, then resolve its
    /// message via the endpoint service: it returns a resolved Function
    /// for the FR and flips the worker status Locked -> Unlocked.
    #[td_test::test(db)]
    async fn test_resolve_worker_message(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("c_resolve")?, &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .try_name("fr_resolve")?
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
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await?;
        let execution = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&function)
            .await?;
        let transaction = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY")?)
            .await?;
        let pfr = db
            .scoped(ProjectId::sandbox())
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
            .scoped(ProjectId::sandbox())
            .seed()
            .function_runner_worker(&execution, &transaction, &pfr, &WorkerMessageStatus::Locked)
            .await?;

        // Resolve the message via the endpoint service (worker-scoped).
        let param = WorkerIdParam {
            worker_id: worker.id,
        };
        let read = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .read(param)
            .in_project(ProjectId::sandbox());
        let response = ResolveWorkerMessageService::with_defaults(db.clone())
            .service()
            .raw_oneshot(read)
            .await?;

        // Returns a resolved Function for this FR.
        let WorkerInput::Function(message) = response else {
            panic!("expected Function");
        };
        assert_eq!(message.info.function_run_id, pfr.id);

        // Status flipped Locked -> Unlocked.
        let worker_after: WorkerDB = db.live().fetch_one::<WorkerDB>(&worker.id).await?;
        assert_eq!(worker_after.message_status, WorkerMessageStatus::Unlocked);

        Ok(())
    }
}
