//
// Copyright 2025 Tabsdata Inc.
//

use crate::function::topology::RunnableVersion;
use crate::internal_api::InternalApi;
use crate::logical::dispatch_mint::MintedDispatch;
use crate::logical::index_lfrs_by_exec_fn;
use crate::scheduler::message_builder::{build_function_v2, prefetch_dispatch};
use std::collections::HashMap;
use td_bridge::messaging::{FunctionRequest, FunctionStartRequest, OrchestratorMessagePayload};
use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_common::worker::{MessageAction, RequestMessagePayload, WorkerClass, WorkerName};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_execution::SchedulerConfig;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, PhysicalFunctionRunToExecute};
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::dxo::yaml::WorkerInput;
use td_objects::types::basic::{
    AtTime, Enabled, ExecutionId, LogicalFunctionRunId, PhysicalFunctionRunId, WorkerId,
    WorkerMessageStatus, WorkerStatus, WorkerType,
};
use td_queries::{ChunkedQueries, LfrEntry, PfrEntry};
use td_storage::Storage;
use tracing::{debug, info, instrument, warn};

/// LFR-rooted readiness fetch for the no-pre-mint dispatch path. Oldest
/// first, capped at `max_functions_per_tick`.
#[instrument(
    name = "scheduler.fetch_lfr_to_dispatch",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn fetch_lfr_to_dispatch(
    conn: &mut DomainConnection<Apiserver>,
    at: &AtTime,
    config: &SchedulerConfig,
    saturated: &mut bool,
) -> Result<Vec<LogicalFunctionRunDB>, TdError> {
    let limit = config.max_functions_per_tick.0;
    let result = conn.lfr().to_execute(at, limit).await?;
    if result.is_empty() {
        debug!("scheduler.dispatch_tick: 0 LFRs ready");
    } else {
        debug!(
            ready_count = result.len(),
            "scheduler.dispatch_tick: {} LFR(s) ready to dispatch",
            result.len()
        );
    }
    if result.len() as u64 >= limit {
        info!(
            limit,
            "scheduler.dispatch_tick: LFR readiness fetch returned a full batch; \
             the backlog continues on the next tick"
        );
        *saturated = true;
    }
    Ok(result)
}

/// Replay-side readiness fetch: PFRs in `Requested` status with no
/// `WorkerDB` row yet — what the replay endpoint left behind for the
/// scheduler to pick up. Returns the PFRs directly; the next layer
/// resolves LFRs + output PTDVs to assemble `MintedDispatch`. Oldest
/// first, capped at `max_functions_per_tick`.
#[instrument(
    name = "scheduler.fetch_pfrs_to_dispatch",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn fetch_pfrs_to_dispatch(
    conn: &mut DomainConnection<Apiserver>,
    config: &SchedulerConfig,
    saturated: &mut bool,
) -> Result<Vec<PhysicalFunctionRunDB>, TdError> {
    let limit = config.max_functions_per_tick.0;
    let result = conn.pfr().to_dispatch(limit).await?;
    if result.is_empty() {
        debug!("scheduler.dispatch_tick: 0 replay PFRs ready");
    } else {
        debug!(
            ready_count = result.len(),
            "scheduler.dispatch_tick: {} replay PFR(s) ready",
            result.len()
        );
    }
    if result.len() as u64 >= limit {
        info!(
            limit,
            "scheduler.dispatch_tick: replay readiness fetch returned a full batch; \
             the backlog continues on the next tick"
        );
        *saturated = true;
    }
    Ok(result)
}

/// Materialize already-minted replay PFRs into `MintedDispatch` shape.
/// No inserts — the FR and its output TDVs were created by the replay
/// endpoint; this layer just gathers LFR + output PTDVs and zips them.
#[instrument(
    name = "scheduler.materialize_replay_dispatch",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn materialize_replay_dispatch(
    conn: &mut DomainConnection<Apiserver>,
    pfrs: &[PhysicalFunctionRunDB],
) -> Result<Vec<MintedDispatch>, TdError> {
    if pfrs.is_empty() {
        return Ok(Vec::new());
    }

    let pfr_ids: Vec<PhysicalFunctionRunId> = pfrs.iter().map(|p| p.id.clone()).collect();

    let exec_ids: Vec<ExecutionId> = pfrs
        .iter()
        .map(|p| p.execution_id.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let all_lfrs: Vec<LogicalFunctionRunDB> = conn
        .fetch_all_chunked::<LogicalFunctionRunDB, ExecutionId>(&exec_ids)
        .await?;
    let lfr_by_exec_fn = index_lfrs_by_exec_fn(all_lfrs);

    let tdvs: Vec<PhysicalTableDataVersionDB> = conn
        .fetch_all_chunked::<PhysicalTableDataVersionDB, PhysicalFunctionRunId>(&pfr_ids)
        .await?;
    let mut tdvs_by_fr: HashMap<PhysicalFunctionRunId, Vec<PhysicalTableDataVersionDB>> =
        HashMap::new();
    for t in tdvs {
        tdvs_by_fr
            .entry(t.physical_function_run_id.clone())
            .or_default()
            .push(t);
    }

    let mut output = Vec::with_capacity(pfrs.len());
    for pfr in pfrs.iter() {
        let Some(lfr) = lfr_by_exec_fn
            .get(&pfr.execution_id)
            .and_then(|inner| inner.get(&pfr.function_id))
        else {
            return Err(api_error!(
                ApiError::InternalError,
                "replay-dispatch: no LFR for PFR {} via (execution_id={}, function_id={})",
                pfr.id,
                pfr.execution_id,
                pfr.function_id,
            ));
        };
        let runnable = RunnableVersion {
            function_version_id: pfr.function_version_id.clone(),
            function_version_patch_id: pfr.function_version_patch_id.clone(),
        };
        let tdvs = tdvs_by_fr.remove(&pfr.id).unwrap_or_default();
        output.push(MintedDispatch {
            lfr: lfr.clone(),
            fr: pfr.clone(),
            runnable,
            tdvs,
        });
    }

    debug!(
        replay_count = output.len(),
        "scheduler.dispatch: materialized replay PFRs for dispatch"
    );
    Ok(output)
}

/// Project the minted FRs into the denormalized `PhysicalFunctionRunToExecute`
/// shape the worker-yaml builder consumes. One round-trip via the shared
/// dispatch projection (patch-aware COALESCE for patch fvids).
#[instrument(
    name = "scheduler.enrich_minted_dispatch",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn enrich_minted_dispatch(
    conn: &mut DomainConnection<Apiserver>,
    minted: &[MintedDispatch],
) -> Result<Vec<PhysicalFunctionRunToExecute>, TdError> {
    if minted.is_empty() {
        return Ok(Vec::new());
    }
    let fr_ids: Vec<PhysicalFunctionRunId> = minted.iter().map(|m| m.fr.id.clone()).collect();
    let result = conn.lfr().for_dispatch(&fr_ids).await?;
    Ok(result)
}

/// Extract the LFR ids from the minted dispatch set so the requested
/// status flip can update_chunked across them.
pub fn lfr_ids_from_minted_dispatch(
    minted: &[MintedDispatch],
) -> Result<Vec<LogicalFunctionRunId>, TdError> {
    Ok(minted.iter().map(|m| m.lfr.id.clone()).collect())
}

/// Extract the PFR ids from the successfully dispatched workers so the
/// `Requested → Dispatched` status flip only covers runs whose start message
/// was enqueued; a skipped run's PFR stays `Requested` (with no WorkerDB) and
/// is re-picked by the replay-side readiness next tick. Used by both initial
/// and replay pipelines after WorkerDB insert.
pub fn pfr_ids_from_dispatched_workers(
    workers: &[WorkerDB],
) -> Result<Vec<PhysicalFunctionRunId>, TdError> {
    Ok(workers
        .iter()
        .filter_map(|w| w.physical_function_run_id.clone())
        .collect())
}

// The argument list is the full dependency set of the request build:
// each `Ref<T>` / `MutRef<T>` is extracted from the layer context by type.
// Bundling them into a struct would just rebuild the same 7 extractions
// behind one layer of indirection without simplifying the call site.
#[allow(clippy::too_many_arguments)]
/// Queue puts reach the orchestrator's API, so they are collected while the
/// connection is open and enqueued once it is released.
pub type WorkerMessages = Vec<(String, OrchestratorMessagePayload<serde_yaml::Value>)>;

/// A failed put leaves the run `Dispatched` with no callback, or the trigger
/// worker with no workload; recovery re-dispatches or recreates it.
pub async fn put_worker_messages(bridge: &BridgeProviderHandle, messages: WorkerMessages) {
    for (id, payload) in messages {
        if let Err(e) = bridge.message_queue().put(id.clone(), payload).await {
            warn!(
                worker_id = %id,
                "request: worker message put failed after commit; \
                 recovery will re-dispatch or recreate it: {e:#?}"
            );
        }
    }
}

#[instrument(
    name = "scheduler.create_function_runner_workers",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn create_locked_function_runner_workers_start_request(
    orchestrator: Orchestrator,
    storage: &Storage,
    internal_api: &InternalApi,
    conn: &mut DomainConnection<Apiserver>,
    at: &AtTime,
    physical_function_runs: &Vec<PhysicalFunctionRunToExecute>,
    messages: &mut WorkerMessages,
) -> Result<Vec<WorkerDB>, TdError> {
    // Supervisor resolves the message eagerly at the dispatch tick; the
    // kubernetes path defers resolution to the resolve endpoint (the pod
    // pulls its message), so it skips the tick-wide prefetch + build and
    // ships a lean message carrying only a resolver callback.
    let prefetch = match orchestrator {
        Orchestrator::Supervisor => {
            Some(prefetch_dispatch(&mut *conn, physical_function_runs.as_slice(), at).await?)
        }
        Orchestrator::Kubernetes => None,
    };

    // One run's failure must not abort the tick: skip it (its PFR stays
    // `Requested` with no WorkerDB, re-picked by the replay-side readiness
    // next tick) and keep the rest.
    let mut results = Vec::new();
    for f in physical_function_runs.iter() {
        let result = async {
            let worker_id = WorkerId::new();

            // Responder: where the worker POSTs its status back (both paths).
            let responder = internal_api.function_run_responder(&f.base.base.id, &worker_id)?;

            // The message kind differs by orchestrator: supervisor ships a flat
            // request carrying the fully-resolved Function context; kubernetes
            // ships a per-action FunctionRequest carrying the decorator + resolver,
            // resolved later via the endpoint.
            let payload = match prefetch.as_ref() {
                Some(prefetch) => {
                    let function_input_v2 =
                        build_function_v2(&mut *conn, storage, f, prefetch).await?;
                    let function_input = WorkerInput::Function(Box::new(function_input_v2));
                    let context = serde_yaml::to_value(&function_input).map_err(|e| {
                        api_error!(
                            ApiError::InternalError,
                            "Failed to serialize function input: {e}"
                        )
                    })?;
                    OrchestratorMessagePayload::OrchestratorRequestMessagePayload(
                        RequestMessagePayload::<serde_yaml::Value> {
                            class: WorkerClass::Ephemeral,
                            worker: WorkerName::Function.as_ref().to_string(),
                            worker_id: worker_id.to_string(),
                            action: MessageAction::Start,
                            arguments: Vec::new(),
                            callback: Some(responder),
                            context: Some(context),
                        },
                    )
                }
                None => {
                    let resolver = internal_api.worker_resolver(&worker_id)?;
                    OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
                        FunctionRequest::Start(FunctionStartRequest {
                            worker: WorkerName::Function,
                            worker_id: worker_id.clone(),
                            decorator: f.decorator.to_message(),
                            resolver,
                            responder,
                            project_id: f.base.base.project_id,
                            project: f.base.project.clone(),
                            collection_id: f.base.base.collection_id,
                            collection: f.base.collection.clone(),
                            connection_id: f.connection_id,
                            connection: f.base.connection.clone(),
                            function_id: f.base.base.function_id,
                            function: f.base.name.clone(),
                        }),
                    )
                }
            };

            // Create worker message. The worker inherits the function-run's
            // project so its callbacks land in the right RLS scope.
            let message = WorkerDB {
                id: worker_id,
                project_id: f.base.base.project_id,
                collection_id: f.base.base.collection_id,
                execution_id: Some(f.base.base.execution_id.clone()),
                transaction_id: Some(f.base.base.transaction_id.clone()),
                physical_function_run_id: Some(f.base.base.id.clone()),
                function_version_id: f.base.base.function_version_id.clone(),
                message_status: WorkerMessageStatus::Locked,
                started_on: None,
                ended_on: None,
                status: WorkerStatus::Requested,
                worker_type: WorkerType::FunctionRunner,
                worker_enabled: Enabled::TRUE,
                worker_config: None,
            };

            Ok::<(WorkerDB, OrchestratorMessagePayload<serde_yaml::Value>), TdError>((
                message, payload,
            ))
        }
        .await;
        match result {
            Ok((message, payload)) => {
                messages.push((message.id.to_string(), payload));
                results.push(message);
            }
            Err(e) => warn!(
                pfr_id = %f.base.base.id,
                reason = ?e,
                "request: skipping function-run dispatch; will retry next tick"
            ),
        }
    }

    if !results.is_empty() {
        debug!(
            dispatched_count = results.len(),
            "scheduler.request_tick: dispatched {} worker(s)",
            results.len()
        );
    }
    Ok(results)
}
