//
// Copyright 2025 Tabs Data Inc.
//

use super::shared::{internal_get_callback, internal_post_callback};
use crate::function::layers::topology::RunnableVersion;
use crate::logical::layers::dispatch_mint::MintedDispatch;
use crate::logical::layers::index_lfrs_by_exec_fn;
use crate::scheduler::message_builder::{build_function_v2, prefetch_dispatch};
use std::collections::HashMap;
use td_authz::jwt::JwtConfig;
use td_common::server::{
    FunctionRequest, FunctionStartRequest, MessageAction, Orchestrator, OrchestratorMessagePayload,
    OrchestratorMessageQueueHandle, RequestMessagePayload, WorkerClass, WorkerName,
};
use td_database::dynamic::DynamicConnection;
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, PhysicalFunctionRunToExecute};
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::dxo::yaml::WorkerInput;
use td_objects::rest_urls::internal::{RESOLVE_FUNCTION_RUN_WORKER, UPDATE_FUNCTION_RUN_WORKER};
use td_objects::types::addresses::InternalServerAddresses;
use td_objects::types::basic::{
    AtTime, Enabled, ExecutionId, LogicalFunctionRunId, PhysicalFunctionRunId, WorkerId,
    WorkerMessageStatus, WorkerStatus, WorkerType,
};
use td_queries::{ChunkedQueries, LfrEntry, PfrEntry};
use td_storage::Storage;
use td_tower::extractors::{MutRef, Ref};
use tracing::{debug, instrument};

/// LFR-rooted readiness fetch for the no-pre-mint dispatch path.
#[instrument(
    name = "scheduler.fetch_lfr_to_dispatch",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn fetch_lfr_to_dispatch(
    mut conn: MutRef<DynamicConnection<'static>>,
    at: Ref<AtTime>,
) -> Result<Vec<LogicalFunctionRunDB>, TdError> {
    let result = conn.lfr().to_execute(&at).await?;
    if result.is_empty() {
        debug!("scheduler.dispatch_tick: 0 LFRs ready");
    } else {
        debug!(
            ready_count = result.len(),
            "scheduler.dispatch_tick: {} LFR(s) ready to dispatch",
            result.len()
        );
    }
    Ok(result)
}

/// Replay-side readiness fetch: PFRs in `Requested` status with no
/// `WorkerDB` row yet — what the replay endpoint left behind for the
/// scheduler to pick up. Returns the PFRs directly; the next layer
/// resolves LFRs + output PTDVs to assemble `MintedDispatch`.
#[instrument(
    name = "scheduler.fetch_pfrs_to_dispatch",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn fetch_pfrs_to_dispatch(
    mut conn: MutRef<DynamicConnection<'static>>,
) -> Result<Vec<PhysicalFunctionRunDB>, TdError> {
    let result = conn.pfr().to_dispatch().await?;
    if result.is_empty() {
        debug!("scheduler.dispatch_tick: 0 replay PFRs ready");
    } else {
        debug!(
            ready_count = result.len(),
            "scheduler.dispatch_tick: {} replay PFR(s) ready",
            result.len()
        );
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
    mut conn: MutRef<DynamicConnection<'static>>,
    pfrs: Ref<Vec<PhysicalFunctionRunDB>>,
) -> Result<Vec<MintedDispatch>, TdError> {
    if pfrs.is_empty() {
        return Ok(Vec::new());
    }

    let pfr_ids: Vec<PhysicalFunctionRunId> = pfrs.iter().map(|p| p.id.clone()).collect();

    // LFR per PFR via (execution_id, function_id).
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

    // Output TDVs for these PFRs.
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
        // Reconstruct the RunnableVersion from the PFR row.
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
    mut conn: MutRef<DynamicConnection<'static>>,
    minted: Ref<Vec<MintedDispatch>>,
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
pub async fn lfr_ids_from_minted_dispatch(
    minted: Ref<Vec<MintedDispatch>>,
) -> Result<Vec<LogicalFunctionRunId>, TdError> {
    Ok(minted.iter().map(|m| m.lfr.id.clone()).collect())
}

/// Extract the PFR ids from the minted dispatch set so the
/// `Requested → Dispatched` status flip can update_chunked across them.
/// Used by both initial and replay pipelines after WorkerDB insert.
pub async fn pfr_ids_from_minted_dispatch(
    minted: Ref<Vec<MintedDispatch>>,
) -> Result<Vec<PhysicalFunctionRunId>, TdError> {
    Ok(minted.iter().map(|m| m.fr.id.clone()).collect())
}

#[instrument(
    name = "scheduler.create_function_runner_workers",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn create_locked_function_runner_workers_start_request(
    message_queue: Ref<OrchestratorMessageQueueHandle>,
    storage: Ref<Storage>,
    server_addresses: Ref<InternalServerAddresses>,
    mut conn: MutRef<DynamicConnection<'static>>,
    at: Ref<AtTime>,
    orchestrator: Ref<Orchestrator>,
    jwt: Ref<JwtConfig>,
    physical_function_runs: Ref<Vec<PhysicalFunctionRunToExecute>>,
) -> Result<Vec<WorkerDB>, TdError> {
    // Supervisor resolves the message eagerly at the dispatch tick; the
    // kubernetes path defers resolution to the resolve endpoint (the pod
    // pulls its message), so it skips the tick-wide prefetch + build and
    // ships a lean message carrying only a resolver callback.
    let prefetch = match *orchestrator {
        Orchestrator::Supervisor => {
            Some(prefetch_dispatch(&mut conn, physical_function_runs.as_slice(), &at).await?)
        }
        Orchestrator::Kubernetes => None,
    };

    let mut results = Vec::new();
    for f in physical_function_runs.iter() {
        let worker_id = WorkerId::default();

        let server_address = server_addresses.first();
        let fr_id = f.id.to_string();
        let w_id = worker_id.to_string();
        let bindings = [
            ("function_run_id", fr_id.as_str()),
            ("worker_id", w_id.as_str()),
        ];
        // Responder: where the worker POSTs its status back (both paths).
        let responder =
            internal_post_callback(&jwt, server_address, UPDATE_FUNCTION_RUN_WORKER, &bindings)?;

        // The message kind differs by orchestrator: supervisor ships a flat
        // request carrying the fully-resolved FunctionV2 context; kubernetes
        // ships a per-action FunctionRequest carrying the decorator + resolver,
        // resolved later via the endpoint.
        let payload = match prefetch.as_ref() {
            Some(prefetch) => {
                let function_input_v2 = build_function_v2(&mut conn, &storage, f, prefetch).await?;
                let function_input = WorkerInput::FunctionV2(Box::new(function_input_v2));
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
                        run_id: worker_id.to_string(),
                        action: MessageAction::Start,
                        arguments: Vec::new(),
                        callback: Some(responder),
                        context: Some(context),
                    },
                )
            }
            None => {
                let resolver = internal_get_callback(
                    &jwt,
                    server_address,
                    RESOLVE_FUNCTION_RUN_WORKER,
                    &bindings,
                )?;
                OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
                    FunctionRequest::Start(FunctionStartRequest {
                        worker: WorkerName::Function.as_ref().to_string(),
                        run_id: worker_id.to_string(),
                        decorator: f.decorator.to_message(),
                        resolver,
                        responder,
                    }),
                )
            }
        };

        // Create worker message
        let message = WorkerDB {
            id: worker_id,
            collection_id: f.collection_id,
            execution_id: Some(f.execution_id.clone()),
            transaction_id: Some(f.transaction_id.clone()),
            physical_function_run_id: Some(f.id.clone()),
            function_version_id: f.function_version_id.clone(),
            message_status: WorkerMessageStatus::Locked,
            started_on: None,
            ended_on: None,
            status: WorkerStatus::Requested,
            worker_type: WorkerType::FunctionRunner,
            worker_enabled: Enabled::TRUE,
            worker_config: None,
        };

        // Add it to the queue
        message_queue.put(message.id.to_string(), payload).await?;
        results.push(message);
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
