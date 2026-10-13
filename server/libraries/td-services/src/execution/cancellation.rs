//
// Copyright 2026 Tabsdata Inc.
//

//! Cancelling runs. A transaction publishes all its tables or none, and a
//! planned run's inputs are pinned, so a cancel ends whole transactions: every
//! unfinished run of a transaction it reaches, and every run reading any of
//! their outputs, until nothing new is reached.

use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use td_bridge::messaging::{FunctionRequest, FunctionStopRequest, OrchestratorMessagePayload};
use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_common::worker::WorkerName;
use td_database::access::{InProject, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::worker::WorkerDBWithNames;
use td_objects::types::basic::{
    AtTime, Decorator, FunctionVersionId, LogicalFunctionRunId, LogicalFunctionRunStatus,
    PhysicalFunctionRunId, PhysicalFunctionRunStatus, TransactionId,
};
use td_queries::Queries;
use tracing::{debug, warn};

fn unfinished(lfr: &LogicalFunctionRunDB) -> bool {
    !matches!(
        lfr.base.status,
        LogicalFunctionRunStatus::Committed
            | LogicalFunctionRunStatus::Canceled
            | LogicalFunctionRunStatus::Yanked
    )
}

/// What a cancel did: the runs it cancelled, and the stops of their workers
/// to send once the unit of work commits.
pub struct Cancelled {
    pub runs: Vec<LogicalFunctionRunId>,
    pub stops: Vec<FunctionStopRequest>,
}

/// Cancels `named` and everything that goes with them: each run's whole
/// transaction and every run downstream, to a fixpoint. It never reaches a
/// Committed run through its inputs: a transaction commits only once every
/// transaction it read from has. Ends the cancelled runs' attempts.
pub async fn cancel_runs(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    at: &AtTime,
    named: &[LogicalFunctionRunDB],
) -> Result<Cancelled, TdError> {
    let cancelled = cancel_closure(conn, named).await?;
    if cancelled.is_empty() {
        return Ok(Cancelled {
            runs: Vec::new(),
            stops: Vec::new(),
        });
    }
    let stops = end_cancelled_attempts(conn, at, &cancelled).await?;
    let ids: Vec<LogicalFunctionRunId> = cancelled.iter().map(|lfr| lfr.base.id.clone()).collect();
    conn.logical_runs()
        .set_status(&ids, LogicalFunctionRunStatus::Canceled)
        .await?;
    debug!(
        named_count = named.len(),
        cancelled_count = ids.len(),
        "execution.cancel: cancelled LFR(s)"
    );
    Ok(Cancelled { runs: ids, stops })
}

/// The unfinished runs a cancel of `named` ends. Alternates two steps from
/// the newly reached runs: their transactions' other runs, and the runs
/// reading their outputs (across executions). Finished runs end nothing, so
/// the walk does not continue from them.
async fn cancel_closure(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    named: &[LogicalFunctionRunDB],
) -> Result<Vec<LogicalFunctionRunDB>, TdError> {
    let mut reached: HashMap<LogicalFunctionRunId, LogicalFunctionRunDB> = HashMap::new();
    let mut lifted: HashSet<TransactionId> = HashSet::new();
    let mut frontier: Vec<LogicalFunctionRunDB> =
        named.iter().filter(|l| unfinished(l)).cloned().collect();
    for lfr in &frontier {
        reached.insert(lfr.base.id.clone(), lfr.clone());
    }

    while !frontier.is_empty() {
        let transactions: Vec<TransactionId> = frontier
            .iter()
            .map(|lfr| lfr.base.transaction_id.clone())
            .filter(|t| lifted.insert(t.clone()))
            .unique()
            .collect();
        let siblings: Vec<LogicalFunctionRunDB> = if transactions.is_empty() {
            Vec::new()
        } else {
            conn.chunked()
                .fetch_all::<LogicalFunctionRunDB>(&transactions, ())
                .await?
        };
        let mut next: Vec<LogicalFunctionRunDB> = Vec::new();
        for lfr in siblings {
            if unfinished(&lfr) && !reached.contains_key(&lfr.base.id) {
                reached.insert(lfr.base.id.clone(), lfr.clone());
                next.push(lfr);
            }
        }

        let from: Vec<LogicalFunctionRunId> = frontier
            .iter()
            .chain(next.iter())
            .map(|lfr| lfr.base.id.clone())
            .collect();
        let exclude: Vec<LogicalFunctionRunId> = reached.keys().cloned().collect();
        let downstream_ids: Vec<LogicalFunctionRunId> = conn
            .logical_runs()
            .downstream_one_hop(&from, &exclude)
            .await?;
        if !downstream_ids.is_empty() {
            let downstream: Vec<LogicalFunctionRunDB> = conn
                .chunked()
                .fetch_all::<LogicalFunctionRunDB>(&downstream_ids, ())
                .await?;
            for lfr in downstream {
                if unfinished(&lfr) && !reached.contains_key(&lfr.base.id) {
                    reached.insert(lfr.base.id.clone(), lfr.clone());
                    next.push(lfr);
                }
            }
        }
        frontier = next;
    }
    Ok(reached.into_values().collect())
}

/// Ends the in-flight attempts of the runs a cancel is about to cancel the way
/// recovery would end them once their workloads are gone: attempt and worker
/// `Failed`. Returns the stops for the workers that have a workload.
async fn end_cancelled_attempts(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    at: &AtTime,
    cancelled: &[LogicalFunctionRunDB],
) -> Result<Vec<FunctionStopRequest>, TdError> {
    let lfr_ids: Vec<LogicalFunctionRunId> =
        cancelled.iter().map(|lfr| lfr.base.id.clone()).collect();
    let current: Vec<PhysicalFunctionRunDB> = conn
        .scd2()
        .chunked()
        .fetch_all::<PhysicalFunctionRunDB>(&lfr_ids, ())
        .await?;
    let ended: Vec<PhysicalFunctionRunId> = current
        .iter()
        .filter(|pfr| PhysicalFunctionRunStatus::IN_FLIGHT.contains(&pfr.base.status))
        .map(|pfr| pfr.base.id.clone())
        .collect();
    let workers = conn.physical_runs().end(&ended, at).await?;
    function_stops(conn, workers).await
}

pub(crate) async fn function_stops(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    workers: Vec<WorkerDBWithNames>,
) -> Result<Vec<FunctionStopRequest>, TdError> {
    if workers.is_empty() {
        return Ok(Vec::new());
    }
    let function_version_ids: Vec<FunctionVersionId> = workers
        .iter()
        .map(|worker| worker.base.base.function_version_id.clone())
        .unique()
        .collect();
    let decorators: HashMap<FunctionVersionId, Decorator> = conn
        .timeline()
        .chunked()
        .fetch_all::<FunctionDB>(&function_version_ids, ())
        .await?
        .into_iter()
        .map(|function| (function.base.id, function.base.decorator))
        .collect();
    Ok(workers
        .into_iter()
        .filter_map(|worker| {
            let decorator = decorators.get(&worker.base.base.function_version_id)?;
            Some(FunctionStopRequest {
                worker: WorkerName::Function,
                worker_id: worker.base.base.id,
                decorator: decorator.to_message(),
                function_id: worker.function_id,
                function: worker.function,
            })
        })
        .collect())
}

/// Best effort: the recovery orphan pass removes a workload whose stop was lost.
pub async fn stop_function_workers(bridge: &BridgeProviderHandle, stops: Vec<FunctionStopRequest>) {
    if !matches!(bridge.orchestrator(), Orchestrator::Kubernetes) {
        return;
    }
    for stop in stops {
        let id = stop.worker_id.to_string();
        let payload = OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
            FunctionRequest::Stop(stop),
        );
        if let Err(error) = bridge.message_queue().put(id.clone(), payload).await {
            warn!(worker_id = %id, "stop message put failed: {error:#?}");
        }
    }
}
