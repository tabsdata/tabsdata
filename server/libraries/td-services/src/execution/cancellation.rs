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
use td_database::access::{Holds, InProject, LockInstant, ProjectLock};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::cancel::{CancelReason, CanceledRun};
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::logical::{FunctionRunDBWithNames, LogicalFunctionRunDB};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::worker::WorkerDBWithNames;
use td_objects::types::basic::{
    Decorator, FunctionId, FunctionVersionId, LogicalFunctionRunId, LogicalFunctionRunStatus,
    PhysicalFunctionRunId, PhysicalFunctionRunStatus, TransactionId,
};
use td_queries::{Queries, Reach};
use tracing::{debug, warn};

fn unfinished(lfr: &LogicalFunctionRunDB) -> bool {
    !matches!(
        lfr.status,
        LogicalFunctionRunStatus::Committed
            | LogicalFunctionRunStatus::Canceled
            | LogicalFunctionRunStatus::Yanked
    )
}

/// What a cancel did: the runs it cancelled, with how it reached each, and
/// the stops of their workers to send once the unit of work commits.
pub struct Cancelled {
    pub runs: Vec<CanceledRun>,
    pub stops: Vec<FunctionStopRequest>,
}

/// Cancels `named` and everything that goes with them: each run's whole
/// transaction and every run downstream, to a fixpoint. It never reaches a
/// Committed run through its inputs: a transaction commits only once every
/// transaction it read from has. Ends the cancelled runs' attempts.
pub async fn cancel_runs(
    conn: &mut DomainConnection<Apiserver, InProject, impl Holds<ProjectLock>>,
    at: LockInstant,
    named: &[LogicalFunctionRunDB],
) -> Result<Cancelled, TdError> {
    let cancelled = cancel_closure(conn, named).await?;
    if cancelled.is_empty() {
        return Ok(Cancelled {
            runs: Vec::new(),
            stops: Vec::new(),
        });
    }
    let lfrs: Vec<LogicalFunctionRunDB> = cancelled
        .iter()
        .map(|(run, _)| LogicalFunctionRunDB::clone(run))
        .collect();
    let stops = end_cancelled_attempts(conn, at, &lfrs).await?;
    let ids: Vec<LogicalFunctionRunId> = lfrs.iter().map(|lfr| lfr.id.clone()).collect();
    conn.logical_runs()
        .set_status(&ids, LogicalFunctionRunStatus::Canceled)
        .await?;
    debug!(
        named_count = named.len(),
        cancelled_count = ids.len(),
        "execution.cancel: cancelled LFR(s)"
    );
    let runs = cancelled
        .into_iter()
        .map(|(run, reason)| CanceledRun {
            execution_id: run.execution_id.clone(),
            transaction_id: run.transaction_id.clone(),
            function_run_id: run.id.clone(),
            function: run.name,
            collection: run.collection,
            reason,
        })
        .sorted_by(|a, b| {
            (&a.execution_id, &a.function_run_id).cmp(&(&b.execution_id, &b.function_run_id))
        })
        .collect();
    Ok(Cancelled { runs, stops })
}

/// The unfinished runs a cancel of `named` ends, each with how it was
/// reached. Alternates two steps from the newly reached runs: their
/// transactions' other runs, and the runs reading their outputs (across
/// executions); a reader of the same function reads the state its earlier
/// run leaves. Finished runs end nothing, so the walk does not continue from
/// them.
async fn cancel_closure(
    conn: &mut DomainConnection<Apiserver, InProject, impl Holds<ProjectLock>>,
    named: &[LogicalFunctionRunDB],
) -> Result<Vec<(FunctionRunDBWithNames, CancelReason)>, TdError> {
    let mut reached: HashMap<LogicalFunctionRunId, (FunctionRunDBWithNames, CancelReason)> =
        HashMap::new();
    let mut lifted: HashSet<TransactionId> = HashSet::new();
    let mut frontier: Vec<LogicalFunctionRunDB> =
        named.iter().filter(|l| unfinished(l)).cloned().collect();
    let requested: HashSet<LogicalFunctionRunId> =
        frontier.iter().map(|lfr| lfr.id.clone()).collect();

    while !frontier.is_empty() {
        let transactions: Vec<TransactionId> = frontier
            .iter()
            .map(|lfr| lfr.transaction_id.clone())
            .filter(|t| lifted.insert(t.clone()))
            .unique()
            .collect();
        let siblings: Vec<FunctionRunDBWithNames> = if transactions.is_empty() {
            Vec::new()
        } else {
            conn.chunked()
                .fetch_all::<FunctionRunDBWithNames>(&transactions, ())
                .await?
        };
        let mut next: Vec<LogicalFunctionRunDB> = Vec::new();
        for run in siblings {
            if unfinished(&run) && !reached.contains_key(&run.id) {
                let reason = if requested.contains(&run.id) {
                    CancelReason::Requested
                } else {
                    next.push(LogicalFunctionRunDB::clone(&run));
                    CancelReason::Transaction
                };
                reached.insert(run.id.clone(), (run, reason));
            }
        }

        let from: Vec<LogicalFunctionRunId> = frontier
            .iter()
            .chain(next.iter())
            .map(|lfr| lfr.id.clone())
            .collect();
        let functions: HashSet<FunctionId> = frontier
            .iter()
            .chain(next.iter())
            .map(|lfr| lfr.function_id)
            .collect();
        let exclude: Vec<LogicalFunctionRunId> = reached.keys().cloned().collect();
        let downstream_ids: Vec<LogicalFunctionRunId> = conn
            .logical_runs()
            .downstream_one_hop(&from, &exclude, Reach::Data)
            .await?;
        if !downstream_ids.is_empty() {
            let downstream: Vec<FunctionRunDBWithNames> = conn
                .chunked()
                .fetch_all::<FunctionRunDBWithNames>(&downstream_ids, ())
                .await?;
            for run in downstream {
                if unfinished(&run) && !reached.contains_key(&run.id) {
                    let reason = if functions.contains(&run.function_id) {
                        CancelReason::SameFunction
                    } else {
                        CancelReason::Downstream
                    };
                    next.push(LogicalFunctionRunDB::clone(&run));
                    reached.insert(run.id.clone(), (run, reason));
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
    conn: &mut DomainConnection<Apiserver, InProject, impl Holds<ProjectLock>>,
    at: LockInstant,
    cancelled: &[LogicalFunctionRunDB],
) -> Result<Vec<FunctionStopRequest>, TdError> {
    let lfr_ids: Vec<LogicalFunctionRunId> = cancelled.iter().map(|lfr| lfr.id.clone()).collect();
    let current: Vec<PhysicalFunctionRunDB> = conn
        .scd2()
        .chunked()
        .fetch_all::<PhysicalFunctionRunDB>(&lfr_ids, ())
        .await?;
    let ended: Vec<PhysicalFunctionRunId> = current
        .iter()
        .filter(|pfr| PhysicalFunctionRunStatus::IN_FLIGHT.contains(&pfr.status))
        .map(|pfr| pfr.id.clone())
        .collect();
    let workers = conn.physical_runs().end(&ended, at).await?;
    function_stops(conn, workers).await
}

pub(crate) async fn function_stops(
    conn: &mut DomainConnection<Apiserver, InProject, impl Holds<ProjectLock>>,
    workers: Vec<WorkerDBWithNames>,
) -> Result<Vec<FunctionStopRequest>, TdError> {
    if workers.is_empty() {
        return Ok(Vec::new());
    }
    let function_version_ids: Vec<FunctionVersionId> = workers
        .iter()
        .map(|worker| worker.function_version_id.clone())
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
            let decorator = decorators.get(&worker.function_version_id)?;
            Some(FunctionStopRequest {
                worker: WorkerName::Function,
                worker_id: worker.id.clone(),
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
