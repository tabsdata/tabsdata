//
// Copyright 2026 Tabsdata Inc.
//

//! A function version's cron and stage workers: the previous version's stop,
//! the new version's start. Workers are not versioned rows, so they sit beside
//! the SCD2 engine rather than in it.

use crate::execution::update_status::move_workers;
use std::collections::HashMap;
use td_database::access::{AccessMode, InProject, LockInstant, SeesProjectRows, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::function::FunctionInsertDB;
use td_objects::dxo::worker::{WorkerDB, WorkerInsertDB, WorkerUpdateDB};
use td_objects::types::basic::{
    FunctionVersionId, WorkerConfig, WorkerId, WorkerStatus, WorkerType, WorkerTypeDto,
};
use td_objects::{and, or};
use td_queries::Queries;
use tracing::{debug, error};

pub async fn register_workers(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    function: &FunctionInsertDB,
    previous_function_version_id: &Option<FunctionVersionId>,
    new_workers: &Option<HashMap<WorkerTypeDto, WorkerConfig>>,
    at: LockInstant,
) -> Result<(), TdError> {
    stop_current_workers(conn, previous_function_version_id, at).await?;
    let workers = build_workers(conn, function, previous_function_version_id, new_workers).await?;
    conn.chunked().insert(&workers).await?;
    Ok(())
}

pub async fn stop_current_workers<S: SeesProjectRows, M: WriteMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    function_version_id: &Option<FunctionVersionId>,
    at: LockInstant,
) -> Result<(), TdError> {
    if let Some(function_version_id) = function_version_id {
        // Function-runner workers cannot be stopped here.
        let to_stop = and!(
            function_version_id,
            or!(WorkerType::CronTrigger, WorkerType::StageTrigger),
        );
        let workers = conn.fetch_all::<WorkerDB>(to_stop).await?;
        let stopped = stop_triggers(conn, &workers, at).await?;
        debug!(
            function_version_id = %function_version_id,
            stopped,
            "Stopped the previous version's triggers"
        );
    }

    Ok(())
}

/// Stops cron and stage `workers`. One never started has no workload, so it
/// ends at once; the rest wait for the tick to send their stop.
pub async fn stop_triggers<S: SeesProjectRows, M: WriteMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    workers: &[WorkerDB],
    at: LockInstant,
) -> Result<u64, TdError> {
    let (unstarted, started): (Vec<WorkerDB>, Vec<WorkerDB>) = workers
        .iter()
        .cloned()
        .partition(|w| w.base.status == WorkerStatus::Scheduled);
    let ended = move_workers(conn, &unstarted, &WorkerUpdateDB::stopped(&at.into())).await?;
    let stopping = move_workers(conn, &started, &WorkerUpdateDB::stop_scheduled()).await?;
    Ok(ended + stopping)
}

pub async fn build_workers<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    function: &FunctionInsertDB,
    function_version_id: &Option<FunctionVersionId>,
    new: &Option<HashMap<WorkerTypeDto, WorkerConfig>>,
) -> Result<Vec<WorkerInsertDB>, TdError> {
    let new_worker =
        |worker_type: WorkerType, worker_config: Option<WorkerConfig>| WorkerInsertDB {
            id: WorkerId::new(),
            collection_id: function.collection_id,
            execution_id: None,
            transaction_id: None,
            physical_function_run_id: None,
            function_version_id: function.id.clone(),
            started_on: None,
            ended_on: None,
            status: WorkerStatus::Scheduled,
            worker_type,
            worker_enabled: function.enabled,
            worker_config,
        };
    let mut new_workers = vec![];
    if let Some(new) = new {
        match new.len() {
            0 => {
                debug!("No workers specified.");
            }
            1 => {
                for (worker_type, worker_config) in new {
                    let worker_db =
                        new_worker(worker_type.clone().into(), Some(worker_config.clone()));
                    new_workers.push(worker_db);
                    debug!(worker_type = %worker_type, "function.register: new worker created");
                }
            }
            _ => {
                error!("Multiple workers specified. This is not supported.");
                Err(api_error!(
                    ApiError::InputError,
                    "Multiple workers are not supported."
                ))?
            }
        }
    } else if let Some(function_version_id) = function_version_id {
        // No worker info in the request: carry the previous version's forward.
        let to_recreate = and!(
            function_version_id,
            or!(WorkerType::CronTrigger, WorkerType::StageTrigger),
        );
        let current_workers = conn.fetch_all::<WorkerDB>(to_recreate).await?;
        for worker in current_workers {
            let worker_db = new_worker(worker.base.worker_type, worker.base.worker_config.clone());
            new_workers.push(worker_db);
        }
    }

    debug!("Created {} new workers.", new_workers.len());
    Ok(new_workers)
}
