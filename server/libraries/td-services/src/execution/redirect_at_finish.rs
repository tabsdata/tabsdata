//
// Copyright 2026 Tabsdata Inc.
//

//! On `Done` from a replay PFR, append `Bound(PFR_new, ReplayRedirect)`
//! to the LFR pointer log and `Bound(TDV_new, ReplayRedirect)` to each
//! output LTDV pointer log so downstream readers see the new outputs.
//! Initial dispatches and non-`Done` outcomes are no-ops here.

use crate::logical::index_lfrs_by_exec_fn;
use itertools::Itertools;
use std::collections::HashMap;
use td_common::execution_status::WorkerCallbackStatus;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalTableDataVersionDB};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::worker::UpdateWorkerExecution;
use td_objects::types::basic::{AtTime, ExecutionId, LogicalFunctionRunId, PhysicalFunctionRunId};
use td_queries::{ChunkedQueries, LfrEntry, LtdvEntry, OutputRedirect, ReplayRedirect};
use tracing::{debug, instrument};

#[instrument(
    name = "callback.redirect_at_finish",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn redirect_pointers_at_finish(
    conn: &mut DomainConnection<Apiserver>,
    physical_function_runs: &Vec<PhysicalFunctionRunDB>,
    update: &UpdateWorkerExecution,
    at: &AtTime,
) -> Result<(), TdError> {
    if !matches!(update.status, WorkerCallbackStatus::Done) {
        return Ok(());
    }
    if physical_function_runs.is_empty() {
        return Ok(());
    }

    // Resolve all LFRs via (execution_id, function_id) in one chunked
    // fetch over distinct executions.
    let exec_ids: Vec<ExecutionId> = physical_function_runs
        .iter()
        .map(|f| f.execution_id.clone())
        .unique()
        .collect();
    let all_lfrs: Vec<LogicalFunctionRunDB> = conn
        .fetch_all_chunked::<LogicalFunctionRunDB, ExecutionId>(&exec_ids)
        .await?;
    let lfr_by_fr_key = index_lfrs_by_exec_fn(all_lfrs);

    let lfr_items: Vec<ReplayRedirect> = physical_function_runs
        .iter()
        .filter_map(|fr| {
            let lfr = lfr_by_fr_key
                .get(&fr.execution_id)
                .and_then(|inner| inner.get(&fr.function_id))?;
            Some(ReplayRedirect {
                project_id: lfr.project_id,
                logical_function_run_id: lfr.id.clone(),
                new_physical_function_run_id: fr.id.clone(),
                reporting_triggered_on: fr.triggered_on,
                set_by_id: fr.triggered_by_id,
            })
        })
        .collect();
    let redirected = conn
        .lfr()
        .pointers()
        .redirect_replay_attempts(&lfr_items, at)
        .await?;
    if redirected.is_empty() {
        return Ok(());
    }
    debug!(
        redirected = redirected.len(),
        "redirect_at_finish: LFR pointers flipped to replay attempts"
    );

    let fr_ids: Vec<PhysicalFunctionRunId> = physical_function_runs
        .iter()
        .map(|f| f.id.clone())
        .collect();
    let all_tdvs: Vec<PhysicalTableDataVersionDB> = conn
        .fetch_all_chunked::<PhysicalTableDataVersionDB, PhysicalFunctionRunId>(&fr_ids)
        .await?;
    let tdvs_by_fr: HashMap<PhysicalFunctionRunId, Vec<PhysicalTableDataVersionDB>> = all_tdvs
        .into_iter()
        .into_group_map_by(|t| t.physical_function_run_id.clone());

    let producer_filters: Vec<Option<LogicalFunctionRunId>> =
        redirected.iter().map(|id| Some(id.clone())).collect();
    let all_ltdvs: Vec<LogicalTableDataVersionDB> = conn
        .fetch_all_chunked::<LogicalTableDataVersionDB, Option<LogicalFunctionRunId>>(
            &producer_filters,
        )
        .await?;
    let ltdvs_by_producer: HashMap<LogicalFunctionRunId, Vec<LogicalTableDataVersionDB>> =
        all_ltdvs
            .into_iter()
            .filter_map(|f| f.producer_logical_function_run_id.clone().map(|p| (p, f)))
            .into_group_map();

    let mut ltdv_items: Vec<OutputRedirect> = Vec::new();
    for fr in physical_function_runs.iter() {
        let Some(lfr) = lfr_by_fr_key
            .get(&fr.execution_id)
            .and_then(|inner| inner.get(&fr.function_id))
        else {
            continue;
        };
        if !redirected.contains(&lfr.id) {
            continue;
        }
        let Some(tdvs) = tdvs_by_fr.get(&fr.id) else {
            continue;
        };
        let empty = Vec::new();
        let ltdvs = ltdvs_by_producer.get(&lfr.id).unwrap_or(&empty);
        for tdv in tdvs {
            let Some(ltdv) = ltdvs.iter().find(|l| l.table_id == tdv.table_id) else {
                continue;
            };
            ltdv_items.push(OutputRedirect {
                project_id: ltdv.project_id,
                logical_table_data_version_id: ltdv.id.clone(),
                new_physical_table_data_version_id: tdv.id.clone(),
                set_by_id: fr.triggered_by_id,
            });
        }
    }
    conn.ltdv()
        .pointers()
        .redirect_outputs(&ltdv_items, at)
        .await?;
    Ok(())
}
