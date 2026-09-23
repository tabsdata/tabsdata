//
// Copyright 2026 Tabsdata Inc.
//

//! Assembles [`EndedAttempt`]s from the base rows, in the rung's `ORDER BY`.

use std::collections::{HashMap, HashSet};

use crate::Queries;
use crate::daos::metrics::rows::{EndedAttempt, Version};
use ta_objects::errors::SqlError;
use td_database::access::{AccessMode, InProject};
use td_database::domain::{Apiserver, DomainConnection};
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalTableDataVersionDB,
};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::types::basic::{
    LogicalFunctionRunId, LogicalTableDataVersionId, PhysicalFunctionRunId,
};

pub async fn load_ended_attempts<M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, InProject, M>,
    pfr_ids: &[PhysicalFunctionRunId],
) -> Result<Vec<EndedAttempt>, SqlError> {
    if pfr_ids.is_empty() {
        return Ok(Vec::new());
    }
    let attempts: Vec<PhysicalFunctionRunDB> = conn
        .timeline()
        .chunked()
        .fetch_all::<PhysicalFunctionRunDB>(pfr_ids, ())
        .await?
        .into_iter()
        .filter(|pfr| pfr.base.ended_on.is_some())
        .collect();
    if attempts.is_empty() {
        return Ok(Vec::new());
    }

    let lfr_ids: Vec<LogicalFunctionRunId> = attempts
        .iter()
        .map(|pfr| pfr.base.logical_function_run_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let lfrs: Vec<LogicalFunctionRunDB> = conn.chunked().fetch_all(&lfr_ids, ()).await?;
    let siblings: Vec<PhysicalFunctionRunDB> =
        conn.timeline().chunked().fetch_all(&lfr_ids, ()).await?;

    let producers: Vec<Option<LogicalFunctionRunId>> = lfr_ids.iter().cloned().map(Some).collect();
    let outputs: Vec<LogicalTableDataVersionDB> = conn
        .timeline()
        .chunked()
        .fetch_all::<LogicalTableDataVersionDB>(&producers, ())
        .await?
        .into_iter()
        .filter(is_user_table)
        .collect();
    let attempt_ids: Vec<PhysicalFunctionRunId> =
        attempts.iter().map(|pfr| pfr.base.id.clone()).collect();
    let written: Vec<PhysicalTableDataVersionDB> = conn
        .timeline()
        .chunked()
        .fetch_all(&attempt_ids, ())
        .await?;

    let inputs: Vec<LogicalFunctionRunInputDB> = conn
        .chunked()
        .fetch_all::<LogicalFunctionRunInputDB>(&lfr_ids, ())
        .await?
        .into_iter()
        .filter(is_qualified_input)
        .collect();
    let source_ids: Vec<LogicalTableDataVersionId> = inputs
        .iter()
        .filter_map(|i| i.base.requirement_logical_table_data_version_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let sources: Vec<LogicalTableDataVersionDB> =
        conn.timeline().chunked().fetch_all(&source_ids, ()).await?;
    let current: Vec<PhysicalTableDataVersionDB> =
        conn.scd2().chunked().fetch_all(&source_ids, ()).await?;

    Ok(assemble(Base {
        attempts,
        lfrs,
        siblings,
        outputs,
        written,
        inputs,
        sources,
        current,
    }))
}

struct Base {
    attempts: Vec<PhysicalFunctionRunDB>,
    lfrs: Vec<LogicalFunctionRunDB>,
    siblings: Vec<PhysicalFunctionRunDB>,
    outputs: Vec<LogicalTableDataVersionDB>,
    written: Vec<PhysicalTableDataVersionDB>,
    inputs: Vec<LogicalFunctionRunInputDB>,
    sources: Vec<LogicalTableDataVersionDB>,
    current: Vec<PhysicalTableDataVersionDB>,
}

fn is_qualified_input(input: &LogicalFunctionRunInputDB) -> bool {
    input
        .base
        .requirement_logical_table_data_version_id
        .is_some()
        && input
            .base
            .requirement_dependency_pos
            .is_some_and(|p| *p >= 0)
}

/// System tables sit at negative positions.
fn is_user_table(ltdv: &LogicalTableDataVersionDB) -> bool {
    ltdv.base.function_param_pos.is_some_and(|p| *p >= 0)
}

fn version(ltdv: &LogicalTableDataVersionDB, ptdv: Option<&PhysicalTableDataVersionDB>) -> Version {
    Version {
        collection_id: ltdv.base.collection_id,
        group_id: ltdv.base.group_id,
        table_id: ltdv.base.table_id,
        table_version_id: ltdv.base.id.clone(),
        private: *ltdv.base.private,
        has_data: ptdv.and_then(|p| p.base.has_data).is_some_and(|h| *h),
        row_count: ptdv.and_then(|p| p.base.row_count).map_or(0, |r| *r),
        data_bytes: ptdv.and_then(|p| p.base.data_bytes).map_or(0, |b| *b),
    }
}

fn assemble(base: Base) -> Vec<EndedAttempt> {
    let lfr_by_id: HashMap<&LogicalFunctionRunId, &LogicalFunctionRunDB> =
        base.lfrs.iter().map(|l| (&l.base.id, l)).collect();

    let mut outputs_by_run: HashMap<&LogicalFunctionRunId, Vec<&LogicalTableDataVersionDB>> =
        HashMap::new();
    for ltdv in &base.outputs {
        if let Some(producer) = &ltdv.base.producer_logical_function_run_id {
            outputs_by_run.entry(producer).or_default().push(ltdv);
        }
    }
    for outputs in outputs_by_run.values_mut() {
        outputs.sort_by(|a, b| a.base.id.cmp(&b.base.id));
    }

    let written: HashMap<
        (&PhysicalFunctionRunId, &LogicalTableDataVersionId),
        &PhysicalTableDataVersionDB,
    > = base
        .written
        .iter()
        .map(|p| {
            (
                (
                    &p.base.physical_function_run_id,
                    &p.base.logical_table_data_version_id,
                ),
                p,
            )
        })
        .collect();
    let current: HashMap<&LogicalTableDataVersionId, &PhysicalTableDataVersionDB> = base
        .current
        .iter()
        .map(|p| (&p.base.logical_table_data_version_id, p))
        .collect();
    let source_by_id: HashMap<&LogicalTableDataVersionId, &LogicalTableDataVersionDB> =
        base.sources.iter().map(|l| (&l.base.id, l)).collect();

    let mut has_inputs: HashSet<&LogicalFunctionRunId> = HashSet::new();
    let mut read_by_run: HashMap<&LogicalFunctionRunId, Vec<&LogicalTableDataVersionId>> =
        HashMap::new();
    for input in &base.inputs {
        let run = &input.base.logical_function_run_id;
        has_inputs.insert(run);
        if let Some(ltdv_id) = &input.base.requirement_logical_table_data_version_id
            && source_by_id.get(ltdv_id).is_some_and(|s| is_user_table(s))
        {
            read_by_run.entry(run).or_default().push(ltdv_id);
        }
    }
    for read in read_by_run.values_mut() {
        read.sort();
        read.dedup();
    }

    let mut replays: HashMap<&LogicalFunctionRunId, Vec<&PhysicalFunctionRunDB>> = HashMap::new();
    for pfr in &base.siblings {
        replays
            .entry(&pfr.base.logical_function_run_id)
            .or_default()
            .push(pfr);
    }
    for attempts in replays.values_mut() {
        attempts.sort_by_key(|a| a.interval.valid_from);
    }

    let mut ended = Vec::with_capacity(base.attempts.len());
    for pfr in &base.attempts {
        let run_id = &pfr.base.logical_function_run_id;
        let Some(lfr) = lfr_by_id.get(run_id) else {
            continue;
        };
        let Some(ended_on) = &pfr.base.ended_on else {
            continue;
        };
        let replay = replays
            .get(run_id)
            .and_then(|a| a.iter().position(|p| p.base.id == pfr.base.id))
            .unwrap_or(0) as i64;
        let outs = outputs_by_run
            .get(run_id)
            .into_iter()
            .flatten()
            .map(|ltdv| version(ltdv, written.get(&(&pfr.base.id, &ltdv.base.id)).copied()))
            .collect();
        let ins = read_by_run
            .get(run_id)
            .into_iter()
            .flatten()
            .filter_map(|ltdv_id| source_by_id.get(ltdv_id))
            .map(|ltdv| version(ltdv, current.get(&ltdv.base.id).copied()))
            .collect();
        ended.push(EndedAttempt {
            collection_id: lfr.base.collection_id,
            group_id: lfr.base.group_id,
            execution_id: lfr.base.execution_id.clone(),
            transaction_id: lfr.base.transaction_id.clone(),
            function_id: lfr.base.function_id,
            function_run_id: lfr.base.id.clone(),
            attempt_id: pfr.base.id.clone(),
            attempt_outcome: pfr.base.status,
            run_status: lfr.base.status,
            replay,
            current: pfr.interval.valid_to.is_infinity(),
            run_has_inputs: has_inputs.contains(run_id),
            ended_on: **ended_on,
            outs,
            ins,
        });
    }
    ended
}
