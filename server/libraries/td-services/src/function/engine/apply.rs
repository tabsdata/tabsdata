//
// Copyright 2026 Tabsdata Inc.
//

use crate::function::engine::diff::{Changes, TableTransition};
use crate::function::on_top::ensure_flips_on_top;
use itertools::Itertools;
use std::collections::HashMap;
use td_database::access::{AccessMode, InProject, LockInstant, SeesProjectRows, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::{TriggerDB, TriggerInsertDB};
use td_objects::types::basic::{FunctionId, TableId, TableStatus, TriggerStatus, TriggerVersionId};
use td_queries::Queries;

/// Land `changes`. Tables first, so the trigger flips their transitions imply
/// read the new table versions; the triggers reading a table that froze or
/// thawed belong to other functions, so they are fetched here, not diffed.
pub async fn apply(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    changes: &Changes,
    at: LockInstant,
) -> Result<(), TdError> {
    // Closed first: a table that gets a new id keeps its name, and live names
    // are unique.
    conn.scd2()
        .chunked()
        .close::<TableDB>(&changes.close_tables, (), at)
        .await?;
    conn.scd2().chunked().open(&changes.tables, at).await?;
    let flips = trigger_flips(conn, &changes.table_transitions, at).await?;
    conn.scd2().chunked().open(&flips, at).await?;
    conn.scd2()
        .chunked()
        .open(&changes.dependencies, at)
        .await?;
    conn.scd2()
        .chunked()
        .close::<DependencyDB>(&changes.close_dependencies, (), at)
        .await?;
    conn.scd2().chunked().open(&changes.triggers, at).await?;
    conn.scd2()
        .chunked()
        .close::<TriggerDB>(&changes.close_triggers, (), at)
        .await?;
    Ok(())
}

async fn trigger_flips(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    transitions: &[TableTransition],
    at: LockInstant,
) -> Result<Vec<TriggerInsertDB>, TdError> {
    if transitions.is_empty() {
        return Ok(Vec::new());
    }
    let table_ids: Vec<TableId> = transitions.iter().map(|t| t.table_id).collect();
    let triggers: Vec<TriggerDB> = conn.scd2().chunked().fetch_all(&table_ids, ()).await?;
    let by_table: HashMap<TableId, &TableTransition> =
        transitions.iter().map(|t| (t.table_id, t)).collect();
    let mut flipped = Vec::new();
    let mut flips = Vec::new();
    for trigger in triggers {
        let Some(transition) = by_table.get(&trigger.base.trigger_by_table_id) else {
            continue;
        };
        match (&transition.status, &trigger.base.status) {
            (TableStatus::Frozen, TriggerStatus::Active) => flips.push(TriggerInsertDB {
                id: TriggerVersionId::default(),
                status: TriggerStatus::Frozen,
                ..trigger.base.clone()
            }),
            (TableStatus::Active, TriggerStatus::Frozen) => flips.push(TriggerInsertDB {
                id: TriggerVersionId::default(),
                trigger_by_function_id: transition.function_id.into(),
                status: TriggerStatus::Active,
                ..trigger.base.clone()
            }),
            _ => continue,
        }
        flipped.push(trigger);
    }
    ensure_flips_on_top(conn, &flipped, at).await?;
    Ok(flips)
}

/// The active functions depending on any of `table_ids`, the ones a table or
/// collection deletion freezes.
pub async fn dependants_of_tables<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    table_ids: &[TableId],
) -> Result<Vec<FunctionDB>, TdError> {
    let dependencies: Vec<DependencyDB> = conn.scd2().chunked().fetch_all(table_ids, ()).await?;
    // A function depending on several of the tables has a row for each; one
    // id per function, or a chunked read would return it once per chunk.
    let function_ids: Vec<FunctionId> = dependencies
        .iter()
        .map(|d| d.base.function_id)
        .unique()
        .collect();
    let functions = conn
        .scd2()
        .chunked()
        .fetch_all(&function_ids, and!(FunctionDB::active()))
        .await?;
    Ok(functions)
}
