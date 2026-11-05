//
// Copyright 2026 Tabsdata Inc.
//

//! A function version's declarations, as an update would send them. An
//! update that leaves a list `None` keeps the current one, as it keeps the
//! scalars it leaves out; an empty list clears it.

use crate::function::services::read::vec_create_table_dependency;
use td_database::access::{AccessMode, InProject};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::{FunctionDBWithNames, FunctionUpdate};
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::{System, TableId, TableNameDto, TableStatus};
use td_objects::types::composed::{LabeledTableTrigger, TableDependencyDto, TableTriggerDto};
use td_queries::Queries;

pub async fn carried_forward(
    conn: &mut DomainConnection<Apiserver, InProject, impl AccessMode>,
    current: &FunctionDBWithNames,
    update: &FunctionUpdate,
) -> Result<FunctionUpdate, TdError> {
    let mut update = update.clone();
    let function_id = current.function_id;
    if update.tables.is_none() {
        let tables: Vec<TableDBWithNames> = conn
            .scd2()
            .fetch_all(and!(&function_id, TableStatus::Active, System::FALSE))
            .await?;
        update.tables = Some(
            tables
                .iter()
                .map(|t| TableNameDto::try_from(t.name.to_string()))
                .collect::<Result<_, _>>()?,
        );
    }
    if update.dependencies.is_none() {
        let dependencies: Vec<DependencyDBWithNames> = conn
            .scd2()
            .fetch_all(and!(&function_id, DependencyDBWithNames::user()))
            .await?;
        let table_ids: Vec<TableId> = dependencies.iter().map(|d| d.table_id).collect();
        let tables: Vec<TableDBWithNames> = conn.scd2().chunked().fetch_all(&table_ids, ()).await?;
        update.dependencies = Some(
            vec_create_table_dependency(&dependencies, &tables)
                .iter()
                .map(|d| TableDependencyDto::try_from(d.to_string().as_str()))
                .collect::<Result<_, _>>()?,
        );
    }
    if update.triggers.is_none() {
        let triggers: Vec<TriggerDBWithNames> = conn
            .scd2()
            .fetch_all(and!(&function_id, TriggerDBWithNames::active()))
            .await?;
        let table_ids: Vec<TableId> = triggers.iter().map(|t| t.trigger_by_table_id).collect();
        let tables: Vec<TableDBWithNames> = conn.scd2().chunked().fetch_all(&table_ids, ()).await?;
        update.triggers = Some(
            tables
                .iter()
                .map(LabeledTableTrigger::from)
                .map(|t| TableTriggerDto::try_from(t.to_string().as_str()))
                .collect::<Result<_, _>>()?,
        );
    }
    Ok(update)
}
