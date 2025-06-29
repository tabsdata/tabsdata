//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use td_error::{td_error, TdError};
use td_objects::crudl::handle_sql_err;
use td_objects::sql::{DerefQueries, FindBy};
use td_objects::types::basic::{
    CollectionId, CollectionName, DataLocation, DependencyId, DependencyPos, DependencyStatus,
    FunctionId, ReuseFrozen, TableDependency, TableDependencyDto, TableFunctionParamPos, TableId,
    TableName, TableNameDto, TableStatus, TableTrigger, TableTriggerDto, TriggerId, TriggerStatus,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::dependency::{DependencyDB, DependencyDBBuilder};
use td_objects::types::table::{TableDB, TableDBBuilder, TableDBWithNames};
use td_objects::types::trigger::{TriggerDB, TriggerDBBuilder, TriggerDBWithNames};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, ReqCtx, SrvCtx};

#[td_error]
pub enum RegisterFunctionError {
    #[error("Table '{0}' already exists in collection '{1}'")]
    TableAlreadyExists(TableName, CollectionName) = 0,
    #[error("Dependency table '{0}' does not exist")]
    DependencyTableDoesNotExist(TableDependency) = 1,
    #[error("Table '{0}' cannot trigger its own function")]
    SelfTrigger(TableTrigger) = 2,
    #[error("Trigger table '{0}' does not exist")]
    TriggerTableDoesNotExist(TableTrigger) = 3,

    #[error("Table '{0}' exists in collection '{1}' but is in frozen state: {2}")]
    FrozenTableAlreadyExists(TableName, CollectionName, String) = 4,
}

pub async fn data_location(
    Input(_collection): Input<CollectionDB>,
    // Input(_function): Input<FunctionDBWithNames>,
) -> Result<DataLocation, TdError> {
    Ok(DataLocation::default())
}

pub const SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES: [&str; 1] = ["td.fn_state"];
pub const SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES: [&str; 1] = ["td.fn_state"];

#[allow(clippy::too_many_arguments)]
pub async fn build_table_versions(
    ReqCtx(ctx): ReqCtx,
    Input(collection_id): Input<CollectionId>,
    Input(collection_name): Input<CollectionName>,
    Input(function_id): Input<FunctionId>,
    Input(existing_versions): Input<Vec<TableDB>>,
    Input(new_tables): Input<Option<Vec<TableNameDto>>>,
    Input(table_version_builder): Input<TableDBBuilder>,
    Input(reuse_frozen): Input<ReuseFrozen>,
) -> Result<Vec<TableDB>, TdError> {
    let mut new_table_versions = HashMap::new();

    // Existing versions
    let existing_versions: HashMap<_, _> = existing_versions
        .iter()
        .map(|t| ((t.collection_id(), t.name()), t))
        .collect();

    // Add iterators of new tables (system tables with negative pos)
    let user_tables = new_tables
        .as_deref()
        .unwrap_or(Default::default())
        .iter()
        .map(TableName::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let user_tables = user_tables
        .iter()
        .enumerate()
        .map(|(pos, table_name)| (pos as i32, table_name));

    let new_system_tables = SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES
        .iter()
        .map(|prefix| {
            // system tables are the same for the same function
            // TODO we need to make sure these names are not going to conflict with user tables
            let table_name = format!("{prefix}__{function_id}");
            TableName::try_from(table_name)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let system_tables = new_system_tables
        .iter()
        .enumerate()
        .map(|(pos, table_name)| (-(pos as i32 + 1), table_name));

    let new_tables = user_tables.chain(system_tables).collect::<Vec<_>>();

    // Create new table versions
    for (pos, table_name) in new_tables {
        // Reuse table id if the table is the same
        let existing_version = existing_versions.get(&(&*collection_id, table_name));
        let table_id = if let Some(existing_version) = existing_version {
            match existing_version.status() {
                TableStatus::Active => existing_version.table_id(),
                TableStatus::Frozen => {
                    // We can only unfreeze a frozen table if reuse_frozen is enabled
                    if **reuse_frozen {
                        ctx.warning(RegisterFunctionError::FrozenTableAlreadyExists(
                            table_name.clone(),
                            collection_name.deref().clone(),
                            "unfreezing with new table definition".to_string(),
                        ))
                        .await;

                        existing_version.table_id()
                    } else {
                        Err(RegisterFunctionError::FrozenTableAlreadyExists(
                            table_name.clone(),
                            collection_name.deref().clone(),
                            "could not reuse frozen table".to_string(),
                        ))?
                    }
                }
                _ => {
                    // Status deleted, table is detached from the created table
                    &TableId::default()
                }
            }
        } else {
            // Straight up new table
            &TableId::default()
        };

        let table_version = table_version_builder
            .deref()
            .clone()
            .table_id(table_id)
            .private(table_name.is_private())
            .name(table_name)
            .function_param_pos(Some(TableFunctionParamPos::try_from(pos)?))
            .status(TableStatus::Active)
            .build()?;

        new_table_versions.insert((&*collection_id, table_name), table_version);
    }

    // Freeze if dropped
    for (key, existing_version) in existing_versions {
        if !new_table_versions.contains_key(&key) {
            let table_version = table_version_builder
                .deref()
                .clone()
                .table_id(existing_version.table_id())
                .name(existing_version.name())
                .function_param_pos(existing_version.function_param_pos().clone())
                .status(TableStatus::Frozen)
                .build()?;

            new_table_versions.insert((&*collection_id, existing_version.name()), table_version);
        }
    }

    let new_table_versions = new_table_versions.into_values().collect();
    Ok(new_table_versions)
}

pub async fn build_dependency_versions<Q: DerefQueries>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(existing_versions): Input<Vec<DependencyDB>>,
    Input(new_dependencies): Input<Option<Vec<TableDependencyDto>>>,
    Input(collection_in_context): Input<CollectionName>,
    Input(function_id): Input<FunctionId>,
    Input(dependency_version_builder): Input<DependencyDBBuilder>,
) -> Result<Vec<DependencyDB>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let mut new_dependency_versions = HashMap::new();

    // Existing versions
    let existing_versions: HashMap<_, _> = existing_versions
        .deref()
        .iter()
        .map(|d| {
            (
                (
                    d.collection_id(),
                    d.table_name(),
                    d.table_versions().deref(),
                ),
                d,
            )
        })
        .collect();

    // Add iterators of new dependencies (dependency tables with negative pos)
    let user_dependencies = new_dependencies
        .as_deref()
        .unwrap_or(Default::default())
        .iter()
        .map(TableDependency::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let user_dependencies = user_dependencies
        .iter()
        .enumerate()
        .map(|(pos, table_name)| (pos as i32, table_name));

    let system_dependencies = SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES
        .iter()
        .map(|prefix| {
            // system tables are the same for the same function
            // TODO we need to make sure these names are not going to conflict with user tables
            let table_name = format!("{prefix}__{function_id}");
            TableDependency::try_from(table_name)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let system_dependencies = system_dependencies
        .iter()
        .enumerate()
        .map(|(pos, table_name)| (-(pos as i32 + 1), table_name));

    let new_dependencies = user_dependencies
        .chain(system_dependencies)
        .collect::<Vec<_>>();

    // Create new dependency versions
    let tables_found = if new_dependencies.is_empty() {
        Vec::new()
    } else {
        // Here, even function created tables are already populated in the tables list.
        // TODO this is not getting chunked. If there are too many we can have issues.
        let dependency_tables_lookup: Vec<_> = new_dependencies
            .iter()
            .map(|(_, d)| {
                (
                    d.collection().as_ref().unwrap_or(&*collection_in_context),
                    d.table(),
                )
            })
            .collect();
        let tables_found: Vec<TableDBWithNames> = queries
            .find_by::<TableDBWithNames>(&dependency_tables_lookup)?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_sql_err)?;
        tables_found
    };

    let tables_found: HashMap<_, _> = tables_found
        .iter()
        .map(|t| ((t.collection(), t.name()), t))
        .collect();

    // Create new versions
    for (pos, dependency_table) in new_dependencies {
        let table_db = match tables_found.get(&(
            dependency_table
                .collection()
                .as_ref()
                .unwrap_or(&*collection_in_context),
            dependency_table.table(),
        )) {
            Some(table_db) => Ok(table_db),
            None => Err(RegisterFunctionError::DependencyTableDoesNotExist(
                dependency_table.clone(),
            )),
        }?;

        // Reuse dependency id if the dependency is the same
        let existing_version = existing_versions.get(&(
            table_db.collection_id(),
            table_db.name(),
            dependency_table.versions(),
        ));
        let dependency_id = if let Some(existing_version) = existing_version {
            existing_version.dependency_id()
        } else {
            &DependencyId::default()
        };

        let dependency_version = dependency_version_builder
            .deref()
            .clone()
            .dependency_id(dependency_id)
            .table_collection_id(table_db.collection_id())
            .table_id(table_db.table_id())
            .table_version_id(table_db.id())
            .table_function_version_id(table_db.function_version_id())
            .table_name(table_db.name())
            .table_versions(dependency_table.versions())
            .dep_pos(DependencyPos::try_from(pos)?)
            .status(DependencyStatus::Active)
            .build()?;

        new_dependency_versions.insert(
            (
                table_db.collection_id(),
                table_db.name(),
                dependency_table.versions(),
            ),
            dependency_version,
        );
    }

    // Dependency deleted if dropped
    for (key, existing_version) in existing_versions {
        if !new_dependency_versions.contains_key(&key) {
            let dependency_version = dependency_version_builder
                .deref()
                .clone()
                .dependency_id(existing_version.dependency_id())
                .table_collection_id(existing_version.table_collection_id())
                .table_id(existing_version.table_id())
                .table_version_id(existing_version.table_version_id())
                .table_function_version_id(existing_version.table_function_version_id())
                .table_name(existing_version.table_name())
                .table_versions(existing_version.table_versions().clone())
                .dep_pos(existing_version.dep_pos())
                .status(DependencyStatus::Deleted)
                .build()?;

            new_dependency_versions.insert(
                (
                    existing_version.table_collection_id(),
                    existing_version.table_name(),
                    existing_version.table_versions(),
                ),
                dependency_version,
            );
        }
    }

    let new_dependency_versions = new_dependency_versions.into_values().collect();
    Ok(new_dependency_versions)
}

#[allow(clippy::too_many_arguments)]
pub async fn build_trigger_versions<Q: DerefQueries>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(existing_versions): Input<Vec<TriggerDBWithNames>>,
    Input(new_tables): Input<Option<Vec<TableNameDto>>>,
    Input(new_triggers): Input<Option<Vec<TableTriggerDto>>>,
    Input(new_dependencies): Input<Option<Vec<TableDependencyDto>>>,
    Input(collection_in_context): Input<CollectionName>,
    Input(trigger_version_builder): Input<TriggerDBBuilder>,
) -> Result<Vec<TriggerDB>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let mut new_trigger_versions = HashMap::new();

    // Fetch existing versions
    let existing_versions: HashMap<_, _> = existing_versions
        .iter()
        .map(|t| ((t.trigger_by_collection_id(), t.trigger_by_table_name()), t))
        .collect();

    let new_triggers = if let Some(new_triggers) = new_triggers.as_deref() {
        // function specifies triggers, we use those
        new_triggers
            .iter()
            .map(TableTrigger::try_from)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        // function does not specify triggers (different than empty triggers), then all dependencies are triggers,
        // except those that are produced by the function itself and dedup them
        let new_tables = new_tables.as_deref().unwrap_or_default();
        new_dependencies
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|t| {
                !((t.collection().is_none()
                    || t.collection().as_deref() == Some(&*collection_in_context))
                    && new_tables.contains(t.table()))
            })
            .map(TableTrigger::try_from)
            .collect::<Result<HashSet<_>, _>>()?
            .into_iter()
            .collect()
    };

    // Create new trigger versions
    let tables_found = if new_triggers.is_empty() {
        Vec::new()
    } else {
        // Here, even function created tables are already populated in the tables list.
        // TODO this is not getting chunked. If there are too many we can have issues.
        let trigger_tables_lookup: Vec<_> = new_triggers
            .iter()
            .map(|d| {
                (
                    d.collection().as_ref().unwrap_or(&*collection_in_context),
                    d.table(),
                )
            })
            .collect();

        // Here, even function created tables are already populated in the tables list.
        let tables_found: Vec<_> = queries
            .find_by::<TableDBWithNames>(&trigger_tables_lookup)?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_sql_err)?;
        tables_found
    };

    let tables_found: HashMap<_, _> = tables_found
        .iter()
        .map(|t: &TableDBWithNames| ((t.collection(), t.name()), t))
        .collect();

    // Create new versions
    for trigger_table in new_triggers {
        let table_db = match tables_found.get(&(
            trigger_table
                .collection()
                .as_ref()
                .unwrap_or(&*collection_in_context),
            trigger_table.table(),
        )) {
            None => Err(RegisterFunctionError::TriggerTableDoesNotExist(
                trigger_table.clone(),
            )),
            Some(table_db)
                if table_db.collection() == &*collection_in_context
                    && new_tables
                        .as_deref()
                        .is_some_and(|t| t.iter().any(|t| **t == **table_db.name())) =>
            {
                Err(RegisterFunctionError::SelfTrigger(trigger_table.clone()))
            }
            Some(table_db) => Ok(table_db),
        }?;

        // Reuse trigger id if the trigger is the same
        let existing_version = existing_versions.get(&(table_db.collection_id(), table_db.name()));
        let trigger_id = if let Some(existing_version) = existing_version {
            existing_version.trigger_id()
        } else {
            &TriggerId::default()
        };

        let trigger_version = trigger_version_builder
            .deref()
            .clone()
            .trigger_id(trigger_id)
            .trigger_by_collection_id(table_db.collection_id())
            .trigger_by_function_id(table_db.function_id())
            .trigger_by_function_version_id(table_db.function_version_id())
            .trigger_by_table_id(table_db.table_id())
            .trigger_by_table_version_id(table_db.id())
            .status(TriggerStatus::Active)
            .build()?;

        new_trigger_versions.insert((table_db.collection_id(), table_db.name()), trigger_version);
    }

    // Delete if dropped
    for (key, existing_version) in existing_versions {
        if !new_trigger_versions.contains_key(&key) {
            let trigger_version = trigger_version_builder
                .deref()
                .clone()
                .trigger_id(existing_version.trigger_id())
                .trigger_by_collection_id(existing_version.trigger_by_collection_id())
                .trigger_by_function_id(existing_version.trigger_by_function_id())
                .trigger_by_function_version_id(existing_version.trigger_by_function_version_id())
                .trigger_by_table_id(existing_version.trigger_by_table_id())
                .trigger_by_table_version_id(existing_version.trigger_by_table_version_id())
                .status(TriggerStatus::Deleted)
                .build()?;

            new_trigger_versions.insert(
                (
                    existing_version.trigger_by_collection_id(),
                    existing_version.trigger_by_table_name(),
                ),
                trigger_version,
            );
        }
    }

    let new_trigger_versions = new_trigger_versions.into_values().collect();
    Ok(new_trigger_versions)
}
