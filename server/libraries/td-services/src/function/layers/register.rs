//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use td_error::display_vec::DisplayVec;
use td_error::{TdError, td_error};
use td_objects::crudl::handle_sql_err;
use td_objects::sql::cte::CteQueries;
use td_objects::sql::{DaoQueries, FindBy};
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionName, DataLocation, DependencyId, DependencyPos,
    DependencyStatus, FunctionId, ReuseFrozen, TableDependency, TableDependencyDto,
    TableFunctionParamPos, TableId, TableName, TableNameDto, TableStatus, TableTrigger,
    TableTriggerDto, TriggerId, TriggerStatus, TriggerVersionId,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::dependency::{DependencyDB, DependencyDBBuilder};
use td_objects::types::table::{TableDB, TableDBBuilder, TableDBWithNames};
use td_objects::types::trigger::{TriggerDB, TriggerDBBuilder, TriggerDBWithNames};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, ReqCtx, SrvCtx};

#[td_error]
pub enum RegisterFunctionError {
    #[error("Tables [{0}] already exists in collection '{1}'")]
    TableAlreadyExists(DisplayVec<TableName>, CollectionName) = 0,
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

pub async fn validate_tables_do_not_exist(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Input(collection_id): Input<CollectionId>,
    Input(collection_name): Input<CollectionName>,
    Input(existing_versions): Input<Vec<TableDB>>,
    Input(new_tables): Input<Option<Vec<TableNameDto>>>,
    Input(at_time): Input<AtTime>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    if let Some(new_tables) = new_tables.as_ref() {
        // We use existing tables to discard lookups in updates (as when registering a function,
        // there is no existing tables).
        let existing_tables_map = existing_versions
            .iter()
            .map(|t| ((t.collection_id(), t.name()), t))
            .collect::<HashMap<_, _>>();
        let new_tables_lookup = new_tables
            .iter()
            .map(TableName::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let new_tables_lookup: Vec<_> = new_tables_lookup
            .iter()
            .filter_map(|t| {
                if existing_tables_map.contains_key(&(collection_id.deref(), t)) {
                    None
                } else {
                    Some((collection_id.deref(), t))
                }
            })
            .collect();

        let tables_found: Vec<TableDBWithNames> = queries
            .find_versions_at::<TableDBWithNames>(
                Some(&at_time),
                Some(&[&TableStatus::Active, &TableStatus::Frozen]),
                &new_tables_lookup,
            )?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_sql_err)?;

        if !tables_found.is_empty() {
            let new_tables_set: HashSet<_> = new_tables_lookup.iter().collect();
            let tables_found: HashSet<_> = tables_found
                .iter()
                .map(|t| (collection_id.deref(), t.name()))
                .collect();

            let duplicate_tables: Vec<_> = tables_found
                .into_iter()
                .filter_map(|(c, t)| {
                    if new_tables_set.contains(&(c, t)) {
                        Some(t.clone())
                    } else {
                        None
                    }
                })
                .collect();

            Err(RegisterFunctionError::TableAlreadyExists(
                duplicate_tables.into(),
                collection_name.deref().clone(),
            ))?
        }
    }

    Ok(())
}

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
                TableStatus::Deleted => {
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

pub async fn build_tables_trigger_versions(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Input(at_time): Input<AtTime>,
    Input(new_table_versions): Input<Vec<TableDB>>,
) -> Result<Vec<TriggerDB>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    // Find existing downstream triggers (using table ids)
    let table_ids = new_table_versions
        .iter()
        .map(|t| t.table_id())
        .collect::<Vec<_>>();
    let existing_triggers: Vec<TriggerDB> = queries
        .find_versions_at::<TriggerDB>(Some(&at_time), None, &table_ids)?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    // Freeze triggers if the table is frozen, or reactivate them if the table is active again.
    // This has to be done downstream, because tables trigger functions.
    // This is needed because tables can change the function they are generated by. So
    // triggers need to change that too.
    let table_versions_map: HashMap<_, _> = new_table_versions
        .iter()
        .map(|t| (t.table_id(), t))
        .collect();
    let trigger_versions = existing_triggers
        .into_iter()
        .filter_map(|trigger| {
            let table = match table_versions_map.get(&trigger.trigger_by_table_id()) {
                Some(table) => table,
                None => return None, // No matching table version found
            };
            match (table.status(), trigger.status()) {
                (TableStatus::Frozen, TriggerStatus::Active) => {
                    // If the table is now frozen, and the trigger was active, we freeze the trigger
                    Some(
                        trigger
                            .to_builder()
                            .id(TriggerVersionId::default())
                            .status(TriggerStatus::Frozen)
                            .defined_on(&*at_time)
                            .build(),
                    )
                }
                (TableStatus::Active, TriggerStatus::Frozen) => {
                    // If the table is now active, and the trigger was frozen, we can reactivate the trigger
                    // for the new function id
                    Some(
                        trigger
                            .to_builder()
                            .id(TriggerVersionId::default())
                            .trigger_by_function_id(table.function_id())
                            .status(TriggerStatus::Active)
                            .defined_on(&*at_time)
                            .build(),
                    )
                }
                _ => None,
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(trigger_versions)
}

pub async fn build_dependency_versions(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<DaoQueries>,
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
                    d.table_collection_id(),
                    d.table_id(),
                    d.table_versions().deref(),
                    **d.dep_pos(),
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
            table_db.table_id(),
            dependency_table.versions(),
            pos,
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
            .table_function_id(table_db.function_id())
            .table_id(table_db.table_id())
            .table_versions(dependency_table.versions())
            .dep_pos(DependencyPos::try_from(pos)?)
            .status(DependencyStatus::Active)
            .system(pos < 0)
            .build()?;

        new_dependency_versions.insert(
            (
                table_db.collection_id(),
                table_db.table_id(),
                dependency_table.versions(),
                pos,
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
                .table_function_id(existing_version.function_id())
                .table_id(existing_version.table_id())
                .table_versions(existing_version.table_versions().clone())
                .dep_pos(existing_version.dep_pos())
                .status(DependencyStatus::Deleted)
                .system(existing_version.system())
                .build()?;

            new_dependency_versions.insert(
                (
                    existing_version.table_collection_id(),
                    existing_version.table_id(),
                    existing_version.table_versions(),
                    **existing_version.dep_pos(),
                ),
                dependency_version,
            );
        }
    }

    let new_dependency_versions = new_dependency_versions.into_values().collect();
    Ok(new_dependency_versions)
}

#[allow(clippy::too_many_arguments)]
pub async fn build_trigger_versions(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<DaoQueries>,
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
        .map(|t| ((t.trigger_by_collection_id(), t.trigger_by_table_id()), t))
        .collect();

    let new_triggers = if let Some(new_triggers) = new_triggers.as_deref() {
        // function specifies triggers, we use those
        new_triggers
            .iter()
            .map(TableTrigger::try_from)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        // function does not specify triggers (different from empty triggers), then all dependencies are triggers,
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
        let existing_version =
            existing_versions.get(&(table_db.collection_id(), table_db.table_id()));
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
            .trigger_by_function_id(*table_db.function_id())
            .trigger_by_table_id(table_db.table_id())
            .status(TriggerStatus::Active)
            .system(table_db.system())
            .build()?;

        new_trigger_versions.insert(
            (table_db.collection_id(), table_db.table_id()),
            trigger_version,
        );
    }

    // Delete if dropped
    for (key, existing_version) in existing_versions {
        if !new_trigger_versions.contains_key(&key) {
            let trigger_version = trigger_version_builder
                .deref()
                .clone()
                .trigger_id(existing_version.trigger_id())
                .trigger_by_collection_id(existing_version.trigger_by_collection_id())
                .trigger_by_function_id(*existing_version.trigger_by_function_id())
                .trigger_by_table_id(existing_version.trigger_by_table_id())
                .status(TriggerStatus::Deleted)
                .system(existing_version.system())
                .build()?;

            new_trigger_versions.insert(
                (
                    existing_version.trigger_by_collection_id(),
                    existing_version.trigger_by_table_id(),
                ),
                trigger_version,
            );
        }
    }

    let new_trigger_versions = new_trigger_versions.into_values().collect();
    Ok(new_trigger_versions)
}
