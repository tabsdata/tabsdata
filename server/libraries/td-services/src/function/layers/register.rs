//
// Copyright 2025 Tabs Data Inc.
//

use lazy_static::lazy_static;
use std::collections::HashMap;
use std::ops::Deref;
use td_error::{td_error, TdError};
use td_objects::crudl::handle_sql_err;
use td_objects::sql::{DeleteBy, DerefQueries, FindBy, Insert, UpdateBy};
use td_objects::types::basic::{
    CollectionId, CollectionName, DependencyId, DependencyPos, DependencyStatus, FunctionId,
    ReuseFrozen, TableDependency, TableFunctionParamPos, TableId, TableName, TableStatus,
    TableTrigger, TriggerId, TriggerStatus,
};
use td_objects::types::dependency::{
    DependencyDB, DependencyDBBuilder, DependencyDBWithNames, DependencyVersionDB,
    DependencyVersionDBBuilder,
};
use td_objects::types::table::{
    TableDB, TableDBBuilder, TableDBWithNames, TableVersionDB, TableVersionDBBuilder,
};
use td_objects::types::trigger::{
    TriggerDB, TriggerDBBuilder, TriggerDBWithNames, TriggerVersionDB, TriggerVersionDBBuilder,
    TriggerVersionDBWithNames,
};
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

lazy_static! {
    static ref ACTIVE_TABLE_STATUS: TableStatus = TableStatus::active();
    static ref FROZEN_TABLE_STATUS: TableStatus = TableStatus::frozen();
    static ref ACTIVE_DEPENDENCY_STATUS: DependencyStatus = DependencyStatus::active();
    static ref DELETED_DEPENDENCY_STATUS: DependencyStatus = DependencyStatus::deleted();
    static ref DELETED_TRIGGER_STATUS: TriggerStatus = TriggerStatus::deleted();
}

pub async fn build_table_versions(
    ReqCtx(ctx): ReqCtx,
    Input(collection_id): Input<CollectionId>,
    Input(collection_name): Input<CollectionName>,
    Input(existing_versions): Input<Vec<TableVersionDB>>,
    Input(new_tables): Input<Option<Vec<TableName>>>,
    Input(table_version_builder): Input<TableVersionDBBuilder>,
    Input(reuse_frozen): Input<ReuseFrozen>,
) -> Result<Vec<TableVersionDB>, TdError> {
    let mut new_table_versions = HashMap::new();

    // Existing versions
    let existing_versions: HashMap<_, _> = existing_versions
        .iter()
        .map(|t| ((t.collection_id(), t.name()), t))
        .collect();

    // Create new table versions
    if let Some(new_tables) = new_tables.as_deref().filter(|t| !t.is_empty()) {
        for (pos, table_name) in new_tables.iter().enumerate() {
            // Reuse table id if the table is the same
            let existing_version = existing_versions.get(&(&*collection_id, table_name));
            let table_id = if let Some(existing_version) = existing_version {
                if existing_version.status() == ACTIVE_TABLE_STATUS.deref() {
                    existing_version.table_id()
                } else if existing_version.status() == FROZEN_TABLE_STATUS.deref() {
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
                } else {
                    // Status deleted, table is detached from the created table
                    &TableId::default()
                }
            } else {
                // Straight up new table
                &TableId::default()
            };

            let table_version = table_version_builder
                .deref()
                .clone()
                .table_id(table_id)
                .name(table_name)
                .function_param_pos(Some(TableFunctionParamPos::try_from(pos as i16)?))
                .status(TableStatus::active())
                .build()?;

            new_table_versions.insert((&*collection_id, table_name), table_version);
        }
    };

    // Freeze if dropped
    for (key, existing_version) in existing_versions {
        if !new_table_versions.contains_key(&key) {
            let table_version = table_version_builder
                .deref()
                .clone()
                .table_id(existing_version.table_id())
                .name(existing_version.name())
                .function_param_pos(existing_version.function_param_pos().clone())
                .status(TableStatus::frozen())
                .build()?;

            new_table_versions.insert((&*collection_id, existing_version.name()), table_version);
        }
    }

    let new_table_versions = new_table_versions.into_values().collect();
    Ok(new_table_versions)
}

pub async fn insert_and_update_tables<Q: DerefQueries>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(function_id): Input<FunctionId>,
    Input(table_versions): Input<Vec<TableVersionDB>>,
    Input(existing_tables): Input<Vec<TableDBWithNames>>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let existing_tables: HashMap<_, _> = existing_tables.iter().map(|t| (t.id(), t)).collect();
    for table_version in &*table_versions {
        let frozen = table_version.status() != ACTIVE_TABLE_STATUS.deref();
        let new_table_db = TableDBBuilder::try_from(table_version)?
            .function_id(&*function_id)
            .frozen(frozen)
            .build()?;

        if let Some(existing_table) = existing_tables.get(table_version.table_id()) {
            queries
                .update_by::<_, TableDB>(&new_table_db, &(existing_table.id()))?
                .build()
                .execute(&mut *conn)
                .await
                .map_err(handle_sql_err)?;
        } else {
            queries
                .insert(&new_table_db)?
                .build()
                .execute(&mut *conn)
                .await
                .map_err(handle_sql_err)?;
        }
    }

    Ok(())
}

pub async fn build_dependency_versions<Q: DerefQueries>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(existing_versions): Input<Vec<DependencyVersionDB>>,
    Input(new_dependencies): Input<Option<Vec<TableDependency>>>,
    Input(collection_in_context): Input<CollectionName>,
    Input(dependency_version_builder): Input<DependencyVersionDBBuilder>,
) -> Result<Vec<DependencyVersionDB>, TdError> {
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

    let new_dependencies = new_dependencies.as_deref().unwrap_or(&[]);

    // Create new dependency versions
    let tables_found = if new_dependencies.is_empty() {
        Vec::new()
    } else {
        // Here, even function created tables are already populated in the tables list.
        // TODO this is not getting chunked. If there are too many we can have issues.
        let dependency_tables_lookup: Vec<_> = new_dependencies
            .iter()
            .map(|d| {
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
    for (pos, dependency_table) in new_dependencies.iter().enumerate() {
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
            .table_id(table_db.id())
            .table_name(table_db.name())
            .table_versions(dependency_table.versions())
            .dep_pos(DependencyPos::try_from(pos as i16)?)
            .status(DependencyStatus::active())
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
                .table_name(existing_version.table_name())
                .table_versions(existing_version.table_versions().clone())
                .dep_pos(existing_version.dep_pos())
                .status(DependencyStatus::deleted())
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

pub async fn insert_and_update_dependencies<Q: DerefQueries>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(dependency_versions): Input<Vec<DependencyVersionDB>>,
    Input(existing_dependencies): Input<Vec<DependencyDBWithNames>>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let existing_dependencies: HashMap<_, _> =
        existing_dependencies.iter().map(|t| (t.id(), t)).collect();
    for dependency_version in &*dependency_versions {
        let new_dependency_db = DependencyDBBuilder::try_from(dependency_version)?.build()?;

        if let Some(existing_dependency) =
            existing_dependencies.get(dependency_version.dependency_id())
        {
            if dependency_version.status() == DELETED_DEPENDENCY_STATUS.deref() {
                queries
                    .delete_by::<DependencyDB>(&(existing_dependency.id()))?
                    .build()
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;
            } else {
                queries
                    .update_by::<_, DependencyDB>(&new_dependency_db, &(existing_dependency.id()))?
                    .build()
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;
            }
        } else {
            queries
                .insert(&new_dependency_db)?
                .build()
                .execute(&mut *conn)
                .await
                .map_err(handle_sql_err)?;
        }
    }

    Ok(())
}

pub async fn build_trigger_versions<Q: DerefQueries>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(existing_versions): Input<Vec<TriggerVersionDBWithNames>>,
    Input(new_tables): Input<Option<Vec<TableName>>>,
    Input(new_triggers): Input<Option<Vec<TableTrigger>>>,
    Input(collection_in_context): Input<CollectionName>,
    Input(trigger_version_builder): Input<TriggerVersionDBBuilder>,
) -> Result<Vec<TriggerVersionDB>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let mut new_trigger_versions = HashMap::new();

    // Fetch existing versions
    let existing_versions: HashMap<_, _> = existing_versions
        .iter()
        .map(|t| ((t.trigger_by_collection_id(), t.trigger_by_table_name()), t))
        .collect();

    let new_triggers = new_triggers.as_deref().unwrap_or(&[]);

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
                        .is_some_and(|t| t.iter().any(|t| t == table_db.name())) =>
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
            .trigger_by_table_id(table_db.id())
            .status(TriggerStatus::active())
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
                .status(TriggerStatus::deleted())
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

pub async fn insert_and_update_triggers<Q: DerefQueries>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(trigger_versions): Input<Vec<TriggerVersionDB>>,
    Input(existing_triggers): Input<Vec<TriggerDBWithNames>>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let existing_triggers: HashMap<_, _> = existing_triggers.iter().map(|t| (t.id(), t)).collect();
    for trigger_version in &*trigger_versions {
        let new_trigger_db = TriggerDBBuilder::try_from(trigger_version)?.build()?;

        if let Some(existing_trigger) = existing_triggers.get(trigger_version.trigger_id()) {
            if trigger_version.status() == DELETED_TRIGGER_STATUS.deref() {
                queries
                    .delete_by::<TriggerDB>(&(existing_trigger.id()))?
                    .build()
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;
            } else {
                queries
                    .update_by::<_, TriggerDB>(&new_trigger_db, &(existing_trigger.id()))?
                    .build()
                    .execute(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;
            }
        } else {
            queries
                .insert(&new_trigger_db)?
                .build()
                .execute(&mut *conn)
                .await
                .map_err(handle_sql_err)?;
        }
    }

    Ok(())
}
