//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::HashMap;
use std::ops::Deref;

use td_error::{td_error, TdError};
use td_objects::crudl::handle_sql_err;
use td_objects::sql::{DerefQueries, FindBy, Insert, UpdateBy};
use td_objects::types::basic::{
    CollectionId, CollectionName, DependencyPos, ReuseFrozen, TableDependency,
    TableFunctionParamPos, TableId, TableName, TableTrigger,
};
use td_objects::types::dependency::{DependencyVersionDB, DependencyVersionDBBuilder};
use td_objects::types::function::FunctionDB;
use td_objects::types::table::{
    TableDB, TableDBBuilder, TableDBWithNames, TableVersionDB, TableVersionDBBuilder,
    UpdateTableDBBuilder,
};
use td_objects::types::trigger::{TriggerVersionDB, TriggerVersionDBBuilder};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, ReqCtx, SrvCtx};

#[td_error]
enum RegisterFunctionError {
    #[error("Table '{0}' already exists in collection '{0}'")]
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

pub async fn build_table_versions<Q: DerefQueries>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(table_version_builder): Input<TableVersionDBBuilder>,
    Input(tables): Input<Option<Vec<TableName>>>,
    Input(collection_id): Input<CollectionId>,
) -> Result<Vec<TableVersionDB>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let table_versions = match tables.as_deref() {
        Some(tables) if !tables.is_empty() => {
            // TODO this is not getting chunked. If there are too many we can have issues.
            let tables_lookup: Vec<_> = tables.iter().map(|t| (&*collection_id, t)).collect();

            let tables_found: Vec<_> = queries
                .find_by::<TableDB>(&tables_lookup)?
                .build_query_as()
                .fetch_all(&mut *conn)
                .await
                .map_err(handle_sql_err)?;
            let tables_found: HashMap<_, _> = tables_found
                .iter()
                .map(|t: &TableDB| (t.name(), t))
                .collect();

            let mut table_versions = vec![];
            for (pos, table) in tables.iter().enumerate() {
                let table_db = tables_found.get(table);

                let id = match table_db {
                    Some(table) if **table.frozen() => table.id(),
                    _ => &TableId::default(),
                };

                let table_version = table_version_builder
                    .deref()
                    .clone()
                    .table_id(id)
                    .name(table)
                    .function_param_pos(Some(TableFunctionParamPos::try_from(pos as i16)?))
                    .build()?;
                table_versions.push(table_version);
            }

            table_versions
        }
        _ => vec![],
    };

    Ok(table_versions)
}

pub async fn build_dependency_versions<Q: DerefQueries>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(dependency_tables): Input<Option<Vec<TableDependency>>>,
    Input(collection_in_context): Input<CollectionName>,
    Input(dependency_version_builder): Input<DependencyVersionDBBuilder>,
) -> Result<Vec<DependencyVersionDB>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let dependency_versions = match dependency_tables.as_deref() {
        Some(dependency_tables) if !dependency_tables.is_empty() => {
            // TODO this is not getting chunked. If there are too many we can have issues.
            let dependency_tables_lookup: Vec<_> = dependency_tables
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
                .find_by::<TableDBWithNames>(&dependency_tables_lookup)?
                .build_query_as()
                .fetch_all(&mut *conn)
                .await
                .map_err(handle_sql_err)?;
            let tables_found: HashMap<_, _> = tables_found
                .iter()
                .map(|t: &TableDBWithNames| ((t.collection(), t.name()), t))
                .collect();

            let mut dependency_versions = vec![];
            for (pos, dependency_table) in dependency_tables.iter().enumerate() {
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

                let dependency_version = dependency_version_builder
                    .deref()
                    .clone()
                    .table_collection_id(table_db.collection_id())
                    .table_id(table_db.id())
                    .table_name(table_db.name())
                    .table_version_id(table_db.table_version_id())
                    .table_versions(dependency_table.versions())
                    .dep_pos(DependencyPos::try_from(pos as i16)?)
                    .build()?;
                dependency_versions.push(dependency_version);
            }

            dependency_versions
        }
        _ => vec![],
    };

    Ok(dependency_versions)
}

pub async fn build_trigger_versions<Q: DerefQueries>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(tables): Input<Option<Vec<TableName>>>,
    Input(trigger_tables): Input<Option<Vec<TableTrigger>>>,
    Input(collection_in_context): Input<CollectionName>,
    Input(trigger_version_builder): Input<TriggerVersionDBBuilder>,
) -> Result<Vec<TriggerVersionDB>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let trigger_versions = match trigger_tables.as_deref() {
        Some(trigger_tables) if !trigger_tables.is_empty() => {
            // TODO this is not getting chunked. If there are too many we can have issues.
            let trigger_tables_lookup: Vec<_> = trigger_tables
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
            let tables_found: HashMap<_, _> = tables_found
                .iter()
                .map(|t: &TableDBWithNames| ((t.collection(), t.name()), t))
                .collect();

            let mut trigger_versions = vec![];
            for trigger_table in trigger_tables {
                let (trigger_collection, trigger_table_name) = {
                    let collection = trigger_table
                        .collection()
                        .as_ref()
                        .unwrap_or(&*collection_in_context);
                    (collection, trigger_table.table())
                };

                let table_db = match tables_found.get(&(trigger_collection, trigger_table_name)) {
                    None => Err(RegisterFunctionError::TriggerTableDoesNotExist(
                        trigger_table.clone(),
                    )),
                    Some(table_db)
                        if table_db.collection() == &*collection_in_context
                            && tables
                                .as_deref()
                                .is_some_and(|t| t.iter().any(|t| t == table_db.name())) =>
                    {
                        Err(RegisterFunctionError::SelfTrigger(trigger_table.clone()))
                    }
                    Some(table_db) => Ok(table_db),
                }?;

                let trigger_version = trigger_version_builder
                    .deref()
                    .clone()
                    .trigger_by_collection_id(table_db.collection_id())
                    .trigger_by_function_id(table_db.function_id())
                    .trigger_by_function_version_id(table_db.function_version_id())
                    .trigger_by_table_id(table_db.id())
                    .build()?;
                trigger_versions.push(trigger_version);
            }

            trigger_versions
        }
        _ => vec![],
    };

    Ok(trigger_versions)
}

pub async fn insert_and_update_output_tables<Q: DerefQueries>(
    ReqCtx(ctx): ReqCtx,
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(table_versions): Input<Vec<TableVersionDB>>,
    Input(function_db): Input<FunctionDB>,
    Input(collection_name): Input<CollectionName>,
    Input(reuse_frozen): Input<ReuseFrozen>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    // TODO this is not getting chunked. If there are too many we can have issues.
    let tables_lookup: Vec<_> = table_versions.iter().map(|t| t.table_id()).collect();
    let tables_found: Vec<_> = queries
        .find_by::<TableDB>(&tables_lookup)?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;
    let tables_found: HashMap<_, _> = tables_found.iter().map(|t: &TableDB| (t.id(), t)).collect();

    for table_version in &*table_versions {
        let table_db = TableDBBuilder::try_from(table_version)?
            .function_id(function_db.id())
            .build()?;

        if let Some(found_table_db) = tables_found.get(table_db.id()) {
            if **found_table_db.frozen() {
                if **reuse_frozen {
                    let update_table_db = UpdateTableDBBuilder::try_from(table_version)?.build()?;
                    queries
                        .update_by::<_, TableDB>(&update_table_db, &(table_db.id()))?
                        .build()
                        .execute(&mut *conn)
                        .await
                        .map_err(handle_sql_err)?;

                    ctx.warning(RegisterFunctionError::FrozenTableAlreadyExists(
                        table_db.name().clone(),
                        collection_name.deref().clone(),
                        "unfreezing with new table definition".to_string(),
                    ))
                    .await;
                } else {
                    Err(RegisterFunctionError::FrozenTableAlreadyExists(
                        table_db.name().clone(),
                        collection_name.deref().clone(),
                        "could not reuse frozen table".to_string(),
                    ))?
                }
            } else {
                Err(RegisterFunctionError::TableAlreadyExists(
                    table_db.name().clone(),
                    collection_name.deref().clone(),
                ))?
            }
        } else {
            queries
                .insert(&table_db)?
                .build()
                .execute(&mut *conn)
                .await
                .map_err(handle_sql_err)?;
        }
    }

    Ok(())
}
