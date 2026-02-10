//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::TryUpdater;
use td_objects::dxo::crudl::{RequestContext, handle_sql_err};
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::worker::{UpdateWorkerDB, WorkerDB};
use td_objects::sql::{DaoQueries, UpdateBy};
use td_objects::types::basic::{
    CollectionId, FunctionStatus, FunctionVersionId, TableStatus, TableVersionId, WorkerType,
};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};
use tracing::debug;

pub async fn build_deleted_functions(
    Input(request_context): Input<RequestContext>,
    Input(functions): Input<Vec<FunctionDB>>,
) -> Result<Vec<FunctionDB>, TdError> {
    let deleted = functions
        .iter()
        .map(|v| {
            v.clone()
                .to_builder()
                .try_update_from(request_context.deref())?
                .id(FunctionVersionId::new())
                .status(FunctionStatus::Deleted)
                .build()
        })
        .collect::<Result<_, _>>()?;
    Ok(deleted)
}

pub async fn build_deleted_tables(
    Input(request_context): Input<RequestContext>,
    Input(tables): Input<Vec<TableDB>>,
) -> Result<Vec<TableDB>, TdError> {
    let deleted = tables
        .iter()
        .map(|v| {
            v.clone()
                .to_builder()
                .try_update_from(request_context.deref())?
                .id(TableVersionId::default())
                .status(TableStatus::Deleted)
                .build()
        })
        .collect::<Result<_, _>>()?;
    Ok(deleted)
}

pub async fn update_deleted_workers(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Input(collection_id): Input<CollectionId>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let update = UpdateWorkerDB::stop_scheduled().await?;
    let to_stop = [
        (*collection_id, WorkerType::CronTrigger),
        (*collection_id, WorkerType::StageTrigger),
    ];

    let res = queries
        .update_all_by::<_, WorkerDB>(&update, &to_stop)?
        .build()
        .execute(&mut *conn)
        .await
        .map_err(handle_sql_err)?;
    debug!(
        "Set stopped status in {} workers for collection {}.",
        res.rows_affected(),
        collection_id
    );

    Ok(())
}
