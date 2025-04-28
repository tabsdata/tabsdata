//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use td_error::td_error;
use td_error::TdError;
use td_objects::crudl::{assert_one, handle_sql_err};
use td_objects::sql::recursive::RecursiveQueries;
use td_objects::sql::{DerefQueries, UpdateBy};
use td_objects::types::basic::FunctionRunId;
use td_objects::types::execution::FunctionRunStatus;
use td_objects::types::execution::{
    CallbackRequest, FunctionRequirementDBWithNames, FunctionRunDB, TableDataVersionDB,
    UpdateFunctionRunDB, UpdateTableDataVersionDB,
};
use td_objects::types::worker::v2::WrittenTableV2;
use td_objects::types::worker::FunctionOutput;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

#[td_error]
enum UpdateStatusRunError {
    #[error("Cannot change final 'Done' status for function run [{0}]")]
    AlreadyDone(FunctionRunId) = 0,
    #[error("Cannot change final 'Canceled' status for function run [{0}]")]
    AlreadyCanceled(FunctionRunId) = 1,
    #[error("Unexpected function run status transition for function run [{0}]: {1:?} -> {2:?}")]
    UnexpectedFunctionRunStatusTransition(FunctionRunId, FunctionRunStatus, FunctionRunStatus) = 2,
    #[error("Cannot update data version table status for function run [{0}] because the function output is not valid")]
    InvalidFunctionOutputVersion(FunctionRunId) = 3,
}

pub async fn update_function_run_status<Q: DerefQueries>(
    SrvCtx(queries): SrvCtx<Q>,
    Connection(connection): Connection,
    Input(function_runs): Input<Vec<FunctionRunDB>>,
    Input(update): Input<UpdateFunctionRunDB>,
    // Input(update_requirement): Input<UpdateFunctionRequirementDB>,
) -> Result<(), TdError> {
    let function_run_ids: Vec<_> = function_runs
        .iter()
        .filter_map(|current| {
            match (current.status(), update.status()) {
                // Final status
                (FunctionRunStatus::Done, FunctionRunStatus::Canceled) => {
                    // Special case, canceling something done is a no-op, not an error,
                    // so we can cancel all function_runs at the same time.
                    None
                }
                (FunctionRunStatus::Done, _) => {
                    Some(Err(UpdateStatusRunError::AlreadyDone(*current.id())))
                }
                (FunctionRunStatus::Canceled, _) => {
                    Some(Err(UpdateStatusRunError::AlreadyCanceled(*current.id())))
                }

                // Mutable status
                (FunctionRunStatus::Scheduled, FunctionRunStatus::RunRequested) => {
                    Some(Ok(current.id()))
                }
                (FunctionRunStatus::ReScheduled, FunctionRunStatus::RunRequested) => {
                    Some(Ok(current.id()))
                }
                (FunctionRunStatus::RunRequested, FunctionRunStatus::Running) => {
                    Some(Ok(current.id()))
                }
                (FunctionRunStatus::Running, FunctionRunStatus::Done) => Some(Ok(current.id())),
                (
                    FunctionRunStatus::RunRequested | FunctionRunStatus::Running,
                    FunctionRunStatus::Error,
                ) => Some(Ok(current.id())),
                (
                    FunctionRunStatus::RunRequested | FunctionRunStatus::Running,
                    FunctionRunStatus::Failed,
                ) => Some(Ok(current.id())),

                // Recover status.
                (
                    FunctionRunStatus::RunRequested
                    | FunctionRunStatus::Running
                    | FunctionRunStatus::Failed
                    | FunctionRunStatus::OnHold,
                    FunctionRunStatus::ReScheduled,
                ) => Some(Ok(current.id())),
                (
                    FunctionRunStatus::Scheduled
                    | FunctionRunStatus::RunRequested
                    | FunctionRunStatus::ReScheduled
                    | FunctionRunStatus::Running
                    | FunctionRunStatus::Error
                    | FunctionRunStatus::Failed
                    | FunctionRunStatus::OnHold,
                    FunctionRunStatus::Canceled,
                ) => Some(Ok(current.id())),

                // No-op for transitions between the same states
                (current, new) if current == new => None,

                // Error in transition, not safe to proceed.
                _ => Some(Err(
                    UpdateStatusRunError::UnexpectedFunctionRunStatusTransition(
                        *current.id(),
                        current.status().clone(),
                        update.status().clone(),
                    ),
                )),
            }
        })
        .collect::<Result<_, _>>()?;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    // TODO this is not getting chunked
    let _ = queries
        .update_all_by::<_, FunctionRunDB>(update.deref(), &(function_run_ids))?
        .build()
        .execute(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    // Downstream
    let mut downstream_function_run_updates = HashMap::new();
    for current in function_runs.iter() {
        match update.status() {
            FunctionRunStatus::Canceled => {
                let function_runs: Vec<FunctionRequirementDBWithNames> = queries
                    .select_recursive_versions_at::<FunctionRequirementDBWithNames, FunctionRunDB, _>(
                        None,
                        None,
                        None,
                        None,
                        current.id(),
                    )?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;
                let function_run_ids: Vec<_> = function_runs
                    .iter()
                    .map(|f| *f.function_run_id())
                    .filter(|id| id != current.id()) // current is already canceled
                    .collect();

                downstream_function_run_updates
                    .entry(FunctionRunStatus::Canceled)
                    .or_insert_with(HashSet::new)
                    .extend(function_run_ids);
            }
            FunctionRunStatus::ReScheduled => {
                let function_runs: Vec<FunctionRequirementDBWithNames> = queries
                    .select_recursive_versions_at::<FunctionRequirementDBWithNames, FunctionRunDB, _>(
                        None,
                        None,
                        None,
                        None,
                        current.id(),
                    )?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;
                let function_run_ids: Vec<_> = function_runs
                    .iter()
                    .map(|f| *f.function_run_id())
                    .filter(|id| id != current.id()) // current is already rescheduled
                    .collect();

                downstream_function_run_updates
                    .entry(FunctionRunStatus::ReScheduled)
                    .or_insert_with(HashSet::new)
                    .extend(function_run_ids);
            }
            FunctionRunStatus::Failed => {
                let function_runs: Vec<FunctionRequirementDBWithNames> = queries
                    .select_recursive_versions_at::<FunctionRequirementDBWithNames, FunctionRunDB, _>(
                        None,
                        None,
                        None,
                        None,
                        current.id(),
                    )?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;
                let function_run_ids: Vec<_> = function_runs
                    .iter()
                    .map(|f| *f.function_run_id())
                    .filter(|id| id != current.id()) // current is already failed
                    .collect();

                downstream_function_run_updates
                    .entry(FunctionRunStatus::OnHold)
                    .or_insert_with(HashSet::new)
                    .extend(function_run_ids);
            }
            _ => {
                // No downstream function runs to update
            }
        }
    }

    for (status, function_run_ids) in downstream_function_run_updates {
        let update = UpdateFunctionRunDB::builder().status(status).build()?;
        let function_run_ids: Vec<_> = function_run_ids.iter().collect();
        // TODO this is not getting chunked
        let _ = queries
            .update_all_by::<_, FunctionRunDB>(&update, &(function_run_ids))?
            .build()
            .execute(&mut *conn)
            .await
            .map_err(handle_sql_err)?;
    }

    Ok(())
}

pub async fn update_table_data_version_status_v2<Q: DerefQueries>(
    SrvCtx(queries): SrvCtx<Q>,
    Connection(connection): Connection,
    Input(function_run_id): Input<FunctionRunId>,
    Input(callback): Input<CallbackRequest>,
) -> Result<(), TdError> {
    if let Some(context) = callback.context() {
        let output = match context {
            FunctionOutput::V2(output) => output,
            _ => Err(UpdateStatusRunError::InvalidFunctionOutputVersion(
                *function_run_id,
            ))?,
        };

        let futures: Vec<_> = output
            .output()
            .iter()
            .map(|written| {
                let queries = queries.clone();
                let connection = connection.clone();
                let function_run_id = function_run_id.clone();
                async move {
                    let (table_name, has_data) = match written {
                        WrittenTableV2::NoData { table } => (table, false),
                        WrittenTableV2::Data { table } => (table, true),
                        // TODO partitions should be handled differently, creating partitions and setting
                        // the table to has_data = true and partition = true.
                        WrittenTableV2::Partitions { table, .. } => (table, true),
                    };

                    let update = UpdateTableDataVersionDB::builder()
                        .has_data(Some(has_data.into()))
                        .build()?;

                    let mut conn = connection.lock().await;
                    let conn = conn.get_mut_connection()?;

                    let res = queries
                        .update_by::<_, TableDataVersionDB>(
                            &update,
                            &(&*function_run_id, table_name),
                        )?
                        .build()
                        .execute(&mut *conn)
                        .await
                        .map_err(handle_sql_err)?;
                    assert_one(res)?;
                    Ok::<_, TdError>(())
                }
            })
            .collect();

        let _ = futures::future::try_join_all(futures).await?;
    }

    Ok(())
}
