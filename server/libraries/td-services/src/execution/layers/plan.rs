//
// Copyright 2025 Tabs Data Inc.
//

use itertools::{Either, Itertools};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use ta_execution::graphs::ExecutionGraph;
use ta_execution::transaction::{TransactionMap, TransactionMapper};
use td_error::TdError;
use td_execution::planner::ExecutionPlanner;
use td_execution::version_resolver::VersionResolver;
use td_objects::dxo::crudl::handle_sql_err;
use td_objects::dxo::execution::defs::{
    ExecutionDB, ExecutionResponse, FunctionNodeResponseBuilder, TableNodeResponseBuilder,
};
use td_objects::dxo::function_requirement::defs::FunctionRequirementDB;
use td_objects::dxo::function_run::defs::{
    FunctionRunDB, FunctionRunDBBuilder, UpdateFunctionRunDB,
};
use td_objects::dxo::table_data_version::defs::{
    ExecutionTableDataVersionReadBuilder, TableDataVersionDB,
};
use td_objects::dxo::transaction::defs::{TransactionDB, TransactionDBBuilder};
use td_objects::execution::graph::{GraphEdge, ResolvedVersion};
use td_objects::sql::{DaoQueries, FindBy, UpdateBy};
use td_objects::table_ref::Versions;
use td_objects::types::i32::{InputIdx, VersionPos};
use td_objects::types::string::Dot;
use td_objects::types::typed_enum::{FunctionRunStatus, Trigger};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};
use te_execution::transaction::TransactionBy;

pub async fn build_transaction_map(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(execution): Input<ExecutionDB>,
    Input(template): Input<ExecutionGraph<Versions>>,
) -> Result<TransactionMap<TransactionBy>, TdError> {
    let mut transaction_map = TransactionMap::empty(transaction_by.deref().clone());

    let manual_trigger = template.manual_trigger_function();
    transaction_map.add(&execution, manual_trigger)?;

    for function in template.triggered_functions() {
        transaction_map.add(&execution, function)?;
    }

    Ok(transaction_map)
}

pub async fn build_transactions(
    Input(transaction_map): Input<TransactionMap<TransactionBy>>,
    Input(transaction_builder): Input<TransactionDBBuilder>,
) -> Result<Vec<TransactionDB>, TdError> {
    let transactions = transaction_map
        .iter()
        .map(|t| {
            let transaction = transaction_map.get(t)?;
            transaction_builder
                .deref()
                .clone()
                .id(transaction.id)
                .collection_id(transaction.collection_id)
                .transaction_by(transaction.transaction_by.clone())
                .transaction_key(transaction.transaction_key.clone())
                .build()
                .map_err(TdError::from)
        })
        .collect::<Result<_, _>>()?;

    Ok(transactions)
}

pub async fn build_function_runs(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(transaction_map): Input<TransactionMap<TransactionBy>>,
    Input(template): Input<ExecutionGraph<Versions>>,
    Input(function_run_builder): Input<FunctionRunDBBuilder>,
) -> Result<Vec<FunctionRunDB>, TdError> {
    let manual_trigger = template.manual_trigger_function();
    let transaction = transaction_map.get(&transaction_by.key(manual_trigger)?)?;
    let manual_trigger_function_run = function_run_builder
        .deref()
        .clone()
        .collection_id(manual_trigger.collection_id)
        .function_version_id(manual_trigger.function_version_id)
        .transaction_id(transaction.id)
        .trigger(Trigger::Manual)
        .build()?;

    let dependency_function_runs = template
        .triggered_functions()
        .iter()
        .map(|f| {
            let transaction = transaction_map.get(&transaction_by.key(f)?)?;
            function_run_builder
                .deref()
                .clone()
                .collection_id(f.collection_id)
                .function_version_id(f.function_version_id)
                .transaction_id(transaction.id)
                .trigger(Trigger::Dependency)
                .build()
                .map_err(TdError::from)
        })
        .collect::<Result<_, _>>()?;

    Ok([vec![manual_trigger_function_run], dependency_function_runs].concat())
}

pub async fn build_table_data_versions(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(transaction_map): Input<TransactionMap<TransactionBy>>,
    Input(function_runs): Input<Vec<FunctionRunDB>>,
    Input(template): Input<ExecutionGraph<Versions>>,
) -> Result<Vec<TableDataVersionDB>, TdError> {
    let function_runs_map: HashMap<_, _> = function_runs
        .iter()
        .map(|f| (f.function_version_id, f))
        .collect();

    let new_table_data_versions = template
        .output_tables()
        .iter()
        .map(|(f, t, edge)| {
            let transaction = transaction_map.get(&transaction_by.key(f)?)?;
            TableDataVersionDB::builder()
                .collection_id(f.collection_id)
                .table_id(t.table_id)
                .name(t.name.clone())
                .table_version_id(t.table_version_id)
                .function_version_id(t.function_version_id)
                .execution_id(transaction.execution_id)
                .transaction_id(transaction.id)
                .function_run_id(function_runs_map[&f.function_version_id].id)
                .function_param_pos(edge.output_pos().cloned().expect("must have output pos"))
                .build()
                .map_err(TdError::from)
        })
        .collect::<Result<_, _>>()?;

    Ok(new_table_data_versions)
}

pub async fn build_execution_plan(
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Connection(connection): Connection,
    Input(execution): Input<ExecutionDB>,
    Input(execution_template): Input<ExecutionGraph<Versions>>,
) -> Result<ExecutionGraph<ResolvedVersion>, TdError> {
    let execution_plan = execution_template
        .versioned(|table, versions, self_dependency| {
            let queries = queries.clone();
            let connection = connection.clone();
            let triggered_on = execution.triggered_on.clone();
            async move {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let lookup_versions = if self_dependency {
                    // Correct self HEAD references to the previous version to lookup.
                    let versions = versions.shift(-1);
                    Cow::Owned(versions)
                } else {
                    Cow::Borrowed(versions)
                };

                let found = VersionResolver::builder()
                    .table_id(&table.table_id)
                    .versions(&lookup_versions)
                    .triggered_on(&triggered_on)
                    .build()?
                    .resolve(queries.deref(), &mut *conn)
                    .await?;

                let found = found
                    .into_iter()
                    .map(|v| v.map(|v| ExecutionTableDataVersionReadBuilder::try_from(&v)?.build()))
                    .map(|opt| opt.transpose())
                    .collect::<Result<Vec<_>, _>>()?;

                let versions = versions.clone();
                let resolved_version = ResolvedVersion::builder()
                    .inner(found)
                    .original(versions)
                    .build()?;
                Ok::<_, TdError>(resolved_version)
            }
        })
        .await?;

    Ok(execution_plan)
}

// similar to `update_function_run_status`
pub async fn update_initial_function_run_status(
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Connection(connection): Connection,
    Input(function_runs): Input<Vec<FunctionRunDB>>,
    Input(function_reqs): Input<Vec<FunctionRequirementDB>>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let function_reqs_upstream = function_reqs
        .iter()
        .filter_map(|req| req.requirement_function_run_id)
        .unique()
        .collect::<Vec<_>>();

    let function_runs_upstream: Vec<FunctionRunDB> = queries
        .find_by::<FunctionRunDB>(&function_reqs_upstream)?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    let mut function_run_updates = HashMap::new();
    for fr in function_runs.iter() {
        // collect all upstream runs for this function_run
        let upstream_runs = function_reqs
            .iter()
            .filter(|req| req.function_run_id == fr.id)
            .filter_map(|req| req.requirement_function_run_id)
            .filter_map(|id| function_runs_upstream.iter().find(|ufr| ufr.id == id))
            .collect::<Vec<_>>();

        // and update status accordingly
        if upstream_runs.iter().any(|ufr| {
            matches!(
                ufr.status,
                FunctionRunStatus::Failed | FunctionRunStatus::OnHold
            )
        }) {
            // any upstream Failed, OnHold → OnHold
            function_run_updates
                .entry(FunctionRunStatus::OnHold)
                .or_insert_with(HashSet::new)
                .extend(std::iter::once(fr.id));
        }
    }

    for (status, function_run_ids) in function_run_updates {
        let update = UpdateFunctionRunDB::builder().status(status).build()?;
        let function_run_ids: Vec<_> = function_run_ids.iter().collect();
        // TODO this is not getting chunked
        let _ = queries
            .update_all_by::<_, FunctionRunDB>(&update, &function_run_ids)?
            .build()
            .execute(&mut *conn)
            .await
            .map_err(handle_sql_err)?;
    }

    Ok(())
}

pub async fn build_function_requirements(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(transaction_map): Input<TransactionMap<TransactionBy>>,
    Input(function_runs): Input<Vec<FunctionRunDB>>,
    Input(plan): Input<ExecutionGraph<ResolvedVersion>>,
) -> Result<Vec<FunctionRequirementDB>, TdError> {
    let mut conditions = vec![];

    let function_runs_map: HashMap<_, _> = function_runs
        .iter()
        .map(|f| (f.function_version_id, f))
        .collect();

    let mut input_idx = 0;
    for (function, table, edge) in plan.function_version_requirements() {
        let is_multiple_versions = edge.versions().original.is_multiple();

        for (version_pos, version) in edge.versions().inner.iter().enumerate() {
            // If single version, we set version_pos to -1 to indicate so.
            // There should always be a single inner version in that case.
            let version_pos = if is_multiple_versions {
                version_pos as i32
            } else {
                -1
            };

            let transaction = transaction_map.get(&transaction_by.key(function)?)?;
            let mut builder = FunctionRequirementDB::builder();
            builder
                // current
                .collection_id(function.collection_id)
                .execution_id(transaction.execution_id)
                .transaction_id(transaction.id)
                .function_run_id(function_runs_map[&function.function_version_id].id)
                // condition
                .requirement_table_id(table.table_id)
                .requirement_function_version_id(table.function_version_id)
                .requirement_table_version_id(table.table_version_id)
                .requirement_version_pos(VersionPos::try_from(version_pos)?);

            if let Some(dependency) = edge.dependency_pos() {
                builder
                    .requirement_input_idx(InputIdx::try_from(input_idx)?)
                    .requirement_dependency_pos(Some(dependency.clone()));
                input_idx += 1;
            }

            if let Some(version) = version {
                builder
                    .requirement_function_run_id(version.function_run_id)
                    .requirement_table_data_version_id(version.id);
            }

            conditions.push(builder.build()?);
        }
    }

    Ok(conditions)
}

pub async fn build_response(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(transaction_map): Input<TransactionMap<TransactionBy>>,
    Input(execution): Input<ExecutionDB>,
    Input(plan): Input<ExecutionGraph<ResolvedVersion>>,
) -> Result<ExecutionResponse, TdError> {
    // function info
    let plan_functions_set = plan.functions();
    let all_functions = plan_functions_set
        .iter()
        .map(|f| {
            Ok((
                f.function_version_id,
                FunctionNodeResponseBuilder::try_from(*f)?.build()?,
            ))
        })
        .collect::<Result<HashMap<_, _>, TdError>>()?;

    let triggered_functions_set = plan.triggered_functions();
    let triggered_functions = triggered_functions_set
        .iter()
        .map(|f| f.function_version_id)
        .collect::<HashSet<_>>();

    let manual_trigger = plan.manual_trigger_function();

    // transactions info
    let transactions = triggered_functions_set
        .into_iter()
        .chain(std::iter::once(manual_trigger))
        .try_fold(HashMap::new(), |mut acc, f| {
            let transaction = transaction_map.get(&transaction_by.key(f)?)?;
            let entry: &mut HashSet<_> = acc.entry(transaction.id).or_default();
            entry.insert(f.function_version_id);
            Ok::<_, TdError>(acc)
        })?;

    // tables info
    let all_tables_set = plan.tables();
    let all_tables = all_tables_set
        .iter()
        .map(|t| {
            Ok((
                t.table_version_id,
                TableNodeResponseBuilder::try_from(*t)?.build()?,
            ))
        })
        .collect::<Result<HashMap<_, _>, TdError>>()?;

    let output_tables_set = plan.output_tables();
    let created_tables = output_tables_set
        .iter()
        .map(|(_, t, _)| t.table_version_id)
        .collect::<HashSet<_>>();

    let (system_tables, user_tables): (HashSet<_>, HashSet<_>) =
        all_tables_set.iter().partition_map(|t| {
            if *t.system {
                Either::Left(t.table_version_id)
            } else {
                Either::Right(t.table_version_id)
            }
        });

    // Relations info
    let mut relations_info = HashMap::new();
    let relations = plan
        .function_version_requirements()
        .into_iter()
        .chain(plan.output_tables().into_iter())
        .map(|(f, t, e)| {
            let edge = match e {
                GraphEdge::Output { versions, output } => {
                    let (response, info) = versions.to_response()?;
                    relations_info.extend(info);
                    GraphEdge::Output {
                        versions: response,
                        output: output.clone(),
                    }
                }
                GraphEdge::Trigger { versions } => {
                    let (response, info) = versions.to_response()?;
                    relations_info.extend(info);
                    GraphEdge::Trigger { versions: response }
                }
                GraphEdge::Dependency {
                    versions,
                    dependency,
                } => {
                    let (response, info) = versions.to_response()?;
                    relations_info.extend(info);
                    GraphEdge::Dependency {
                        versions: response,
                        dependency: dependency.clone(),
                    }
                }
            };
            Ok((f.function_version_id, t.table_version_id, edge))
        })
        .collect::<Result<Vec<_>, TdError>>()?;

    let triggered_on = &execution.triggered_on;
    let dot = Dot::try_from(plan.dot().to_string())?;

    let response = ExecutionResponse::builder()
        .id(execution.id)
        .name(execution.name.clone())
        .triggered_on(triggered_on)
        .dot(dot)
        .all_functions(all_functions)
        .triggered_functions(triggered_functions)
        .manual_trigger(manual_trigger.function_version_id)
        .transactions(transactions)
        .all_tables(all_tables)
        .created_tables(created_tables)
        .system_tables(system_tables)
        .user_tables(user_tables)
        .relations(relations)
        .relations_info(relations_info)
        .build()?;
    Ok(response)
}
