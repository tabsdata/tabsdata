//
// Copyright 2025 Tabs Data Inc.
//

use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Deref;
use ta_execution::graphs::ExecutionGraph;
use ta_execution::transaction::{TransactionMap, TransactionMapper};
use td_error::TdError;
use td_execution::planner::ExecutionPlanner;
use td_execution::version_resolver::VersionResolver;
use td_objects::sql::DerefQueries;
use td_objects::types::basic::{Dot, Trigger, VersionPos};
use td_objects::types::execution::{
    ExecutionDB, ExecutionResponse, FunctionRequirementDB, FunctionRunDB, FunctionRunDBBuilder,
    FunctionVersionResponseBuilder, ResolvedVersion, TableDataVersionDB,
    TableVersionResponseBuilder, TransactionDB, TransactionDBBuilder,
};
use td_objects::types::table_ref::Versions;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};
use te_execution::transaction::TransactionBy;

pub async fn build_transaction_map(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(template): Input<ExecutionGraph<Versions>>,
) -> Result<TransactionMap<TransactionBy>, TdError> {
    let mut transaction_map = TransactionMap::new(transaction_by.deref().clone());

    let manual_trigger = template.manual_trigger_function();
    transaction_map.add(manual_trigger)?;

    for function in template.triggered_functions() {
        transaction_map.add(function)?;
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
            transaction_builder
                .deref()
                .clone()
                .id(transaction_map.get(t)?)
                .transaction_by(transaction_map.mapper().transaction_by()?)
                .transaction_key(t)
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
    let manual_trigger_function_run = function_run_builder
        .deref()
        .clone()
        .collection_id(manual_trigger.collection_id())
        .function_version_id(manual_trigger.function_version_id())
        .transaction_id(transaction_map.get(&transaction_by.key(manual_trigger)?)?)
        .trigger(Trigger::Manual)
        .build()?;

    let dependency_function_runs = template
        .triggered_functions()
        .iter()
        .map(|f| {
            function_run_builder
                .deref()
                .clone()
                .collection_id(f.collection_id())
                .function_version_id(f.function_version_id())
                .transaction_id(transaction_map.get(&transaction_by.key(f)?)?)
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
    Input(execution): Input<ExecutionDB>,
    Input(function_runs): Input<Vec<FunctionRunDB>>,
    Input(template): Input<ExecutionGraph<Versions>>,
) -> Result<Vec<TableDataVersionDB>, TdError> {
    let function_runs_map: HashMap<_, _> = function_runs
        .iter()
        .map(|f| (f.function_version_id(), f))
        .collect();

    let new_table_data_versions = template
        .output_tables()
        .iter()
        .map(|(f, t, edge)| {
            TableDataVersionDB::builder()
                .collection_id(f.collection_id())
                .table_id(t.table_id())
                .name(t.name())
                .table_version_id(t.table_version_id())
                .function_version_id(t.function_version_id())
                .execution_id(execution.id())
                .transaction_id(transaction_map.get(&transaction_by.key(f)?)?)
                .function_run_id(function_runs_map[f.function_version_id()].id())
                .function_param_pos(edge.output_pos().cloned())
                .build()
                .map_err(TdError::from)
        })
        .collect::<Result<_, _>>()?;

    Ok(new_table_data_versions)
}

pub async fn build_execution_plan<Q: DerefQueries>(
    SrvCtx(queries): SrvCtx<Q>,
    Connection(connection): Connection,
    Input(execution): Input<ExecutionDB>,
    Input(execution_template): Input<ExecutionGraph<Versions>>,
) -> Result<ExecutionGraph<ResolvedVersion>, TdError> {
    let execution_plan = execution_template
        .versioned(|table, versions, self_dependency| {
            let queries = queries.clone();
            let connection = connection.clone();
            let triggered_on = execution.triggered_on().clone();
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

                let found = VersionResolver::new(table.table_id(), &lookup_versions, &triggered_on)
                    .resolve(queries.deref(), &mut *conn)
                    .await?;

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

pub async fn build_function_requirements(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(transaction_map): Input<TransactionMap<TransactionBy>>,
    Input(execution): Input<ExecutionDB>,
    Input(function_runs): Input<Vec<FunctionRunDB>>,
    Input(plan): Input<ExecutionGraph<ResolvedVersion>>,
) -> Result<Vec<FunctionRequirementDB>, TdError> {
    let mut conditions = vec![];

    let function_runs_map: HashMap<_, _> = function_runs
        .iter()
        .map(|f| (f.function_version_id(), f))
        .collect();

    for (function, table, edge) in plan.function_version_requirements() {
        for (version_pos, version) in edge.versions().inner().iter().enumerate() {
            let mut builder = FunctionRequirementDB::builder();
            builder
                // current
                .collection_id(function.collection_id())
                .execution_id(execution.id())
                .transaction_id(transaction_map.get(&transaction_by.key(function)?)?)
                .function_run_id(function_runs_map[function.function_version_id()].id())
                // condition
                .requirement_table_id(table.table_id())
                .requirement_table_version_id(table.table_version_id())
                .requirement_dependency_pos(edge.dependency_pos().cloned())
                .requirement_version_pos(VersionPos::try_from(version_pos as i16)?);

            let condition = if let Some(version) = version {
                builder
                    .requirement_function_run_id(*version.function_run_id())
                    .requirement_table_data_version_id(*version.id())
                    .build()?
            } else {
                builder.build()?
            };

            conditions.push(condition);
        }
    }

    Ok(conditions)
}

pub async fn build_response(
    Input(execution): Input<ExecutionDB>,
    Input(plan): Input<ExecutionGraph<ResolvedVersion>>,
) -> Result<ExecutionResponse, TdError> {
    let all_functions: Result<Vec<_>, TdError> = plan
        .functions()
        .iter()
        .map(|f| Ok(FunctionVersionResponseBuilder::try_from(*f)?.build()?))
        .collect();
    let triggered_functions: Result<Vec<_>, TdError> = plan
        .triggered_functions()
        .iter()
        .map(|f| Ok(FunctionVersionResponseBuilder::try_from(*f)?.build()?))
        .collect();
    let manual_trigger =
        FunctionVersionResponseBuilder::try_from(plan.manual_trigger_function())?.build()?;
    let all_tables: Result<Vec<_>, TdError> = plan
        .tables()
        .iter()
        .map(|t| Ok(TableVersionResponseBuilder::try_from(*t)?.build()?))
        .collect();
    let created_tables: Result<Vec<_>, TdError> = plan
        .output_tables()
        .iter()
        .map(|(_, t, _)| Ok(TableVersionResponseBuilder::try_from(*t)?.build()?))
        .collect();
    let triggered_on = execution.triggered_on();
    let dot = Dot::try_from(plan.dot().to_string())?;
    let response = ExecutionResponse::builder()
        .id(execution.id())
        .name(execution.name().clone())
        .all_functions(all_functions?)
        .triggered_functions(triggered_functions?)
        .manual_trigger(manual_trigger)
        .all_tables(all_tables?)
        .created_tables(created_tables?)
        .triggered_on(triggered_on)
        .dot(dot)
        .build()?;
    Ok(response)
}
