//
// Copyright 2025 Tabs Data Inc.
//

use itertools::{Either, Itertools};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use td_error::{ApiError, TdError, api_error};
use td_execution::{
    EXECUTION_GRAPH_OPTIONS, ExecutionPlan, ExecutionPlanner, ResolvedExecutionPlan, TransactionBy,
    VersionResolver,
};
use td_objects::dxo::crudl::handle_sql_err;
use td_objects::dxo::execution::{
    ExecutionDB, ExecutionResponse, FunctionNodeResponseBuilder, ResolvedVersionResponse,
    TableNodeResponseBuilder,
};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::function_requirement::FunctionRequirementDB;
use td_objects::dxo::function_run::{FunctionRunDB, FunctionRunDBBuilder, UpdateFunctionRunDB};
use td_objects::dxo::table_data_version::{
    ExecutionTableDataVersionReadBuilder, TableDataVersionDB,
};
use td_objects::dxo::transaction::{TransactionDB, TransactionDBBuilder};
use td_objects::graph::{MapGraph, TdGraph};
use td_objects::sql::{DaoQueries, FindBy, UpdateBy};
use td_objects::transaction::map::{TransactionMap, TransactionMapper};
use td_objects::types::basic::{
    Depth, Dot, Enabled, FunctionOnly, FunctionRunStatus, FunctionStatus, InputIdx, Trigger,
    TriggerType, VersionPos,
};
use td_objects::types::graph::{FunctionNodeBuilder, GraphEdge, GraphNode, ResolvedVersion};
use td_objects::types::recursion::RecursionOptions;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

pub async fn trigger_type_manual() -> Result<TriggerType, TdError> {
    Ok(TriggerType::Manual)
}

pub async fn trigger_type_cron() -> Result<TriggerType, TdError> {
    Ok(TriggerType::Cron)
}

// pub async fn trigger_type_data_listener(
// ) -> Result<TriggerType, TdError> {
//     Ok(TriggerType::DataListener)
// }

pub async fn assert_function_status(
    Input(function): Input<FunctionDBWithNames>,
) -> Result<(), TdError> {
    if !matches!(function.status, FunctionStatus::Active) {
        Err(api_error!(
            ApiError::InputError,
            "Function '{}' must be in an active status to execute. Current status: '{}'",
            function.name,
            function.status
        ))?
    }
    if !matches!(function.enabled, Enabled::TRUE) {
        Err(api_error!(
            ApiError::InputError,
            "Function '{}' must be enabled to execute",
            function.name,
        ))?
    }
    Ok(())
}

pub async fn with_recursion_options(
    Input(function_only): Input<FunctionOnly>,
) -> Result<RecursionOptions, TdError> {
    let recursion_options = if **function_only {
        RecursionOptions::new(Some(Depth::ZERO), Some(Depth::ZERO))
    } else {
        EXECUTION_GRAPH_OPTIONS
    };
    Ok(recursion_options)
}

pub async fn build_execution_plan(
    Input(td_graph): Input<TdGraph>,
    Input(trigger_function): Input<FunctionDBWithNames>,
) -> Result<ExecutionPlan, TdError> {
    let trigger = FunctionNodeBuilder::try_from(&*trigger_function)?.build()?;
    let execution_plan = td_graph.with_trigger(&trigger)?;
    Ok(execution_plan)
}

pub async fn validate_execution_plan(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(execution_plan): Input<ExecutionPlan>,
) -> Result<(), TdError> {
    execution_plan.validate_dag_trigger()?;
    execution_plan.validate_dag_dependency()?;
    execution_plan.validate_dag_transaction(transaction_by.deref())?;
    Ok(())
}

pub async fn build_transaction_map(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(execution): Input<ExecutionDB>,
    Input(execution_plan): Input<ExecutionPlan>,
) -> Result<TransactionMap<TransactionBy>, TdError> {
    let mut transaction_map = TransactionMap::empty(transaction_by.deref().clone());

    for function in execution_plan.triggered_functions() {
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
    Input(execution_plan): Input<ExecutionPlan>,
    Input(function_run_builder): Input<FunctionRunDBBuilder>,
) -> Result<Vec<FunctionRunDB>, TdError> {
    let main_trigger = execution_plan.main_trigger();
    let function_runs = execution_plan
        .triggered_functions()
        .into_iter()
        .map(|f| {
            let transaction = transaction_map.get(&transaction_by.key(f)?)?;
            let trigger = if f == main_trigger {
                Trigger::Manual
            } else {
                Trigger::Dependency
            };

            function_run_builder
                .deref()
                .clone()
                .collection_id(f.collection_id)
                .function_version_id(f.function_version_id)
                .transaction_id(transaction.id)
                .trigger(trigger)
                .build()
                .map_err(TdError::from)
        })
        .collect::<Result<_, _>>()?;
    Ok(function_runs)
}

pub async fn build_table_data_versions(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(transaction_map): Input<TransactionMap<TransactionBy>>,
    Input(function_runs): Input<Vec<FunctionRunDB>>,
    Input(execution_plan): Input<ExecutionPlan>,
) -> Result<Vec<TableDataVersionDB>, TdError> {
    let function_runs_map: HashMap<_, _> = function_runs
        .iter()
        .map(|f| (f.function_version_id, f))
        .collect();

    let new_table_data_versions = execution_plan
        .table_function_output_versions()
        .iter()
        .map(|(t, f, edge)| {
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

pub async fn resolve_execution_plan_versions(
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Connection(connection): Connection,
    Input(execution): Input<ExecutionDB>,
    Input(execution_plan): Input<ExecutionPlan>,
) -> Result<ResolvedExecutionPlan, TdError> {
    let execution_plan = execution_plan
        .map(
            |node| async move { Ok::<_, TdError>(node.clone()) },
            |versions, source, target| {
                let queries = queries.clone();
                let connection = connection.clone();
                let triggered_on = execution.triggered_on;
                async move {
                    let mut conn = connection.lock().await;
                    let conn = conn.get_mut_connection()?;

                    let (versions, function, table, self_dependency) = match versions {
                        GraphEdge::Output { versions, .. } => (versions, source, target, false),
                        GraphEdge::Trigger { versions } => (versions, target, source, false),
                        GraphEdge::Dependency {
                            versions,
                            dependency,
                        } => (versions, target, source, *dependency.self_dependency),
                    };
                    let function = match function {
                        GraphNode::Function(function) => function,
                        _ => unreachable!(),
                    };
                    let table = match table {
                        GraphNode::Table(table) => table,
                        _ => unreachable!(),
                    };

                    let lookup_versions = if self_dependency {
                        // Correct self HEAD references to the previous version to lookup.
                        let versions = versions.shift(-1);
                        Cow::Owned(versions)
                    } else {
                        Cow::Borrowed(versions)
                    };

                    let found = VersionResolver::builder()
                        .function_id(&function.function_id)
                        .table_id(&table.table_id)
                        .versions(&lookup_versions)
                        .triggered_on(&triggered_on)
                        .build()?
                        .resolve(queries.deref(), &mut *conn)
                        .await?;

                    let found = found
                        .into_iter()
                        .map(|v| {
                            v.map(|v| ExecutionTableDataVersionReadBuilder::try_from(&v)?.build())
                        })
                        .map(|opt| opt.transpose())
                        .collect::<Result<Vec<_>, _>>()?;

                    let versions = versions.clone();
                    let resolved_version = ResolvedVersion::builder()
                        .inner(found)
                        .original(versions)
                        .build()?;
                    Ok::<_, TdError>(resolved_version)
                }
            },
        )
        .await?;

    Ok(execution_plan)
}

pub async fn build_function_requirements(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(transaction_map): Input<TransactionMap<TransactionBy>>,
    Input(function_runs): Input<Vec<FunctionRunDB>>,
    Input(execution_plan): Input<ResolvedExecutionPlan>,
) -> Result<Vec<FunctionRequirementDB>, TdError> {
    let mut conditions = vec![];

    let function_runs_map: HashMap<_, _> = function_runs
        .iter()
        .map(|f| (f.function_version_id, f))
        .collect();

    let mut input_idx = 0;
    for (function, table, edge) in execution_plan.function_table_input_versions() {
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
            .requirement_table_version_id(table.table_version_id);

        if let Some(dependency) = edge.dependency_pos() {
            builder
                .requirement_input_idx(InputIdx::try_from(input_idx)?)
                .requirement_dependency_pos(Some(*dependency));
            input_idx += 1;
        }

        if edge.versions().inner.is_empty() {
            // If empty was resolved, we set version_pos to NO_VERSIONS to indicate so.
            builder.requirement_version_pos(VersionPos::NO_VERSIONS);
            conditions.push(builder.build()?);
        } else {
            for (version_pos, version) in edge.versions().inner.iter().enumerate() {
                // If single version, we set version_pos to SINGLE_VERSION to indicate so.
                // There should always be a single inner version in that case.
                let version_pos = if edge.versions().original.is_multiple() {
                    VersionPos::try_from(version_pos as i32)?
                } else {
                    VersionPos::SINGLE_VERSION
                };
                builder.requirement_version_pos(version_pos);

                if let Some(version) = version {
                    builder
                        .requirement_function_run_id(version.function_run_id)
                        .requirement_table_data_version_id(version.id);
                }

                conditions.push(builder.build()?);
            }
        }
    }

    Ok(conditions)
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

pub async fn build_response(
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Input(transaction_map): Input<TransactionMap<TransactionBy>>,
    Input(execution): Input<ExecutionDB>,
    Input(execution_plan): Input<ResolvedExecutionPlan>,
) -> Result<ExecutionResponse, TdError> {
    let dot = Dot::try_from(execution_plan.dot().to_string())?;

    // Transactions info
    let transactions = execution_plan
        .triggered_functions()
        .into_iter()
        .chain(std::iter::once(execution_plan.main_trigger()))
        .try_fold(HashMap::new(), |mut acc, f| {
            let transaction = transaction_map.get(&transaction_by.key(f)?)?;
            let entry: &mut HashSet<_> = acc.entry(transaction.id).or_default();
            entry.insert(f.function_version_id);
            Ok::<_, TdError>(acc)
        })?;

    // Execution plan
    let execution_plan = execution_plan
        .map(
            |node| async move {
                let response = match node {
                    GraphNode::Function(function) => GraphNode::Function(
                        FunctionNodeResponseBuilder::try_from(function)?.build()?,
                    ),
                    GraphNode::Table(table) => {
                        GraphNode::Table(TableNodeResponseBuilder::try_from(table)?.build()?)
                    }
                };
                Ok::<_, TdError>(response)
            },
            |edge, _, _| async move {
                let mut collected_info = Vec::new();
                let inner = edge
                    .versions()
                    .inner
                    .iter()
                    .map(|v| {
                        v.as_ref()
                            .map(|t| {
                                collected_info.push((t.id, t.clone()));
                                Ok::<_, TdError>(t.id)
                            })
                            .transpose()
                    })
                    .collect::<Result<_, _>>()?;

                let response = ResolvedVersionResponse {
                    inner,
                    original: edge.versions().original.clone(),
                };
                Ok::<_, TdError>((response, collected_info))
            },
        )
        .await?;

    // Relations
    let mut relations_info = HashMap::new();
    let relations = execution_plan
        .relations()
        .into_iter()
        .map(|(f, t, e)| {
            let (resolved_version, info) = e.versions();
            let resolved_version = e.versioned(resolved_version.clone());
            relations_info.extend(info.clone());
            (f.function_version_id, t.table_version_id, resolved_version)
        })
        .sorted()
        .collect();

    // Functions info
    let all_functions = execution_plan
        .functions()
        .into_iter()
        .map(|f| (f.function_version_id, f.clone()))
        .collect();
    let triggered_functions = execution_plan
        .triggered_functions()
        .into_iter()
        .map(|f| f.function_version_id)
        .collect();

    // Tables info
    let all_tables_set = execution_plan.tables();
    let all_tables = all_tables_set
        .iter()
        .map(|t| (t.table_version_id, (*t).clone()))
        .collect();
    let created_tables = execution_plan
        .created_tables()
        .iter()
        .map(|t| t.table_version_id)
        .collect();
    let (system_tables, user_tables): (HashSet<_>, HashSet<_>) =
        all_tables_set.iter().partition_map(|t| {
            if *t.system {
                Either::Left(t.table_version_id)
            } else {
                Either::Right(t.table_version_id)
            }
        });

    // We could directly return execution_plan if we want, instead of building all these maps and sets
    // as it is serializable as is. But all these intermediate structures can be useful for other purposes as well.
    let response = ExecutionResponse {
        id: execution.id,
        name: execution.name.clone(),
        triggered_on: execution.triggered_on,
        transactions,
        dot,
        all_functions,
        triggered_functions,
        manual_trigger: execution.function_version_id,
        all_tables,
        created_tables,
        system_tables,
        user_tables,
        relations,
        relations_info,
    };
    Ok(response)
}
