//
// Copyright 2025 Tabsdata Inc.
//

use itertools::{Either, Itertools};
use petgraph::prelude::EdgeRef;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use ta_services::api_message;
use ta_services::ctx_service::ReqCtx;
use td_database::access::{AccessMode, InProject, SeesProjectRows, XW};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_execution::graph::{MapGraph, TdGraph};
use td_execution::{
    EXECUTION_GRAPH_OPTIONS, ExecutionPlan, ExecutionPlanner, ResolvedExecutionPlan, TransactionBy,
    VersionResolver,
};
use td_objects::FromDao;
use td_objects::dxo::execution::{
    ExecutionInsertDB, ExecutionResponse, FunctionNodeResponse, ResolvedVersionResponse,
    TableNodeResponse,
};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::graph::FunctionNode;
use td_objects::dxo::graph::{GraphEdge, GraphNode, ResolvedVersion};
use td_objects::dxo::logical::{
    ActiveLogicalTableDataVersionDB, LogicalFunctionRunInputInsertDB, LogicalFunctionRunInsertDB,
    LogicalTableDataVersionInsertDB, TableDataVersion, TableDataVersionDBWithNames,
};
use td_objects::dxo::transaction::TransactionInsertDB;
use td_objects::types::basic::{
    AtTime, DependencyPos, Depth, Dot, FunctionId, FunctionOnly, FunctionStatus, InputIdx,
    LogicalFunctionRunId, LogicalFunctionRunInputId, LogicalFunctionRunStatus,
    LogicalTableDataVersionId, OverrideDisabledTrigger, TableId, Trigger, VersionPos,
};
use td_objects::types::recursion::RecursionOptions;
use td_objects::types::table_ref::Versions;
use td_queries::Queries;
use tracing::{debug, instrument};
use {ta_execution::TransactionMapper, td_execution::transaction_map::TransactionMap};

pub fn assert_function_status(
    msgs: &mut ReqCtx,
    function: &FunctionDBWithNames,
    override_disabled_trigger: &OverrideDisabledTrigger,
) -> Result<(), TdError> {
    if !matches!(function.base.base.status, FunctionStatus::Active) {
        Err(api_error!(
            ApiError::InputError,
            "Function '{}' must be in an active status to execute. Current status: '{}'",
            function.base.base.name,
            function.base.base.status
        ))?
    }
    if !*function.base.base.enabled {
        if **override_disabled_trigger {
            msgs.notification(api_message!(
                "Trigger function '{}' is disabled but override_disabled_trigger is set to true",
                function.base.base.name
            ));
        } else {
            Err(api_error!(
                ApiError::InputError,
                "Function '{}' must be enabled to be executed",
                function.base.base.name,
            ))?
        }
    }
    Ok(())
}

pub fn with_recursion_options(
    function_only: &FunctionOnly,
    override_disabled_trigger: &OverrideDisabledTrigger,
) -> Result<RecursionOptions, TdError> {
    let mut recursion_options = if **function_only {
        RecursionOptions::new(Some(Depth::ZERO), Some(Depth::ZERO))
    } else {
        EXECUTION_GRAPH_OPTIONS
    };
    recursion_options.with_override_disabled_trigger(*override_disabled_trigger);
    Ok(recursion_options)
}

pub fn build_execution_plan(
    td_graph: &TdGraph,
    trigger_function: &FunctionDBWithNames,
) -> Result<ExecutionPlan, TdError> {
    let trigger = FunctionNode::from(trigger_function);
    let execution_plan = td_graph.with_trigger(&trigger)?;
    Ok(execution_plan)
}

pub fn validate_execution_plan(
    transaction_by: &TransactionBy,
    execution_plan: &ExecutionPlan,
) -> Result<(), TdError> {
    execution_plan.validate_dag_trigger()?;
    execution_plan.validate_dag_dependency()?;
    execution_plan.validate_dag_transaction(transaction_by)?;
    Ok(())
}

pub fn build_transaction_map(
    transaction_by: &TransactionBy,
    execution: &ExecutionInsertDB,
    execution_plan: &ExecutionPlan,
) -> Result<TransactionMap<TransactionBy>, TdError> {
    let mut transaction_map = TransactionMap::empty(transaction_by.clone());

    for function in execution_plan.triggered_functions() {
        transaction_map.add(execution, function)?;
    }

    Ok(transaction_map)
}

pub fn build_transactions(
    transaction_map: &TransactionMap<TransactionBy>,
    execution: &ExecutionInsertDB,
) -> Result<Vec<TransactionInsertDB>, TdError> {
    let transactions = transaction_map
        .iter()
        .map(|t| {
            let transaction = transaction_map.get(t)?;
            Ok(TransactionInsertDB {
                id: transaction.id.clone(),
                collection_id: transaction.collection_id,
                execution_id: execution.id.clone(),
                transaction_by: transaction.transaction_by.clone(),
                transaction_key: transaction.transaction_key.clone(),
                triggered_on: execution.triggered_on,
                triggered_by_id: execution.triggered_by_id,
                group_id: transaction.group_id,
            })
        })
        .collect::<Result<_, TdError>>()?;

    Ok(transactions)
}

/// One LFR per triggered function; dispatch mints its first attempt.
pub fn build_logical_function_runs(
    transaction_by: &TransactionBy,
    transaction_map: &TransactionMap<TransactionBy>,
    execution_plan: &ExecutionPlan,
    execution: &ExecutionInsertDB,
) -> Result<Vec<LogicalFunctionRunInsertDB>, TdError> {
    let main_trigger = execution_plan.main_trigger();
    execution_plan
        .triggered_functions()
        .into_iter()
        .map(|f| {
            let transaction = transaction_map.get(&transaction_by.key(f)?)?;
            let trigger = if f == main_trigger {
                Trigger::Manual
            } else {
                Trigger::Dependency
            };
            Ok(LogicalFunctionRunInsertDB {
                id: LogicalFunctionRunId::new(),
                collection_id: f.collection_id,
                execution_id: execution.id.clone(),
                transaction_id: transaction.id.clone(),
                function_id: f.function_id,
                function_version_id: f.function_version_id.clone(),
                trigger,
                triggered_on: execution.triggered_on,
                triggered_by_id: execution.triggered_by_id,
                status: LogicalFunctionRunStatus::Scheduled,
                group_id: f.group_id,
            })
        })
        .collect()
}

/// One LTDV per declared output edge; the producer is the LFR of the
/// function on the edge's source side. Which materialization it ends up
/// bound to is the materialization's own business.
pub fn build_logical_table_data_versions(
    transaction_by: &TransactionBy,
    transaction_map: &TransactionMap<TransactionBy>,
    execution_plan: &ExecutionPlan,
    execution: &ExecutionInsertDB,
    logical_function_runs: &[LogicalFunctionRunInsertDB],
) -> Result<Vec<LogicalTableDataVersionInsertDB>, TdError> {
    // Keyed by function_id (logical identity); carries group_id so the LTDV
    // snapshot stays equal-by-construction to the LFR's.
    let ffr_by_function: HashMap<_, _> = logical_function_runs
        .iter()
        .map(|lfr| (lfr.function_id, (lfr.id.clone(), lfr.group_id)))
        .collect();

    execution_plan
        .table_function_output_versions()
        .iter()
        .map(|(t, f, edge)| {
            let transaction = transaction_map.get(&transaction_by.key(f)?)?;
            let (producer, group_id) = ffr_by_function.get(&f.function_id).ok_or_else(|| {
                api_error!(
                    ApiError::InternalError,
                    "Producer LFR not found for function_id {} while building LTDVs",
                    f.function_id
                )
            })?;
            Ok(LogicalTableDataVersionInsertDB {
                id: LogicalTableDataVersionId::new(),
                collection_id: f.collection_id,
                execution_id: transaction.execution_id.clone(),
                transaction_id: transaction.id.clone(),
                table_id: t.table_id,
                producer_logical_function_run_id: Some(producer.clone()),
                function_param_pos: edge.output_pos().cloned(),
                private: t.private,
                triggered_on: execution.triggered_on,
                triggered_by_id: execution.triggered_by_id,
                group_id: *group_id,
            })
        })
        .collect()
}

pub async fn resolve_execution_plan_versions<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    execution: &ExecutionInsertDB,
    execution_plan: &ExecutionPlan,
) -> Result<ResolvedExecutionPlan, TdError> {
    let at_time = AtTime::from(execution.triggered_on);

    // Per-edge cache keys, in plan-iteration order; resolves are shared
    // across edges asking for the same (table, versions).
    let mut edge_meta: Vec<(Versions, (TableId, Versions))> = Vec::new();
    let mut ltdv_cache: HashMap<(TableId, Versions), Vec<Option<LogicalTableDataVersionId>>> =
        HashMap::new();

    for edge in execution_plan.graph.inner.edge_references() {
        let source = &execution_plan.graph.inner[edge.source()];
        let target = &execution_plan.graph.inner[edge.target()];
        let versions = edge.weight();

        let (versions_inner, function, table, self_dependency) = match versions {
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
            // Self references shift by -1: HEAD means the previous version.
            Cow::Owned(versions_inner.shift(-1))
        } else {
            Cow::Borrowed(versions_inner)
        };

        let cache_key = (table.table_id, lookup_versions.clone().into_owned());
        if !ltdv_cache.contains_key(&cache_key) {
            let ltdvs = VersionResolver::builder()
                .function_id(&function.function_id)
                .table_id(&table.table_id)
                .versions(&lookup_versions)
                .triggered_on(&at_time)
                .build()?
                .resolve(&mut *conn)
                .await?
                .into_iter()
                .map(|ltdv| ltdv.map(|ltdv| ltdv.base.base.id))
                .collect();
            ltdv_cache.insert(cache_key.clone(), ltdvs);
        }

        edge_meta.push((versions_inner.clone(), cache_key));
    }

    let edge_results: Vec<ResolvedVersion> = edge_meta
        .into_iter()
        .map(|(original, cache_key)| ResolvedVersion {
            inner: ltdv_cache[&cache_key].clone(),
            original: original.into(),
        })
        .collect();

    let mut edge_iter = edge_results.into_iter();
    let execution_plan = execution_plan
        .map(
            |node| async move { Ok::<_, TdError>(node.clone()) },
            |_versions, _source, _target| {
                let resolved = edge_iter.next();
                async move {
                    resolved.ok_or_else(|| {
                        api_error!(
                            ApiError::InternalError,
                            "Pre-computed edge results exhausted during execution plan resolution"
                        )
                    })
                }
            },
        )
        .await?;

    Ok(execution_plan)
}

/// Every data version the plan's edges reference, as its logical identity
/// merged with whatever materialization is current for it right now.
pub async fn fetch_relations_info<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    execution_plan: &ResolvedExecutionPlan,
) -> Result<HashMap<LogicalTableDataVersionId, TableDataVersion>, TdError> {
    let ids: Vec<LogicalTableDataVersionId> = execution_plan
        .graph
        .inner
        .edge_weights()
        .flat_map(|edge| edge.versions().inner.iter().flatten().cloned())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    conn.timeline()
        .chunked()
        .fetch_all::<TableDataVersionDBWithNames>(&ids, ())
        .await?
        .into_iter()
        .map(|dao| {
            let info = TableDataVersion::from_dao(dao)?;
            Ok((info.id.clone(), info))
        })
        .collect()
}

/// One row per resolved-version per input edge of every triggered function:
/// edges without resolved versions (trigger edges, or dep edges that
/// resolved to nothing) emit a single `requirement_version_pos = NO_VERSIONS`
/// row; dep edges with resolved versions emit one row per version.
///
/// Provenance `input_idx`: a single index across the plan's resolved input
/// versions. Edges are walked in `(function_id, dep_pos)` order so the
/// assignment is reproducible whenever the plan is rebuilt. Rows without a
/// bound version carry `InputIdx::NO_DATA`; trigger rows carry NULL.
pub async fn build_logical_function_run_inputs<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    transaction_by: &TransactionBy,
    transaction_map: &TransactionMap<TransactionBy>,
    execution_plan: &ExecutionPlan,
    execution: &ExecutionInsertDB,
    logical_function_runs: &[LogicalFunctionRunInsertDB],
) -> Result<Vec<LogicalFunctionRunInputInsertDB>, TdError> {
    let triggered_on = execution.triggered_on;

    let lfr_id_by_function: HashMap<_, _> = logical_function_runs
        .iter()
        .map(|lfr| (lfr.function_id, lfr.id.clone()))
        .collect();

    // Resolves are deterministic per (function_id, table_id, versions) at
    // this triggered_on, so dedupe across edges.
    let mut resolve_cache: HashMap<
        (FunctionId, TableId, Versions),
        Vec<Option<ActiveLogicalTableDataVersionDB>>,
    > = HashMap::new();

    let mut edges: Vec<_> = execution_plan
        .function_table_input_versions()
        .into_iter()
        .collect();
    edges.sort_by_key(|(function, _, edge)| (function.function_id, edge.dependency_pos().cloned()));

    let mut next_input_idx: i32 = 0;
    let mut rows = Vec::new();
    for (function, table, edge) in edges {
        let Some(consumer_lfr_id) = lfr_id_by_function.get(&function.function_id) else {
            return Err(api_error!(
                ApiError::InternalError,
                "Consumer LFR not found for function_id {} while building logical inputs",
                function.function_id
            ));
        };
        let transaction = transaction_map.get(&transaction_by.key(function)?)?;

        let dependency_pos = edge.dependency_pos().cloned();

        // Self references shift by -1: HEAD means the previous version.
        let lookup_versions = if edge.is_self_dependency() {
            Cow::Owned(edge.versions().shift(-1))
        } else {
            Cow::Borrowed(edge.versions())
        };

        let cache_key = (
            function.function_id,
            table.table_id,
            lookup_versions.clone().into_owned(),
        );
        let resolved = if let Some(cached) = resolve_cache.get(&cache_key) {
            cached.clone()
        } else {
            let r = VersionResolver::builder()
                .function_id(&function.function_id)
                .table_id(&table.table_id)
                .versions(&lookup_versions)
                .triggered_on(&AtTime::from(triggered_on))
                .build()?
                .resolve(&mut *conn)
                .await?;
            resolve_cache.insert(cache_key, r.clone());
            r
        };

        // Unresolved slots still emit rows (NULL requirement LTDV): the
        // worker yaml at dispatch falls back to `DependencyDB` for those.
        if resolved.is_empty() {
            rows.push(LogicalFunctionRunInputInsertDB {
                id: LogicalFunctionRunInputId::default(),
                collection_id: function.collection_id,
                execution_id: transaction.execution_id.clone(),
                transaction_id: transaction.id.clone(),
                logical_function_run_id: consumer_lfr_id.clone(),
                requirement_logical_table_data_version_id: None,
                requirement_input_idx: dependency_pos.is_some().then_some(InputIdx::NO_DATA),
                requirement_dependency_pos: dependency_pos,
                requirement_version_pos: VersionPos::NO_VERSIONS,
            });
            continue;
        }

        let multiple = lookup_versions.is_multiple();
        for (slot, ftdv_opt) in resolved.iter().enumerate() {
            let version_pos = if multiple {
                VersionPos::try_from(slot as i32)?
            } else {
                VersionPos::SINGLE_VERSION
            };
            let requirement_input_idx = if dependency_pos.is_none() {
                None
            } else if ftdv_opt.is_some() {
                let idx = InputIdx::try_from(next_input_idx)?;
                next_input_idx += 1;
                Some(idx)
            } else {
                Some(InputIdx::NO_DATA)
            };
            rows.push(LogicalFunctionRunInputInsertDB {
                id: LogicalFunctionRunInputId::default(),
                collection_id: function.collection_id,
                execution_id: transaction.execution_id.clone(),
                transaction_id: transaction.id.clone(),
                logical_function_run_id: consumer_lfr_id.clone(),
                requirement_logical_table_data_version_id: ftdv_opt
                    .as_ref()
                    .map(|f| f.base.base.id.clone()),
                requirement_input_idx,
                requirement_dependency_pos: dependency_pos,
                requirement_version_pos: version_pos,
            });
        }
    }

    Ok(rows)
}

/// Flips to OnHold the plan's LFRs whose upstream inputs are Failed/OnHold.
#[instrument(
    name = "execution.update_initial_function_run_status",
    skip_all,
    fields(execution_id = %execution.id),
    level = "debug",
    err(Debug)
)]
pub async fn update_initial_function_run_status(
    conn: &mut DomainConnection<Apiserver, InProject, XW>,
    execution: &ExecutionInsertDB,
) -> Result<(), TdError> {
    let lfr_ids = conn.logical_runs().blocked_ids(&execution.id).await?;

    debug!(
        execution_id = %execution.id,
        on_hold_count = lfr_ids.len(),
        "execution.initial_on_hold: {} LFR(s) start OnHold",
        lfr_ids.len()
    );

    conn.logical_runs()
        .set_status(&lfr_ids, LogicalFunctionRunStatus::OnHold)
        .await?;

    Ok(())
}

pub async fn build_response(
    transaction_by: &TransactionBy,
    transaction_map: &TransactionMap<TransactionBy>,
    execution: &ExecutionInsertDB,
    execution_plan: &ResolvedExecutionPlan,
    logical_function_runs: &[LogicalFunctionRunInsertDB],
    logical_function_run_inputs: &[LogicalFunctionRunInputInsertDB],
    relations_info: HashMap<LogicalTableDataVersionId, TableDataVersion>,
) -> Result<ExecutionResponse, TdError> {
    let dot = Dot::try_from(execution_plan.dot().to_string())?;

    // `input_idx` comes from the persisted LFRI rows so the response carries
    // the idxs the plan actually ran with — identically on execute and read.
    let function_by_lfr: HashMap<_, _> = logical_function_runs
        .iter()
        .map(|lfr| (&lfr.id, lfr.function_id))
        .collect();
    let idx_by_slot: HashMap<(FunctionId, DependencyPos, VersionPos), InputIdx> =
        logical_function_run_inputs
            .iter()
            .filter_map(|lfri| {
                let idx = lfri.requirement_input_idx?;
                let dep_pos = lfri.requirement_dependency_pos?;
                let function_id = function_by_lfr.get(&lfri.logical_function_run_id)?;
                Some(((*function_id, dep_pos, lfri.requirement_version_pos), idx))
            })
            .collect();
    let idx_by_slot = &idx_by_slot;

    let transactions = execution_plan
        .triggered_functions()
        .into_iter()
        .chain(std::iter::once(execution_plan.main_trigger()))
        .try_fold(HashMap::new(), |mut acc, f| {
            let transaction = transaction_map.get(&transaction_by.key(f)?)?;
            let entry: &mut HashSet<_> = acc.entry(transaction.id.clone()).or_default();
            entry.insert(f.function_version_id.clone());
            Ok::<_, TdError>(acc)
        })?;

    let execution_plan = execution_plan
        .map(
            |node| async move {
                let response = match node {
                    GraphNode::Function(function) => {
                        GraphNode::Function(FunctionNodeResponse::from(function))
                    }
                    GraphNode::Table(table) => GraphNode::Table(TableNodeResponse::from(table)),
                };
                Ok::<_, TdError>(response)
            },
            |edge, _, target| async move {
                let inner = edge.versions().inner.clone();

                // Pair each resolved slot with its persisted `input_idx`;
                // slots without a matching LFRI row render as NO_DATA.
                let input_idxs = match (edge.dependency_pos(), target) {
                    (Some(dep_pos), GraphNode::Function(function)) => {
                        let multiple = edge.versions().original.is_multiple();
                        (0..inner.len())
                            .map(|slot| {
                                let version_pos = if multiple {
                                    VersionPos::try_from(slot as i32)?
                                } else {
                                    VersionPos::SINGLE_VERSION
                                };
                                Ok(idx_by_slot
                                    .get(&(function.function_id, *dep_pos, version_pos))
                                    .copied()
                                    .unwrap_or(InputIdx::NO_DATA))
                            })
                            .collect::<Result<Vec<_>, TdError>>()?
                    }
                    _ => Vec::new(),
                };

                let response = ResolvedVersionResponse {
                    inner,
                    input_idxs,
                    original: edge.versions().original.clone(),
                };
                Ok::<_, TdError>(response)
            },
        )
        .await?;

    let relations = execution_plan
        .relations()
        .into_iter()
        .map(|(f, t, e)| {
            let resolved_version = e.versioned(e.versions().clone());
            (
                f.function_version_id.clone(),
                t.table_version_id,
                resolved_version,
            )
        })
        .sorted()
        .collect();

    let all_functions = execution_plan
        .functions()
        .into_iter()
        .map(|f| (f.function_version_id.clone(), f.clone()))
        .collect();
    let triggered_functions = execution_plan
        .triggered_functions()
        .into_iter()
        .map(|f| f.function_version_id.clone())
        .collect();

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

    let response = ExecutionResponse {
        id: execution.id.clone(),
        name: execution.name.clone(),
        triggered_on: execution.triggered_on,
        trigger_type: execution.trigger_type.clone(),
        transactions,
        dot,
        all_functions,
        triggered_functions,
        manual_trigger: execution.function_version_id.clone(),
        all_tables,
        created_tables,
        system_tables,
        user_tables,
        relations,
        relations_info,
    };
    Ok(response)
}
