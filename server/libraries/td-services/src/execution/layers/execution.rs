//
// Copyright 2025 Tabs Data Inc.
//

use itertools::{Either, Itertools};
use petgraph::prelude::EdgeRef;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use td_database::dynamic::DynamicConnection;
use td_error::{ApiError, TdError, api_error};
use td_execution::{
    EXECUTION_GRAPH_OPTIONS, ExecutionPlan, ExecutionPlanner, ResolvedExecutionPlan, TransactionBy,
    VersionResolver,
};
use td_objects::Buildable;
use td_objects::dxo::execution::{
    ExecutionDB, ExecutionResponse, FunctionNodeResponseBuilder, ResolvedVersionResponse,
    TableNodeResponseBuilder,
};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::graph::{FunctionNodeBuilder, GraphEdge, GraphNode, ResolvedVersion};
use td_objects::dxo::logical::{
    ActiveLogicalTableDataVersionDB, LogicalFunctionRunDB, LogicalFunctionRunInputDB,
    LogicalFunctionRunPointerDB, LogicalTableDataVersionDB, LogicalTableDataVersionPointerDB,
    UpdateLogicalFunctionRunDB,
};
use td_objects::dxo::physical_table_data_version::{
    ExecutionTableDataVersionReadBuilder, PhysicalTableDataVersionDBWithFunction,
};
use td_objects::dxo::transaction::{TransactionDB, TransactionDBBuilder};
use td_objects::graph::{MapGraph, TdGraph};
use td_objects::transaction::map::{TransactionMap, TransactionMapper};
use td_objects::types::basic::{
    AtTime, Depth, Dot, FunctionId, FunctionOnly, FunctionStatus, LogicalFunctionRunStatus,
    LogicalPointerReason, LogicalPointerStatus, LogicalTableDataVersionId, OverrideDisabledTrigger,
    PhysicalTableDataVersionId, TableId, Trigger, TriggerType, VersionPos,
};
use td_objects::types::recursion::RecursionOptions;
use td_objects::types::table_ref::Versions;
use td_queries::{ChunkedQueries, LfrEntry, VersionedEntry};
use td_tower::api_message;
use td_tower::ctx_service::ReqCtx;
use td_tower::extractors::{MutRef, Ref};
use tracing::{debug, instrument};

pub async fn trigger_type_manual() -> Result<TriggerType, TdError> {
    Ok(TriggerType::Manual)
}

pub async fn assert_function_status(
    mut ctx: MutRef<ReqCtx>,
    function: Ref<FunctionDBWithNames>,
    override_disabled_trigger: Ref<OverrideDisabledTrigger>,
) -> Result<(), TdError> {
    if !matches!(function.status, FunctionStatus::Active) {
        Err(api_error!(
            ApiError::InputError,
            "Function '{}' must be in an active status to execute. Current status: '{}'",
            function.name,
            function.status
        ))?
    }
    if !*function.enabled {
        if **override_disabled_trigger {
            ctx.notification(api_message!(
                "Trigger function '{}' is disabled but override_disabled_trigger is set to true",
                function.name
            ));
        } else {
            Err(api_error!(
                ApiError::InputError,
                "Function '{}' must be enabled to be executed",
                function.name,
            ))?
        }
    }
    Ok(())
}

pub async fn with_recursion_options(
    function_only: Ref<FunctionOnly>,
    override_disabled_trigger: Ref<OverrideDisabledTrigger>,
) -> Result<RecursionOptions, TdError> {
    let mut recursion_options = if **function_only {
        RecursionOptions::new(Some(Depth::ZERO), Some(Depth::ZERO))
    } else {
        EXECUTION_GRAPH_OPTIONS
    };
    recursion_options.with_override_disabled_trigger(*override_disabled_trigger);
    Ok(recursion_options)
}

pub async fn build_execution_plan(
    td_graph: Ref<TdGraph>,
    trigger_function: Ref<FunctionDBWithNames>,
) -> Result<ExecutionPlan, TdError> {
    let trigger = FunctionNodeBuilder::try_from(&*trigger_function)?.build()?;
    let execution_plan = td_graph.with_trigger(&trigger)?;
    Ok(execution_plan)
}

pub async fn validate_execution_plan(
    transaction_by: Ref<TransactionBy>,
    execution_plan: Ref<ExecutionPlan>,
) -> Result<(), TdError> {
    execution_plan.validate_dag_trigger()?;
    execution_plan.validate_dag_dependency()?;
    execution_plan.validate_dag_transaction(transaction_by.deref())?;
    Ok(())
}

pub async fn build_transaction_map(
    transaction_by: Ref<TransactionBy>,
    execution: Ref<ExecutionDB>,
    execution_plan: Ref<ExecutionPlan>,
) -> Result<TransactionMap<TransactionBy>, TdError> {
    let mut transaction_map = TransactionMap::empty(transaction_by.deref().clone());

    for function in execution_plan.triggered_functions() {
        transaction_map.add(&execution, function)?;
    }

    Ok(transaction_map)
}

pub async fn build_transactions(
    transaction_map: Ref<TransactionMap<TransactionBy>>,
    transaction_builder: Ref<TransactionDBBuilder>,
) -> Result<Vec<TransactionDB>, TdError> {
    let transactions = transaction_map
        .iter()
        .map(|t| {
            let transaction = transaction_map.get(t)?;
            transaction_builder
                .deref()
                .clone()
                .id(transaction.id.clone())
                .collection_id(transaction.collection_id)
                .transaction_by(transaction.transaction_by.clone())
                .transaction_key(transaction.transaction_key.clone())
                .build()
                .map_err(TdError::from)
        })
        .collect::<Result<_, _>>()?;

    Ok(transactions)
}

/// Build logical function-run rows for the execution.
///
/// One LFR per triggered function, born `Pending` with a NULL pointer.
/// At dispatch the pointer flips to the concrete `physical_function_runs` row that
/// already lives alongside (the placeholder is the dispatch target).
pub async fn build_logical_function_runs(
    transaction_by: Ref<TransactionBy>,
    transaction_map: Ref<TransactionMap<TransactionBy>>,
    execution_plan: Ref<ExecutionPlan>,
    execution: Ref<ExecutionDB>,
    all_functions_map: Ref<HashMap<FunctionId, FunctionDBWithNames>>,
) -> Result<Vec<LogicalFunctionRunDB>, TdError> {
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
            // Group snapshot from the planner's `all_functions_map`
            // (view-joined `collections.group_id`). Map was populated
            // upstream from these same function_ids — missing entry is
            // an internal invariant violation.
            let function = all_functions_map.get(&f.function_id).ok_or_else(|| {
                api_error!(
                    ApiError::InternalError,
                    "Function {} missing from all_functions_map while stamping LFR group_id",
                    f.function_id
                )
            })?;
            LogicalFunctionRunDB::builder()
                .project_id(function.project_id.clone())
                .collection_id(f.collection_id)
                .execution_id(execution.id.clone())
                .transaction_id(transaction.id.clone())
                .function_id(f.function_id)
                .function_version_id(f.function_version_id.clone())
                .trigger(trigger)
                .triggered_on(execution.triggered_on)
                .triggered_by_id(execution.triggered_by_id)
                .group_id(function.group_id)
                .build()
                .map_err(TdError::from)
        })
        .collect()
}

/// Build the `InitialMint` pointer-event row that pairs an LFR identity row.
///
/// `set_on` / `set_by_id` come from the LFR's plan-time `triggered_on` /
/// `triggered_by_id` — the identity and its first pointer event are
/// simultaneous at plan-build.
pub fn initial_mint_lfr_pointer(
    lfr: &LogicalFunctionRunDB,
) -> Result<LogicalFunctionRunPointerDB, TdError> {
    Ok(LogicalFunctionRunPointerDB::builder()
        .project_id(lfr.project_id.clone())
        .logical_function_run_id(lfr.id.clone())
        .status(LogicalPointerStatus::Pending)
        .physical_function_run_id(None)
        .set_on(AtTime::try_from(lfr.triggered_on)?)
        .set_by_id(lfr.triggered_by_id)
        .reason(LogicalPointerReason::InitialMint)
        .build()?)
}

pub async fn build_initial_mint_lfr_pointers(
    logical_function_runs: Ref<Vec<LogicalFunctionRunDB>>,
) -> Result<Vec<LogicalFunctionRunPointerDB>, TdError> {
    logical_function_runs
        .iter()
        .map(initial_mint_lfr_pointer)
        .collect()
}

/// Build logical table-data-version rows for the execution.
///
/// One LTDV per declared output edge, born `Pending` with a NULL pointer.
/// The producer is the LFR for the function on the source side of the
/// output edge; LTDVs and LFRs are paired by `function_version_id`.
pub async fn build_logical_table_data_versions(
    transaction_by: Ref<TransactionBy>,
    transaction_map: Ref<TransactionMap<TransactionBy>>,
    execution_plan: Ref<ExecutionPlan>,
    execution: Ref<ExecutionDB>,
    logical_function_runs: Ref<Vec<LogicalFunctionRunDB>>,
) -> Result<Vec<LogicalTableDataVersionDB>, TdError> {
    // Producer LFR lookup is keyed by function_id (logical identity), not
    // function_version_id — see `build_function_runs` for why. The map
    // also carries `group_id` so the LTDV snapshot stays equal-by-
    // construction to the LFR's, with no second `all_functions_map` lookup.
    let ffr_by_function: HashMap<_, _> = logical_function_runs
        .iter()
        .map(|lfr| {
            (
                lfr.function_id,
                (lfr.id.clone(), lfr.group_id, lfr.project_id.clone()),
            )
        })
        .collect();

    execution_plan
        .table_function_output_versions()
        .iter()
        .map(|(t, f, edge)| {
            let transaction = transaction_map.get(&transaction_by.key(f)?)?;
            let (producer, group_id, project_id) =
                ffr_by_function.get(&f.function_id).ok_or_else(|| {
                    api_error!(
                        ApiError::InternalError,
                        "Producer LFR not found for function_id {} while building LTDVs",
                        f.function_id
                    )
                })?;
            LogicalTableDataVersionDB::builder()
                .project_id(project_id.clone())
                .collection_id(f.collection_id)
                .execution_id(transaction.execution_id.clone())
                .transaction_id(transaction.id.clone())
                .table_id(t.table_id)
                .producer_logical_function_run_id(Some(producer.clone()))
                .function_param_pos(edge.output_pos().cloned())
                .triggered_on(execution.triggered_on)
                .triggered_by_id(execution.triggered_by_id)
                .group_id(*group_id)
                .build()
                .map_err(TdError::from)
        })
        .collect()
}

/// Build the `InitialMint` pointer-event row that pairs an LTDV identity row.
pub fn initial_mint_ltdv_pointer(
    ltdv: &LogicalTableDataVersionDB,
) -> Result<LogicalTableDataVersionPointerDB, TdError> {
    Ok(LogicalTableDataVersionPointerDB::builder()
        .project_id(ltdv.project_id.clone())
        .logical_table_data_version_id(ltdv.id.clone())
        .status(LogicalPointerStatus::Pending)
        .physical_table_data_version_id(None)
        .set_on(AtTime::try_from(ltdv.triggered_on)?)
        .set_by_id(ltdv.triggered_by_id)
        .reason(LogicalPointerReason::InitialMint)
        .build()?)
}

pub async fn build_initial_mint_ltdv_pointers(
    logical_table_data_versions: Ref<Vec<LogicalTableDataVersionDB>>,
) -> Result<Vec<LogicalTableDataVersionPointerDB>, TdError> {
    logical_table_data_versions
        .iter()
        .map(initial_mint_ltdv_pointer)
        .collect()
}

pub async fn resolve_execution_plan_versions(
    mut conn: MutRef<DynamicConnection<'static>>,
    execution: Ref<ExecutionDB>,
    execution_plan: Ref<ExecutionPlan>,
) -> Result<ResolvedExecutionPlan, TdError> {
    let triggered_on = execution.triggered_on;
    let at_time = AtTime::try_from(triggered_on)?;

    // Per-edge metadata, in plan-iteration order. `cache_key` lets us
    // share resolved LTDV vectors across edges that ask for the same
    // (table, versions) — common in execution plans where multiple
    // downstream functions depend on the same table at the same version.
    struct EdgeMeta {
        versions_original: Versions,
        cache_key: (TableId, Versions),
    }
    let mut edge_meta: Vec<EdgeMeta> = Vec::new();
    let mut ltdv_cache: HashMap<(TableId, Versions), Vec<Option<ActiveLogicalTableDataVersionDB>>> =
        HashMap::new();

    // Pass 1 — resolve LTDVs once per (table, versions). Pointer→TDV
    // translation is deferred to a single batched pass below.
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
            // Correct self HEAD references to the previous version to lookup.
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
                .triggered_on(&triggered_on)
                .build()?
                .resolve(&mut conn)
                .await?;
            ltdv_cache.insert(cache_key.clone(), ltdvs);
        }

        edge_meta.push(EdgeMeta {
            versions_original: versions_inner.clone(),
            cache_key,
        });
    }

    // Pass 2 — translate every LTDV referenced anywhere in the plan to
    // its currently-bound `PhysicalTableDataVersionDBWithFunction` in two batched
    // queries: latest pointer event per LTDV at `at_time`, then the TDV
    // rows for every distinct bound id.
    let all_ftdv_ids: Vec<LogicalTableDataVersionId> = ltdv_cache
        .values()
        .flat_map(|ltdvs| {
            ltdvs
                .iter()
                .filter_map(|f| f.as_ref().map(|f| f.id.clone()))
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let pointers: Vec<LogicalTableDataVersionPointerDB> = if all_ftdv_ids.is_empty() {
        Vec::new()
    } else {
        conn.versioned(&at_time)
            .fetch_all_chunked::<LogicalTableDataVersionPointerDB>(&all_ftdv_ids)
            .await?
    };
    let bound_tdv_id_by_ftdv: HashMap<LogicalTableDataVersionId, PhysicalTableDataVersionId> =
        pointers
            .into_iter()
            .filter_map(|p| {
                p.physical_table_data_version_id
                    .map(|tdv| (p.logical_table_data_version_id, tdv))
            })
            .collect();
    let all_tdv_ids: Vec<PhysicalTableDataVersionId> = bound_tdv_id_by_ftdv
        .values()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let tdvs: Vec<PhysicalTableDataVersionDBWithFunction> = if all_tdv_ids.is_empty() {
        Vec::new()
    } else {
        conn.fetch_all_chunked::<PhysicalTableDataVersionDBWithFunction, _>(&all_tdv_ids)
            .await?
    };
    let tdv_by_id: HashMap<PhysicalTableDataVersionId, PhysicalTableDataVersionDBWithFunction> =
        tdvs.into_iter().map(|t| (t.id.clone(), t)).collect();

    // Per-cache-key resolved TDV vector, computed once.
    let resolved_cache: HashMap<(TableId, Versions), Vec<Option<PhysicalTableDataVersionDBWithFunction>>> =
        ltdv_cache
            .into_iter()
            .map(|(k, ltdvs)| {
                let resolved: Vec<Option<PhysicalTableDataVersionDBWithFunction>> = ltdvs
                    .into_iter()
                    .map(|ftdv_opt| -> Option<PhysicalTableDataVersionDBWithFunction> {
                        let ltdv = ftdv_opt?;
                        let tdv_id = bound_tdv_id_by_ftdv.get(&ltdv.id)?;
                        Some(tdv_by_id.get(tdv_id)?.clone())
                    })
                    .collect();
                (k, resolved)
            })
            .collect();

    // Pass 3 — assemble per-edge `ResolvedVersion` rows in plan order.
    let mut edge_results: Vec<ResolvedVersion> = Vec::with_capacity(edge_meta.len());
    for meta in edge_meta {
        let resolved = resolved_cache
            .get(&meta.cache_key)
            .expect("cache populated for every edge in pass 1");
        let found = resolved
            .iter()
            .map(|tdv_opt| {
                tdv_opt
                    .as_ref()
                    .map(|tdv| ExecutionTableDataVersionReadBuilder::try_from(tdv)?.build())
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;

        let resolved_version = ResolvedVersion::builder()
            .inner(found)
            .original(meta.versions_original)
            .build()?;
        edge_results.push(resolved_version);
    }

    // Phase 2: Use map() with pre-computed results (pure, no DB access)
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

/// Build logical function-run-input rows for the execution.
///
/// One row per resolved-version per input edge of every triggered function:
/// - Edges without resolved versions (trigger edges, or dep edges that
///   resolved to nothing) emit a single row with
///   `requirement_version_pos = NO_VERSIONS`. Trigger edges additionally
///   carry NULL `requirement_input_idx` / `requirement_dependency_pos`.
/// - Dep edges with one or more resolved versions emit one row per
///   version, with `requirement_version_pos = SINGLE_VERSION` (single)
///   or the iteration index (multiple).
///
/// The `requirement_logical_table_data_version_id` is resolved per row
/// via the logic version resolver, which consults the in-plan LTDV
/// map first (LTDVs minted earlier in this same plan-build pass) and
/// then the committed LTDV timeline. Every concrete TDV has an LTDV by
/// construction, so resolution never falls off the end of the timeline.
pub async fn build_logical_function_run_inputs(
    mut conn: MutRef<DynamicConnection<'static>>,
    transaction_by: Ref<TransactionBy>,
    transaction_map: Ref<TransactionMap<TransactionBy>>,
    execution_plan: Ref<ExecutionPlan>,
    execution: Ref<ExecutionDB>,
    logical_function_runs: Ref<Vec<LogicalFunctionRunDB>>,
) -> Result<Vec<LogicalFunctionRunInputDB>, TdError> {
    let triggered_on = execution.triggered_on;

    // Consumer LFR lookup: function_id → LFR.id (see `build_function_runs`).
    let lfr_id_by_function: HashMap<_, _> = logical_function_runs
        .iter()
        .map(|lfr| (lfr.function_id, lfr.id.clone()))
        .collect();

    // Resolves are deterministic for a given `(function_id, table_id,
    // versions)` triple at this `triggered_on`, so dedupe across edges
    // that ask for the same upstream slice — common with multi-consumer
    // plans.
    let mut resolve_cache: HashMap<
        (FunctionId, TableId, Versions),
        Vec<Option<ActiveLogicalTableDataVersionDB>>,
    > = HashMap::new();

    let mut rows = Vec::new();
    for (function, table, edge) in execution_plan.function_table_input_versions() {
        let Some(consumer_lfr_id) = lfr_id_by_function.get(&function.function_id) else {
            return Err(api_error!(
                ApiError::InternalError,
                "Consumer LFR not found for function_id {} while building logical inputs",
                function.function_id
            ));
        };
        let transaction = transaction_map.get(&transaction_by.key(function)?)?;

        // `input_idx` was assigned onto the dependency edge at plan-build
        // (`assign_requirement_input_idx`); trigger edges carry neither.
        let dependency_pos = edge.dependency_pos().cloned();
        let requirement_input_idx = match edge {
            GraphEdge::Dependency { dependency, .. } => Some(dependency.input_idx),
            _ => None,
        };

        // Self-dependency: read the *previous* version, not the current one.
        // Shift the relative reference by -1 so HEAD becomes HEAD~1 etc.
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
                .triggered_on(&triggered_on)
                .build()?
                .resolve(&mut conn)
                .await?;
            resolve_cache.insert(cache_key, r.clone());
            r
        };

        // Emit one row per dep slot, even when the slot didn't resolve to
        // an LTDV (NULL `requirement_logical_table_data_version_id`). The
        // worker yaml at dispatch falls back to `DependencyDB` for the
        // static dep info on those rows. Empty-Vec emits a single
        // `NO_VERSIONS` placeholder.
        if resolved.is_empty() {
            rows.push(
                LogicalFunctionRunInputDB::builder()
                    .collection_id(function.collection_id)
                    .execution_id(transaction.execution_id.clone())
                    .transaction_id(transaction.id.clone())
                    .logical_function_run_id(consumer_lfr_id.clone())
                    .requirement_logical_table_data_version_id(None)
                    .requirement_input_idx(requirement_input_idx)
                    .requirement_dependency_pos(dependency_pos)
                    .requirement_version_pos(VersionPos::NO_VERSIONS)
                    .build()?,
            );
            continue;
        }

        let multiple = lookup_versions.is_multiple();
        for (slot, ftdv_opt) in resolved.iter().enumerate() {
            let version_pos = if multiple {
                VersionPos::try_from(slot as i32)?
            } else {
                VersionPos::SINGLE_VERSION
            };
            rows.push(
                LogicalFunctionRunInputDB::builder()
                    .collection_id(function.collection_id)
                    .execution_id(transaction.execution_id.clone())
                    .transaction_id(transaction.id.clone())
                    .logical_function_run_id(consumer_lfr_id.clone())
                    .requirement_logical_table_data_version_id(
                        ftdv_opt.as_ref().map(|f| f.id.clone()),
                    )
                    .requirement_input_idx(requirement_input_idx)
                    .requirement_dependency_pos(dependency_pos)
                    .requirement_version_pos(version_pos)
                    .build()?,
            );
        }
    }

    Ok(rows)
}

/// Walks `logical_function_run_inputs → LTDV → latest pointer → TDV →
/// upstream FR` to find consumers whose upstream is Failed/OnHold, and
/// flips them to OnHold. The latest-pointer subquery is bounded by the
/// plan's `at_time` for temporal consistency.
#[instrument(
    name = "execution.layer.update_initial_function_run_status",
    skip_all,
    fields(execution_id = %execution.id),
    level = "debug",
    err(Debug)
)]
pub async fn update_initial_function_run_status(
    mut conn: MutRef<DynamicConnection<'static>>,
    execution: Ref<ExecutionDB>,
    at_time: Ref<AtTime>,
) -> Result<(), TdError> {
    let lfr_ids = conn.lfr().blocked_ids(&execution.id, &at_time).await?;

    debug!(
        execution_id = %execution.id,
        on_hold_count = lfr_ids.len(),
        "execution.initial_on_hold: {} LFR(s) start OnHold",
        lfr_ids.len()
    );

    if !lfr_ids.is_empty() {
        let update = UpdateLogicalFunctionRunDB::builder()
            .status(LogicalFunctionRunStatus::OnHold)
            .build()?;
        let _ = conn
            .update_chunked::<_, LogicalFunctionRunDB, _>(&update, &lfr_ids)
            .await?;
    }

    Ok(())
}

pub async fn build_response(
    transaction_by: Ref<TransactionBy>,
    transaction_map: Ref<TransactionMap<TransactionBy>>,
    execution: Ref<ExecutionDB>,
    execution_plan: Ref<ResolvedExecutionPlan>,
) -> Result<ExecutionResponse, TdError> {
    let dot = Dot::try_from(execution_plan.dot().to_string())?;

    // Transactions info
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
                                collected_info.push((t.id.clone(), t.clone()));
                                Ok::<_, TdError>(t.id.clone())
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
            (
                f.function_version_id.clone(),
                t.table_version_id,
                resolved_version,
            )
        })
        .sorted()
        .collect();

    // Functions info
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
