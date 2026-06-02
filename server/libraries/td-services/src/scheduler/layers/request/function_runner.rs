//
// Copyright 2025 Tabs Data Inc.
//

use super::shared::{build_external_location, loopback_post_callback};
use crate::function::layers::topology::RunnableVersion;
use crate::logical::layers::dispatch_mint::MintedDispatch;
use crate::logical::layers::index_lfrs_by_exec_fn;
use itertools::Itertools;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use td_common::server::{
    MessageAction, OrchestratorMessageQueueHandle, RequestMessagePayload, WorkerClass, WorkerName,
};
use td_database::dynamic::DynamicConnection;
use td_error::{ApiError, TdError, api_error};
use td_objects::Buildable;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalTableDataVersionPointerDB,
};
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, PhysicalFunctionRunToExecute};
use td_objects::dxo::physical_table_data_version::{
    PhysicalTableDataVersionDB, PhysicalTableDataVersionDBWithNames,
};
use td_objects::dxo::table::TableDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::dxo::yaml::WorkerInput;
use td_objects::dxo::yaml::function_runner::v2::{
    FunctionInfo, FunctionInput, InputTable, InputTableVersion, OutputTable, OutputTableVersion,
};
use td_objects::rest_urls::internal::UPDATE_FUNCTION_RUN_WORKER;
use td_objects::types::addresses::InternalServerAddresses;
use td_objects::types::basic::{
    AtTime, CollectionId, DependencyPos, Enabled, ExecutionId, HasData, LogicalFunctionRunId,
    LogicalPointerStatus, LogicalTableDataVersionId, PhysicalFunctionRunId,
    PhysicalTableDataVersionId, TableId, TriggeredOn, WorkerId, WorkerMessageStatus, WorkerStatus,
    WorkerType,
};
use td_queries::{
    ChunkedQueries, DependenciesEntry, LfrEntry, LtdvEntry, PfrEntry, VersionedEntry,
};
use td_storage::Storage;
use td_storage::location::StorageLocation;
use td_tower::extractors::{MutRef, Ref};
use tracing::{debug, instrument};

/// LFR-rooted readiness fetch for the no-pre-mint dispatch path.
#[instrument(
    name = "scheduler.fetch_lfr_to_dispatch",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn fetch_lfr_to_dispatch(
    mut conn: MutRef<DynamicConnection<'static>>,
    at: Ref<AtTime>,
) -> Result<Vec<LogicalFunctionRunDB>, TdError> {
    let result = conn.lfr().to_execute(&at).await?;
    if result.is_empty() {
        debug!("scheduler.dispatch_tick: 0 LFRs ready");
    } else {
        debug!(
            ready_count = result.len(),
            "scheduler.dispatch_tick: {} LFR(s) ready to dispatch",
            result.len()
        );
    }
    Ok(result)
}

/// Replay-side readiness fetch: PFRs in `Requested` status with no
/// `WorkerDB` row yet — what the replay endpoint left behind for the
/// scheduler to pick up. Returns the PFRs directly; the next layer
/// resolves LFRs + output PTDVs to assemble `MintedDispatch`.
#[instrument(
    name = "scheduler.fetch_pfrs_to_dispatch",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn fetch_pfrs_to_dispatch(
    mut conn: MutRef<DynamicConnection<'static>>,
) -> Result<Vec<PhysicalFunctionRunDB>, TdError> {
    let result = conn.pfr().to_dispatch().await?;
    if result.is_empty() {
        debug!("scheduler.dispatch_tick: 0 replay PFRs ready");
    } else {
        debug!(
            ready_count = result.len(),
            "scheduler.dispatch_tick: {} replay PFR(s) ready",
            result.len()
        );
    }
    Ok(result)
}

/// Materialize already-minted replay PFRs into `MintedDispatch` shape.
/// No inserts — the FR and its output TDVs were created by the replay
/// endpoint; this layer just gathers LFR + output PTDVs and zips them.
#[instrument(
    name = "scheduler.materialize_replay_dispatch",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn materialize_replay_dispatch(
    mut conn: MutRef<DynamicConnection<'static>>,
    pfrs: Ref<Vec<PhysicalFunctionRunDB>>,
) -> Result<Vec<MintedDispatch>, TdError> {
    if pfrs.is_empty() {
        return Ok(Vec::new());
    }

    let pfr_ids: Vec<PhysicalFunctionRunId> = pfrs.iter().map(|p| p.id.clone()).collect();

    // LFR per PFR via (execution_id, function_id).
    let exec_ids: Vec<ExecutionId> = pfrs
        .iter()
        .map(|p| p.execution_id.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let all_lfrs: Vec<LogicalFunctionRunDB> = conn
        .fetch_all_chunked::<LogicalFunctionRunDB, ExecutionId>(&exec_ids)
        .await?;
    let lfr_by_exec_fn = index_lfrs_by_exec_fn(all_lfrs);

    // Output TDVs for these PFRs.
    let tdvs: Vec<PhysicalTableDataVersionDB> = conn
        .fetch_all_chunked::<PhysicalTableDataVersionDB, PhysicalFunctionRunId>(&pfr_ids)
        .await?;
    let mut tdvs_by_fr: HashMap<PhysicalFunctionRunId, Vec<PhysicalTableDataVersionDB>> =
        HashMap::new();
    for t in tdvs {
        tdvs_by_fr
            .entry(t.physical_function_run_id.clone())
            .or_default()
            .push(t);
    }

    let mut output = Vec::with_capacity(pfrs.len());
    for pfr in pfrs.iter() {
        let Some(lfr) = lfr_by_exec_fn
            .get(&pfr.execution_id)
            .and_then(|inner| inner.get(&pfr.function_id))
        else {
            return Err(api_error!(
                ApiError::InternalError,
                "replay-dispatch: no LFR for PFR {} via (execution_id={}, function_id={})",
                pfr.id,
                pfr.execution_id,
                pfr.function_id,
            ));
        };
        // Reconstruct the RunnableVersion from the PFR row.
        let runnable = RunnableVersion {
            function_version_id: pfr.function_version_id.clone(),
            function_version_patch_id: pfr.function_version_patch_id.clone(),
        };
        let tdvs = tdvs_by_fr.remove(&pfr.id).unwrap_or_default();
        output.push(MintedDispatch {
            lfr: lfr.clone(),
            fr: pfr.clone(),
            runnable,
            tdvs,
        });
    }

    debug!(
        replay_count = output.len(),
        "scheduler.dispatch: materialized replay PFRs for dispatch"
    );
    Ok(output)
}

/// Project the minted FRs into the denormalized `PhysicalFunctionRunToExecute`
/// shape the worker-yaml builder consumes. One round-trip via the shared
/// dispatch projection (patch-aware COALESCE for patch fvids).
#[instrument(
    name = "scheduler.enrich_minted_dispatch",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn enrich_minted_dispatch(
    mut conn: MutRef<DynamicConnection<'static>>,
    minted: Ref<Vec<MintedDispatch>>,
) -> Result<Vec<PhysicalFunctionRunToExecute>, TdError> {
    if minted.is_empty() {
        return Ok(Vec::new());
    }
    let fr_ids: Vec<PhysicalFunctionRunId> = minted.iter().map(|m| m.fr.id.clone()).collect();
    let result = conn.lfr().for_dispatch(&fr_ids).await?;
    Ok(result)
}

/// Extract the LFR ids from the minted dispatch set so the requested
/// status flip can update_chunked across them.
pub async fn lfr_ids_from_minted_dispatch(
    minted: Ref<Vec<MintedDispatch>>,
) -> Result<Vec<LogicalFunctionRunId>, TdError> {
    Ok(minted.iter().map(|m| m.lfr.id.clone()).collect())
}

/// Extract the PFR ids from the minted dispatch set so the
/// `Requested → Dispatched` status flip can update_chunked across them.
/// Used by both initial and replay pipelines after WorkerDB insert.
pub async fn pfr_ids_from_minted_dispatch(
    minted: Ref<Vec<MintedDispatch>>,
) -> Result<Vec<PhysicalFunctionRunId>, TdError> {
    Ok(minted.iter().map(|m| m.fr.id.clone()).collect())
}

#[instrument(
    name = "scheduler.create_function_runner_workers",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn create_locked_function_runner_workers_start_request(
    message_queue: Ref<OrchestratorMessageQueueHandle>,
    storage: Ref<Storage>,
    server_addresses: Ref<InternalServerAddresses>,
    mut conn: MutRef<DynamicConnection<'static>>,
    at: Ref<AtTime>,
    physical_function_runs: Ref<Vec<PhysicalFunctionRunToExecute>>,
) -> Result<Vec<WorkerDB>, TdError> {
    // Pre-fetch tick-wide batches the per-FR loop reads from: outputs,
    // LFR inputs, latest LTDV pointer events at `at`, and bound TDVs.
    let fr_ids: Vec<PhysicalFunctionRunId> = physical_function_runs
        .iter()
        .map(|f| f.id.clone())
        .collect();
    // Derive each FR's LFR via (execution_id, function_id). Fetch all
    // LFRs for the tick's executions in one chunked query and index
    // them by (execution_id, function_id).
    let exec_ids: Vec<ExecutionId> = physical_function_runs
        .iter()
        .map(|f| f.execution_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let all_lfrs: Vec<LogicalFunctionRunDB> = if exec_ids.is_empty() {
        Vec::new()
    } else {
        conn.fetch_all_chunked::<LogicalFunctionRunDB, ExecutionId>(&exec_ids)
            .await?
    };
    let lfr_by_fr_key = index_lfrs_by_exec_fn(all_lfrs);
    let lfr_id_for = |fr: &PhysicalFunctionRunToExecute| -> Option<LogicalFunctionRunId> {
        lfr_by_fr_key
            .get(&fr.execution_id)
            .and_then(|m| m.get(&fr.function_id))
            .map(|l| l.id.clone())
    };
    let lfr_ids: Vec<LogicalFunctionRunId> = physical_function_runs
        .iter()
        .filter_map(lfr_id_for)
        .collect();

    let all_output_tables: Vec<PhysicalTableDataVersionDBWithNames> = if fr_ids.is_empty() {
        Vec::new()
    } else {
        conn.fetch_all_chunked::<PhysicalTableDataVersionDBWithNames, PhysicalFunctionRunId>(
            &fr_ids,
        )
        .await?
    };
    let mut output_tables_by_fr: HashMap<
        PhysicalFunctionRunId,
        Vec<PhysicalTableDataVersionDBWithNames>,
    > = all_output_tables
        .into_iter()
        .into_group_map_by(|t| t.physical_function_run_id.clone());

    let all_lfr_inputs: Vec<LogicalFunctionRunInputDB> = if lfr_ids.is_empty() {
        Vec::new()
    } else {
        conn.fetch_all_chunked::<LogicalFunctionRunInputDB, LogicalFunctionRunId>(&lfr_ids)
            .await?
    };
    let input_ftdv_ids: Vec<LogicalTableDataVersionId> = all_lfr_inputs
        .iter()
        .filter_map(|i| i.requirement_logical_table_data_version_id.clone())
        .collect();
    let lfr_inputs_by_lfr: HashMap<LogicalFunctionRunId, Vec<LogicalFunctionRunInputDB>> =
        all_lfr_inputs
            .into_iter()
            .into_group_map_by(|inp| inp.logical_function_run_id.clone());

    let mut bound_tdv_id_by_ftdv: HashMap<
        LogicalTableDataVersionId,
        Option<PhysicalTableDataVersionId>,
    > = input_ftdv_ids.iter().map(|id| (id.clone(), None)).collect();
    if !input_ftdv_ids.is_empty() {
        let latest_pointers: Vec<LogicalTableDataVersionPointerDB> = conn
            .versioned(at.deref())
            .fetch_all_chunked::<LogicalTableDataVersionPointerDB>(&input_ftdv_ids)
            .await?;
        for p in latest_pointers {
            // Only Bound events carry the canonical concrete; Pending /
            // Voided events have a NULL concrete by construction, but
            // filter explicitly so the contract reads from the code.
            if !matches!(p.status, LogicalPointerStatus::Bound) {
                continue;
            }
            if let Some(slot) = bound_tdv_id_by_ftdv.get_mut(&p.logical_table_data_version_id) {
                *slot = p.physical_table_data_version_id;
            }
        }
    }
    let bound_tdv_ids: Vec<PhysicalTableDataVersionId> = bound_tdv_id_by_ftdv
        .values()
        .filter_map(|slot| slot.clone())
        .collect();
    let bound_tdv_by_id: HashMap<PhysicalTableDataVersionId, PhysicalTableDataVersionDBWithNames> =
        if bound_tdv_ids.is_empty() {
            HashMap::new()
        } else {
            conn.fetch_all_chunked::<PhysicalTableDataVersionDBWithNames, PhysicalTableDataVersionId>(&bound_tdv_ids)
            .await?
            .into_iter()
            .map(|t| (t.id.clone(), t))
            .collect()
        };

    let mut results = Vec::new();
    for f in physical_function_runs.iter() {
        // Worker id
        let worker_id = WorkerId::default();

        let server_address = server_addresses.first();
        let endpoint = UPDATE_FUNCTION_RUN_WORKER
            .replace("{function_run_id}", &f.id.to_string())
            .replace("{worker_id}", &worker_id.to_string());
        let callback = loopback_post_callback(server_address, &endpoint)?;

        // Build states map
        let mut get_states = HashSet::new();

        // Build storage location
        let storage_location = StorageLocation::from(&f.storage_version);

        // Bundle location
        let (path, _) = storage_location
            .builder(&f.data_location)
            .collection(&f.collection_id)
            .function(&f.bundle_id)
            .build()?;
        let (bundle_location, env_prefix) = build_external_location(&storage, &path)?;
        get_states.insert(env_prefix);

        // Staging location
        let (path, _) = storage_location
            .builder(&f.data_location)
            .collection(&f.collection_id)
            .stage(&f.function_id)
            .build()?;
        let (stage_location, env_prefix) = build_external_location(&storage, &path)?;
        get_states.insert(env_prefix);

        // Patch if resolved, otherwise head — drives the worker's data uri
        // and the yaml `function_version_id` so reads/writes go under the
        // patched identity.
        let effective_fvid = f
            .function_version_patch_id
            .as_ref()
            .unwrap_or(&f.function_version_id);

        // Function run content location
        let (path, _) = storage_location
            .builder(&f.data_location)
            .collection(&f.collection_id)
            .transaction(&f.transaction_id)
            .function_version(effective_fvid)
            .build()?;
        let (function_run_location, env_prefix) = build_external_location(&storage, &path)?;
        get_states.insert(env_prefix);

        // Build message info
        let info = FunctionInfo::builder()
            .collection_id(f.collection_id)
            .collection(f.collection.clone())
            .function_version_id(effective_fvid)
            .function_defined_on(f.defined_on)
            .function(f.name.clone())
            .function_run_id(f.id.clone())
            .function_bundle(bundle_location)
            .function_stage(stage_location)
            .triggered_on(f.triggered_on)
            .transaction_id(&f.transaction_id)
            .execution_id(&f.execution_id)
            .execution_name(f.execution.clone())
            .function_data(function_run_location)
            .build()?;

        // Build input tables from the logical layer.
        let empty_inputs = Vec::new();
        let lfr_id = lfr_id_for(f);
        let lfr_inputs: &Vec<LogicalFunctionRunInputDB> = lfr_id
            .as_ref()
            .and_then(|id| lfr_inputs_by_lfr.get(id))
            .unwrap_or(&empty_inputs);

        // Dep-fallback is used only when an input slot has no bound TDV;
        // skip the fetch until the first miss. On the first miss we
        // resolve the function's dependencies once, then batch-fetch
        // every dep's `TableDB` and `CollectionDB` in two chunked
        // queries — collapsing a per-slot pair of round-trips into a
        // per-FR pair.
        struct FallbackCache {
            deps: HashMap<DependencyPos, DependencyDB>,
            tables: HashMap<TableId, TableDB>,
            colls: HashMap<CollectionId, CollectionDB>,
        }
        let mut fallback_cache: Option<FallbackCache> = None;

        // Per-FR cache for the "chase the latest with-data row" lookup
        // fired when a bound TDV has `has_data = FALSE`. Two slots
        // pointing at the same upstream `(table_id, triggered_on)` would
        // otherwise issue the same `fetch_latest_before` twice.
        let mut latest_with_data_cache: HashMap<
            (TableId, TriggeredOn),
            Option<PhysicalTableDataVersionDBWithNames>,
        > = HashMap::new();

        let mut input_tables_map: HashMap<i32, Vec<InputTableVersion>> = HashMap::new();
        for lfri in lfr_inputs.iter() {
            // Skip NO_VERSIONS placeholders (NULL idx/dep_pos).
            let (Some(input_idx), Some(dependency_pos)) = (
                lfri.requirement_input_idx.as_ref(),
                lfri.requirement_dependency_pos.as_ref(),
            ) else {
                continue;
            };

            // Resolve LTDV → bound TDV from the pre-fetched maps.
            let bound_tdv: Option<&PhysicalTableDataVersionDBWithNames> = lfri
                .requirement_logical_table_data_version_id
                .as_ref()
                .and_then(|ltdv_id| bound_tdv_id_by_ftdv.get(ltdv_id))
                .and_then(|slot| slot.as_ref())
                .and_then(|tdv_id| bound_tdv_by_id.get(tdv_id));

            // Static dep info: prefer the bound TDV's identity columns;
            // fall back to the dep declaration + the table's current
            // version when no TDV is bound.
            let (name, collection_id, collection, table_id, table_version_id) = match bound_tdv {
                Some(tdv) => (
                    tdv.name.clone(),
                    tdv.collection_id,
                    tdv.collection.clone(),
                    tdv.table_id,
                    tdv.table_version_id,
                ),
                None => {
                    if fallback_cache.is_none() {
                        let deps: HashMap<DependencyPos, DependencyDB> = conn
                            .dependencies()
                            .active_for_function_at(&f.function_id, &f.defined_on)
                            .await?
                            .into_iter()
                            .map(|d| (d.dep_pos, d))
                            .collect();
                        let table_ids: Vec<TableId> = deps
                            .values()
                            .map(|d| d.table_id)
                            .collect::<HashSet<_>>()
                            .into_iter()
                            .collect();
                        let coll_ids: Vec<CollectionId> = deps
                            .values()
                            .map(|d| d.table_collection_id)
                            .collect::<HashSet<_>>()
                            .into_iter()
                            .collect();
                        let triggered_at = AtTime::try_from(f.triggered_on)?;
                        let tables: HashMap<TableId, TableDB> = if table_ids.is_empty() {
                            HashMap::new()
                        } else {
                            conn.versioned(&triggered_at)
                                .fetch_all_chunked::<TableDB>(&table_ids)
                                .await?
                                .into_iter()
                                .map(|t| (t.table_id, t))
                                .collect()
                        };
                        let colls: HashMap<CollectionId, CollectionDB> = if coll_ids.is_empty() {
                            HashMap::new()
                        } else {
                            conn.fetch_all_chunked::<CollectionDB, CollectionId>(&coll_ids)
                                .await?
                                .into_iter()
                                .map(|c| (c.id, c))
                                .collect()
                        };
                        fallback_cache = Some(FallbackCache {
                            deps,
                            tables,
                            colls,
                        });
                    }
                    let cache = fallback_cache.as_ref().unwrap();
                    let dep = cache.deps.get(dependency_pos).ok_or_else(|| {
                        api_error!(
                            ApiError::InternalError,
                            "LFR_input dep_pos {dependency_pos} not found in function_version's deps"
                        )
                    })?;
                    let table = cache.tables.get(&dep.table_id).ok_or_else(|| {
                        api_error!(
                            ApiError::InternalError,
                            "TableDB not found for dep table_id {} during fallback",
                            dep.table_id
                        )
                    })?;
                    let coll = cache.colls.get(&dep.table_collection_id).ok_or_else(|| {
                        api_error!(
                            ApiError::InternalError,
                            "CollectionDB not found for dep table_collection_id {} during fallback",
                            dep.table_collection_id
                        )
                    })?;
                    (
                        table.name.clone(),
                        dep.table_collection_id,
                        coll.name.clone(),
                        dep.table_id,
                        table.id,
                    )
                }
            };

            // Build location from the bound TDV (chasing the latest
            // with-data row when the bound TDV has `has_data = FALSE`).
            let (found_data_version, location) = match bound_tdv {
                None => (None, None),
                Some(found_data_version) => {
                    let data_version_with_data =
                        if matches!(found_data_version.has_data, Some(HasData::TRUE)) {
                            Some(Cow::Borrowed(found_data_version))
                        } else {
                            let key =
                                (found_data_version.table_id, found_data_version.triggered_on);
                            let res = if let Some(cached) = latest_with_data_cache.get(&key) {
                                cached.clone()
                            } else {
                                // Pointer-walk through LTDV history for this
                                // table at or before key.1 (triggered_on),
                                // returning the latest canonically-bound TDV
                                // with data — the same row a downstream reader
                                // at that time would actually see.
                                let r = conn
                                    .ltdv()
                                    .latest_bound_tdv_with_data_for_table(&key.0, &key.1, false)
                                    .await?;
                                latest_with_data_cache.insert(key, r.clone());
                                r
                            };
                            res.map(Cow::Owned)
                        };

                    if let Some(data_version_with_data) = data_version_with_data.as_ref() {
                        let (path, _) = storage_location
                            .builder(&data_version_with_data.data_location)
                            .collection(&data_version_with_data.collection_id)
                            .data(&data_version_with_data.id)
                            .table(
                                &data_version_with_data.table_id,
                                &data_version_with_data.table_version_id,
                            )
                            .build()?;
                        let (location, env_prefix) = build_external_location(&storage, &path)?;
                        get_states.insert(env_prefix);
                        (Some(found_data_version.clone()), Some(location))
                    } else {
                        (Some(found_data_version.clone()), None)
                    }
                }
            };

            let input_table = InputTableVersion::builder()
                .name(name)
                .collection_id(collection_id)
                .collection(collection)
                .table_id(table_id)
                .table_version_id(table_version_id)
                .execution_id(found_data_version.as_ref().map(|v| v.execution_id.clone()))
                .transaction_id(
                    found_data_version
                        .as_ref()
                        .map(|v| v.transaction_id.clone()),
                )
                .function_run_id(
                    found_data_version
                        .as_ref()
                        .map(|v| v.physical_function_run_id.clone()),
                )
                .triggered_on(
                    found_data_version
                        .as_ref()
                        .map(|v| v.triggered_on.try_into())
                        .transpose()?,
                )
                .table_data_version_id(found_data_version.as_ref().map(|v| v.id.clone()))
                .location(location)
                .input_idx(input_idx)
                .table_pos(dependency_pos)
                .version_pos(lfri.requirement_version_pos)
                .build()?;

            input_tables_map
                .entry(**dependency_pos)
                .or_default()
                .push(input_table);
        }

        // Build output tables — read from the tick-wide pre-fetch.
        let tables = output_tables_by_fr.remove(&f.id).unwrap_or_default();

        let mut output_tables_map = HashMap::new();
        for table in tables.iter() {
            let (path, _) = StorageLocation::current()
                .builder(&f.data_location)
                .collection(&table.collection_id)
                .data(&table.id)
                .table(&table.table_id, &table.table_version_id)
                .build()?;
            let (location, env_prefix) = build_external_location(&storage, &path)?;
            get_states.insert(env_prefix);

            let input_table = OutputTableVersion::builder()
                .name(table.name.clone())
                .collection_id(table.collection_id)
                .collection(table.collection.clone())
                .table_id(table.table_id)
                .table_version_id(table.table_version_id)
                .table_data_version_id(&table.id)
                .location(location)
                .table_pos(table.function_param_pos)
                .build()?;

            output_tables_map.insert(*table.function_param_pos, input_table);
        }

        // Build system/user tables
        fn partition_and_sort<T, U>(
            map: HashMap<i32, T>,
            transform: impl Fn(T) -> U,
        ) -> (Vec<U>, Vec<U>) {
            let mut system_tables = Vec::new();
            let mut user_tables = Vec::new();

            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by_key(|(i, _)| *i);

            for (i, value) in entries {
                let transformed = transform(value);
                if i < 0 {
                    system_tables.push(transformed);
                } else {
                    user_tables.push(transformed);
                }
            }

            (system_tables, user_tables)
        }
        let (system_input, input) = partition_and_sort(input_tables_map, |mut tables| {
            tables.sort_by_key(|t| *t.version_pos);
            InputTable::new(tables)
        });
        let (system_output, output) = partition_and_sort(output_tables_map, OutputTable::Table);

        // Build message context
        let function_input_v2 = FunctionInput::builder()
            .info(info)
            .system_input(system_input)
            .input(input)
            .system_output(system_output)
            .output(output)
            .build()?;
        let function_input = WorkerInput::FunctionV2(Box::new(function_input_v2));

        // Build message payload
        // TODO ADD get_states to states
        let message_payload = RequestMessagePayload::<serde_yaml::Value> {
            class: WorkerClass::Ephemeral,
            worker: WorkerName::Function.as_ref().to_string(),
            run_id: worker_id.to_string(),
            action: MessageAction::Start,
            arguments: Vec::new(),
            callback: Some(callback),
            context: Some(serde_yaml::to_value(&function_input).map_err(|e| {
                api_error!(
                    ApiError::InternalError,
                    "Failed to serialize function input: {e}"
                )
            })?),
        };

        // Create worker message
        let message = WorkerDB {
            id: worker_id,
            collection_id: f.collection_id,
            execution_id: Some(f.execution_id.clone()),
            transaction_id: Some(f.transaction_id.clone()),
            physical_function_run_id: Some(f.id.clone()),
            function_version_id: f.function_version_id.clone(),
            message_status: WorkerMessageStatus::Locked,
            started_on: None,
            ended_on: None,
            status: WorkerStatus::Requested,
            worker_type: WorkerType::FunctionRunner,
            worker_enabled: Enabled::TRUE,
            worker_config: None,
        };

        // Add it to the queue
        message_queue
            .put(message.id.to_string(), message_payload)
            .await?;
        results.push(message);
    }

    if !results.is_empty() {
        debug!(
            dispatched_count = results.len(),
            "scheduler.request_tick: dispatched {} worker(s)",
            results.len()
        );
    }
    Ok(results)
}
