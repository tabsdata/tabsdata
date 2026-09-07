//
// Copyright 2026 Tabsdata Inc.
//

//! Input-version resolution: the logical→physical step that maps each
//! input slot's bound `LogicalTableDataVersionId` to a concrete PTDV +
//! storage location, chasing the latest with-data row when needed.

use super::{build_external_location, partition_and_sort};
use crate::logical::index_lfrs_by_exec_fn;
use itertools::Itertools;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalTableDataVersionPointerDB,
};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunToExecute;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBWithNames;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::yaml::function::{InputTable, InputTableVersion};
use td_objects::types::basic::{
    AtTime, CollectionId, DependencyPos, ExecutionId, FunctionId, HasData, LogicalFunctionRunId,
    LogicalPointerStatus, LogicalTableDataVersionId, PhysicalFunctionRunId,
    PhysicalTableDataVersionId, TableId, TriggeredOn,
};
use td_queries::{ChunkedQueries, DependenciesEntry, LtdvEntry, VersionedEntry};
use td_storage::Storage;
use td_storage::location::StorageLocation;

/// Tick-wide batches the per-FR input/output build reads from: the LFR
/// index (for `(execution_id, function_id) → LFR`), output PTDVs, LFR
/// input rows, the latest LTDV pointer events at `at`, and the bound TDVs.
///
/// Resolved once per dispatch (a batch for the supervisor request stage,
/// a batch-of-one for the resolve endpoint) so the per-FR loop is pure
/// map lookups except for the cold dep-fallback / has-data-chase paths.
pub(crate) struct DispatchPrefetch {
    lfr_by_fr_key: HashMap<ExecutionId, HashMap<FunctionId, LogicalFunctionRunDB>>,
    output_tables_by_fr: HashMap<PhysicalFunctionRunId, Vec<PhysicalTableDataVersionDBWithNames>>,
    lfr_inputs_by_lfr: HashMap<LogicalFunctionRunId, Vec<LogicalFunctionRunInputDB>>,
    bound_tdv_id_by_ftdv: HashMap<LogicalTableDataVersionId, Option<PhysicalTableDataVersionId>>,
    bound_tdv_by_id: HashMap<PhysicalTableDataVersionId, PhysicalTableDataVersionDBWithNames>,
}

impl DispatchPrefetch {
    fn lfr_id_for(&self, f: &PhysicalFunctionRunToExecute) -> Option<LogicalFunctionRunId> {
        self.lfr_by_fr_key
            .get(&f.base.base.execution_id)
            .and_then(|m| m.get(&f.base.base.function_id))
            .map(|l| l.id.clone())
    }

    /// The minted output PTDVs for one FR (empty if none).
    pub(crate) fn output_tables_for(
        &self,
        fr_id: &PhysicalFunctionRunId,
    ) -> &[PhysicalTableDataVersionDBWithNames] {
        self.output_tables_by_fr
            .get(fr_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}

/// Pre-fetch the tick-wide batches the per-FR builder reads from.
pub(crate) async fn prefetch_dispatch(
    conn: &mut DomainConnection<Apiserver>,
    physical_function_runs: &[PhysicalFunctionRunToExecute],
    at: &AtTime,
) -> Result<DispatchPrefetch, TdError> {
    let fr_ids: Vec<PhysicalFunctionRunId> = physical_function_runs
        .iter()
        .map(|f| f.base.base.id.clone())
        .collect();
    // Derive each FR's LFR via (execution_id, function_id). Fetch all
    // LFRs for the tick's executions in one chunked query and index
    // them by (execution_id, function_id).
    let exec_ids: Vec<ExecutionId> = physical_function_runs
        .iter()
        .map(|f| f.base.base.execution_id.clone())
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
    let lfr_ids: Vec<LogicalFunctionRunId> = physical_function_runs
        .iter()
        .filter_map(|f| {
            lfr_by_fr_key
                .get(&f.base.base.execution_id)
                .and_then(|m| m.get(&f.base.base.function_id))
                .map(|l| l.id.clone())
        })
        .collect();

    let all_output_tables: Vec<PhysicalTableDataVersionDBWithNames> = if fr_ids.is_empty() {
        Vec::new()
    } else {
        conn.fetch_all_chunked::<PhysicalTableDataVersionDBWithNames, PhysicalFunctionRunId>(
            &fr_ids,
        )
        .await?
    };
    let output_tables_by_fr: HashMap<
        PhysicalFunctionRunId,
        Vec<PhysicalTableDataVersionDBWithNames>,
    > = all_output_tables
        .into_iter()
        .into_group_map_by(|t| t.base.base.physical_function_run_id.clone());

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
            .versioned(at)
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
            .map(|t| (t.base.base.id.clone(), t))
            .collect()
        };

    Ok(DispatchPrefetch {
        lfr_by_fr_key,
        output_tables_by_fr,
        lfr_inputs_by_lfr,
        bound_tdv_id_by_ftdv,
        bound_tdv_by_id,
    })
}

/// Resolve one FR's input tables to physical versions + locations,
/// partitioned into (system, user) by dependency position.
pub(crate) async fn resolve_input_tables(
    conn: &mut DomainConnection<Apiserver>,
    storage: &Storage,
    f: &PhysicalFunctionRunToExecute,
    prefetch: &DispatchPrefetch,
) -> Result<(Vec<InputTable>, Vec<InputTable>), TdError> {
    let storage_location = StorageLocation::from(&f.base.storage_version);

    let empty_inputs = Vec::new();
    let lfr_id = prefetch.lfr_id_for(f);
    let lfr_inputs: &Vec<LogicalFunctionRunInputDB> = lfr_id
        .as_ref()
        .and_then(|id| prefetch.lfr_inputs_by_lfr.get(id))
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

        let bound_tdv: Option<&PhysicalTableDataVersionDBWithNames> = lfri
            .requirement_logical_table_data_version_id
            .as_ref()
            .and_then(|ltdv_id| prefetch.bound_tdv_id_by_ftdv.get(ltdv_id))
            .and_then(|slot| slot.as_ref())
            .and_then(|tdv_id| prefetch.bound_tdv_by_id.get(tdv_id));

        // Static dep info: prefer the bound TDV's identity columns;
        // fall back to the dep declaration + the table's current
        // version when no TDV is bound.
        let (name, collection_id, collection, table_id, table_version_id) = match bound_tdv {
            Some(tdv) => (
                tdv.base.base.name.clone(),
                tdv.base.base.collection_id,
                tdv.collection.clone(),
                tdv.base.base.table_id,
                tdv.base.base.table_version_id,
            ),
            None => {
                if fallback_cache.is_none() {
                    let deps: HashMap<DependencyPos, DependencyDB> = conn
                        .dependencies()
                        .active_for_function_at(&f.base.base.function_id, &f.defined_on)
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
                    let triggered_at = AtTime::try_from(f.base.base.triggered_on)?;
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
                            .map(|c| (c.base.id, c))
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
                    coll.base.name.clone(),
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
                    if matches!(found_data_version.base.base.has_data, Some(HasData::TRUE)) {
                        Some(Cow::Borrowed(found_data_version))
                    } else {
                        let key = (
                            found_data_version.base.base.table_id,
                            found_data_version.base.base.triggered_on,
                        );
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
                        .builder(&data_version_with_data.base.base.base_path)
                        .collection(&data_version_with_data.base.base.collection_id)
                        .data(&data_version_with_data.base.base.id)
                        .table(
                            &data_version_with_data.base.base.table_id,
                            &data_version_with_data.base.base.table_version_id,
                        )
                        .build()?;
                    let (location, _) = build_external_location(storage, &path)?;
                    (Some(found_data_version.clone()), Some(location))
                } else {
                    (Some(found_data_version.clone()), None)
                }
            }
        };

        let input_table = InputTableVersion {
            name,
            collection_id,
            collection,
            table_id,
            table_version_id,
            execution_id: found_data_version
                .as_ref()
                .map(|v| v.base.base.execution_id.clone()),
            transaction_id: found_data_version
                .as_ref()
                .map(|v| v.base.base.transaction_id.clone()),
            function_run_id: found_data_version
                .as_ref()
                .map(|v| v.base.base.physical_function_run_id.clone()),
            triggered_on: found_data_version
                .as_ref()
                .map(|v| v.base.base.triggered_on.try_into())
                .transpose()?,
            table_data_version_id: found_data_version.as_ref().map(|v| v.base.base.id.clone()),
            location,
            input_idx: *input_idx,
            table_pos: *dependency_pos,
            version_pos: lfri.requirement_version_pos,
        };

        input_tables_map
            .entry(**dependency_pos)
            .or_default()
            .push(input_table);
    }

    Ok(partition_and_sort(input_tables_map, |mut tables| {
        tables.sort_by_key(|t| *t.version_pos);
        InputTable::new(tables)
    }))
}
