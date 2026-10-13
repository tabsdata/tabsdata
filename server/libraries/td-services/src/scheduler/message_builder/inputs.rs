//
// Copyright 2026 Tabsdata Inc.
//

//! Input-version resolution: the logical→physical step that maps each
//! input slot's bound `LogicalTableDataVersionId` to a concrete PTDV +
//! storage location, chasing the latest with-data row when needed.

use super::{build_external_location, partition_and_sort};
use crate::logical::index_lfrs_by_exec_fn;
use http::StatusCode;
use itertools::Itertools;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use td_database::access::{AccessMode, SeesProjectRows};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::collection::ResolvedCollectionDB;
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunInputDB};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunToExecute;
use td_objects::dxo::physical_table_data_version::{
    PhysicalTableDataVersionDB, PhysicalTableDataVersionDBWithNames,
};
use td_objects::dxo::table::TableDB;
use td_objects::dxo::yaml::function::{InputTable, InputTableVersion};
use td_objects::types::basic::{
    AtTime, CollectionId, DependencyPos, ExecutionId, FunctionId, HasData, LogicalFunctionRunId,
    LogicalTableDataVersionId, PhysicalFunctionRunId, PhysicalTableDataVersionId, TableVersionId,
};
use td_queries::Queries;
use td_storage::Storage;
use td_storage::location::StorageLocation;

/// Tick-wide batches the per-FR input/output build reads from: the LFR
/// index (for `(execution_id, function_id) → LFR`), output PTDVs, LFR
/// input rows, and the input TDVs current at `at`.
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
            .get(&f.base.base.base.execution_id)
            .and_then(|m| m.get(&f.base.base.base.function_id))
            .map(|l| l.base.id.clone())
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
pub(crate) async fn prefetch_dispatch<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    physical_function_runs: &[PhysicalFunctionRunToExecute],
    at: &AtTime,
) -> Result<DispatchPrefetch, TdError> {
    let fr_ids: Vec<PhysicalFunctionRunId> = physical_function_runs
        .iter()
        .map(|f| f.base.base.base.id.clone())
        .collect();
    let exec_ids: Vec<ExecutionId> = physical_function_runs
        .iter()
        .map(|f| f.base.base.base.execution_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let all_lfrs: Vec<LogicalFunctionRunDB> = if exec_ids.is_empty() {
        Vec::new()
    } else {
        conn.chunked()
            .fetch_all::<LogicalFunctionRunDB>(&exec_ids, ())
            .await?
    };
    let lfr_by_fr_key = index_lfrs_by_exec_fn(all_lfrs);
    let lfr_ids: Vec<LogicalFunctionRunId> = physical_function_runs
        .iter()
        .filter_map(|f| {
            lfr_by_fr_key
                .get(&f.base.base.base.execution_id)
                .and_then(|m| m.get(&f.base.base.base.function_id))
                .map(|l| l.base.id.clone())
        })
        .collect();

    let all_output_tables: Vec<PhysicalTableDataVersionDBWithNames> = if fr_ids.is_empty() {
        Vec::new()
    } else {
        conn.timeline()
            .chunked()
            .fetch_all::<PhysicalTableDataVersionDBWithNames>(&fr_ids, ())
            .await?
    };
    let output_tables_by_fr: HashMap<
        PhysicalFunctionRunId,
        Vec<PhysicalTableDataVersionDBWithNames>,
    > = all_output_tables
        .into_iter()
        .into_group_map_by(|t| t.base.base.base.physical_function_run_id.clone());

    let all_lfr_inputs: Vec<LogicalFunctionRunInputDB> = if lfr_ids.is_empty() {
        Vec::new()
    } else {
        conn.chunked()
            .fetch_all::<LogicalFunctionRunInputDB>(&lfr_ids, ())
            .await?
    };
    let input_ftdv_ids: Vec<LogicalTableDataVersionId> = all_lfr_inputs
        .iter()
        .filter_map(|i| i.base.requirement_logical_table_data_version_id.clone())
        .collect();
    let lfr_inputs_by_lfr: HashMap<LogicalFunctionRunId, Vec<LogicalFunctionRunInputDB>> =
        all_lfr_inputs
            .into_iter()
            .into_group_map_by(|inp| inp.base.logical_function_run_id.clone());

    let mut bound_tdv_id_by_ftdv: HashMap<
        LogicalTableDataVersionId,
        Option<PhysicalTableDataVersionId>,
    > = input_ftdv_ids.iter().map(|id| (id.clone(), None)).collect();
    if !input_ftdv_ids.is_empty() {
        // An input with no materialization visible at `at` stays `None`:
        // never produced, or voided.
        let bound: Vec<PhysicalTableDataVersionDB> = conn
            .scd2()
            .as_of(at)
            .chunked()
            .fetch_all::<PhysicalTableDataVersionDB>(&input_ftdv_ids, ())
            .await?;
        for tdv in bound {
            if let Some(slot) =
                bound_tdv_id_by_ftdv.get_mut(&tdv.base.logical_table_data_version_id)
            {
                *slot = Some(tdv.base.id);
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
            conn.timeline()
                .chunked()
                .fetch_all::<PhysicalTableDataVersionDBWithNames>(&bound_tdv_ids, ())
                .await?
                .into_iter()
                .map(|t| (t.base.base.base.id.clone(), t))
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
pub(crate) async fn resolve_input_tables<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
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

    // Dep-fallback is used only when an input slot has no bound TDV: on the
    // first miss resolve the function's deps once and batch-fetch their
    // `TableDB`/`ResolvedCollectionDB` rows.
    struct FallbackCache {
        deps: HashMap<DependencyPos, DependencyDB>,
        tables: HashMap<TableVersionId, TableDB>,
        colls: HashMap<CollectionId, ResolvedCollectionDB>,
    }
    let mut fallback_cache: Option<FallbackCache> = None;

    // Per-FR cache for the committed data a no-data input stands for — slots
    // bound to the same logical version reuse one query.
    let mut latest_with_data_cache: HashMap<
        LogicalTableDataVersionId,
        Option<PhysicalTableDataVersionDBWithNames>,
    > = HashMap::new();

    let mut input_tables_map: HashMap<i32, Vec<InputTableVersion>> = HashMap::new();
    for lfri in lfr_inputs.iter() {
        // Skip NO_VERSIONS placeholders (NULL idx/dep_pos).
        let (Some(input_idx), Some(dependency_pos)) = (
            lfri.base.requirement_input_idx.as_ref(),
            lfri.base.requirement_dependency_pos.as_ref(),
        ) else {
            continue;
        };

        let bound_tdv: Option<&PhysicalTableDataVersionDBWithNames> = lfri
            .base
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
                tdv.base.base.base.name.clone(),
                tdv.base.base.base.collection_id,
                tdv.collection.clone(),
                tdv.base.base.base.table_id,
                tdv.base.base.base.table_version_id,
            ),
            None => {
                if fallback_cache.is_none() {
                    let deps: HashMap<DependencyPos, DependencyDB> = conn
                        .dependencies()
                        .active_for_function_at(&f.base.base.base.function_id, &f.defined_on)
                        .await?
                        .into_iter()
                        .map(|d| (d.base.dep_pos, d))
                        .collect();
                    // The table versions the plan read, never the tables as of
                    // a later instant: a delete committing after the plan has
                    // closed them since.
                    let pinned: Vec<TableVersionId> = lfr_inputs
                        .iter()
                        .filter_map(|i| i.base.requirement_table_version_id)
                        .collect::<HashSet<_>>()
                        .into_iter()
                        .collect();
                    let coll_ids: Vec<CollectionId> = deps
                        .values()
                        .map(|d| *d.base.table_collection_id)
                        .collect::<HashSet<_>>()
                        .into_iter()
                        .collect();
                    let tables: HashMap<TableVersionId, TableDB> = if pinned.is_empty() {
                        HashMap::new()
                    } else {
                        conn.timeline()
                            .chunked()
                            .fetch_all::<TableDB>(&pinned, ())
                            .await?
                            .into_iter()
                            .map(|t| (t.base.id, t))
                            .collect()
                    };
                    let colls: HashMap<CollectionId, ResolvedCollectionDB> = if coll_ids.is_empty()
                    {
                        HashMap::new()
                    } else {
                        conn.chunked()
                            .fetch_all::<ResolvedCollectionDB>(&coll_ids, ())
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
                let pinned = lfri.base.requirement_table_version_id.ok_or_else(|| {
                    api_error!(
                        status = StatusCode::CONFLICT,
                        "The input at dependency position {dependency_pos} was planned before \
                         runs recorded their table versions; recover the run to plan it again"
                    )
                })?;
                let table = cache.tables.get(&pinned).ok_or_else(|| {
                    api_error!(
                        ApiError::InternalError,
                        "TableDB not found for planned table version {pinned}"
                    )
                })?;
                let coll = cache.colls.get(&*dep.base.table_collection_id).ok_or_else(|| {
                    api_error!(
                        ApiError::InternalError,
                        "ResolvedCollectionDB not found for dep table_collection_id {} during fallback",
                        dep.base.table_collection_id
                    )
                })?;
                (
                    table.base.name.clone(),
                    *dep.base.table_collection_id,
                    coll.base.name.clone(),
                    dep.base.table_id,
                    pinned,
                )
            }
        };

        // Build location from the bound TDV (chasing the latest
        // with-data row when the bound TDV has `has_data = FALSE`).
        let (found_data_version, location) = match bound_tdv {
            None => (None, None),
            Some(found_data_version) => {
                let data_version_with_data = if matches!(
                    found_data_version.base.base.base.has_data,
                    Some(HasData::TRUE)
                ) {
                    Some(Cow::Borrowed(found_data_version))
                } else {
                    let key = found_data_version
                        .base
                        .base
                        .base
                        .logical_table_data_version_id
                        .clone();
                    let res = if let Some(cached) = latest_with_data_cache.get(&key) {
                        cached.clone()
                    } else {
                        let r = conn
                            .table_data_versions()
                            .committed_data_before(&key)
                            .await?;
                        latest_with_data_cache.insert(key, r.clone());
                        r
                    };
                    res.map(Cow::Owned)
                };

                if let Some(data_version_with_data) = data_version_with_data.as_ref() {
                    let (path, _) = storage_location
                        .builder(&data_version_with_data.base.base.base.base_path)
                        .collection(&data_version_with_data.base.base.base.collection_id)
                        .data(&data_version_with_data.base.base.base.id)
                        .table(
                            &data_version_with_data.base.base.base.table_id,
                            &data_version_with_data.base.base.base.table_version_id,
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
                .map(|v| v.base.base.base.execution_id.clone()),
            transaction_id: found_data_version
                .as_ref()
                .map(|v| v.base.base.base.transaction_id.clone()),
            function_run_id: found_data_version
                .as_ref()
                .map(|v| v.base.base.base.physical_function_run_id.clone()),
            triggered_on: found_data_version
                .as_ref()
                .map(|v| AtTime::from(v.base.base.interval.valid_from)),
            table_data_version_id: found_data_version
                .as_ref()
                .map(|v| v.base.base.base.id.clone()),
            location,
            input_idx: *input_idx,
            table_pos: *dependency_pos,
            version_pos: lfri.base.requirement_version_pos,
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
