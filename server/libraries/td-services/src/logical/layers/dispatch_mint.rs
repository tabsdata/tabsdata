//
// Copyright 2026 Tabs Data Inc.
//

//! Mint-at-dispatch: mint FR + output TDV placeholders for each ready
//! LFR and bind the LFR/LTDV pointers to the freshly-minted ids in one
//! transaction. Every DB read is hoisted to a chunked fetch before the
//! per-LFR loop; FR and TDV rows are bulk-inserted; only the
//! pointer-event appends remain per-row.

use crate::function::layers::topology::{RunnableVersion, latest_patch_or_head_batch};
use crate::logical::layers::mint::{build_function_run, build_output_table_data_versions};
use std::collections::HashMap;
use std::ops::Deref;
use td_database::dynamic::DynamicConnection;
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunPointerDB, LogicalTableDataVersionDB,
    LogicalTableDataVersionPointerDB,
};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::table::TableDB;
use td_objects::types::basic::{
    AtTime, FunctionVersionId, LogicalFunctionRunId, LogicalTableDataVersionId, UserId,
};
use td_queries::{ChunkedQueries, LfrEntry, LtdvEntry, VersionedEntry};
use td_tower::extractors::{MutRef, Ref};
use tracing::{debug, instrument};

/// One ready LFR's mint output. Carries everything downstream layers
/// (worker yaml builder, status flip) need to emit a worker request
/// without re-querying.
#[derive(Debug, Clone)]
pub struct MintedDispatch {
    pub lfr: LogicalFunctionRunDB,
    pub fr: PhysicalFunctionRunDB,
    pub runnable: RunnableVersion,
    pub tdvs: Vec<PhysicalTableDataVersionDB>,
}

#[instrument(
    name = "scheduler.mint_function_runs_on_dispatch",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn mint_function_runs_on_dispatch(
    mut conn: MutRef<DynamicConnection<'static>>,
    ready_lfrs: Ref<Vec<LogicalFunctionRunDB>>,
    at: Ref<AtTime>,
) -> Result<Vec<MintedDispatch>, TdError> {
    if ready_lfrs.is_empty() {
        return Ok(Vec::new());
    }

    // InitialDispatch is system-class (scheduler auto-binds the
    // freshly-minted PFR/PTDV to its identity); attribute to SYSTEM,
    // not to the operator who happened to trigger the original
    // execution.
    let by = UserId::system();

    let lfr_ids: Vec<LogicalFunctionRunId> = ready_lfrs.iter().map(|f| f.id.clone()).collect();
    let producer_filters: Vec<Option<LogicalFunctionRunId>> =
        lfr_ids.iter().cloned().map(Some).collect();

    // Pre-fetch every read the per-LFR mint+bind loop needs.

    let ltdvs_all: Vec<LogicalTableDataVersionDB> = conn
        .fetch_all_chunked::<LogicalTableDataVersionDB, Option<LogicalFunctionRunId>>(
            &producer_filters,
        )
        .await?;

    let latest_lfr_pointers: Vec<LogicalFunctionRunPointerDB> = conn
        .versioned(at.deref())
        .fetch_all_chunked::<LogicalFunctionRunPointerDB>(&lfr_ids)
        .await?;
    let latest_lfr_by_id: HashMap<LogicalFunctionRunId, LogicalFunctionRunPointerDB> =
        latest_lfr_pointers
            .into_iter()
            .map(|p| (p.logical_function_run_id.clone(), p))
            .collect();

    let ltdv_ids: Vec<LogicalTableDataVersionId> = ltdvs_all.iter().map(|f| f.id.clone()).collect();
    let latest_ltdv_pointers: Vec<LogicalTableDataVersionPointerDB> = if ltdv_ids.is_empty() {
        Vec::new()
    } else {
        conn.versioned(at.deref())
            .fetch_all_chunked::<LogicalTableDataVersionPointerDB>(&ltdv_ids)
            .await?
    };
    let latest_ltdv_by_id: HashMap<LogicalTableDataVersionId, LogicalTableDataVersionPointerDB> =
        latest_ltdv_pointers
            .into_iter()
            .map(|p| (p.logical_table_data_version_id.clone(), p))
            .collect();

    // Resolve latest_patch_or_head for every distinct fvid in the tick.
    let distinct_fvids: Vec<FunctionVersionId> = ready_lfrs
        .iter()
        .map(|f| f.function_version_id.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let runnable_by_fvid =
        latest_patch_or_head_batch(&mut conn, distinct_fvids, at.deref()).await?;

    // Tables for every head fvid we'll be minting against.
    let distinct_head_fvids: Vec<FunctionVersionId> = runnable_by_fvid
        .values()
        .map(|r| r.function_version_id.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let all_tables: Vec<TableDB> = if distinct_head_fvids.is_empty() {
        Vec::new()
    } else {
        conn.fetch_all_chunked::<TableDB, FunctionVersionId>(&distinct_head_fvids)
            .await?
    };
    let mut tables_by_head_fvid: HashMap<FunctionVersionId, Vec<TableDB>> = HashMap::new();
    for t in all_tables {
        tables_by_head_fvid
            .entry(t.function_version_id.clone())
            .or_default()
            .push(t);
    }

    // Effective output base_path per run: function-version override
    // (functions.base_path) if set, else the collection base_path, resolved in a
    // single functions ⋈ collections_active COALESCE query (chunked internally).
    // Stamped onto every output TDV so reads never recompute it after a later
    // base_path change.
    let effective = conn
        .lfr()
        .effective_output_base_paths(&distinct_head_fvids)
        .await?;

    // Build all FRs and TDVs in-memory, then bulk-insert each set. The
    // post-insert loop drains both Vecs in order via iterators rather
    // than cloning rows out by index — `frs` and `all_tdvs` aren't
    // needed after the inserts, so move ownership into `MintedDispatch`
    // entries directly.
    let mut frs: Vec<PhysicalFunctionRunDB> = Vec::with_capacity(ready_lfrs.len());
    let mut all_tdvs: Vec<PhysicalTableDataVersionDB> = Vec::new();
    let mut per_lfr: Vec<(LogicalFunctionRunDB, usize)> = Vec::with_capacity(ready_lfrs.len());

    for lfr in ready_lfrs.iter() {
        let runnable = runnable_by_fvid
            .get(&lfr.function_version_id)
            .ok_or_else(|| {
                api_error!(
                    ApiError::InternalError,
                    "no runnable resolved for LFR {} (fvid {})",
                    lfr.id,
                    lfr.function_version_id
                )
            })?;
        let fr = build_function_run(lfr, runnable, at.deref())?;
        let empty = Vec::new();
        let tables = tables_by_head_fvid
            .get(&runnable.function_version_id)
            .unwrap_or(&empty);
        let base_path = effective
            .get(&runnable.function_version_id)
            .cloned()
            .unwrap_or_default();
        let tdvs = build_output_table_data_versions(lfr, runnable, &fr, tables, &base_path)?;
        let tdv_count = tdvs.len();
        all_tdvs.extend(tdvs);
        frs.push(fr);
        per_lfr.push((lfr.clone(), tdv_count));
    }

    if !frs.is_empty() {
        conn.insert_vec::<PhysicalFunctionRunDB>(&frs).await?;
    }
    if !all_tdvs.is_empty() {
        conn.insert_vec::<PhysicalTableDataVersionDB>(&all_tdvs)
            .await?;
    }

    // Bind LFR pointer + each output LTDV pointer per minted run.
    let mut output = Vec::with_capacity(ready_lfrs.len());
    let mut frs_iter = frs.into_iter();
    let mut tdvs_iter = all_tdvs.into_iter();
    for (lfr, tdv_count) in per_lfr {
        let fr = frs_iter
            .next()
            .expect("frs and per_lfr were built in lockstep");
        let tdvs: Vec<PhysicalTableDataVersionDB> = (&mut tdvs_iter).take(tdv_count).collect();
        let runnable = runnable_by_fvid
            .get(&lfr.function_version_id)
            .cloned()
            .ok_or_else(|| {
                api_error!(
                    ApiError::InternalError,
                    "no runnable resolved for LFR {} (fvid {})",
                    lfr.id,
                    lfr.function_version_id
                )
            })?;

        let latest_lfr = latest_lfr_by_id.get(&lfr.id).ok_or_else(|| {
            api_error!(
                ApiError::InternalError,
                "no LFR pointer event for {} at dispatch (identity should always carry an InitialMint)",
                lfr.id
            )
        })?;
        conn.lfr()
            .pointers()
            .bind_with_known_state(
                &lfr.project_id,
                &lfr.id,
                &fr.id,
                latest_lfr,
                at.deref(),
                &by,
            )
            .await?;

        for tdv in &tdvs {
            let ltdv = ltdvs_all
                .iter()
                .find(|f| {
                    f.producer_logical_function_run_id.as_ref() == Some(&lfr.id)
                        && f.table_id == tdv.table_id
                })
                .ok_or_else(|| {
                    api_error!(
                        ApiError::InternalError,
                        "no LTDV for LFR {} table {} during mint+bind",
                        lfr.id,
                        tdv.table_id
                    )
                })?;
            let latest_ltdv = latest_ltdv_by_id.get(&ltdv.id).ok_or_else(|| {
                api_error!(
                    ApiError::InternalError,
                    "no LTDV pointer event for {} at dispatch",
                    ltdv.id
                )
            })?;
            conn.ltdv()
                .pointers()
                .bind_with_known_state(
                    &ltdv.project_id,
                    &ltdv.id,
                    &tdv.id,
                    latest_ltdv,
                    at.deref(),
                    &by,
                )
                .await?;
        }

        output.push(MintedDispatch {
            lfr,
            fr,
            runnable,
            tdvs,
        });
    }

    debug!(
        minted = output.len(),
        "scheduler.dispatch: minted FR + TDVs and bound pointers"
    );
    Ok(output)
}
