//
// Copyright 2026 Tabsdata Inc.
//

//! Mint-at-dispatch: mint FR + output TDV placeholders for each ready LFR in
//! one transaction; the minted rows become their logical rows' current
//! versions. DB reads are hoisted to chunked fetches before the per-LFR
//! loop, and FR and TDV rows are bulk-inserted.

use td_database::access::InProject;
use td_database::access::LockInstant;

use crate::function::topology::{RunnableVersion, latest_patch_or_head_batch};
use crate::logical::mint::{build_function_run, build_output_table_data_versions};
use std::collections::HashMap;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalTableDataVersionDB};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunInsertDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionInsertDB;
use td_objects::dxo::table::TableDB;
use td_objects::types::basic::{AtTime, FunctionVersionId, LogicalFunctionRunId};
use td_queries::Queries;
use tracing::{debug, instrument};

/// One ready LFR's mint output. Carries everything downstream steps
/// (worker yaml builder, status flip) need to emit a worker request
/// without re-querying.
#[derive(Debug, Clone)]
pub struct MintedDispatch {
    pub lfr: LogicalFunctionRunDB,
    pub fr: PhysicalFunctionRunInsertDB,
    pub runnable: RunnableVersion,
    pub tdvs: Vec<PhysicalTableDataVersionInsertDB>,
}

#[instrument(
    name = "scheduler.mint_function_runs_on_dispatch",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn mint_function_runs_on_dispatch(
    conn: &mut DomainConnection<Apiserver, InProject>,
    ready_lfrs: &[LogicalFunctionRunDB],
    at: LockInstant,
) -> Result<Vec<MintedDispatch>, TdError> {
    if ready_lfrs.is_empty() {
        return Ok(Vec::new());
    }

    let lfr_ids: Vec<LogicalFunctionRunId> = ready_lfrs.iter().map(|f| f.base.id.clone()).collect();
    let producer_filters: Vec<Option<LogicalFunctionRunId>> =
        lfr_ids.iter().cloned().map(Some).collect();

    let ltdvs_all: Vec<LogicalTableDataVersionDB> = conn
        .timeline()
        .chunked()
        .fetch_all::<LogicalTableDataVersionDB>(&producer_filters, ())
        .await?;

    let distinct_fvids: Vec<FunctionVersionId> = ready_lfrs
        .iter()
        .map(|f| f.base.function_version_id.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let runnable_by_fvid =
        latest_patch_or_head_batch(&mut *conn, distinct_fvids, &AtTime::from(at)).await?;

    let distinct_head_fvids: Vec<FunctionVersionId> = runnable_by_fvid
        .values()
        .map(|r| r.function_version_id.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let all_tables: Vec<TableDB> = if distinct_head_fvids.is_empty() {
        Vec::new()
    } else {
        conn.timeline()
            .chunked()
            .fetch_all::<TableDB>(&distinct_head_fvids, ())
            .await?
    };
    let mut tables_by_head_fvid: HashMap<FunctionVersionId, Vec<TableDB>> = HashMap::new();
    for t in all_tables {
        tables_by_head_fvid
            .entry(t.base.function_version_id.clone())
            .or_default()
            .push(t);
    }

    // Effective output base_path per run: function-version override
    // (functions.base_path) if set, else the collection base_path, resolved in a
    // single functions ⋈ collections_active COALESCE query (chunked internally).
    // Stamped onto every output TDV so reads never recompute it after a later
    // base_path change.
    let effective = conn
        .logical_runs()
        .effective_output_base_paths(&distinct_head_fvids)
        .await?;

    // Build all FRs and TDVs in-memory, then bulk-insert each set. The
    // post-insert loop drains both Vecs in order via iterators rather
    // than cloning rows out by index — `frs` and `all_tdvs` aren't
    // needed after the inserts, so move ownership into `MintedDispatch`
    // entries directly.
    let mut frs: Vec<PhysicalFunctionRunInsertDB> = Vec::with_capacity(ready_lfrs.len());
    let mut all_tdvs: Vec<PhysicalTableDataVersionInsertDB> = Vec::new();
    let mut per_lfr: Vec<(LogicalFunctionRunDB, usize)> = Vec::with_capacity(ready_lfrs.len());

    for lfr in ready_lfrs.iter() {
        let runnable = runnable_by_fvid
            .get(&lfr.base.function_version_id)
            .ok_or_else(|| {
                api_error!(
                    ApiError::InternalError,
                    "no runnable resolved for LFR {} (fvid {})",
                    lfr.base.id,
                    lfr.base.function_version_id
                )
            })?;
        let base_path = effective
            .get(&runnable.function_version_id)
            .cloned()
            .unwrap_or_default();
        let fr = build_function_run(lfr, runnable, at, &base_path)?;
        let empty = Vec::new();
        let tables = tables_by_head_fvid
            .get(&runnable.function_version_id)
            .unwrap_or(&empty);
        let logical_by_table: HashMap<_, _> = ltdvs_all
            .iter()
            .filter(|f| f.base.producer_logical_function_run_id.as_ref() == Some(&lfr.base.id))
            .map(|f| (f.base.table_id, f.base.id.clone()))
            .collect();
        let tdvs = build_output_table_data_versions(
            lfr,
            runnable,
            &fr,
            tables,
            &base_path,
            &logical_by_table,
        )?;
        let tdv_count = tdvs.len();
        all_tdvs.extend(tdvs);
        frs.push(fr);
        per_lfr.push((lfr.clone(), tdv_count));
    }

    if !frs.is_empty() {
        conn.scd2().chunked().open(&frs, at).await?;
    }
    if !all_tdvs.is_empty() {
        conn.scd2().chunked().open(&all_tdvs, at).await?;
    }

    let mut output = Vec::with_capacity(ready_lfrs.len());
    let mut frs_iter = frs.into_iter();
    let mut tdvs_iter = all_tdvs.into_iter();
    for (lfr, tdv_count) in per_lfr {
        let fr = frs_iter
            .next()
            .expect("frs and per_lfr were built in lockstep");
        let tdvs: Vec<PhysicalTableDataVersionInsertDB> =
            (&mut tdvs_iter).take(tdv_count).collect();
        let runnable = runnable_by_fvid
            .get(&lfr.base.function_version_id)
            .cloned()
            .ok_or_else(|| {
                api_error!(
                    ApiError::InternalError,
                    "no runnable resolved for LFR {} (fvid {})",
                    lfr.base.id,
                    lfr.base.function_version_id
                )
            })?;
        output.push(MintedDispatch {
            lfr,
            fr,
            runnable,
            tdvs,
        });
    }

    debug!(
        minted = output.len(),
        "scheduler.dispatch: minted FR + TDVs"
    );
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::fixtures::register;
    use td_database::access::InProject;
    use td_database::access::LockInstant;
    use td_database::connection::PgConnectionExt;
    use td_database::pool::TdPool;
    use td_objects::dxo::collection::ActiveCollectionDB;
    use td_objects::types::basic::{
        CollectionName, LogicalFunctionRunStatus, PhysicalFunctionRunId, ProjectId, TransactionKey,
        UserId,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    /// An identity that already ran (the recover re-run case).
    async fn seed_run_lfr(
        db: &TdPool,
        project_id: ProjectId,
        collection: &ActiveCollectionDB,
        name: &str,
    ) -> Result<(LogicalFunctionRunDB, PhysicalFunctionRunId), TdError> {
        let func = db
            .scoped(project_id)
            .seed()
            .function(collection, &register(name, None))
            .await?;
        let exec = db.scoped(project_id).seed().execution(&func).await?;
        let trx = db
            .scoped(project_id)
            .seed()
            .transaction(&exec, &TransactionKey::try_from("ANY")?)
            .await?;
        let pfr = db
            .scoped(project_id)
            .seed()
            .physical_function_run(
                collection,
                &func,
                &exec,
                &trx,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;
        let lfr = db
            .fetch_one::<LogicalFunctionRunDB>(&pfr.base.logical_function_run_id)
            .await?;
        Ok((lfr, pfr.base.id))
    }

    /// Recover re-run of a run that already has an attempt mints a new one.
    #[td_test::test(db)]
    async fn test_dispatch_rerun_mints_a_new_attempt(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let coll = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let (lfr, previous_pfr) = seed_run_lfr(&db, p.id, &coll, "fr").await?;

        let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());
        conn.scope_in_project(&p.id.to_string()).await.unwrap();
        let ready = vec![lfr.clone()];
        let at = LockInstant::asserted(*AtTime::now());
        let minted = mint_function_runs_on_dispatch(&mut conn, &ready, at).await?;

        assert_eq!(minted.len(), 1, "the LFR is re-minted");
        assert_ne!(
            minted[0].fr.id, previous_pfr,
            "re-run mints a NEW PFR, distinct from the previous attempt"
        );
        Ok(())
    }
}
