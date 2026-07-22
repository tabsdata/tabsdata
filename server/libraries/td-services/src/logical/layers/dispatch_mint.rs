//
// Copyright 2026 Tabsdata Inc.
//

//! Mint-at-dispatch: mint FR + output TDV placeholders for each ready LFR in
//! one transaction. A first dispatch (pointer `Pending`) also binds the
//! LFR/LTDV pointers to the minted ids; a re-run (recover — pointer `Bound`)
//! skips the bind and lets `redirect_pointers_at_finish` rebind on success,
//! as replay does. DB reads are hoisted to chunked fetches before the per-LFR
//! loop; FR and TDV rows are bulk-inserted; only pointer appends stay per-row.

use crate::function::layers::topology::{RunnableVersion, latest_patch_or_head_batch};
use crate::logical::layers::mint::{build_function_run, build_output_table_data_versions};
use std::collections::HashMap;
use std::ops::Deref;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalTableDataVersionDB};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::table::TableDB;
use td_objects::types::basic::{
    AtTime, FunctionVersionId, LogicalFunctionRunId, LogicalTableDataVersionId,
    PhysicalFunctionRunId, PhysicalTableDataVersionId, UserId,
};
use td_queries::{Bind, ChunkedQueries, LfrEntry, LtdvEntry};
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
    mut conn: MutRef<DomainConnection<Apiserver>>,
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
        output.push(MintedDispatch {
            lfr,
            fr,
            runnable,
            tdvs,
        });
    }

    // A re-run comes back in `deferred`: its rebind waits for redirect_at_finish.
    let lfr_binds: Vec<Bind<LogicalFunctionRunId, PhysicalFunctionRunId>> = output
        .iter()
        .map(|m| Bind {
            project_id: m.lfr.project_id,
            logical_id: m.lfr.id.clone(),
            new_concrete_id: m.fr.id.clone(),
        })
        .collect();
    let deferred = conn
        .lfr()
        .pointers()
        .bind_all(&lfr_binds, at.deref(), &by)
        .await?;

    let mut ltdv_binds: Vec<Bind<LogicalTableDataVersionId, PhysicalTableDataVersionId>> =
        Vec::new();
    for m in &output {
        if deferred.contains(&m.lfr.id) {
            debug!(
                lfr_id = %m.lfr.id,
                "scheduler.dispatch: re-run of bound LFR — deferring pointer rebind to redirect_at_finish"
            );
            continue;
        }
        for tdv in &m.tdvs {
            let ltdv = ltdvs_all
                .iter()
                .find(|f| {
                    f.producer_logical_function_run_id.as_ref() == Some(&m.lfr.id)
                        && f.table_id == tdv.table_id
                })
                .ok_or_else(|| {
                    api_error!(
                        ApiError::InternalError,
                        "no LTDV for LFR {} table {} during mint+bind",
                        m.lfr.id,
                        tdv.table_id
                    )
                })?;
            ltdv_binds.push(Bind {
                project_id: ltdv.project_id,
                logical_id: ltdv.id.clone(),
                new_concrete_id: tdv.id.clone(),
            });
        }
    }
    // Deferral only applies to LFRs; an output LTDV of a Pending LFR must
    // itself be Pending — its TDV was minted in this very tick.
    let bound_ltdvs = conn
        .ltdv()
        .pointers()
        .bind_all(&ltdv_binds, at.deref(), &by)
        .await?;
    if let Some(ltdv_id) = bound_ltdvs.into_iter().next() {
        return Err(api_error!(
            ApiError::InternalError,
            "LTDV {} already Bound at dispatch while its producer LFR is Pending",
            ltdv_id
        ));
    }

    debug!(
        minted = output.len(),
        "scheduler.dispatch: minted FR + TDVs and bound pointers"
    );
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::pool::TdPool;
    use td_objects::Buildable;
    use td_objects::TryUpdater;
    use td_objects::and;
    use td_objects::dxo::collection::{
        CollectionDB, CollectionUpdateDB, CollectionUpdateDBBuilder,
    };
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::logical::{LogicalFunctionRunPointerDB, LogicalPointerDao};
    use td_objects::types::basic::{
        AccessTokenId, BasePath, BundleId, CollectionName, Decorator, FunctionRuntimeValues,
        LogicalFunctionRunStatus, LogicalPointerReason, LogicalPointerStatus,
        PhysicalFunctionRunId, ProjectId, TableNameDto, TransactionKey, UserId,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::extractors::{MutRef, Ref};

    /// Set a collection's `base_path` via the live-update seam (no
    /// service oneshot): seed the updater from the current `CollectionDB`,
    /// stamp modified_on/by from a RequestContext, override base_path,
    /// then `live().update`.
    async fn set_collection_base_path(
        db: &TdPool,
        project_id: ProjectId,
        collection: &CollectionDB,
        base_path: &BasePath,
    ) -> Result<(), TdError> {
        let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .read::<String>("")
            .context;
        let mut builder = CollectionUpdateDBBuilder::try_from(collection)?;
        let builder = TryUpdater::try_update_from(&mut builder, &request_context)?;
        let update = builder
            .clone()
            .base_path(Some(Some(base_path.clone())))
            .build()?;
        db.scoped(project_id)
            .live()
            .update::<CollectionUpdateDB, CollectionDB>(&update, &collection.id)
            .await?;
        Ok(())
    }

    /// Seed a function with a single regular output table and mint a
    /// Scheduled LFR/PFR pair (plus the LFR/LTDV pointer events the
    /// dispatch loop reads) against it. `base_path` becomes the
    /// function-version override (`FunctionRegister::base_path`).
    async fn seed_dispatch_lfr(
        db: &TdPool,
        project_id: ProjectId,
        collection: &CollectionDB,
        name: &str,
        base_path: Option<BasePath>,
    ) -> Result<LogicalFunctionRunDB, TdError> {
        let create = FunctionRegister::builder()
            .try_name(name)?
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(Decorator::Publisher)
            .base_path(base_path)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("t0")?]))
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .reuse_frozen_tables(false)
            .build()?;
        let func = db
            .scoped(project_id)
            .seed()
            .function(collection, &create)
            .await?;
        let exec = db.scoped(project_id).seed().execution(&func).await?;
        let txn = db
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
                &txn,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;
        let lfr = db
            .live()
            .fetch_one::<LogicalFunctionRunDB>(and!(&pfr.execution_id, &pfr.function_id))
            .await?;

        // `seed().physical_function_run` leaves the LFR pointer Bound to its
        // seeded PFR. Real dispatch sees a Pending (InitialMint) LFR pointer and
        // binds it to the freshly-minted PFR. Append that Pending event so the
        // mint loop has an unbound pointer to bind, matching plan-build output.
        let pending = LogicalFunctionRunPointerDB::build_event(
            lfr.project_id,
            &lfr.id,
            LogicalPointerStatus::Pending,
            None,
            AtTime::now(),
            UserId::admin(),
            LogicalPointerReason::InitialMint,
        )?;
        db.scoped(project_id)
            .live()
            .insert::<LogicalFunctionRunPointerDB>(&pending)
            .await?;
        Ok(lfr)
    }

    /// Two functions in one collection (base_path "/coll") dispatched in
    /// the same tick: function A overrides base_path to "/a", function B
    /// has no override. Minting must stamp A's output TDVs with "/a" and
    /// B's with the collection's "/coll" — proving per-LFR resolution
    /// (override vs collection fallback) within a single batch.
    #[td_test::test(db)]
    async fn test_dispatch_batch_stamps_per_lfr_base_path(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let coll = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let coll_base = BasePath::try_from("/coll")?;
        set_collection_base_path(&db, p.id, &coll, &coll_base).await?;
        let coll: CollectionDB = db.live().fetch_one::<CollectionDB>(&coll.id).await?;
        assert_eq!(coll.base_path, Some(coll_base.clone()));

        let override_base = BasePath::try_from("/a")?;
        let lfr_a = seed_dispatch_lfr(&db, p.id, &coll, "fa", Some(override_base.clone())).await?;
        let lfr_b = seed_dispatch_lfr(&db, p.id, &coll, "fb", None).await?;
        let fvid_a = lfr_a.function_version_id.clone();
        let fvid_b = lfr_b.function_version_id.clone();

        let conn = MutRef::new(DomainConnection::ready(
            db.begin().await.unwrap().into_connection(),
        ));
        let ready = Ref::new(vec![lfr_a, lfr_b]);
        let at = Ref::new(AtTime::now());
        let minted = mint_function_runs_on_dispatch(conn, ready, at).await?;

        assert_eq!(minted.len(), 2, "both LFRs minted in one batch");
        let a = minted
            .iter()
            .find(|m| m.lfr.function_version_id == fvid_a)
            .expect("function A minted");
        let b = minted
            .iter()
            .find(|m| m.lfr.function_version_id == fvid_b)
            .expect("function B minted");

        assert!(!a.tdvs.is_empty(), "function A produced output TDVs");
        for tdv in &a.tdvs {
            assert_eq!(
                tdv.base_path, override_base,
                "function A's output TDVs carry its base_path override, not the collection's"
            );
        }
        assert!(!b.tdvs.is_empty(), "function B produced output TDVs");
        for tdv in &b.tdvs {
            assert_eq!(
                tdv.base_path, coll_base,
                "function B (no override) falls back to the collection base_path"
            );
        }
        // Distinct, sanity check the two branches actually diverged.
        assert_ne!(override_base, coll_base);
        Ok(())
    }

    /// Seed a function + Scheduled PFR whose LFR pointer is left `Bound` to that
    /// seeded PFR — an identity that already ran (the recover re-run case).
    /// Returns the LFR and the id of the PFR its pointer is bound to.
    async fn seed_bound_lfr(
        db: &TdPool,
        project_id: ProjectId,
        collection: &CollectionDB,
        name: &str,
    ) -> Result<(LogicalFunctionRunDB, PhysicalFunctionRunId), TdError> {
        let create = FunctionRegister::builder()
            .try_name(name)?
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(Decorator::Publisher)
            .base_path(None)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("t0")?]))
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .reuse_frozen_tables(false)
            .build()?;
        let func = db
            .scoped(project_id)
            .seed()
            .function(collection, &create)
            .await?;
        let exec = db.scoped(project_id).seed().execution(&func).await?;
        let txn = db
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
                &txn,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;
        let lfr = db
            .live()
            .fetch_one::<LogicalFunctionRunDB>(and!(&pfr.execution_id, &pfr.function_id))
            .await?;
        // No Pending append (unlike seed_dispatch_lfr): the pointer stays Bound
        // to `pfr`, so the mint loop sees an already-run identity.
        Ok((lfr, pfr.id))
    }

    /// Recover re-run (LFR pointer already Bound): mint must succeed without
    /// rebinding — pre-fix the eager bind made this Err (the recover deadlock).
    #[td_test::test(db)]
    async fn test_dispatch_rerun_of_bound_lfr_does_not_rebind(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let coll = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let (lfr, bound_pfr) = seed_bound_lfr(&db, p.id, &coll, "fr").await?;

        let conn = MutRef::new(DomainConnection::ready(
            db.begin().await.unwrap().into_connection(),
        ));
        let ready = Ref::new(vec![lfr.clone()]);
        let at = Ref::new(AtTime::now());
        let minted = mint_function_runs_on_dispatch(conn, ready, at).await?;

        assert_eq!(minted.len(), 1, "the bound LFR is re-minted");
        assert_ne!(
            minted[0].fr.id, bound_pfr,
            "re-run mints a NEW PFR, distinct from the one the pointer is bound to"
        );
        Ok(())
    }
}
