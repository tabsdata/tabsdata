//
// Copyright 2026 Tabsdata Inc.
//

//! Mint-at-dispatch: mint FR + output TDV placeholders for each ready LFR in
//! one transaction; the minted rows become their logical rows' current
//! versions. DB reads are hoisted to chunked fetches before the per-LFR
//! loop, and FR and TDV rows are bulk-inserted.

use td_database::access::LockInstant;
use td_database::access::{Holds, InProject, ProjectLock};

use crate::function::topology::{RunnableVersion, latest_patch_or_head_batch};
use crate::logical::mint::{build_function_run, build_output_table_data_versions};
use crate::scheduler::partial::{Parked, Partial};
use std::collections::{HashMap, HashSet};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalTableDataVersionDB};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunInsertDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionInsertDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::topology::ProjectTopologyDB;
use td_objects::errors::SqlError;
use td_objects::types::basic::ExecutionId;
use td_objects::types::basic::TopologyVersionId;
use td_objects::types::basic::{AtTime, FunctionId, FunctionVersionId, LogicalFunctionRunId};
use td_queries::Queries;
use td_queries::metrics_facts::runs_reattempted;
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

/// The runs minted, and those whose attempt cannot be minted from their plan.
pub struct Dispatch {
    pub minted: Vec<MintedDispatch>,
    pub unmintable: Vec<Parked<LogicalFunctionRunDB>>,
}

#[instrument(
    name = "scheduler.mint_function_runs_on_dispatch",
    skip_all,
    level = "debug",
    err(Debug, level = "debug")
)]
pub async fn mint_function_runs_on_dispatch(
    conn: &mut DomainConnection<Apiserver, InProject, impl Holds<ProjectLock>>,
    ready_lfrs: &[LogicalFunctionRunDB],
    at: LockInstant,
) -> Result<Dispatch, TdError> {
    if ready_lfrs.is_empty() {
        return Ok(Dispatch {
            minted: Vec::new(),
            unmintable: Vec::new(),
        });
    }

    let lfr_ids: Vec<LogicalFunctionRunId> = ready_lfrs.iter().map(|f| f.id.clone()).collect();
    let producer_filters: Vec<Option<LogicalFunctionRunId>> =
        lfr_ids.iter().cloned().map(Some).collect();

    let ltdvs_all: Vec<LogicalTableDataVersionDB> = conn
        .timeline()
        .chunked()
        .fetch_all::<LogicalTableDataVersionDB>(&producer_filters, ())
        .await?;

    let distinct_fvids: Vec<FunctionVersionId> = ready_lfrs
        .iter()
        .map(|f| f.function_version_id.clone())
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
    // As of the plan: a table dropped since planning still gets the version its LTDV promises.
    let runs: Vec<&LogicalFunctionRunDB> = ready_lfrs.iter().collect();
    // Each run's tables as its plan read them: its execution's topology version.
    let execution_ids: Vec<ExecutionId> = runs
        .iter()
        .map(|r| r.execution_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let executions: Vec<ExecutionDB> = conn.chunked().fetch_all(&execution_ids, ()).await?;
    let versions: Vec<TopologyVersionId> = executions
        .iter()
        .map(|e| e.topology_version_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let topologies: HashMap<TopologyVersionId, ProjectTopologyDB> = conn
        .timeline()
        .chunked()
        .fetch_all::<ProjectTopologyDB>(&versions, ())
        .await?
        .into_iter()
        .map(|v| (v.id, v))
        .collect();
    let planned_by_execution: HashMap<ExecutionId, &ProjectTopologyDB> = executions
        .iter()
        .map(|e| {
            topologies
                .get(&e.topology_version_id)
                .map(|t| (e.id.clone(), t))
                .ok_or_else(SqlError::not_found::<ProjectTopologyDB>)
        })
        .collect::<Result<_, _>>()?;
    let mut function_ids_by_version: HashMap<
        TopologyVersionId,
        (&ProjectTopologyDB, HashSet<FunctionId>),
    > = HashMap::new();
    for lfr in ready_lfrs {
        let topology = planned_by_execution[&lfr.execution_id];
        function_ids_by_version
            .entry(topology.id)
            .or_insert_with(|| (topology, HashSet::new()))
            .1
            .insert(lfr.function_id);
    }
    let mut tables_by_run: HashMap<(TopologyVersionId, FunctionId), Vec<TableDB>> = HashMap::new();
    for (version, (topology, function_ids)) in &function_ids_by_version {
        let function_ids: Vec<FunctionId> = function_ids.iter().copied().collect();
        let tables: Vec<TableDB> = conn
            .scd2()
            .as_of(&topology.interval.valid_from)
            .chunked()
            .fetch_all(&function_ids, TableDB::active())
            .await?;
        for t in tables {
            tables_by_run
                .entry((*version, t.function_id))
                .or_default()
                .push(t);
        }
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

    let partial = Partial::build(ready_lfrs.iter().cloned(), |lfr| {
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
        let base_path = effective
            .get(&runnable.function_version_id)
            .cloned()
            .unwrap_or_default();
        let fr = build_function_run(lfr, runnable, at, &base_path)?;
        let empty = Vec::new();
        let tables = tables_by_run
            .get(&(planned_by_execution[&lfr.execution_id].id, lfr.function_id))
            .unwrap_or(&empty);
        let logical_by_table: HashMap<_, _> = ltdvs_all
            .iter()
            .filter(|f| f.producer_logical_function_run_id.as_ref() == Some(&lfr.id))
            .map(|f| (f.table_id, f.id.clone()))
            .collect();
        let tdvs = build_output_table_data_versions(
            lfr,
            runnable,
            &fr,
            tables,
            &base_path,
            &logical_by_table,
        )?;
        Ok((lfr.clone(), fr, tdvs))
    });
    for (lfr, fr, tdvs) in partial.built {
        per_lfr.push((lfr, tdvs.len()));
        all_tdvs.extend(tdvs);
        frs.push(fr);
    }
    let unmintable = partial.parked;

    if !frs.is_empty() {
        conn.scd2().chunked().open(&frs, at).await?;
        let reattempted: Vec<LogicalFunctionRunId> =
            per_lfr.iter().map(|(lfr, _)| lfr.id.clone()).collect();
        runs_reattempted(conn, &reattempted).await?;
    }
    if !all_tdvs.is_empty() {
        conn.table_data_versions().pending(&all_tdvs, at).await?;
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

    debug!(
        minted = output.len(),
        unmintable = unmintable.len(),
        "scheduler.dispatch: minted FR + TDVs"
    );
    Ok(Dispatch {
        minted: output,
        unmintable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::tests::fixtures::register;
    use std::str::FromStr;
    use td_database::access::Grant;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt;
    use td_objects::and;
    use td_objects::dxo::collection::{CollectionDB, CollectionParam};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionRequest;
    use td_objects::dxo::function::{FunctionDB, FunctionParam, FunctionUpdate};
    use td_objects::types::basic::{
        AccessTokenId, CollectionIdName, CollectionName, FunctionIdName, FunctionOnly,
        LogicalFunctionRunStatus, OverrideDisabledTrigger, PhysicalFunctionRunId, ProjectId,
        SkipIfRunning, TableStatus, TransactionKey, UserId,
    };
    use td_queries::Queries;
    use td_queries::test_utils::{SeedEntry, begin_locked};

    /// An identity that already ran (the recover re-run case).
    async fn seed_run_lfr(
        db: &TdPool,
        project_id: ProjectId,
        collection: &CollectionDB,
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
            .fetch_one::<LogicalFunctionRunDB>(&pfr.logical_function_run_id)
            .await?;
        Ok((lfr, pfr.base.id))
    }

    /// A code-only update leaves the output tables on the version that
    /// defined them; dispatch must still mint a TDV for every output.
    #[td_test::test(db)]
    async fn test_dispatch_mints_tdvs_after_function_update(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let coll = db
            .scoped(p.project_id)
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let services = Services::with_defaults(db.clone());
        let collection_param = || CollectionParam {
            collection: CollectionIdName::from_str(&format!("~{}", coll.collection_id)).unwrap(),
        };
        services
            .register_function(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .create(collection_param(), register("fr", None))
                    .in_project(p.project_id),
            )
            .await?;
        let update = FunctionUpdate {
            snippet: Some("s2".try_into()?),
            tables: register("fr", None).tables,
            ..Default::default()
        };
        services
            .update_function(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .update(
                        (
                            collection_param(),
                            FunctionParam {
                                function: FunctionIdName::from_str("fr")?,
                            },
                        ),
                        update,
                    )
                    .in_project(p.project_id),
            )
            .await?;

        let functions: Vec<FunctionDB> = db
            .scd2()
            .fetch_all::<FunctionDB>(&coll.collection_id)
            .await?;
        let func = &functions[0];
        let tables: Vec<TableDB> = db.scd2().fetch_all::<TableDB>(&func.function_id).await?;
        assert_eq!(tables.len(), 2, "the user table and the state table");
        assert!(
            tables.iter().all(|t| t.function_version_id != func.id),
            "an unchanged table keeps the version that defined it"
        );

        let exec = db.scoped(p.project_id).seed().execution(func).await?;
        let trx = db
            .scoped(p.project_id)
            .seed()
            .transaction(&exec, &TransactionKey::try_from("ANY")?)
            .await?;
        let pfr = db
            .scoped(p.project_id)
            .seed()
            .physical_function_run(
                &coll,
                func,
                &exec,
                &trx,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;
        let lfr = db
            .fetch_one::<LogicalFunctionRunDB>(&pfr.logical_function_run_id)
            .await?;

        let mut trx = begin_locked(&db, p.project_id).await?;
        let at = trx.locked_at();
        let conn = trx.get(Grant::asserted("dispatch mint test"));
        let minted = mint_function_runs_on_dispatch(conn, &[lfr], at)
            .await?
            .minted;
        assert_eq!(minted.len(), 1);
        assert_eq!(minted[0].tdvs.len(), tables.len());
        Ok(())
    }

    /// An update that drops a table freezes it on the function; a run planned
    /// after that has no logical version for it, and dispatch mints only what
    /// the plan has.
    #[td_test::test(db)]
    async fn test_dispatch_skips_a_table_a_later_version_dropped(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let coll = db
            .scoped(p.project_id)
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let services = Services::with_defaults(db.clone());
        let context = || RequestContext::with(AccessTokenId::default(), UserId::admin());
        let collection_param = || CollectionParam {
            collection: CollectionIdName::from_str(&format!("~{}", coll.collection_id)).unwrap(),
        };
        let function_param = || FunctionParam {
            function: FunctionIdName::from_str("fr").unwrap(),
        };
        services
            .register_function(
                context()
                    .create(collection_param(), register("fr", None))
                    .in_project(p.project_id),
            )
            .await?;
        services
            .update_function(
                context()
                    .update(
                        (collection_param(), function_param()),
                        FunctionUpdate {
                            tables: Some(vec![]),
                            ..Default::default()
                        },
                    )
                    .in_project(p.project_id),
            )
            .await?;
        let tables: Vec<TableDB> = db
            .scd2()
            .fetch_all::<TableDB>(and!(coll.collection_id, TableStatus::Frozen))
            .await?;
        assert_eq!(tables.len(), 1, "the dropped user table is frozen");

        let execution = services
            .execute_function(
                context()
                    .create(
                        (collection_param(), function_param()),
                        ExecutionRequest {
                            name: None,
                            function_only: FunctionOnly::default(),
                            override_disabled_trigger: OverrideDisabledTrigger::default(),
                            skip_if_running: SkipIfRunning::default(),
                            external_uid: None,
                        },
                    )
                    .in_project(p.project_id),
            )
            .await?
            .data;
        let planned: Vec<LogicalFunctionRunDB> = db.fetch_all(&execution.id).await?;
        assert_eq!(planned.len(), 1);
        let logical: Vec<LogicalTableDataVersionDB> = db
            .timeline()
            .fetch_all(&Some(planned[0].id.clone()))
            .await?;

        let mut trx = begin_locked(&db, p.project_id).await?;
        let at = trx.locked_at();
        let conn = trx.get(Grant::asserted("dispatch mint test"));
        let minted = mint_function_runs_on_dispatch(conn, &planned, at)
            .await?
            .minted;
        assert_eq!(minted.len(), 1);
        assert_eq!(
            minted[0].tdvs.len(),
            logical.len(),
            "one output per planned logical version, none for the frozen table"
        );
        Ok(())
    }

    /// Recover re-run of a run that already has an attempt mints a new one.
    #[td_test::test(db)]
    async fn test_dispatch_rerun_mints_a_new_attempt(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let coll = db
            .scoped(p.project_id)
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let (lfr, previous_pfr) = seed_run_lfr(&db, p.project_id, &coll, "fr").await?;

        let mut trx = begin_locked(&db, p.project_id).await?;
        let at = trx.locked_at();
        let conn = trx.get(Grant::asserted("dispatch mint test"));
        let ready = vec![lfr.clone()];
        let minted = mint_function_runs_on_dispatch(conn, &ready, at)
            .await?
            .minted;

        assert_eq!(minted.len(), 1, "the LFR is re-minted");
        assert_ne!(
            minted[0].fr.id, previous_pfr,
            "re-run mints a NEW PFR, distinct from the previous attempt"
        );
        Ok(())
    }
}
