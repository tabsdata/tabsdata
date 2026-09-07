//
// Copyright 2026 Tabsdata Inc.
//

use crate::logical::replay::{
    assert_lfr_terminal, assert_no_concurrent_replay, build_replay_response,
    load_current_function_run_id, mint_replay_function_run, mint_replay_table_data_versions,
    resolve_replay_function_version_id,
};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_database::access::{AcrossProjects, DbPool};
use td_error::TdError;
use td_execution::SchedulerNotify;
use td_objects::dxo::crudl::CreateRequest;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunParam, LogicalFunctionRunReplayResponse,
};
use td_queries::Queries;

#[service_factory]
async fn replay_logical_function_run(
    authz: Ctx<AuthzContext>,
    scheduler_notify: Ctx<SchedulerNotify>,
    db: DbPool<AcrossProjects>,
    request: CreateRequest<(), LogicalFunctionRunParam>,
) -> Result<LogicalFunctionRunReplayResponse, TdError> {
    let (ctx, param, _) = request.into_parts();
    let at = ctx.time;
    let lfr_id = param.logical_function_run;

    let (project, grant) = {
        let mut trx = db.read().await?;
        let lfr: LogicalFunctionRunDB = trx.get(authz.none()).fetch_one(&lfr_id).await?;
        let project = lfr.project_id;
        // before the exclusive lock: unauthorized callers must not queue on it
        let grant = authz
            .require::<Project, DevelopProject>(&mut trx, &ctx, &project)
            .await?;
        (project, grant)
    };
    let mut trx = db.in_project(&project).begin_exclusive().await?;
    let locked_at = trx.locked_at();
    let conn = trx.get(grant);
    let lfr: LogicalFunctionRunDB = conn.fetch_one(&lfr_id).await?;

    // The concurrent-replay gate only matters for sticky-Committed LFRs;
    // other terminal statuses cannot have a prior replay.
    assert_lfr_terminal(&lfr)?;
    let bound = load_current_function_run_id(conn, &lfr).await?;
    assert_no_concurrent_replay(conn, &lfr).await?;
    let runnable = resolve_replay_function_version_id(conn, &lfr, &at).await?;
    let fr_new = mint_replay_function_run(conn, &lfr, &runnable, locked_at).await?;
    mint_replay_table_data_versions(conn, &lfr, &runnable, &fr_new, locked_at).await?;
    let response = build_replay_response(&lfr, &bound, &fr_new, &runnable)?;
    trx.commit().await?;

    // Wake the scheduler so the next tick fires immediately instead of
    // waiting for the periodic interval.
    scheduler_notify.notify_scheduler();
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use td_database::access::LockInstant;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::and;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{FunctionPatchInsertDB, FunctionRegister};
    use td_objects::dxo::logical::LogicalFunctionRunInsertDB;
    use td_objects::dxo::logical::LogicalTableDataVersionDB;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunInsertDB;
    use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, Description, FunctionRuntimeValues,
        FunctionStatus, FunctionVersionId, LogicalFunctionRunStatus, PhysicalFunctionRunId,
        PhysicalFunctionRunStatus, ProjectId, RawSnippet, Snippet, TableNameDto, TransactionKey,
        UserId,
    };
    use td_objects::types::basic::{AtTime, LogicalFunctionRunId, Patch, ReuseFrozen};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    fn replay_request(lfr_id: &LogicalFunctionRunId) -> CreateRequest<(), LogicalFunctionRunParam> {
        let param = LogicalFunctionRunParam {
            logical_function_run: lfr_id.clone(),
        };
        RequestContext::with(AccessTokenId::default(), UserId::admin()).create(param, ())
    }

    /// Seed a function with one output table and an LFR + FR at the given
    /// LFR status; returns `(LFR, FR, head_fvid)`.
    async fn seed_runnable(
        db: &TdPool,
        project_id: ProjectId,
        coll_name: &str,
        fn_name: &str,
        status: LogicalFunctionRunStatus,
    ) -> (
        LogicalFunctionRunDB,
        PhysicalFunctionRunDB,
        FunctionVersionId,
    ) {
        let coll = db
            .scoped(project_id)
            .seed()
            .collection(
                &CollectionName::try_from(coll_name).unwrap(),
                &UserId::admin(),
            )
            .await
            .unwrap();
        let create = FunctionRegister {
            name: fn_name.try_into().unwrap(),
            description: "d".try_into().unwrap(),
            bundle_id: BundleId::default(),
            snippet: "s".try_into().unwrap(),
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("t0").unwrap()]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv").unwrap(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let func = db
            .scoped(project_id)
            .seed()
            .function(&coll, &create)
            .await
            .unwrap();
        let exec = db.scoped(project_id).seed().execution(&func).await.unwrap();
        let trx_key = TransactionKey::try_from("ANY").unwrap();
        let trx = db
            .scoped(project_id)
            .seed()
            .transaction(&exec, &trx_key)
            .await
            .unwrap();
        let fr = db
            .scoped(project_id)
            .seed()
            .physical_function_run(&coll, &func, &exec, &trx, &status)
            .await
            .unwrap();
        let lfr = db
            .fetch_one::<LogicalFunctionRunDB>(&fr.base.logical_function_run_id)
            .await
            .unwrap();
        (lfr, fr, func.base.id)
    }

    async fn seed_patch(
        db: &TdPool,
        project_id: ProjectId,
        head: &td_objects::dxo::function::FunctionDB,
    ) -> Result<FunctionVersionId, TdError> {
        let patch = FunctionPatchInsertDB {
            id: FunctionVersionId::new(),
            parent_function_version_id: head.base.id.clone().into(),
            function_id: head.base.function_id,
            collection_id: head.base.collection_id,
            snippet: Snippet::try_from(RawSnippet::try_from("patched").unwrap()).unwrap(),
            bundle_id: BundleId::default(),
            runtime_values: FunctionRuntimeValues::try_from("rv-patch").unwrap(),
            description: Description::try_from("patch").unwrap(),
            defined_by_id: UserId::admin(),
            status: FunctionStatus::Active,
        };
        db.scoped(project_id)
            .scd2()
            .open(&patch, LockInstant::asserted(*AtTime::now()))
            .await
            .unwrap();
        Ok(patch.id)
    }

    /// Replay against a Committed LFR — happy path: a new
    /// PhysicalFunctionRunDB is minted under the same LFR and becomes its
    /// current attempt, LFR.status stays Committed, and the scheduler picks
    /// the new PFR up via `PhysicalRuns::to_dispatch`.
    #[td_test::test(db)]
    async fn test_replay_committed_mints_new_fr(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (lfr, fr_old, _head) =
            seed_runnable(&db, p.id, "c0", "f0", LogicalFunctionRunStatus::Committed).await;

        let service = Services::with_defaults(db.clone());
        let response = service
            .replay_logical_function_run(replay_request(&lfr.base.id))
            .await?
            .data;

        assert_eq!(response.logical_function_run_id, lfr.base.id);
        assert_eq!(response.previous_function_run_id, fr_old.base.id);
        assert_ne!(response.new_function_run_id, fr_old.base.id);

        let frs: Vec<PhysicalFunctionRunDB> = db
            .timeline()
            .fetch_all::<PhysicalFunctionRunDB>(and!(&lfr.base.execution_id, &lfr.base.function_id))
            .await?;
        assert_eq!(frs.len(), 2);

        let lfr_after = db.fetch_one::<LogicalFunctionRunDB>(&lfr.base.id).await?;
        assert_eq!(lfr_after.base.status, LogicalFunctionRunStatus::Committed);

        // The replay attempt is the run's current version from mint.
        let current = db
            .scd2()
            .fetch_one::<PhysicalFunctionRunDB>(&lfr.base.id)
            .await?;
        assert_eq!(current.base.id, response.new_function_run_id);

        let pfr_new = db
            .timeline()
            .fetch_one::<PhysicalFunctionRunDB>(&response.new_function_run_id)
            .await?;
        assert_eq!(pfr_new.base.status, PhysicalFunctionRunStatus::Requested);

        Ok(())
    }

    /// Replay against a Done LFR (terminal but not yet committed) — also
    /// allowed; mints a new FR exactly like the Committed case.
    #[td_test::test(db)]
    async fn test_replay_done_mints_new_fr(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (lfr, fr_old, _head) =
            seed_runnable(&db, p.id, "c0", "f0", LogicalFunctionRunStatus::Done).await;

        let response = Services::with_defaults(db.clone())
            .replay_logical_function_run(replay_request(&lfr.base.id))
            .await?
            .data;

        assert_ne!(response.new_function_run_id, fr_old.base.id);
        let lfr_after = db.fetch_one::<LogicalFunctionRunDB>(&lfr.base.id).await?;
        assert_eq!(lfr_after.base.status, LogicalFunctionRunStatus::Done);
        Ok(())
    }

    /// Replay against an in-flight LFR (Running) — refused.
    #[td_test::test(db)]
    async fn test_replay_in_flight_refused(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (lfr, _fr, _head) =
            seed_runnable(&db, p.id, "c0", "f0", LogicalFunctionRunStatus::Running).await;

        let result = Services::with_defaults(db.clone())
            .replay_logical_function_run(replay_request(&lfr.base.id))
            .await;

        assert!(result.is_err(), "expected refusal, got {result:?}");
        Ok(())
    }

    /// A `Dispatched` attempt is in flight too: replay refused, no new
    /// attempt minted. Guards the gate against the full IN_FLIGHT set —
    /// a dispatched-but-not-yet-running attempt used to be invisible to it.
    #[td_test::test(db)]
    async fn test_replay_dispatched_refused(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (lfr, fr, _head) =
            seed_runnable(&db, p.id, "c0", "f0", LogicalFunctionRunStatus::Committed).await;
        let dispatched = PhysicalFunctionRunInsertDB {
            id: PhysicalFunctionRunId::new(),
            status: PhysicalFunctionRunStatus::Dispatched,
            ..fr.base.clone()
        };
        db.scoped(p.id)
            .scd2()
            .open(&dispatched, LockInstant::asserted(*AtTime::now()))
            .await?;

        let result = Services::with_defaults(db.clone())
            .replay_logical_function_run(replay_request(&lfr.base.id))
            .await;

        assert!(result.is_err(), "expected refusal, got {result:?}");
        let attempts: Vec<PhysicalFunctionRunDB> = db
            .timeline()
            .fetch_all::<PhysicalFunctionRunDB>(and!(&fr.base.execution_id, &fr.base.function_id))
            .await?;
        assert_eq!(attempts.len(), 2, "no new attempt minted");
        Ok(())
    }

    /// Replay picks up the latest patch of the LFR's planned fvid.
    /// Register a patch after the LFR is Committed; replay should
    /// mint FR_new with head fvid intact in `function_version_id` AND
    /// `function_version_patch_id = patch.id`. The response's
    /// `function_version_id` advertises the effective fvid (patch).
    #[td_test::test(db)]
    async fn test_replay_picks_up_latest_patch(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let coll = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            name: "f0".try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("t0")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let head = db.scoped(p.id).seed().function(&coll, &create).await?;
        let exec = db.scoped(p.id).seed().execution(&head).await?;
        let trx = db
            .scoped(p.id)
            .seed()
            .transaction(&exec, &TransactionKey::try_from("ANY")?)
            .await?;
        let fr_old = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &coll,
                &head,
                &exec,
                &trx,
                &LogicalFunctionRunStatus::Committed,
            )
            .await?;
        let lfr_old: LogicalFunctionRunDB = db
            .fetch_one::<LogicalFunctionRunDB>(and!(
                &fr_old.base.execution_id,
                &fr_old.base.function_id
            ))
            .await?;
        let lfr_id = lfr_old.base.id.clone();

        let patch_id = seed_patch(&db, p.id, &head).await?;

        let response = Services::with_defaults(db.clone())
            .replay_logical_function_run(replay_request(&lfr_id))
            .await?
            .data;

        assert_eq!(response.function_version_id, patch_id);

        let fr_new: PhysicalFunctionRunDB = db
            .timeline()
            .fetch_one::<PhysicalFunctionRunDB>(&response.new_function_run_id)
            .await?;
        // FR.function_version_id stays at the head fvid (FK valid);
        // the patch id is recorded on the dedicated column.
        assert_eq!(fr_new.base.function_version_id, head.base.id);
        assert_eq!(
            fr_new.base.function_version_patch_id,
            Some(Patch::from(patch_id.clone()))
        );

        // The minted TDV mirrors the FR's split: head fvid in the bare
        // column, patch id on the dedicated column.
        let tdvs: Vec<PhysicalTableDataVersionDB> = db
            .timeline()
            .fetch_all::<PhysicalTableDataVersionDB>(&fr_new.base.id)
            .await?;
        assert_eq!(tdvs.len(), 1);
        assert_eq!(tdvs[0].base.function_version_id, head.base.id);
        assert_eq!(
            tdvs[0].base.function_version_patch_id,
            Some(Patch::from(patch_id.clone()))
        );

        Ok(())
    }

    /// Replay against an LFR with no attempt — refused with NoAttempt. The
    /// fixture inserts only the LFR identity row, Committed so the terminal
    /// check passes and only the attempt gate rejects.
    #[td_test::test(db)]
    async fn test_replay_no_attempt_refused(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let coll = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            name: "f0".try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("t0")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let head = db.scoped(p.id).seed().function(&coll, &create).await?;
        let exec = db.scoped(p.id).seed().execution(&head).await?;
        let trx = db
            .scoped(p.id)
            .seed()
            .transaction(&exec, &TransactionKey::try_from("ANY")?)
            .await?;
        // Build the LFR directly (skip seed_function_run, which appends a
        // Bound event we don't want here) and append InitialMint Pending.
        let lfr = LogicalFunctionRunInsertDB {
            id: LogicalFunctionRunId::new(),
            collection_id: coll.id,
            execution_id: exec.base.id,
            transaction_id: trx.base.id,
            function_id: head.base.function_id,
            function_version_id: head.base.id,
            trigger: td_objects::types::basic::Trigger::Manual,
            triggered_on: trx.base.triggered_on,
            triggered_by_id: trx.base.triggered_by_id,
            status: LogicalFunctionRunStatus::Committed,
            group_id: coll.group_id,
        };
        // No attempt at all, so the run has no binding to replay.
        db.scoped(p.id)
            .insert::<LogicalFunctionRunInsertDB>(&lfr)
            .await?;

        let result = Services::with_defaults(db.clone())
            .replay_logical_function_run(replay_request(&lfr.id))
            .await;
        assert!(result.is_err(), "expected NoAttempt, got {result:?}");
        Ok(())
    }

    /// Suppress unused-import warnings for types used in only some tests.
    #[allow(dead_code)]
    fn _imports_alive(_: LogicalTableDataVersionDB, _: TableDB) {}
}
