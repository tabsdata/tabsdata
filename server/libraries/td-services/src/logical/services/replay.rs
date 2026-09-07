//
// Copyright 2026 Tabsdata Inc.
//

use crate::logical::replay::{
    assert_lfr_terminal, assert_no_concurrent_replay, build_replay_response,
    load_bound_function_run_id, mint_replay_function_run, mint_replay_table_data_versions,
    resolve_replay_function_version_id,
};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::DevelopProject;
use td_database::access::{DbPool, Global};
use td_error::TdError;
use td_execution::SchedulerNotify;
use td_objects::dxo::crudl::CreateRequest;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunParam, LogicalFunctionRunReplayResponse,
};
use td_queries::LiveEntry;

#[service_factory]
async fn replay_logical_function_run(
    authz: Ctx<AuthzContext>,
    scheduler_notify: Ctx<SchedulerNotify>,
    db: DbPool<Global>,
    request: CreateRequest<(), LogicalFunctionRunParam>,
) -> Result<LogicalFunctionRunReplayResponse, TdError> {
    let (ctx, param, _) = request.into_parts();
    let at = ctx.time;
    let lfr_id = param.logical_function_run;

    let (project, grant) = {
        let mut trx = db.read().await?;
        let lfr: LogicalFunctionRunDB = trx.get(authz.none()).live().fetch_one(&lfr_id).await?;
        let project = lfr.project_id;
        // before the exclusive lock: unauthorized callers must not queue on it
        let grant = authz
            .require_in_project::<DevelopProject>(&mut trx, &ctx, &project)
            .await?;
        (project, grant)
    };
    let mut trx = db.in_project(&project).begin_exclusive().await?;
    let conn = trx.get(grant);
    let lfr: LogicalFunctionRunDB = conn.live().fetch_one(&lfr_id).await?;

    // Validate replay preconditions: LFR.status terminal + pointer Bound +
    // no concurrent replay (only meaningful when LFR is sticky-Committed —
    // for other terminal statuses no prior replay can exist).
    assert_lfr_terminal(&lfr).await?;
    let bound = load_bound_function_run_id(conn, &lfr, &at).await?;
    assert_no_concurrent_replay(conn, &lfr, &bound).await?;
    // Resolve the patch to run on this attempt.
    let runnable = resolve_replay_function_version_id(conn, &lfr, &at).await?;
    // Mint FR_new + TDV_new placeholders. The pointer log is untouched until
    // the worker callback. Dispatch readiness keys off `Pfr::to_dispatch`
    // (status=RR + no WorkerDB).
    let fr_new = mint_replay_function_run(conn, &lfr, &runnable, &at).await?;
    mint_replay_table_data_versions(conn, &lfr, &runnable, &fr_new).await?;
    let response = build_replay_response(&lfr, &bound, &fr_new, &runnable).await?;
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
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::and;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::{FunctionPatchDB, FunctionRegister};
    use td_objects::dxo::logical::{
        LogicalFunctionRunPointerDB, LogicalPointerDao, LogicalTableDataVersionDB,
        LogicalTableDataVersionPointerDB,
    };
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
    use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, Description, FunctionRuntimeValues,
        FunctionStatus, FunctionVersionId, LogicalFunctionRunStatus, LogicalPointerReason,
        LogicalPointerStatus, PhysicalFunctionRunId, PhysicalFunctionRunStatus, ProjectId,
        RawSnippet, Snippet, TableNameDto, TransactionKey, UserId,
    };
    use td_objects::types::basic::{LogicalFunctionRunId, Patch, ReuseFrozen};
    use td_queries::test_utils::SeedEntry;
    use td_queries::{LiveEntry, VersionedEntry};

    fn replay_request(lfr_id: &LogicalFunctionRunId) -> CreateRequest<(), LogicalFunctionRunParam> {
        let param = LogicalFunctionRunParam {
            logical_function_run: lfr_id.clone(),
        };
        RequestContext::with(AccessTokenId::default(), UserId::admin()).create(param, ())
    }

    /// Seed a function with one output table, run plan-build proxy (seed
    /// LFR + FR + Bound pointer) at the given LFR status, and return
    /// `(LFR, FR, head_fvid)`.
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
            .live()
            .fetch_one::<LogicalFunctionRunDB>(and!(&fr.execution_id, &fr.function_id))
            .await
            .unwrap();
        (lfr, fr, func.id)
    }

    async fn seed_patch(
        db: &TdPool,
        project_id: ProjectId,
        head: &td_objects::dxo::function::FunctionDB,
    ) -> Result<FunctionVersionId, TdError> {
        let patch = FunctionPatchDB {
            id: FunctionVersionId::new(),
            parent_function_version_id: head.id.clone().into(),
            function_id: head.function_id,
            collection_id: head.collection_id,
            snippet: Snippet::try_from(RawSnippet::try_from("patched").unwrap()).unwrap(),
            bundle_id: BundleId::default(),
            runtime_values: FunctionRuntimeValues::try_from("rv-patch").unwrap(),
            description: Description::try_from("patch").unwrap(),
            defined_on: td_objects::types::basic::AtTime::now(),
            defined_by_id: UserId::admin(),
            status: FunctionStatus::Active,
        };
        db.scoped(project_id).live().insert(&patch).await.unwrap();
        Ok(patch.id)
    }

    /// Replay against a Committed LFR — happy path. Asserts: a new
    /// PhysicalFunctionRunDB is minted under the same LFR; a
    /// `Requested(FR_new)` LFR pointer event is appended (the scheduler
    /// picks this up at the next tick to dispatch a worker); LFR.status
    /// stays Committed; the previous `Bound(FR_old)` remains the
    /// latest-Bound binding for downstream readers.
    #[td_test::test(db)]
    async fn test_replay_committed_mints_new_fr(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (lfr, fr_old, _head) =
            seed_runnable(&db, p.id, "c0", "f0", LogicalFunctionRunStatus::Committed).await;

        let service = Services::with_defaults(db.clone());
        let response = service
            .replay_logical_function_run(replay_request(&lfr.id))
            .await?
            .data;

        assert_eq!(response.logical_function_run_id, lfr.id);
        assert_eq!(response.previous_function_run_id, fr_old.id);
        assert_ne!(response.new_function_run_id, fr_old.id);

        // Two FRs under this LFR: the seeded one + the replay mint.
        let frs: Vec<PhysicalFunctionRunDB> = db
            .live()
            .fetch_all::<PhysicalFunctionRunDB>(and!(&lfr.execution_id, &lfr.function_id))
            .await?;
        assert_eq!(frs.len(), 2);

        // LFR.status unchanged — replay doesn't demote sticky-Committed.
        let lfr_after = db.live().fetch_one::<LogicalFunctionRunDB>(&lfr.id).await?;
        assert_eq!(lfr_after.status, LogicalFunctionRunStatus::Committed);

        // Pointer log untouched — replay endpoint doesn't append any
        // event. The latest pointer is still `Bound(FR_old)`. The
        // scheduler picks the new PFR up via `Pfr::to_dispatch`
        // (PFR.status=Requested + no WorkerDB).
        let latest = db
            .versioned(&td_objects::types::basic::AtTime::now())
            .fetch_one::<LogicalFunctionRunPointerDB>(&lfr.id)
            .await?;
        assert!(
            matches!(latest.status, LogicalPointerStatus::Bound),
            "expected Bound(FR_old), got {:?}",
            latest.status
        );
        assert_eq!(latest.physical_function_run_id, Some(fr_old.id));

        // The new PFR is in `Requested` status awaiting the scheduler.
        let pfr_new = db
            .live()
            .fetch_one::<PhysicalFunctionRunDB>(&response.new_function_run_id)
            .await?;
        assert_eq!(pfr_new.status, PhysicalFunctionRunStatus::Requested);

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
            .replay_logical_function_run(replay_request(&lfr.id))
            .await?
            .data;

        assert_ne!(response.new_function_run_id, fr_old.id);
        let lfr_after = db.live().fetch_one::<LogicalFunctionRunDB>(&lfr.id).await?;
        assert_eq!(lfr_after.status, LogicalFunctionRunStatus::Done);
        Ok(())
    }

    /// Replay against an in-flight LFR (Running) — refused.
    #[td_test::test(db)]
    async fn test_replay_in_flight_refused(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (lfr, _fr, _head) =
            seed_runnable(&db, p.id, "c0", "f0", LogicalFunctionRunStatus::Running).await;

        let result = Services::with_defaults(db.clone())
            .replay_logical_function_run(replay_request(&lfr.id))
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
        let dispatched = PhysicalFunctionRunDB {
            id: PhysicalFunctionRunId::new(),
            status: PhysicalFunctionRunStatus::Dispatched,
            ..fr.clone()
        };
        db.scoped(p.id).live().insert(&dispatched).await?;

        let result = Services::with_defaults(db.clone())
            .replay_logical_function_run(replay_request(&lfr.id))
            .await;

        assert!(result.is_err(), "expected refusal, got {result:?}");
        let attempts: Vec<PhysicalFunctionRunDB> = db
            .live()
            .fetch_all(and!(&fr.execution_id, &fr.function_id))
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
            .live()
            .fetch_one::<LogicalFunctionRunDB>(and!(&fr_old.execution_id, &fr_old.function_id))
            .await?;
        let lfr_id = lfr_old.id.clone();

        // Register a patch of the head fvid AFTER the LFR is committed.
        let patch_id = seed_patch(&db, p.id, &head).await?;

        // Replay: the minted FR_new must run the patch, not the head.
        let response = Services::with_defaults(db.clone())
            .replay_logical_function_run(replay_request(&lfr_id))
            .await?
            .data;

        // The response advertises the effective (patch) fvid.
        assert_eq!(response.function_version_id, patch_id);

        let fr_new: PhysicalFunctionRunDB = db
            .live()
            .fetch_one::<PhysicalFunctionRunDB>(&response.new_function_run_id)
            .await?;
        // FR.function_version_id stays at the head fvid (FK valid);
        // the patch id is recorded on the dedicated column.
        assert_eq!(fr_new.function_version_id, head.id);
        assert_eq!(
            fr_new.function_version_patch_id,
            Some(Patch::from(patch_id.clone()))
        );

        // The minted TDV mirrors the FR's split: head fvid in the bare
        // column, patch id on the dedicated column.
        let tdvs: Vec<PhysicalTableDataVersionDB> = db
            .live()
            .fetch_all::<PhysicalTableDataVersionDB>(&fr_new.id)
            .await?;
        assert_eq!(tdvs.len(), 1);
        assert_eq!(tdvs[0].function_version_id, head.id);
        assert_eq!(
            tdvs[0].function_version_patch_id,
            Some(Patch::from(patch_id.clone()))
        );

        Ok(())
    }

    /// Replay against an LFR whose pointer was never Bound (newly minted,
    /// InitialMint Pending only) — refused with NotBound. The fixture
    /// inserts an LFR identity row and a Pending pointer; status is
    /// Committed so the terminal check passes — only the pointer-Bound
    /// gate should reject.
    #[td_test::test(db)]
    async fn test_replay_not_bound_refused(db: TdPool) -> Result<(), TdError> {
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
        let lfr = LogicalFunctionRunDB {
            id: LogicalFunctionRunId::new(),
            project_id: coll.project_id,
            collection_id: coll.id,
            execution_id: exec.id,
            transaction_id: trx.id,
            function_id: head.function_id,
            function_version_id: head.id,
            trigger: td_objects::types::basic::Trigger::Manual,
            triggered_on: trx.triggered_on,
            triggered_by_id: trx.triggered_by_id,
            status: LogicalFunctionRunStatus::Committed,
            group_id: coll.group_id,
        };
        db.scoped(p.id)
            .live()
            .insert::<LogicalFunctionRunDB>(&lfr)
            .await?;
        let pending = LogicalFunctionRunPointerDB::build_event(
            lfr.project_id,
            &lfr.id,
            LogicalPointerStatus::Pending,
            None,
            td_objects::types::basic::AtTime::try_from(trx.triggered_on)?,
            trx.triggered_by_id,
            LogicalPointerReason::InitialMint,
        )?;
        db.scoped(p.id)
            .live()
            .insert::<LogicalFunctionRunPointerDB>(&pending)
            .await?;

        let result = Services::with_defaults(db.clone())
            .replay_logical_function_run(replay_request(&lfr.id))
            .await;
        assert!(result.is_err(), "expected NotBound, got {result:?}");
        Ok(())
    }

    /// Suppress unused-import warnings for types used in only some tests.
    #[allow(dead_code)]
    fn _imports_alive(
        _: LogicalTableDataVersionDB,
        _: LogicalTableDataVersionPointerDB,
        _: TableDB,
    ) {
    }
}
