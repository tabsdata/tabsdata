//
// Copyright 2026 Tabs Data Inc.
//

use crate::logical::layers::replay::{
    assert_lfr_terminal, assert_no_concurrent_replay, build_replay_response,
    extract_collection_id_from_lfr, load_bound_function_run_id, mint_replay_function_run,
    mint_replay_table_data_versions, resolve_replay_function_version_id,
};
use crate::scheduler::layers::request::notify_scheduler;
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, CollDev, CollExec};
use td_authz::{Authz, AuthzContext};
use td_execution::SchedulerNotify;
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunReplayResponse};
use td_objects::rest_urls::LogicalFunctionRunParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{AtTime, CollectionId, LogicalFunctionRunId};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ReplayLogicalFunctionRunService,
    request = CreateRequest<LogicalFunctionRunParam, ()>,
    response = LogicalFunctionRunReplayResponse,
    connection = TransactionProvider,
    scope = "Project",
    context = AuthzContext,
    context = SchedulerNotify,
)]
fn service() {
    layers!(
        // Pull the request envelope apart.
        from_fn(With::<CreateRequest<LogicalFunctionRunParam, ()>>::extract::<RequestContext>),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(
            With::<CreateRequest<LogicalFunctionRunParam, ()>>::extract_name::<
                LogicalFunctionRunParam,
            >
        ),
        from_fn(With::<LogicalFunctionRunParam>::extract::<LogicalFunctionRunId>),
        // Load the LFR identity row and authz on its collection.
        from_fn(By::<LogicalFunctionRunId>::fetch_one::<LogicalFunctionRunDB>),
        from_fn(extract_collection_id_from_lfr),
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec>::check),
        // Validate replay preconditions: LFR.status terminal + pointer Bound +
        // no concurrent replay (only meaningful when LFR is sticky-Committed —
        // for other terminal statuses no prior replay can exist).
        from_fn(assert_lfr_terminal),
        from_fn(load_bound_function_run_id),
        from_fn(assert_no_concurrent_replay),
        // Resolve the patch to run on this attempt.
        from_fn(resolve_replay_function_version_id),
        // Mint FR_new + TDV_new placeholders. The pointer log is
        // untouched until the worker callback. Dispatch readiness keys
        // off `Pfr::to_dispatch` (status=RR + no WorkerDB).
        from_fn(mint_replay_function_run),
        from_fn(mint_replay_table_data_versions),
        from_fn(build_replay_response),
        // Wake the scheduler so the next tick fires immediately
        // instead of waiting for the periodic interval.
        from_fn(notify_scheduler),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::and;
    use td_objects::dxo::function::{FunctionPatchDB, FunctionRegister};
    use td_objects::dxo::logical::{
        LogicalFunctionRunPointerDB, LogicalPointerDao, LogicalTableDataVersionDB,
        LogicalTableDataVersionPointerDB,
    };
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
    use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, Description,
        FunctionRuntimeValues, FunctionStatus, FunctionVersionId, LogicalFunctionRunStatus,
        LogicalPointerReason, LogicalPointerStatus, PhysicalFunctionRunStatus, ProjectId, RoleId,
        Snippet, TableNameDto, TransactionKey, UserId,
    };
    use td_queries::test_utils::SeedEntry;
    use td_queries::{LiveEntry, VersionedEntry};
    use td_tower::ctx_service::RawOneshot;
    use td_database::scoped::InProject;
    use td_database::scoped::Scoped;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_replay(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ReplayLogicalFunctionRunService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<CreateRequest<LogicalFunctionRunParam, ()>, ProjectId>, LogicalFunctionRunReplayResponse,>(&[
                // Pull the request envelope apart.
                type_of_val(&With::<CreateRequest<LogicalFunctionRunParam, ()>>::extract::<RequestContext>),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(&
                    With::<CreateRequest<LogicalFunctionRunParam, ()>>::extract_name::<
                        LogicalFunctionRunParam,
                    >
                ),
                type_of_val(&With::<LogicalFunctionRunParam>::extract::<LogicalFunctionRunId>),
                // Load the LFR identity row and authz on its collection.
                type_of_val(&By::<LogicalFunctionRunId>::fetch_one::<LogicalFunctionRunDB>),
                type_of_val(&extract_collection_id_from_lfr),
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec>::check),
                // Validate replay preconditions: LFR.status terminal + pointer Bound +
                // no concurrent replay (only meaningful when LFR is sticky-Committed —
                // for other terminal statuses no prior replay can exist).
                type_of_val(&assert_lfr_terminal),
                type_of_val(&load_bound_function_run_id),
                type_of_val(&assert_no_concurrent_replay),
                // Resolve the patch to run on this attempt.
                type_of_val(&resolve_replay_function_version_id),
                // Mint FR_new + TDV_new placeholders.
                type_of_val(&mint_replay_function_run),
                type_of_val(&mint_replay_table_data_versions),
                type_of_val(&build_replay_response),
                type_of_val(&notify_scheduler),
            ]);
    }

    /// Build a CreateRequest hitting the replay endpoint for an LFR.
    fn replay_request(
        collection: &CollectionName,
        lfr_id: &LogicalFunctionRunId,
    ) -> Scoped<CreateRequest<LogicalFunctionRunParam, ()>, ProjectId> {
        let param = LogicalFunctionRunParam::builder()
            .collection(CollectionIdName::from_str(&collection.to_string()).unwrap())
            .logical_function_run(lfr_id.clone())
            .build()
            .unwrap();
        RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
            .create(param, ())
            .in_project(ProjectId::sandbox())
    }

    /// Seed a function with one output table, run plan-build proxy (seed
    /// LFR + FR + Bound pointer) at the given LFR status, and return
    /// `(LFR, FR, head_fvid)`.
    async fn seed_runnable(
        db: &TdPool,
        coll_name: &str,
        fn_name: &str,
        status: LogicalFunctionRunStatus,
    ) -> (
        LogicalFunctionRunDB,
        PhysicalFunctionRunDB,
        FunctionVersionId,
    ) {
        let coll = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(
                &CollectionName::try_from(coll_name).unwrap(),
                &UserId::admin(),
            )
            .await
            .unwrap();
        let create = FunctionRegister::builder()
            .try_name(fn_name)
            .unwrap()
            .try_description("d")
            .unwrap()
            .bundle_id(BundleId::default())
            .try_snippet("s")
            .unwrap()
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("t0").unwrap()]))
            .runtime_values(FunctionRuntimeValues::try_from("rv").unwrap())
            .reuse_frozen_tables(false)
            .build()
            .unwrap();
        let func = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&coll, &create)
            .await
            .unwrap();
        let exec = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&func)
            .await
            .unwrap();
        let txn_key = TransactionKey::try_from("ANY").unwrap();
        let txn = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&exec, &txn_key)
            .await
            .unwrap();
        let fr = db
            .scoped(ProjectId::sandbox())
            .seed()
            .physical_function_run(&coll, &func, &exec, &txn, &status)
            .await
            .unwrap();
        let lfr = db
            .live()
            .fetch_one::<LogicalFunctionRunDB>(and!(&fr.execution_id, &fr.function_id))
            .await
            .unwrap();
        (lfr, fr, func.id)
    }

    /// Insert a patch of the given head fvid and return its id.
    async fn seed_patch(
        db: &TdPool,
        head: &td_objects::dxo::function::FunctionDB,
    ) -> FunctionVersionId {
        let patch = FunctionPatchDB::builder()
            .parent_function_version_id(head.id.clone())
            .function_id(head.function_id)
            .collection_id(head.collection_id)
            .snippet(Snippet::try_from("patched").unwrap())
            .bundle_id(BundleId::default())
            .runtime_values(FunctionRuntimeValues::try_from("rv-patch").unwrap())
            .description(Description::try_from("patch").unwrap())
            .defined_on(td_objects::types::basic::AtTime::now())
            .defined_by_id(UserId::admin())
            .status(FunctionStatus::Active)
            .build()
            .unwrap();
        db.scoped(ProjectId::sandbox())
            .live()
            .insert(&patch)
            .await
            .unwrap();
        patch.id
    }

    /// Replay against a Committed LFR — happy path. Asserts: a new
    /// PhysicalFunctionRunDB is minted under the same LFR; a
    /// `Requested(FR_new)` LFR pointer event is appended (the scheduler
    /// picks this up at the next tick to dispatch a worker); LFR.status
    /// stays Committed; the previous `Bound(FR_old)` remains the
    /// latest-Bound binding for downstream readers.
    #[td_test::test(db)]
    async fn test_replay_committed_mints_new_fr(db: TdPool) -> Result<(), TdError> {
        let (lfr, fr_old, _head) =
            seed_runnable(&db, "c0", "f0", LogicalFunctionRunStatus::Committed).await;

        let coll_name = CollectionName::try_from("c0")?;
        let service = ReplayLogicalFunctionRunService::with_defaults(db.clone()).service();
        let response = service
            .raw_oneshot(replay_request(&coll_name, &lfr.id))
            .await?;

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
        let (lfr, fr_old, _head) =
            seed_runnable(&db, "c0", "f0", LogicalFunctionRunStatus::Done).await;

        let coll_name = CollectionName::try_from("c0")?;
        let response = ReplayLogicalFunctionRunService::with_defaults(db.clone())
            .service()
            .raw_oneshot(replay_request(&coll_name, &lfr.id))
            .await?;

        assert_ne!(response.new_function_run_id, fr_old.id);
        let lfr_after = db.live().fetch_one::<LogicalFunctionRunDB>(&lfr.id).await?;
        assert_eq!(lfr_after.status, LogicalFunctionRunStatus::Done);
        Ok(())
    }

    /// Replay against an in-flight LFR (Running) — refused.
    #[td_test::test(db)]
    async fn test_replay_in_flight_refused(db: TdPool) -> Result<(), TdError> {
        let (lfr, _fr, _head) =
            seed_runnable(&db, "c0", "f0", LogicalFunctionRunStatus::Running).await;

        let coll_name = CollectionName::try_from("c0")?;
        let result = ReplayLogicalFunctionRunService::with_defaults(db.clone())
            .service()
            .raw_oneshot(replay_request(&coll_name, &lfr.id))
            .await;

        assert!(result.is_err(), "expected refusal, got {result:?}");
        Ok(())
    }

    /// Replay picks up the latest patch of the LFR's planned fvid.
    /// Register a patch after the LFR is Committed; replay should
    /// mint FR_new with head fvid intact in `function_version_id` AND
    /// `function_version_patch_id = patch.id`. The response's
    /// `function_version_id` advertises the effective fvid (patch).
    #[td_test::test(db)]
    async fn test_replay_picks_up_latest_patch(db: TdPool) -> Result<(), TdError> {
        // Seed a Committed LFR for a head function.
        let coll = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .try_name("f0")?
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("t0")?]))
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .reuse_frozen_tables(false)
            .build()?;
        let head = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&coll, &create)
            .await?;
        let exec = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&head)
            .await?;
        let txn = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&exec, &TransactionKey::try_from("ANY")?)
            .await?;
        let fr_old = db
            .scoped(ProjectId::sandbox())
            .seed()
            .physical_function_run(
                &coll,
                &head,
                &exec,
                &txn,
                &LogicalFunctionRunStatus::Committed,
            )
            .await?;
        let lfr_old: LogicalFunctionRunDB = db
            .live()
            .fetch_one::<LogicalFunctionRunDB>(and!(&fr_old.execution_id, &fr_old.function_id))
            .await?;
        let lfr_id = lfr_old.id.clone();

        // Register a patch of the head fvid AFTER the LFR is committed.
        let patch_id = seed_patch(&db, &head).await;

        // Replay: the minted FR_new must run the patch, not the head.
        let coll_name = CollectionName::try_from("c0")?;
        let response = ReplayLogicalFunctionRunService::with_defaults(db.clone())
            .service()
            .raw_oneshot(replay_request(&coll_name, &lfr_id))
            .await?;

        // The response advertises the effective (patch) fvid.
        assert_eq!(response.function_version_id, patch_id);

        let fr_new: PhysicalFunctionRunDB = db
            .live()
            .fetch_one::<PhysicalFunctionRunDB>(&response.new_function_run_id)
            .await?;
        // FR.function_version_id stays at the head fvid (FK valid);
        // the patch id is recorded on the dedicated column.
        assert_eq!(fr_new.function_version_id, head.id);
        assert_eq!(fr_new.function_version_patch_id, Some(patch_id.clone()));

        // The minted TDV mirrors the FR's split: head fvid in the bare
        // column, patch id on the dedicated column.
        let tdvs: Vec<PhysicalTableDataVersionDB> = db
            .live()
            .fetch_all::<PhysicalTableDataVersionDB>(&fr_new.id)
            .await?;
        assert_eq!(tdvs.len(), 1);
        assert_eq!(tdvs[0].function_version_id, head.id);
        assert_eq!(tdvs[0].function_version_patch_id, Some(patch_id));

        Ok(())
    }

    /// Replay against an LFR whose pointer was never Bound (newly minted,
    /// InitialMint Pending only) — refused with NotBound. The fixture
    /// inserts an LFR identity row and a Pending pointer; status is
    /// Committed so the terminal check passes — only the pointer-Bound
    /// gate should reject.
    #[td_test::test(db)]
    async fn test_replay_not_bound_refused(db: TdPool) -> Result<(), TdError> {
        let coll = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let create = FunctionRegister::builder()
            .try_name("f0")?
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(Some(vec![TableNameDto::try_from("t0")?]))
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .reuse_frozen_tables(false)
            .build()?;
        let head = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&coll, &create)
            .await?;
        let exec = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&head)
            .await?;
        let txn = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&exec, &TransactionKey::try_from("ANY")?)
            .await?;
        // Build the LFR directly (skip seed_function_run, which appends a
        // Bound event we don't want here) and append InitialMint Pending.
        let lfr = LogicalFunctionRunDB::builder()
            .project_id(coll.project_id)
            .group_id(coll.group_id)
            .collection_id(coll.id)
            .execution_id(&exec.id)
            .transaction_id(&txn.id)
            .function_id(head.function_id)
            .function_version_id(&head.id)
            .trigger(td_objects::types::basic::Trigger::Manual)
            .triggered_on(txn.triggered_on)
            .triggered_by_id(txn.triggered_by_id)
            .status(LogicalFunctionRunStatus::Committed)
            .build()?;
        db.scoped(ProjectId::sandbox())
            .live()
            .insert::<LogicalFunctionRunDB>(&lfr)
            .await?;
        let pending = LogicalFunctionRunPointerDB::build_event(
            lfr.project_id,
            &lfr.id,
            LogicalPointerStatus::Pending,
            None,
            td_objects::types::basic::AtTime::try_from(txn.triggered_on)?,
            txn.triggered_by_id,
            LogicalPointerReason::InitialMint,
        )?;
        db.scoped(ProjectId::sandbox())
            .live()
            .insert::<LogicalFunctionRunPointerDB>(&pending)
            .await?;

        let coll_name = CollectionName::try_from("c0")?;
        let result = ReplayLogicalFunctionRunService::with_defaults(db.clone())
            .service()
            .raw_oneshot(replay_request(&coll_name, &lfr.id))
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
