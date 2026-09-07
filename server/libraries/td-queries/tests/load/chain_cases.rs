//
// Copyright 2026 Tabsdata Inc.
//

//! Chain-family load cases (`LogicalRuns` / `PhysicalRuns` / `Executions` / `Dependencies`)
//! over one seeded [`chain_dataset`] graph. Queries targeting
//! in-flight/blocked rows legitimately return empty on the all-`Done` graph —
//! the measured cost is the scan under the readiness predicate.

use std::time::Instant;

use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::ListParams;
use td_objects::dxo::execution::Execution;
use td_objects::list::params::ListQueryParams;
use td_objects::types::basic::AtTime;
use td_queries::Queries;

use crate::harness::{record, seed};

// Chain-table queries scale on the function-run count, so `seeded` reports
// that axis rather than the producer-LTDV axis the shared `seed` returns.

fn exec_list_params() -> Result<ListQueryParams<Execution>, TdError> {
    ListQueryParams::<Execution>::try_from(&ListParams::default())
}

#[td_test::test(db)]
async fn load_lfr_to_execute(db: TdPool) -> Result<(), TdError> {
    let (h, profile, _) = seed(&db).await?;
    let seeded = h.fr_ids.len();
    let t = Instant::now();
    let rows = db.logical_runs().to_execute(&h.at, 50).await?;
    record("lfr::to_execute", profile, t, rows.len(), seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_lfr_to_commit(db: TdPool) -> Result<(), TdError> {
    let (h, profile, _) = seed(&db).await?;
    let seeded = h.fr_ids.len();
    let t = Instant::now();
    let rows = db.logical_runs().to_commit(&h.execs).await?;
    record("lfr::to_commit", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty(), "all-Done transactions are committable");
    Ok(())
}

#[td_test::test(db)]
async fn load_lfr_previous_for_function_at(db: TdPool) -> Result<(), TdError> {
    let (h, profile, _) = seed(&db).await?;
    let seeded = h.fr_ids.len();
    let t = Instant::now();
    let row = db
        .logical_runs()
        .previous_for_function_at(&h.consumer_function_id, &AtTime::from(h.before_time))
        .await?;
    record(
        "lfr::previous_for_function_at",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some(), "consumer function has a prior run");
    Ok(())
}

#[td_test::test(db)]
async fn load_lfr_for_dispatch(db: TdPool) -> Result<(), TdError> {
    let (h, profile, _) = seed(&db).await?;
    let seeded = h.fr_ids.len();
    let t = Instant::now();
    let rows = db.logical_runs().for_dispatch(&h.fr_ids).await?;
    record("lfr::for_dispatch", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty(), "seeded FR ids enrich to rows");
    Ok(())
}

#[td_test::test(db)]
async fn load_lfr_blocked_ids(db: TdPool) -> Result<(), TdError> {
    let (h, profile, _) = seed(&db).await?;
    let seeded = h.fr_ids.len();
    let t = Instant::now();
    let rows = db.logical_runs().blocked_ids(&h.execs[0], &h.at).await?;
    record("lfr::blocked_ids", profile, t, rows.len(), seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_lfr_downstream_one_hop(db: TdPool) -> Result<(), TdError> {
    let (h, profile, _) = seed(&db).await?;
    let seeded = h.fr_ids.len();
    let t = Instant::now();
    let rows = db
        .logical_runs()
        .downstream_one_hop(&h.producer_lfrs, &[])
        .await?;
    record("lfr::downstream_one_hop", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty(), "producers have downstream consumers");
    Ok(())
}

#[td_test::test(db)]
async fn load_lfr_effective_output_base_paths(db: TdPool) -> Result<(), TdError> {
    let (h, profile, _) = seed(&db).await?;
    let seeded = h.fr_ids.len();
    let fvids = [h.producer_function_version_id.clone()];
    let t = Instant::now();
    let map = db
        .logical_runs()
        .effective_output_base_paths(&fvids)
        .await?;
    record(
        "lfr::effective_output_base_paths",
        profile,
        t,
        map.len(),
        seeded,
    );
    Ok(())
}

#[td_test::test(db)]
async fn load_lfr_with_bundle_base_path(db: TdPool) -> Result<(), TdError> {
    let (h, profile, _) = seed(&db).await?;
    let seeded = h.fr_ids.len();
    let fvids = [h.producer_function_version_id.clone()];
    let t = Instant::now();
    let rows = db.logical_runs().with_bundle_base_path(&fvids).await?;
    record("lfr::with_bundle_base_path", profile, t, rows.len(), seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_pfr_to_dispatch(db: TdPool) -> Result<(), TdError> {
    let (h, profile, _) = seed(&db).await?;
    let seeded = h.fr_ids.len();
    let t = Instant::now();
    let rows = db.physical_runs().to_dispatch(50).await?;
    record("pfr::to_dispatch", profile, t, rows.len(), seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_pfr_dispatched_before(db: TdPool) -> Result<(), TdError> {
    let (h, profile, _) = seed(&db).await?;
    let seeded = h.fr_ids.len();
    let t = Instant::now();
    let rows = db.physical_runs().dispatched_before(&AtTime::now()).await?;
    record("pfr::dispatched_before", profile, t, rows.len(), seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_executions_list_by_table_data_version(db: TdPool) -> Result<(), TdError> {
    let (h, profile, _) = seed(&db).await?;
    let seeded = h.fr_ids.len();
    let params = exec_list_params()?;
    let t = Instant::now();
    let rows = db
        .executions()
        .list_by_table_data_version(&Some(h.producer_tdv_id.clone()), &params)
        .await?;
    record(
        "executions::list_by_table_data_version",
        profile,
        t,
        rows.len(),
        seeded,
    );
    Ok(())
}

#[td_test::test(db)]
async fn load_executions_list_by_function_version(db: TdPool) -> Result<(), TdError> {
    let (h, profile, _) = seed(&db).await?;
    let seeded = h.fr_ids.len();
    let params = exec_list_params()?;
    let t = Instant::now();
    let rows = db
        .executions()
        .list_by_function_version(&h.producer_function_version_id, &params)
        .await?;
    record(
        "executions::list_by_function_version",
        profile,
        t,
        rows.len(),
        seeded,
    );
    assert!(!rows.is_empty(), "producer fv has an execution");
    Ok(())
}

#[td_test::test(db)]
async fn load_dependencies_active_for_function_at(db: TdPool) -> Result<(), TdError> {
    let (h, profile, _) = seed(&db).await?;
    let seeded = h.fr_ids.len();
    let t = Instant::now();
    let rows = db
        .dependencies()
        .active_for_function_at(&h.consumer_function_id, &h.at)
        .await?;
    record(
        "dependencies::active_for_function_at",
        profile,
        t,
        rows.len(),
        seeded,
    );
    assert!(!rows.is_empty(), "consumer has an active dependency");
    Ok(())
}
