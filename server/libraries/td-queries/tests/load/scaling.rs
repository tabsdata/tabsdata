//
// Copyright 2026 Tabsdata Inc.
//

//! Scaling guards — every emitted query timed at two seed sizes, asserting
//! its cost grows sub-quadratically (`k ≤ 1.8` in `t ∝ n^k`). Each dataset is
//! seeded once per size (cumulatively — the big pass adds to the same pool)
//! and all its queries are timed against that one seeding; id-scoped queries
//! staying flat (`k≈0`) is the correct result for them.

use crate::bulk::bulk_insert;
use crate::cases::LOAD_CASES;
use crate::chunked_cases::done_update;
use crate::dataset::{self, ChainHandle, WINDOW_END_MS, WINDOW_START_MS};
use crate::dataversion_cases;
use crate::default_write_cases::build_lfri;
use crate::harness::{analyze, timed, timed_once};
use crate::profile::{ChainKnobs, MetricsKnobs};
use crate::raw::delete_by_pk;
use crate::scd2_cases::build_attempt;
use std::collections::BTreeSet;
use std::collections::HashSet;
use td_common::datetime::Timestamp;
use td_database::access::LockInstant;
use td_database::connection::PgConnectionExt;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionRead};
use td_objects::dxo::crudl::ListParams;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::execution::Execution;
use td_objects::dxo::function::{Function, FunctionDB};
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunInputInsertDB, LogicalFunctionRunStatusUpdateDB,
};
use td_objects::dxo::metrics::{Dimension, InflightRequest, MetricsRequest};
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, PhysicalFunctionRunUpdateDB};
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::query_filters::{DimensionFilter, TimeWindow};
use td_objects::dxo::table::{Table, TableDB};
use td_objects::list::params::ListQueryParams;
use td_objects::types::basic::{
    AtTime, DependencyPos, Depth, EpochMs, FactSide, IncludePrivate, IncludeReplaced, InputIdx,
    LogicalFunctionRunInputId, LogicalFunctionRunStatus, PhysicalFunctionRunStatus, VersionPos,
};
use td_objects::types::recursion::RecursionOptions;
use td_objects::types::visible_projects::VisibleProjects;
use td_queries::Queries;
use td_queries::test_utils::derive_metrics_facts;

type Ptdv = PhysicalTableDataVersionDB;

/// Cumulative seed sizes (dataset units); the heavier queries must clear the
/// noise floor at "big".
const SMALL: usize = 80;
const BIG: usize = 320;

/// Timings below this (µs) are noise-dominated and not judged.
const NOISE_FLOOR_US: f64 = 3_000.0;

/// Ceiling on any single timed query, at either seed size (µs). Growth alone
/// is not enough of a guard: a plan-level regression can be born slow at
/// every size — uniform slowness yields a harmless-looking exponent (the RLS
/// nested-loop metrics plans measured k≈-2 while taking seconds).
const BUDGET_US: f64 = 500_000.0;

/// Maximum tolerated exponent `k` in `t ∝ n^k`. A genuine O(n²) regression
/// lands near `k≈2`, so this leaves clear margin below it; the ceiling sits
/// above the naturally near-`n^1.6` history-listing queries, whose two-point
/// exponent tips over a tighter bound under CI jitter (a false positive the
/// [`BUDGET_US`] tooth still backstops against born-slow plans).
const MAX_EXPONENT: f64 = 1.8;

/// Two teeth per timed query: an absolute budget at both seed sizes
/// ([`BUDGET_US`]), and the exponent `k` in `t ∝ n^k` required `≤ 1.8`
/// ([`MAX_EXPONENT`]) — a genuine regression to O(n²) blows far past it.
pub(crate) fn assert_subquadratic(
    case: &str,
    n_small: f64,
    t_small_us: f64,
    n_big: f64,
    t_big_us: f64,
) {
    assert_growth(case, n_small, t_small_us, n_big, t_big_us, true)
}

/// [`assert_subquadratic`] without the per-request budget: a bulk build over
/// a whole project answers no request, so only its growth is judged.
pub(crate) fn assert_subquadratic_bulk(
    case: &str,
    n_small: f64,
    t_small_us: f64,
    n_big: f64,
    t_big_us: f64,
) {
    assert_growth(case, n_small, t_small_us, n_big, t_big_us, false)
}

fn assert_growth(
    case: &str,
    n_small: f64,
    t_small_us: f64,
    n_big: f64,
    t_big_us: f64,
    budgeted: bool,
) {
    for (n, t_us) in [(n_small, t_small_us), (n_big, t_big_us)]
        .into_iter()
        .filter(|_| budgeted)
    {
        assert!(
            t_us <= BUDGET_US,
            "{case} took {t_us:.0}us at n {n} — over the {BUDGET_US:.0}us budget. \
             A blown budget with an unremarkable exponent usually means a \
             degraded query plan (e.g. RLS-driven nested loops), not growth."
        );
    }
    // Either endpoint under the noise floor makes the exponent meaningless
    // (a noise-low `t_small` would fail a fast, flat query spuriously).
    if t_big_us < NOISE_FLOOR_US || t_small_us < NOISE_FLOOR_US {
        eprintln!(
            "[query-load] scaling {case:<42} n {n_small:>6}->{n_big:<6} t {t_small_us:>9.0}us->{t_big_us:<9.0}us  (under noise floor)"
        );
        return;
    }
    let exponent = (t_big_us / t_small_us).ln() / (n_big / n_small).ln();
    eprintln!(
        "[query-load] scaling {case:<42} n {n_small:>6}->{n_big:<6} t {t_small_us:>9.0}us->{t_big_us:<9.0}us  k≈{exponent:.2}"
    );
    assert!(
        exponent <= MAX_EXPONENT,
        "{case} scales ~O(n^{exponent:.3}) — over the {MAX_EXPONENT:.3} ceiling \
         (n {n_small}->{n_big}, t {t_small_us:.0}us->{t_big_us:.0}us). \
         Expected sub-quadratic."
    );
}

/// Times every chain-dataset query against the seeded `h`, in a fixed order
/// so two passes zip 1:1.
async fn time_chain_queries(
    db: &TdPool,
    h: &ChainHandle,
) -> Result<Vec<(&'static str, f64)>, TdError> {
    let mut out: Vec<(&'static str, f64)> = Vec::new();
    let now = AtTime::now();
    let at = now;
    let triggered_at = now;
    let exec_params = ListQueryParams::<Execution>::try_from(&ListParams::default())?;

    let _ = timed!(
        out,
        "lfr::to_execute",
        (db).logical_runs().to_execute(&h.at, 50).await?
    );
    let _ = timed!(
        out,
        "lfr::to_commit",
        (db).logical_runs().to_commit(&h.execs).await?
    );
    let _ = timed!(
        out,
        "lfr::previous_for_function_at",
        (db).logical_runs()
            .previous_for_function_at(&h.consumer_function_id, &AtTime::from(h.before_time))
            .await?
    );
    let _ = timed!(
        out,
        "lfr::for_dispatch",
        (db).logical_runs().for_dispatch(&h.fr_ids).await?
    );
    let _ = timed!(
        out,
        "lfr::blocked_ids",
        (db).logical_runs().blocked_ids(&h.execs[0]).await?
    );
    let _ = timed!(
        out,
        "lfr::downstream_one_hop",
        (db).logical_runs()
            .downstream_one_hop(&h.producer_lfrs, &[])
            .await?
    );
    let _ = timed!(
        out,
        "pfr::to_dispatch",
        (db).physical_runs().to_dispatch(50).await?
    );
    let _ = timed!(
        out,
        "pfr::dispatched_before",
        (db).physical_runs().dispatched_before(&at).await?
    );
    let _ = timed!(
        out,
        "executions::list_by_table_data_version",
        (db).executions()
            .list_by_table_data_version(&Some(h.producer_tdv_id.clone()), &exec_params)
            .await?
    );
    let _ = timed!(
        out,
        "executions::list_by_function_version",
        (db).executions()
            .list_by_function_version(&h.producer_function_version_id, &exec_params)
            .await?
    );
    let _ = timed!(
        out,
        "dependencies::active_for_function_at",
        (db).dependencies()
            .active_for_function_at(&h.consumer_function_id, &at)
            .await?
    );

    let _ = timed!(
        out,
        "attempts::current_run",
        (db).scd2()
            .fetch_optional::<PhysicalFunctionRunDB>(&h.producer_lfrs[0])
            .await?
    );
    let _ = timed!(
        out,
        "attempts::current_version",
        (db).scd2().fetch_optional::<Ptdv>(&h.ltdv_ids[0]).await?
    );
    let _ = timed!(
        out,
        "attempts::version_at",
        (db).scd2()
            .as_of(&triggered_at)
            .fetch_optional::<Ptdv>(&h.ltdv_ids[0])
            .await?
    );
    let _ = timed!(
        out,
        "ltdv::latest_bound_tdv_with_data_for_table",
        (db).table_data_versions()
            .latest_bound_tdv_with_data_for_table(&h.table_id, &AtTime::from(h.before_time), false)
            .await?
    );

    let _ = timed!(
        out,
        "as_of::fetch_one_by_partition",
        (db).scd2()
            .as_of(&triggered_at)
            .fetch_one::<Ptdv>(&h.ltdv_ids[0])
            .await?
    );
    let _ = timed!(
        out,
        "as_of::fetch_optional_by_partition",
        (db).scd2()
            .as_of(&triggered_at)
            .fetch_optional::<Ptdv>(&h.ltdv_ids[0])
            .await?
    );
    let _ = timed!(
        out,
        "as_of::fetch_all_chunked_by_partition",
        (db).scd2()
            .as_of(&triggered_at)
            .chunked()
            .fetch_all::<Ptdv>(&h.ltdv_ids, ())
            .await?
    );
    let _ = timed!(
        out,
        "as_of::fetch_one",
        (db).scd2()
            .as_of(&triggered_at)
            .fetch_one::<Ptdv>(&h.ltdv_ids[0])
            .await?
    );
    let _ = timed!(
        out,
        "as_of::fetch_optional",
        (db).scd2()
            .as_of(&triggered_at)
            .fetch_optional::<Ptdv>(&h.ltdv_ids[0])
            .await?
    );
    let _ = timed!(
        out,
        "as_of::fetch_all",
        (db).scd2()
            .as_of(&triggered_at)
            .fetch_all::<Ptdv>(())
            .await?
    );
    let _ = timed!(
        out,
        "as_of::fetch_all_chunked",
        (db).scd2()
            .as_of(&triggered_at)
            .chunked()
            .fetch_all::<Ptdv>(&h.ltdv_ids, ())
            .await?
    );

    let fvids = vec![h.producer_function_version_id.clone()];
    let _ = timed!(
        out,
        "latest::fetch_one",
        (db).scd2().fetch_one::<TableDB>(&h.table_id).await?
    );
    let _ = timed!(
        out,
        "latest::fetch_optional",
        (db).scd2().fetch_optional::<TableDB>(&h.table_id).await?
    );
    let _ = timed!(
        out,
        "latest::fetch_all",
        (db).scd2().fetch_all::<TableDB>(()).await?
    );
    let _ = timed!(
        out,
        "latest::fetch_all_chunked",
        (db).scd2()
            .chunked()
            .fetch_all::<TableDB>(&fvids, ())
            .await?
    );
    let _ = timed!(
        out,
        "default::fetch_one",
        (db).fetch_one::<ActiveCollectionDB>(&h.collection_id)
            .await?
    );
    let _ = timed!(
        out,
        "default::fetch_optional",
        (db).fetch_optional::<ActiveCollectionDB>(&h.collection_id)
            .await?
    );
    let _ = timed!(
        out,
        "default::fetch_all",
        (db).fetch_all::<ActiveCollectionDB>(()).await?
    );
    let _ = timed!(
        out,
        "default::fetch_last",
        (db).fetch_last::<ActiveCollectionDB>(&h.collection_id)
            .await?
    );
    {
        // `FOR UPDATE` needs the read-write transaction; dropping the tx
        // releases the lock.
        let mut conn = db.begin().await.expect("write transaction");
        let _ = timed!(
            out,
            "default::fetch_optional_for_update",
            conn.for_update()
                .fetch_optional::<LogicalFunctionRunDB>(&h.producer_lfrs[0])
                .await?
        );
    }
    {
        let mut conn = db.begin().await.expect("write transaction");
        let _ = timed!(
            out,
            "default::fetch_one_for_update",
            conn.for_update()
                .fetch_one::<LogicalFunctionRunDB>(&h.producer_lfrs[0])
                .await?
        );
    }
    {
        let mut conn = db.begin().await.expect("write transaction");
        let _ = timed!(
            out,
            "default::fetch_all_for_update",
            conn.for_update()
                .fetch_all::<LogicalFunctionRunDB>(&h.producer_lfrs[0])
                .await?
        );
    }
    {
        let mut conn = db.begin().await.expect("write transaction");
        let _ = timed!(
            out,
            "default::serialize_on",
            conn.for_update()
                .serialize_on::<LogicalFunctionRunDB>(&h.producer_lfrs[0])
                .await?
        );
    }
    {
        let mut conn = db.begin().await.expect("write transaction");
        let _ = timed!(
            out,
            "default::claim_all",
            conn.for_update()
                .claim_all::<LogicalFunctionRunDB>(())
                .await?
        );
    }
    let fvids_one = [h.producer_function_version_id.clone()];
    let _ = timed!(
        out,
        "lfr::effective_output_base_paths",
        (db).logical_runs()
            .effective_output_base_paths(&fvids_one)
            .await?
    );
    let _ = timed!(
        out,
        "lfr::with_bundle_base_path",
        (db).logical_runs()
            .with_bundle_base_path(&fvids_one)
            .await?
    );
    // No sessions are seeded: coverage only, the exponent is vacuous.
    timed_once!(out, "sessions::purge", (db).sessions().purge().await?);

    let lfri = LogicalFunctionRunInputInsertDB {
        id: LogicalFunctionRunInputId::default(),
        collection_id: h.collection_id,
        execution_id: h.execs[0].clone(),
        transaction_id: h.transaction_id.clone(),
        logical_function_run_id: h.producer_lfrs[0].clone(),
        requirement_logical_table_data_version_id: Some(h.ltdv_ids[0].clone()),
        requirement_input_idx: Some(InputIdx::try_from(0)?),
        requirement_dependency_pos: Some(DependencyPos::try_from(0)?),
        requirement_version_pos: VersionPos::try_from(0)?,
    };
    // The field-less LFRI sources `project_id` from the scoped connection.
    let scoped = db.scoped(h.project_id);
    let _ = timed_once!(
        out,
        "default::insert",
        scoped
            .insert::<LogicalFunctionRunInputInsertDB>(&lfri)
            .await?
    );
    let _ = timed_once!(
        out,
        "default::delete",
        delete_by_pk::<LogicalFunctionRunInputInsertDB, _>(&scoped, &lfri.id).await?
    );
    let update = PhysicalFunctionRunUpdateDB {
        dispatched_on: None,
        started_on: None,
        ended_on: None,
        status: PhysicalFunctionRunStatus::Done,
        ended_on_hour_ms: None,
        ended_on_day_ms: None,
        ended_on_week_ms: None,
        ended_on_month_ms: None,
        metric_table_versions: None,
        metric_rows: None,
        metric_data_bytes: None,
    };
    let _ = timed_once!(
        out,
        "scd2::update",
        (db).scd2().update(&update, &h.fr_ids[0]).await?
    );
    let lfr_update = LogicalFunctionRunStatusUpdateDB {
        status: LogicalFunctionRunStatus::Done,
    };
    let _ = timed_once!(
        out,
        "default::update",
        (db).update(&lfr_update, &h.producer_lfrs[0]).await?
    );
    let _ = timed_once!(
        out,
        "chunked::update",
        (db).chunked().update(&lfr_update, &h.producer_lfrs).await?
    );
    let _ = timed!(
        out,
        "timeline::fetch_one",
        (db).timeline()
            .fetch_one::<FunctionDB>(&h.producer_function_version_id)
            .await?
    );
    let _ = timed!(
        out,
        "timeline::fetch_optional",
        (db).timeline()
            .fetch_optional::<FunctionDB>(&h.producer_function_version_id)
            .await?
    );
    let _ = timed!(
        out,
        "default::fetch_all",
        (db).fetch_all::<ActiveCollectionDB>(()).await?
    );
    let _ = timed!(
        out,
        "default::fetch_latest_before",
        (db).fetch_nth_last::<ActiveCollectionDB>((), 0).await?
    );

    let depth = Depth::try_from(16)?;
    let opts = RecursionOptions::new(Some(depth), Some(depth));
    let _ = timed!(
        out,
        "recursive::current",
        (db).scd2()
            .recursive::<DependencyDBWithNames, FunctionDB>(opts)
            .start_at(&h.consumer_function_id)
            .fetch_all()
            .await?
    );
    let _ = timed!(
        out,
        "recursive::find_at",
        (db).scd2()
            .as_of(&at)
            .recursive::<DependencyDBWithNames, FunctionDB>(opts)
            .start_at(&h.consumer_function_id)
            .fetch_all()
            .await?
    );

    let coll_params = ListQueryParams::<CollectionRead>::try_from(&ListParams::default())?;
    let table_params = ListQueryParams::<Table>::try_from(&ListParams::default())?;
    let fn_params = ListQueryParams::<Function>::try_from(&ListParams::default())?;
    let _ = timed!(out, "default::list", (db).list((), &coll_params).await?);
    let _ = timed!(
        out,
        "latest::list",
        (db).scd2().list::<Table>((), &table_params).await?
    );
    let _ = timed!(
        out,
        "as_of::list",
        (db).scd2()
            .as_of(&now)
            .list::<Function>((), &fn_params)
            .await?
    );

    let _ = timed!(
        out,
        "default::fetch_first",
        (db).fetch_first::<ActiveCollectionDB>(()).await?
    );
    let _ = timed!(
        out,
        "latest::fetch_first",
        (db).scd2().fetch_first::<TableDB>(()).await?
    );
    let _ = timed!(
        out,
        "history::fetch_all",
        (db).timeline().fetch_all::<TableDB>(&h.table_id).await?
    );
    let _ = timed!(
        out,
        "history::fetch_first",
        (db).timeline().fetch_first::<TableDB>(&h.table_id).await?
    );
    let _ = timed!(
        out,
        "history::fetch_last",
        (db).timeline().fetch_last::<TableDB>(&h.table_id).await?
    );
    let _ = timed!(
        out,
        "history::fetch_all_chunked",
        (db).timeline()
            .chunked()
            .fetch_all::<TableDB>(&fvids, ())
            .await?
    );
    let _ = timed!(
        out,
        "history::list",
        (db).timeline().list::<Table>((), &table_params).await?
    );
    let _ = timed!(
        out,
        "history::list_until",
        (db).timeline()
            .list_until::<Table, _>(&now, (), &table_params)
            .await?
    );
    let _ = timed!(
        out,
        "chunked::fetch_all",
        (db).chunked()
            .fetch_all::<LogicalFunctionRunDB>(&h.execs, ())
            .await?
    );
    {
        let rows: Vec<LogicalFunctionRunInputInsertDB> =
            (0..8).map(|_| build_lfri(h)).collect::<Result<_, _>>()?;
        let scoped = db.scoped(h.project_id);
        timed_once!(
            out,
            "chunked::insert",
            (&scoped).chunked().insert(&rows).await?
        );
        let update = done_update();
        timed_once!(
            out,
            "scd2::update_chunked",
            (db).scd2().chunked().update(&update, &h.fr_ids).await?
        );
        let attempt = build_attempt(db, h).await?;
        let at = LockInstant::asserted(Timestamp::default());
        timed_once!(
            out,
            "scd2::open",
            (&scoped).scd2().open(&attempt, at).await?
        );
        let mut attempts = Vec::with_capacity(8);
        for _ in 0..8 {
            attempts.push(build_attempt(db, h).await?);
        }
        timed_once!(
            out,
            "scd2::open_chunked",
            (&scoped).scd2().chunked().open(&attempts, at).await?
        );
    }
    // Closes go last: they end the seeded table's open interval, which every
    // read above still expects to find.
    let table_ids = [h.table_id];
    let locked_now = LockInstant::asserted(Timestamp::default());
    timed_once!(
        out,
        "chunked::close",
        (db).scd2()
            .chunked()
            .close::<TableDB>(&table_ids, (), locked_now)
            .await?
    );
    timed_once!(
        out,
        "default::close",
        db.scd2().close::<TableDB>(&h.table_id, locked_now).await?
    );

    Ok(out)
}

#[ignore = "expensive: seeds the chain dataset at two sizes and times every chain query"]
#[td_test::test(db)]
async fn report_scaling_chain(db: TdPool) -> Result<(), TdError> {
    let h1 = dataset::chain_dataset(&db, ChainKnobs { units: SMALL }).await?;
    analyze(&db, "after small chain seed").await;
    let t1 = time_chain_queries(&db, &h1).await?;
    let h2 = dataset::chain_dataset(&db, ChainKnobs { units: BIG }).await?;
    analyze(&db, "after big chain seed").await;
    let t2 = time_chain_queries(&db, &h2).await?;

    let n_small = (SMALL * 2) as f64;
    let n_big = ((SMALL + BIG) * 2) as f64;
    assert_eq!(t1.len(), t2.len(), "two passes must time the same queries");
    for ((name, a), (name2, b)) in t1.iter().zip(&t2) {
        assert_eq!(name, name2, "the two passes drifted out of order");
        assert_subquadratic(name, n_small, *a, n_big, *b);
    }
    Ok(())
}

fn metrics_request() -> MetricsRequest {
    MetricsRequest {
        dim: Dimension::Collection,
        window: TimeWindow::new(
            EpochMs::try_from(WINDOW_START_MS).unwrap(),
            EpochMs::try_from(WINDOW_END_MS).unwrap(),
        ),
        collections: DimensionFilter::default(),
        groups: DimensionFilter::default(),
        statuses: LogicalFunctionRunStatus::ENDED.to_vec(),
        include_replaced: IncludeReplaced::FALSE,
        include_private: IncludePrivate::FALSE,
    }
}

async fn time_metrics_queries(db: &TdPool) -> Result<Vec<(&'static str, f64)>, TdError> {
    let req = metrics_request();
    let inflight_req = InflightRequest {
        collections: DimensionFilter::default(),
        groups: DimensionFilter::default(),
        statuses: vec![],
    };
    let mut out = Vec::new();
    timed_once!(out, "metrics::backfill", derive_metrics_facts(db).await?);
    // The metrics service's read context: the read-only transaction from
    // `begin_ro`.
    let mut conn = db.begin_ro().await.unwrap();
    let _ = timed!(
        out,
        "metrics::inflight",
        conn.metrics()
            .inflight(&inflight_req, &VisibleProjects::All)
            .await?
    );
    let _ = timed!(
        out,
        "metrics::per_table",
        conn.metrics()
            .per_table(&req, &VisibleProjects::All)
            .await?
    );
    Ok(out)
}

/// Asserts the metrics read touches only the fact table, bounded on the
/// indexed `ts`, with no base-table probe and no fence. Checked on the SQL.
#[test]
fn metrics_queries_read_only_facts() {
    for dim in [Dimension::Collection, Dimension::Group] {
        let mut req = metrics_request();
        req.dim = dim;
        let statements = td_queries::metrics_plan_sql(&req).expect("render");
        let tag = format!("{dim:?}");
        for (which, sql) in &statements {
            for base in [
                "physical_function_runs",
                "logical_table_data_versions",
                "logical_function_run_inputs",
                "table_data_versions__with_names",
            ] {
                assert!(
                    !sql.contains(&format!("FROM \"{base}\"")),
                    "{which} {tag}: reads {base} instead of the facts:\n{sql}"
                );
            }
            assert!(
                !sql.contains("OFFSET 0"),
                "{which} {tag}: a fenced probe survived:\n{sql}"
            );
            assert!(
                sql.contains(r#""metrics_facts"."ts" >="#),
                "{which} {tag}: not bounded on the indexed timestamp:\n{sql}"
            );
        }
    }
}

/// Deterministic execution guard (cheap, unconditional), both dims: under the
/// Cross-project RLS-bypass path: every metrics row must come back with its dimension
/// identity resolved. Multi-project seed is load-bearing — group ids are only
/// unique per project, so the group-dim name joins must key on
/// `(project, id)`.
#[td_test::test(db)]
async fn metrics_rows_under_bypass(db: TdPool) -> Result<(), TdError> {
    dataset::metrics_dataset(
        &db,
        MetricsKnobs {
            publishers: SMALL,
            consumers: SMALL / 2,
            projects: 8,
        },
    )
    .await?;

    let mut conn = db.begin_ro().await.unwrap();
    for dim in [Dimension::Collection, Dimension::Group] {
        let mut req = metrics_request();
        req.dim = dim;

        let rows = conn
            .metrics()
            .per_table(&req, &VisibleProjects::All)
            .await?;
        assert!(!rows.is_empty(), "{dim:?}: no fact rows on seeded data");
        let sides: HashSet<FactSide> = rows.iter().map(|r| r.side).collect();
        assert!(
            sides.contains(&FactSide::Attempt) && sides.contains(&FactSide::Written),
            "{dim:?}: the seeded publishers produced no attempt or written rows"
        );
        for r in &rows {
            assert!(
                matches!(r.side, FactSide::Attempt) || r.table_id.is_some(),
                "{dim:?} version row lost its table identity: {r:?}"
            );
        }
    }
    Ok(())
}

#[ignore = "expensive: seeds the metrics dataset at two sizes and times every metrics query"]
#[td_test::test(db)]
async fn report_scaling_metrics(db: TdPool) -> Result<(), TdError> {
    let small = MetricsKnobs {
        publishers: SMALL,
        consumers: SMALL / 2,
        projects: 8,
    };
    dataset::metrics_dataset(&db, small).await?;
    analyze(&db, "after small metrics seed").await;
    let t1 = time_metrics_queries(&db).await?;
    let big = MetricsKnobs {
        publishers: BIG,
        consumers: BIG / 2,
        projects: 8,
    };
    dataset::metrics_dataset(&db, big).await?;
    analyze(&db, "after big metrics seed").await;
    let t2 = time_metrics_queries(&db).await?;

    let n_small = (SMALL + SMALL / 2) as f64;
    let n_big = n_small + (BIG + BIG / 2) as f64;
    assert_eq!(t1.len(), t2.len(), "two passes must time the same queries");
    for ((name, a), (name2, b)) in t1.iter().zip(&t2) {
        assert_eq!(name, name2, "the two passes drifted out of order");
        if *name == "metrics::backfill" {
            assert_subquadratic_bulk(name, n_small, *a, n_big, *b);
        } else {
            assert_subquadratic(name, n_small, *a, n_big, *b);
        }
    }
    Ok(())
}

/// Θ(window) guard: `/metrics` cost must track the window, not total history.
/// Both passes keep the same 40 attempts in-window and push the rest 30 days
/// back, so only out-of-window history grows — a history-driven plan fails
/// the ratio. The plan assertion pins the mechanism: each window attempt is
/// resolved to its run by primary key.
#[ignore = "expensive: seeds the metrics dataset at two history sizes"]
#[td_test::test(db)]
async fn report_scaling_metrics_window(db: TdPool) -> Result<(), TdError> {
    const HISTORIC_MS: i64 = WINDOW_START_MS - 30 * 86_400_000;

    async fn push_history(db: &TdPool) {
        let keep = "SELECT id FROM physical_function_runs WHERE ended_on IS NOT NULL \
                    ORDER BY id LIMIT 40";
        let sql = [format!(
            "UPDATE physical_function_runs SET ended_on = to_timestamp({HISTORIC_MS} / 1000.0) \
               WHERE ended_on IS NOT NULL AND id NOT IN ({keep})"
        )];
        db.execute_batch(&sql)
            .await
            .expect("push history out of window");
        derive_metrics_facts(db).await.expect("derive facts");
    }

    let mut passes = Vec::new();
    for knobs in [
        MetricsKnobs {
            publishers: SMALL,
            consumers: SMALL / 2,
            projects: 1,
        },
        MetricsKnobs {
            publishers: BIG,
            consumers: BIG / 2,
            projects: 1,
        },
    ] {
        dataset::metrics_dataset(&db, knobs).await?;
        push_history(&db).await;
        analyze(&db, "after history push").await;
        passes.push(time_metrics_queries(&db).await?);
    }
    let mut conn = db.begin_ro().await.unwrap();
    for (name, sql) in &td_queries::metrics_plan_sql(&metrics_request()).expect("render") {
        let plan = conn.explain(sql).await.expect("explain").join("\n");
        eprintln!("[query-load] window plan {name}:\n{plan}\n");
        assert!(
            plan.contains("metrics_facts___ts___idx")
                || plan.contains("metrics_facts___project_id__ts___idx"),
            "{name} plan does not drive the facts through a covering index — \
             the read became history-driven:\n{plan}"
        );
    }
    drop(conn);

    let (h_small, h_big) = (
        SMALL + SMALL / 2 - 40,
        SMALL + SMALL / 2 + BIG + BIG / 2 - 40,
    );
    for ((name, a), (name2, b)) in passes[0].iter().zip(&passes[1]) {
        assert_eq!(name, name2, "the two passes drifted out of order");
        // No window semantics on /inflight, and the backfill is by definition
        // history-driven; the regular scaling pass covers both.
        if *name == "metrics::inflight" || *name == "metrics::backfill" {
            continue;
        }
        eprintln!(
            "[query-load] window  {name:<42} hist {h_small:>6}->{h_big:<6} t {a:>9.0}us->{b:<9.0}us"
        );
        assert!(
            *b <= a * 1.5 + 1_500.0,
            "{name}: window-fixed cost grew with history ({a:.0}us -> {b:.0}us) — \
             the query is history-driven, not window-driven."
        );
    }
    Ok(())
}

/// Coverage complement to [`crate::audit`]: seeds each scaling dataset once
/// and asserts the union of names timed by the `time_*_queries` passes covers
/// `LOAD_CASES` — a query with a JSONL case but no scaling timing fails here.
#[ignore = "expensive: seeds every scaling dataset once and checks coverage"]
#[td_test::test(db(
    td_database::components::ApiserverComponent,
    crate::dataversion_cases::LoadDvTableOnly
))]
async fn report_scaling_coverage(db: TdPool, dv_db: TdPool) -> Result<(), TdError> {
    let mut timed: BTreeSet<&'static str> = BTreeSet::new();

    let h = dataset::chain_dataset(&db, ChainKnobs { units: SMALL }).await?;
    analyze(&db, "after chain seed").await;
    timed.extend(
        time_chain_queries(&db, &h)
            .await?
            .into_iter()
            .map(|(n, _)| n),
    );

    dataset::metrics_dataset(
        &db,
        MetricsKnobs {
            publishers: SMALL,
            consumers: SMALL / 2,
            projects: 1,
        },
    )
    .await?;
    analyze(&db, "after metrics seed").await;
    timed.extend(time_metrics_queries(&db).await?.into_iter().map(|(n, _)| n));

    bulk_insert(&dv_db, &dataversion_cases::dv_rows(0, SMALL)?).await?;
    analyze(&dv_db, "after dv seed").await;
    timed.extend(
        dataversion_cases::time_dv_queries(&dv_db)
            .await?
            .into_iter()
            .map(|(n, _)| n),
    );

    let missing: Vec<&str> = LOAD_CASES
        .iter()
        .map(|c| c.name)
        .filter(|n| !timed.contains(n))
        .collect();
    assert!(
        missing.is_empty(),
        "these registered cases are timed by no scaling pass (wire them into a \
         time_*_queries fn so their growth is asserted): {missing:#?}"
    );
    Ok(())
}
