//
// Copyright 2026 Tabsdata Inc.
//

//! Scaling guards — every emitted query timed at two seed sizes, asserting
//! its cost grows sub-quadratically (`k ≤ 1.8` in `t ∝ n^k`). Each dataset is
//! seeded once per size (cumulatively — the big pass adds to the same pool)
//! and all its queries are timed against that one seeding; id-scoped queries
//! staying flat (`k≈0`) is the correct result for them.

use sea_query::Condition;
use ta_objects::list_condition::NoFilter;
use ta_objects::{Buildable, StateFilter};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionRead};
use td_objects::dxo::crudl::ListParams;
use td_objects::dxo::dependency::{DependencyDB, DependencyDBWithNames};
use td_objects::dxo::execution::Execution;
use td_objects::dxo::function::{Function, FunctionDB};
use td_objects::dxo::logical::{
    LogicalFunctionRunInputDB, LogicalFunctionRunPointerDB, LogicalPointerDao,
    LogicalTableDataVersionPointerDB,
};
use td_objects::dxo::metrics::{Dim, InflightRequest, MetricsRequest, NodeKind};
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, UpdatePhysicalFunctionRunDB};
use td_objects::dxo::query_filters::{DimensionFilter, TimeWindow, WindowUnit};
use td_objects::dxo::table::{Table, TableDB};
use td_objects::list::params::ListQueryParams;
use td_objects::types::basic::{
    AtTime, DependencyPos, Depth, InputIdx, PhysicalFunctionRunStatus, TriggeredOn, UserId,
    VersionPos,
};
use td_objects::types::recursion::RecursionOptions;
use td_queries::{
    Bind, CurrentEntry, DependenciesEntry, ExecutionsEntry, HistoricEntry, LfrEntry, LiveEntry,
    LtdvEntry, MetricsEntry, OutputRedirect, PfrEntry, RecursiveEntry, ReplayRedirect,
    SessionsEntry, VersionedEntry,
};
use td_security::ENCODED_ID_SYSTEM;

use std::collections::BTreeSet;

use crate::bulk::bulk_insert;
use crate::cases::LOAD_CASES;
use crate::dataset::{self, ChainHandle, WINDOW_END_MS, WINDOW_START_MS};
use crate::dataversion_cases;
use crate::harness::{analyze, seed, timed, timed_once};
use crate::profile::{ChainKnobs, MetricsKnobs};

type Ptr = LogicalTableDataVersionPointerDB;

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
    for (n, t_us) in [(n_small, t_small_us), (n_big, t_big_us)] {
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
    let at = AtTime::now();
    let exec_params = ListQueryParams::<Execution>::try_from(&ListParams::default())?;

    let _ = timed!(out, "lfr::to_execute", (db).lfr().to_execute(&h.at).await?);
    let _ = timed!(out, "lfr::to_commit", (db).lfr().to_commit(&h.execs).await?);
    let _ = timed!(
        out,
        "lfr::previous_for_function_at",
        (db).lfr()
            .previous_for_function_at(&h.consumer_function_id, &h.before_time)
            .await?
    );
    let _ = timed!(
        out,
        "lfr::for_dispatch",
        (db).lfr().for_dispatch(&h.fr_ids).await?
    );
    let _ = timed!(
        out,
        "lfr::blocked_ids",
        (db).lfr().blocked_ids(&h.execs[0], &at).await?
    );
    let _ = timed!(
        out,
        "lfr::downstream_one_hop",
        (db).lfr().downstream_one_hop(&h.producer_lfrs, &[]).await?
    );
    let _ = timed!(out, "pfr::to_dispatch", (db).pfr().to_dispatch().await?);
    let _ = timed!(
        out,
        "pfr::dispatched_before",
        (db).pfr().dispatched_before(&at).await?
    );
    let _ = timed!(
        out,
        "pfr::in_flight_for",
        (db).pfr()
            .in_flight_for(&h.execs[0], &h.consumer_function_id)
            .await?
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
        "pointers::concrete",
        (db).ltdv().pointers().concrete(&h.ltdv_ids[0]).await?
    );
    let _ = timed!(
        out,
        "pointers::at_concrete",
        (db).ltdv()
            .pointers()
            .at(&at)
            .concrete(&h.ltdv_ids[0])
            .await?
    );
    // Every seeded pointer is Bound: binds all defer and redirects report the
    // already-bound concrete, so the timed passes exercise the batched current
    // fetch + guards without mutating the dataset. Setup untimed.
    let user = UserId::try_from(ENCODED_ID_SYSTEM)?;
    let ltdv_latest: Vec<Ptr> = (db)
        .versioned(&at)
        .fetch_all_chunked::<Ptr>(&h.ltdv_ids)
        .await?;
    let binds: Vec<_> = ltdv_latest
        .iter()
        .filter_map(|p| {
            Some(Bind {
                project_id: p.project_id,
                logical_id: p.logical_id().clone(),
                new_concrete_id: p.concrete_id().cloned()?,
            })
        })
        .collect();
    let _ = timed!(
        out,
        "pointers::bind_all",
        (db).ltdv().pointers().bind_all(&binds, &at, &user).await?
    );
    let lfr_latest: Vec<LogicalFunctionRunPointerDB> = (db)
        .versioned(&at)
        .fetch_all_chunked::<LogicalFunctionRunPointerDB>(&h.producer_lfrs)
        .await?;
    let replay_items: Vec<_> = lfr_latest
        .iter()
        .filter_map(|p| {
            Some(ReplayRedirect {
                project_id: p.project_id,
                logical_function_run_id: p.logical_id().clone(),
                new_physical_function_run_id: p.concrete_id().cloned()?,
                reporting_triggered_on: TriggeredOn::now(),
                set_by_id: p.set_by_id,
            })
        })
        .collect();
    let _ = timed!(
        out,
        "pointers::redirect_replay_attempts",
        (db).lfr()
            .pointers()
            .redirect_replay_attempts(&replay_items, &at)
            .await?
    );
    let output_items: Vec<_> = ltdv_latest
        .iter()
        .filter_map(|p| {
            Some(OutputRedirect {
                project_id: p.project_id,
                logical_table_data_version_id: p.logical_id().clone(),
                new_physical_table_data_version_id: p.concrete_id().cloned()?,
                set_by_id: p.set_by_id,
            })
        })
        .collect();
    timed!(
        out,
        "pointers::redirect_outputs",
        (db).ltdv()
            .pointers()
            .redirect_outputs(&output_items, &at)
            .await?
    );
    let _ = timed!(
        out,
        "ltdv::latest_bound_tdv_with_data_for_table",
        (db).ltdv()
            .latest_bound_tdv_with_data_for_table(&h.table_id, &h.before_time, false)
            .await?
    );

    let _ = timed!(
        out,
        "versioned::fetch_one",
        (db).versioned(&at).fetch_one::<Ptr>(&h.ltdv_ids[0]).await?
    );
    let _ = timed!(
        out,
        "versioned::fetch_optional",
        (db).versioned(&at)
            .fetch_optional::<Ptr>(&h.ltdv_ids[0])
            .await?
    );
    let _ = timed!(
        out,
        "versioned::fetch_all_for_partition",
        (db).versioned(&at)
            .fetch_all_for_partition::<Ptr>(&h.ltdv_ids[0])
            .await?
    );
    let _ = timed!(
        out,
        "versioned::fetch_all_chunked",
        (db).versioned(&at)
            .fetch_all_chunked::<Ptr>(&h.ltdv_ids)
            .await?
    );
    let _ = timed!(
        out,
        "historic::fetch_one",
        (db).historic(&at).fetch_one::<Ptr>(&h.ltdv_ids[0]).await?
    );
    let _ = timed!(
        out,
        "historic::fetch_optional",
        (db).historic(&at)
            .fetch_optional::<Ptr>(&h.ltdv_ids[0])
            .await?
    );
    let _ = timed!(
        out,
        "historic::fetch_all",
        (db).historic(&at).fetch_all::<Ptr>(()).await?
    );
    let _ = timed!(
        out,
        "historic::fetch_all_chunked",
        (db).historic(&at)
            .fetch_all_chunked::<Ptr, _>(&h.ltdv_ids)
            .await?
    );

    let fvids = vec![h.producer_function_version_id.clone()];
    let _ = timed!(
        out,
        "current::fetch_one",
        (db).current()
            .fetch_one::<TableDB>(&h.producer_function_version_id)
            .await?
    );
    let _ = timed!(
        out,
        "current::fetch_optional",
        (db).current()
            .fetch_optional::<TableDB>(&h.producer_function_version_id)
            .await?
    );
    let _ = timed!(
        out,
        "current::fetch_all",
        (db).current().fetch_all::<TableDB>(()).await?
    );
    let _ = timed!(
        out,
        "current::fetch_all_chunked",
        (db).current()
            .fetch_all_chunked::<TableDB, _>(&fvids)
            .await?
    );
    let _ = timed!(
        out,
        "live::fetch_one",
        (db).live()
            .fetch_one::<ActiveCollectionDB>(&h.collection_id)
            .await?
    );
    let _ = timed!(
        out,
        "live::fetch_optional",
        (db).live()
            .fetch_optional::<ActiveCollectionDB>(&h.collection_id)
            .await?
    );
    let _ = timed!(
        out,
        "live::fetch_all",
        (db).live().fetch_all::<ActiveCollectionDB>(()).await?
    );
    let _ = timed!(
        out,
        "live::fetch_last",
        (db).live()
            .fetch_last::<ActiveCollectionDB>(&h.collection_id)
            .await?
    );
    {
        // `FOR UPDATE` needs the read-write transaction; dropping the tx
        // releases the lock.
        let mut tx = db.begin().await.expect("write transaction");
        let _ = timed!(
            out,
            "live::fetch_optional_for_update",
            tx.as_mut_conn()
                .live()
                .fetch_optional_for_update::<PhysicalFunctionRunDB>(&h.fr_ids[0])
                .await?
        );
    }
    {
        let mut tx = db.begin().await.expect("write transaction");
        let _ = timed!(
            out,
            "live::fetch_one_for_update",
            tx.as_mut_conn()
                .live()
                .fetch_one_for_update::<PhysicalFunctionRunDB>(&h.fr_ids[0])
                .await?
        );
    }
    let fvids_one = [h.producer_function_version_id.clone()];
    let _ = timed!(
        out,
        "lfr::effective_output_base_paths",
        (db).lfr().effective_output_base_paths(&fvids_one).await?
    );
    let _ = timed!(
        out,
        "lfr::with_bundle_base_path",
        (db).lfr().with_bundle_base_path(&fvids_one).await?
    );
    // No sessions are seeded: coverage only, the exponent is vacuous.
    timed_once!(out, "sessions::purge", (db).sessions().purge().await?);

    let lfri = LogicalFunctionRunInputDB::builder()
        .collection_id(h.collection_id)
        .execution_id(&h.execs[0])
        .transaction_id(&h.transaction_id)
        .logical_function_run_id(h.producer_lfrs[0].clone())
        .requirement_logical_table_data_version_id(Some(h.ltdv_ids[0].clone()))
        .requirement_input_idx(Some(InputIdx::try_from(0)?))
        .requirement_dependency_pos(Some(DependencyPos::try_from(0)?))
        .requirement_version_pos(VersionPos::try_from(0)?)
        .build()?;
    // The field-less LFRI sources `project_id` from the scoped connection.
    let scoped = db.scoped(h.project_id);
    let _ = timed_once!(
        out,
        "live::insert",
        scoped
            .live()
            .insert::<LogicalFunctionRunInputDB>(&lfri)
            .await?
    );
    let _ = timed_once!(
        out,
        "live::delete",
        scoped
            .live()
            .delete::<LogicalFunctionRunInputDB>(&lfri.id)
            .await?
    );
    let update = UpdatePhysicalFunctionRunDB::builder()
        .status(PhysicalFunctionRunStatus::Done)
        .build()?;
    let _ = timed_once!(
        out,
        "live::update",
        (db).live()
            .update::<_, PhysicalFunctionRunDB>(&update, &h.fr_ids[0])
            .await?
    );
    let _ = timed!(
        out,
        "live::fetch_all_in",
        (db).live()
            .fetch_all_in::<ActiveCollectionDB>(
                StateFilter::<ActiveCollectionDB>::new(Condition::all()),
                ()
            )
            .await?
    );
    let _ = timed!(
        out,
        "live::fetch_latest_before",
        (db).live()
            .fetch_latest_before::<ActiveCollectionDB>(
                StateFilter::<ActiveCollectionDB>::new(Condition::all()),
                (),
                0
            )
            .await?
    );

    let depth = Depth::try_from(16)?;
    let opts = RecursionOptions::new(Some(depth), Some(depth));
    let _ = timed!(
        out,
        "recursive::live",
        (db).recursive::<DependencyDB, FunctionDB>(opts)
            .direct_reference(&h.consumer_function_id)
            .fetch_all()
            .await?
    );
    let _ = timed!(
        out,
        "recursive::current",
        (db).current()
            .recursive::<DependencyDBWithNames, FunctionDB>(opts)
            .direct_reference(&h.consumer_function_id)
            .fetch_all()
            .await?
    );
    let _ = timed!(
        out,
        "recursive::historic",
        (db).historic(&at)
            .recursive::<DependencyDBWithNames, FunctionDB>(opts)
            .direct_reference(&h.consumer_function_id)
            .fetch_all()
            .await?
    );

    let coll_params = ListQueryParams::<CollectionRead>::try_from(&ListParams::default())?;
    let table_params = ListQueryParams::<Table>::try_from(&ListParams::default())?;
    let fn_params = ListQueryParams::<Function>::try_from(&ListParams::default())?;
    let _ = timed!(
        out,
        "live::list",
        (db).live().list((), &NoFilter, &coll_params).await?
    );
    let _ = timed!(
        out,
        "current::list",
        (db).current()
            .list::<Table, _>((), &NoFilter, &table_params)
            .await?
    );
    let _ = timed!(
        out,
        "historic::list_versions",
        (db).historic(&at)
            .list_versions::<Function, _>((), &NoFilter, &fn_params)
            .await?
    );
    let _ = timed!(
        out,
        "historic::list_full",
        (db).historic(&at)
            .list_full::<Function, _>((), &NoFilter, &fn_params)
            .await?
    );

    Ok(out)
}

#[ignore = "expensive: seeds the chain dataset at two sizes and times every chain query"]
#[td_test::test(db)]
async fn report_scaling_chain(db: TdPool) -> Result<(), TdError> {
    let h1 = dataset::chain_dataset(&db, ChainKnobs { units: SMALL }).await?;
    db.execute_batch(&["ANALYZE".to_string()])
        .await
        .expect("analyze after small chain seed");
    let t1 = time_chain_queries(&db, &h1).await?;
    let h2 = dataset::chain_dataset(&db, ChainKnobs { units: BIG }).await?;
    db.execute_batch(&["ANALYZE".to_string()])
        .await
        .expect("analyze after big chain seed");
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
        dim: Dim::Collection,
        window: TimeWindow::new(WINDOW_START_MS, WINDOW_END_MS),
        unit: WindowUnit::Day,
        collections: DimensionFilter::default(),
        groups: DimensionFilter::default(),
        statuses: vec![],
    }
}

async fn time_metrics_queries(db: &TdPool) -> Result<Vec<(&'static str, f64)>, TdError> {
    let req = metrics_request();
    let inflight_req = InflightRequest {
        collections: DimensionFilter::default(),
        groups: DimensionFilter::default(),
        statuses: vec![],
    };
    // The metrics service's read context: the read-only transaction from
    // `acquire_for_read`.
    let mut conn = db.acquire_for_read().await.unwrap();
    let mut out = Vec::new();
    let _ = timed!(
        out,
        "metrics::inflight",
        conn.metrics().inflight(&inflight_req).await?
    );
    let _ = timed!(
        out,
        "metrics::metrics_nodes",
        conn.metrics().metrics_nodes(&req).await?
    );
    let _ = timed!(
        out,
        "metrics::metrics_edges",
        conn.metrics().metrics_edges(&req).await?
    );
    let _ = timed!(
        out,
        "metrics::metrics_node_output_tables",
        conn.metrics().metrics_node_output_tables(&req).await?
    );
    let _ = timed!(
        out,
        "metrics::metrics_edge_tables",
        conn.metrics().metrics_edge_tables(&req).await?
    );
    Ok(out)
}

/// Asserts the metrics queries stay window-first with every correlated probe
/// fenced — the shape that keeps them O(window) under RLS. Checked on the SQL
/// text rather than by timing: the cartesian a missing fence causes only
/// surfaces once a window holds thousands of rows, past the suite's seed sizes.
#[cfg(postgres)]
#[test]
fn metrics_queries_hold_window_first_fenced_contract() {
    use td_objects::dxo::query_filters::WindowUnit::{Day, Hour, Month, Quarter, Week, Year};

    for unit in [Hour, Day, Week, Month, Quarter, Year] {
        for dim in [Dim::Collection, Dim::Group] {
            let mut req = metrics_request();
            req.unit = unit;
            req.dim = dim;
            let statements = td_queries::metrics_plan_sql(&req);
            let tag = format!("{unit:?}/{dim:?}");

            for (which, sql) in &statements {
                assert!(
                    sql.contains(r#"FROM "physical_function_runs""#)
                        && sql.contains(r#""physical_function_runs"."ended_on" >="#)
                        && sql.contains(r#""physical_function_runs"."ended_on" <"#),
                    "{which} {tag}: not window-first (no ended_on-bounded \
                     physical_function_runs drive):\n{sql}"
                );
                let probes = sql.matches(r#" AS "p""#).count();
                let fences = sql.matches("OFFSET 0").count();
                assert!(
                    probes > 0 && probes == fences,
                    "{which} {tag}: {probes} correlated probes but {fences} \
                     OFFSET-0 fences — a probe escaped its fence and will \
                     cartesian at wide windows:\n{sql}"
                );
                // Run-resolution probe: both dims reshape to a lean base-LFR
                // probe (names attached after aggregation).
                assert!(
                    sql.contains(r#"FROM "logical_function_runs" AS "p""#),
                    "{which} {tag}: run-resolution probe not fenced:\n{sql}"
                );
                assert!(
                    sql.contains(r#"FROM "table_data_versions__with_names" AS "p""#),
                    "{which} {tag}: magnitude (TDV) probe not fenced:\n{sql}"
                );
            }
            for which in ["edges", "edge_tables"] {
                let sql = &statements.iter().find(|(n, _)| *n == which).unwrap().1;
                assert!(
                    sql.contains(r#"FROM "logical_function_run_inputs" AS "p""#),
                    "{which} {tag}: consumed-input (LFRI) join not fenced — \
                     wide windows will cartesian:\n{sql}"
                );
            }
        }
    }
}

/// Deterministic plan + execution guard (cheap, unconditional), both dims.
/// Under the Global RLS-bypass path the metrics plans must reach the LFR view
/// via `logical_function_runs_pkey`; keying the view by
/// `physical_function_run_id` instead re-expands it per window row (the
/// O(window × history) blowup). Multi-project seed is load-bearing — for the
/// plan guard (single-project folds against MCVs and hides the regression)
/// and for the name assertions (group ids are only unique per project, so the
/// group-dim name joins must key on `(project, id)`).
#[cfg(postgres)]
#[td_test::test(db)]
async fn metrics_plans_and_rows_under_bypass(db: TdPool) -> Result<(), TdError> {
    dataset::metrics_dataset(
        &db,
        MetricsKnobs {
            publishers: SMALL,
            consumers: SMALL / 2,
            projects: 8,
        },
    )
    .await?;
    analyze(&db, "after metrics seed").await;

    let mut conn = db.acquire_for_read().await.unwrap();
    for dim in [Dim::Collection, Dim::Group] {
        let mut req = metrics_request();
        req.dim = dim;
        for (name, sql) in &td_queries::metrics_plan_sql(&req) {
            let plan = conn.explain(sql).await.expect("explain").join("\n");
            assert!(
                plan.contains("logical_function_runs_pkey"),
                "{dim:?} {name} plan does not reach the LFR view by primary key — \
                 the window probe regressed to per-row view re-expansion (the RLS \
                 O(window×history) blowup):\n{plan}"
            );
        }

        let nodes = conn.metrics().metrics_nodes(&req).await?;
        assert!(!nodes.is_empty(), "{dim:?}: no node rows on seeded data");
        for n in &nodes {
            match dim {
                Dim::Collection => assert!(
                    n.collection_id.is_some() && n.collection_name.is_some(),
                    "{dim:?} node lost its collection identity: {n:?}"
                ),
                Dim::Group => assert!(
                    n.collection_id.is_none() && n.collection_name.is_none(),
                    "{dim:?} node must fold collections to NULL: {n:?}"
                ),
            }
            assert!(
                n.group_id.is_some() && n.group_name.is_some(),
                "{dim:?} node group name did not resolve: {n:?}"
            );
            assert!(
                n.project_id.is_some() && n.project_name.is_some(),
                "{dim:?} node project name did not resolve: {n:?}"
            );
        }

        let edges = conn.metrics().metrics_edges(&req).await?;
        assert!(!edges.is_empty(), "{dim:?}: no edge rows on seeded data");
        for e in &edges {
            if matches!(e.source_kind, NodeKind::Dim) {
                assert!(
                    e.source.is_some() && e.source_name.is_some(),
                    "{dim:?} edge source name did not resolve: {e:?}"
                );
            }
            if matches!(e.target_kind, NodeKind::Dim) {
                assert!(
                    e.target.is_some() && e.target_name.is_some(),
                    "{dim:?} edge target name did not resolve: {e:?}"
                );
            }
        }
    }
    Ok(())
}

/// Deterministic plan guard (cheap, unconditional) for the scheduler chain
/// statements (readiness / blocked / commit). Their latest-pointer hop must
/// stay a fenced lateral driven through the pointer partition index: expressed
/// as `JOIN ON id = (subquery)`, the RLS ~1-row estimates demote the probe
/// into a join filter over an inputs × pointers cross-materialization — a
/// SubPlan re-executed O(rows²) per scheduler tick, which pins the
/// dispatcher's pool connection for minutes and starves the apiserver.
#[cfg(postgres)]
#[td_test::test(db)]
async fn chain_plans_use_fenced_pointer_probe(db: TdPool) -> Result<(), TdError> {
    let (h, _, _) = seed(&db).await?;
    analyze(&db, "after chain seed").await;

    let mut conn = db.acquire_for_read().await.unwrap();
    for (name, sql) in &td_queries::chain_plan_sql(&h.at, &h.execs[0]) {
        let plan = conn.explain(sql).await.expect("explain").join("\n");
        assert!(
            plan.contains("logical_table_data_version_pointers___logical_table_data_versio"),
            "{name}: latest-pointer probe is not driven by the pointer \
             partition index:\n{plan}"
        );
        assert!(
            !plan.contains("SubPlan"),
            "{name}: plan re-grew a correlated SubPlan — the latest-pointer \
             hop regressed from the fenced lateral to a join-filter subquery \
             (O(rows²) per scheduler tick):\n{plan}"
        );
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
    db.execute_batch(&["ANALYZE".to_string()])
        .await
        .expect("analyze after small metrics seed");
    let t1 = time_metrics_queries(&db).await?;
    let big = MetricsKnobs {
        publishers: BIG,
        consumers: BIG / 2,
        projects: 8,
    };
    dataset::metrics_dataset(&db, big).await?;
    db.execute_batch(&["ANALYZE".to_string()])
        .await
        .expect("analyze after big metrics seed");
    let t2 = time_metrics_queries(&db).await?;

    let n_small = (SMALL + SMALL / 2) as f64;
    let n_big = n_small + (BIG + BIG / 2) as f64;
    assert_eq!(t1.len(), t2.len(), "two passes must time the same queries");
    for ((name, a), (name2, b)) in t1.iter().zip(&t2) {
        assert_eq!(name, name2, "the two passes drifted out of order");
        assert_subquadratic(name, n_small, *a, n_big, *b);
    }
    Ok(())
}

/// Θ(window) guard: `/metrics` cost must track the window, not total history.
/// Both passes keep the same 40 attempts in-window and push the rest 30 days
/// back, so only out-of-window history grows — a history-driven plan fails
/// the ratio. The plan assertion pins the mechanism: the window is reached
/// through the pointer-current reverse index.
#[cfg(postgres)]
#[ignore = "expensive: seeds the metrics dataset at two history sizes"]
#[td_test::test(db)]
async fn report_scaling_metrics_window(db: TdPool) -> Result<(), TdError> {
    const HISTORIC_MS: i64 = WINDOW_START_MS - 30 * 86_400_000;

    async fn push_history(db: &TdPool) {
        let sql = format!(
            "UPDATE physical_function_runs SET \
               ended_on = to_timestamp({HISTORIC_MS}::double precision / 1000), \
               ended_on_hour_ms = {HISTORIC_MS}, ended_on_day_ms = {HISTORIC_MS}, \
               ended_on_week_ms = {HISTORIC_MS}, ended_on_month_ms = {HISTORIC_MS} \
             WHERE ended_on IS NOT NULL AND id NOT IN (\
               SELECT id FROM physical_function_runs WHERE ended_on IS NOT NULL \
               ORDER BY id LIMIT 40)"
        );
        db.execute_batch(&[sql])
            .await
            .expect("push history out of window");
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
        db.execute_batch(&["ANALYZE".to_string()])
            .await
            .expect("analyze after history push");
        passes.push(time_metrics_queries(&db).await?);
    }
    // Mechanism (deterministic): the attempt is resolved to its run by pk, not
    // by re-expanding the LFR-anchored view per window row. The reverse index on
    // the pointer hop is logged, not asserted — discretionary at these sizes.
    let mut conn = db.acquire_for_read().await.unwrap();
    for (name, sql) in &td_queries::metrics_plan_sql(&metrics_request()) {
        let plan = conn.explain(sql).await.expect("explain").join("\n");
        eprintln!("[query-load] window plan {name}:\n{plan}\n");
        assert!(
            plan.contains("logical_function_runs_pkey"),
            "{name} plan does not resolve the window attempt to its run by \
             primary key — the probe regressed to per-row view re-expansion \
             (the RLS O(window×history) blowup):\n{plan}"
        );
    }
    drop(conn);

    let (h_small, h_big) = (
        SMALL + SMALL / 2 - 40,
        SMALL + SMALL / 2 + BIG + BIG / 2 - 40,
    );
    for ((name, a), (name2, b)) in passes[0].iter().zip(&passes[1]) {
        assert_eq!(name, name2, "the two passes drifted out of order");
        // No window semantics on /inflight; the regular scaling pass covers it.
        if *name == "metrics::inflight" {
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
    db.execute_batch(&["ANALYZE".to_string()])
        .await
        .expect("analyze after chain seed");
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
    db.execute_batch(&["ANALYZE".to_string()])
        .await
        .expect("analyze after metrics seed");
    timed.extend(time_metrics_queries(&db).await?.into_iter().map(|(n, _)| n));

    bulk_insert(&dv_db, &dataversion_cases::dv_rows(0, SMALL)?).await?;
    dv_db
        .execute_batch(&["ANALYZE".to_string()])
        .await
        .expect("analyze after dv seed");
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
