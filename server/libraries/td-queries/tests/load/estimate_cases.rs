//
// Copyright 2026 Tabsdata Inc.
//

//! The status roll-ups under the tenant policy: estimates within a bound, and
//! no aggregate, sort, hash or full scan re-run per outer row. Row counts and
//! loops, not timings.

use sea_query::{Expr, PostgresQueryBuilder};
use serde_json::Value;
use sqlx::PgConnection;
use td_database::access::Grant;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::project::ProjectDB;
use td_queries::Queries;
use td_queries::test_utils::begin_exclusive;
use td_schema::schema::current::{Changed, RollupDef, execution_rollup, transaction_rollup};

use crate::dataset;
use crate::harness::analyze;
use crate::profile::MetricsKnobs;

const PROJECTS: usize = 8;
/// Below the number of projects, the factor a doubled tenant filter misses by.
const UNDERESTIMATE: f64 = 4.0;
const MIN_ROWS: f64 = 20.0;
const SCANS: &[&str] = &[
    "Seq Scan",
    "Index Scan",
    "Index Only Scan",
    "Bitmap Heap Scan",
];
/// What a nested loop may run per outer row.
const PROBES: &[&str] = &[
    "Index Scan",
    "Index Only Scan",
    "Bitmap Index Scan",
    "Bitmap Heap Scan",
    "Memoize",
];

struct Node {
    kind: String,
    estimated: f64,
    actual: f64,
    loops: f64,
}

fn nodes(plan: &Value, out: &mut Vec<Node>) {
    let number = |key: &str| plan.get(key).and_then(Value::as_f64).unwrap_or(0.0);
    out.push(Node {
        kind: plan["Node Type"].as_str().unwrap_or_default().to_string(),
        estimated: number("Plan Rows"),
        actual: number("Actual Rows"),
        loops: number("Actual Loops"),
    });
    for child in plan["Plans"].as_array().into_iter().flatten() {
        nodes(child, out);
    }
}

async fn ids(conn: &mut PgConnection, table: &str) -> Vec<i64> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT id FROM {table} ORDER BY id LIMIT 50"
    )))
    .fetch_all(conn)
    .await
    .expect("ids")
}

async fn offenders(
    conn: &mut PgConnection,
    rollup: &RollupDef,
    changed: Changed,
    ids: &[i64],
) -> Vec<String> {
    let sql = rollup
        .recompute_for(changed, ids.iter().map(|id| Expr::val(*id)))
        .to_string(PostgresQueryBuilder);
    let explained: Value = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "EXPLAIN (ANALYZE, FORMAT JSON) {sql}"
    )))
    .fetch_one(conn)
    .await
    .expect("explain");
    let mut all = Vec::new();
    nodes(&explained[0]["Plan"], &mut all);
    let mut off = Vec::new();
    for node in all {
        if SCANS.contains(&node.kind.as_str())
            && node.actual >= MIN_ROWS
            && node.actual > node.estimated.max(1.0) * UNDERESTIMATE
        {
            off.push(format!(
                "{} estimated {} rows, read {}",
                node.kind, node.estimated, node.actual
            ));
        }
        if node.loops > 1.0 && !PROBES.contains(&node.kind.as_str()) {
            off.push(format!("{} ran {} times", node.kind, node.loops));
        }
    }
    off
}

async fn assert_rollups_plan(db: &TdPool, project: &ProjectDB) -> Result<(), TdError> {
    let mut trx = begin_exclusive(db, project.project_id).await?;
    let conn: &mut PgConnection = trx.get(Grant::asserted("plan guard"));
    let runs = ids(conn, "logical_function_runs").await;
    let attempts = ids(conn, "physical_function_runs").await;
    assert!(
        !runs.is_empty() && !attempts.is_empty(),
        "the project seeded no runs"
    );
    for (name, rollup) in [
        ("transactions", transaction_rollup()),
        ("executions", execution_rollup()),
    ] {
        for (changed, ids) in [
            (Changed::LogicalRuns, &runs),
            (Changed::PhysicalRuns, &attempts),
        ] {
            let off = offenders(conn, &rollup, changed, ids).await;
            assert!(
                off.is_empty(),
                "{name} roll-up in {} from {} changed rows: {off:?}",
                project.name,
                ids.len()
            );
        }
    }
    Ok(())
}

fn knobs(projects: usize) -> MetricsKnobs {
    MetricsKnobs {
        publishers: 160 * projects,
        consumers: 80 * projects,
        projects,
    }
}

#[td_test::test(db)]
async fn rollups_plan_from_estimates_within_a_project(db: TdPool) -> Result<(), TdError> {
    dataset::metrics_dataset(
        &db,
        MetricsKnobs {
            publishers: 160,
            consumers: 80,
            projects: PROJECTS,
        },
    )
    .await?;
    analyze(&db, "roll-up plans").await;

    let projects: Vec<ProjectDB> = db.scd2().fetch_all::<ProjectDB>(()).await?;
    assert!(projects.len() >= PROJECTS);
    for project in &projects[..PROJECTS] {
        assert_rollups_plan(&db, project).await?;
    }
    Ok(())
}

#[td_test::test(db)]
async fn rollups_plan_from_estimates_for_a_project_the_statistics_have_not_seen(
    db: TdPool,
) -> Result<(), TdError> {
    dataset::metrics_dataset(&db, knobs(1)).await?;
    analyze(&db, "roll-up plans").await;
    dataset::metrics_dataset(&db, knobs(2)).await?;

    let projects: Vec<ProjectDB> = db.scd2().fetch_all::<ProjectDB>(()).await?;
    let unseen = projects
        .iter()
        .find(|p| p.name.to_string() == "test_p1")
        .expect("the second dataset seeds test_p1");
    assert_rollups_plan(&db, unseen).await
}
