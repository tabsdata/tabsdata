//
// Copyright 2026 Tabsdata Inc.
//

//! v16 → v17 ends the attempts, and their function workers, that a cancel
//! before 2.2.0 left live under a canceled run: where a cancel ends them now,
//! with their transactions and executions rolled up from the new ends. The
//! replays of committed and yanked runs stay live.

use crate::components::ApiserverComponent;
use crate::generated_sql::generated;
use crate::test_utils::domain::PostgresDomain;
use crate::test_utils::postgres::pool;
use td_schema::schema::{archive, deltas};

use super::{execute, scalars};

const SEED: &[&str] = &[
    "INSERT INTO projects (id, name, description, base_path, created_on, created_by_id, \
       modified_on, modified_by_id) \
     VALUES ('p0', 'p0', '', '/p', '2026-01-01 06:00:00+00', 'u0', now(), 'u0')",
    "INSERT INTO groups (id, project_id, name, description, created_on, created_by_id, \
       modified_on, modified_by_id, system, kind) \
     VALUES ('g0', 'p0', 'g0', '', now(), 'u0', now(), 'u0', false, 'R')",
    "INSERT INTO collections (id, project_id, name, description, created_on, created_by_id, \
       modified_on, modified_by_id, group_id, collection_type) \
     VALUES ('c0', 'p0', 'c0', '', now(), 'u0', now(), 'u0', 'g0', 'R')",
    "INSERT INTO functions (id, project_id, collection_id, name, description, decorator, \
       runtime_values, function_id, stage_id, stage_base_path, storage_version, bundle_id, \
       snippet, valid_from, defined_by_id, status, enabled, valid_to, function_type) \
     VALUES (1, 'p0', 'c0', 'f', '', 'P', '', 'f0', 's0', '/s', 'V1', 'b0', '', \
       '2026-01-01 08:00:00+00', 'u0', 'A', true, 'infinity', 'R')",
    "INSERT INTO project_topologies (id, project_id, valid_from, valid_to) \
     VALUES ('tp0', 'p0', '2026-01-01 06:00:00+00', 'infinity')",
];

/// (execution, transaction, its status, its end)
const TRANSACTIONS: &[(u32, u32, &str, &str)] = &[
    // Cancelled at 09:30.
    (10, 20, "X", "'2026-01-01 09:30:00+00'"),
    // Committed, then yanked; ended at 10:00.
    (11, 21, "Y", "'2026-01-01 10:00:00+00'"),
    // Still running.
    (12, 22, "R", "NULL"),
    // Committed at 09:05.
    (13, 23, "C", "'2026-01-01 09:05:00+00'"),
];

/// A run with one attempt and its function worker. Clock times are on the
/// seed day; `NULL` leaves the instant unset.
struct Run {
    id: u32,
    transaction: u32,
    status: &'static str,
    attempt: &'static str,
    dispatched: &'static str,
    started: &'static str,
    ended: &'static str,
    worker: &'static str,
}

const RUNS: &[Run] = &[
    // Live past its run's cancel: ends at the cancel.
    Run {
        id: 30,
        transaction: 20,
        status: "X",
        attempt: "R",
        dispatched: "09:15",
        started: "09:20",
        ended: "NULL",
        worker: "R",
    },
    // Started after its run's cancel, as 2.0.0 did: ends as it began.
    Run {
        id: 31,
        transaction: 20,
        status: "X",
        attempt: "R",
        dispatched: "09:35",
        started: "09:40",
        ended: "NULL",
        worker: "RR",
    },
    // A replay of a yanked run, dispatched, not started yet: untouched.
    Run {
        id: 32,
        transaction: 21,
        status: "Y",
        attempt: "DP",
        dispatched: "10:30",
        started: "NULL",
        ended: "NULL",
        worker: "RR",
    },
    // Done under a committed run: untouched.
    Run {
        id: 33,
        transaction: 22,
        status: "C",
        attempt: "D",
        dispatched: "09:00",
        started: "09:01",
        ended: "'2026-01-01 09:05:00+00'",
        worker: "D",
    },
    // Live under a live run: untouched.
    Run {
        id: 34,
        transaction: 22,
        status: "R",
        attempt: "R",
        dispatched: "09:00",
        started: "09:01",
        ended: "NULL",
        worker: "R",
    },
    // A replay of a committed run, running: untouched.
    Run {
        id: 35,
        transaction: 23,
        status: "C",
        attempt: "R",
        dispatched: "11:00",
        started: "11:01",
        ended: "NULL",
        worker: "R",
    },
];

fn instant(clock: &str) -> String {
    match clock {
        "NULL" => clock.to_string(),
        _ => format!("'2026-01-01 {clock}:00+00'"),
    }
}

fn ends(table: &str) -> String {
    format!(
        "SELECT id || '=' || status || '@' || \
           COALESCE(to_char(ended_on AT TIME ZONE 'UTC', 'HH24:MI'), '-') \
         FROM {table} ORDER BY id"
    )
}

const TABLES: &[&str] = &[
    "physical_function_runs",
    "workers",
    "transactions",
    "executions",
];

#[td_test::domain("apiserver_postgres_internals")]
#[tokio::test]
async fn v16_to_v17_ends_live_attempts_of_canceled_runs_postgres(
    domain_apiserver_postgres_internals: PostgresDomain,
) {
    let db = pool(&domain_apiserver_postgres_internals).await;
    db.admin
        .upgrade_source(archive::source(16))
        .await
        .expect("installing archive v16");
    for stmt in SEED {
        execute(&db.serving, *stmt).await;
    }
    for (execution, transaction, status, ended) in TRANSACTIONS {
        let execution_status = if *ended == "NULL" { "R" } else { "F" };
        for stmt in [
            format!(
                "INSERT INTO executions (id, project_id, collection_id, function_version_id, \
                   function_id, triggered_on, triggered_by_id, trigger_type, status, ended_on, \
                   topology_version_id) \
                 VALUES ({execution}, 'p0', 'c0', 1, 'f0', '2026-01-01 09:00:00+00', 'u0', 'M', \
                   '{execution_status}', {ended}, 'tp0')"
            ),
            format!(
                "INSERT INTO transactions (id, project_id, collection_id, execution_id, \
                   transaction_by, transaction_key, triggered_on, triggered_by_id, group_id, \
                   status, ended_on) \
                 VALUES ({transaction}, 'p0', 'c0', {execution}, 'F', 'k{transaction}', \
                   '2026-01-01 09:00:00+00', 'u0', 'g0', '{status}', {ended})"
            ),
        ] {
            execute(&db.serving, &stmt).await;
        }
    }
    for Run {
        id: run,
        transaction,
        status: run_status,
        attempt,
        dispatched,
        started,
        ended,
        worker,
    } in RUNS
    {
        let execution = transaction - 10;
        let (dispatched, started) = (instant(dispatched), instant(started));
        for stmt in [
            format!(
                "INSERT INTO logical_function_runs (id, project_id, collection_id, execution_id, \
                   transaction_id, function_id, function_version_id, trigger, triggered_on, \
                   triggered_by_id, status, group_id, recoveries) \
                 VALUES ({run}, 'p0', 'c0', {execution}, {transaction}, 'f{run}', 1, 'M', \
                   '2026-01-01 09:00:00+00', 'u0', '{run_status}', 'g0', 0)"
            ),
            format!(
                "INSERT INTO physical_function_runs (id, project_id, collection_id, \
                   function_version_id, execution_id, transaction_id, valid_from, \
                   triggered_by_id, trigger, function_id, status, base_path, \
                   logical_function_run_id, valid_to, dispatched_on, started_on, ended_on) \
                 VALUES ({run}0, 'p0', 'c0', 1, {execution}, {transaction}, {dispatched}, 'u0', \
                   'M', 'f{run}', '{attempt}', '/p', {run}, 'infinity', {dispatched}, \
                   {started}, {ended})"
            ),
            format!(
                "INSERT INTO workers (id, project_id, collection_id, execution_id, transaction_id, \
                   function_version_id, physical_function_run_id, status, worker_type, \
                   worker_enabled, failures, started_on) \
                 VALUES ({run}00, 'p0', 'c0', {execution}, {transaction}, 1, {run}0, \
                   '{worker}', 'F', true, 0, {started})"
            ),
        ] {
            execute(&db.serving, &stmt).await;
        }
    }

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v16 -> current");

    assert_eq!(
        scalars(&db.serving, ends("physical_function_runs")).await,
        [
            "300=F@09:30",
            "310=F@09:40",
            "320=DP@-",
            "330=D@09:05",
            "340=R@-",
            "350=R@-"
        ]
    );
    assert_eq!(
        scalars(&db.serving, ends("workers")).await,
        [
            "3000=F@09:30",
            "3100=F@09:40",
            "3200=RR@-",
            "3300=D@-",
            "3400=R@-",
            "3500=R@-"
        ]
    );
    assert_eq!(
        scalars(&db.serving, ends("transactions")).await,
        ["20=X@09:40", "21=Y@10:00", "22=R@-", "23=C@09:05"]
    );
    assert_eq!(
        scalars(&db.serving, ends("executions")).await,
        ["10=F@09:40", "11=F@10:00", "12=R@-", "13=F@09:05"]
    );
    assert_eq!(
        scalars(
            &db.serving,
            "SELECT id || '=' || status FROM logical_function_runs ORDER BY id"
        )
        .await,
        ["30=X", "31=X", "32=Y", "33=C", "34=R", "35=C"]
    );

    let mut upgraded = Vec::new();
    for table in TABLES {
        upgraded.push(scalars(&db.serving, ends(table)).await);
    }
    let up = deltas::get(17).up;
    let repair = &up[1..up.len() - 1];
    let mut transaction = db.serving.as_ref().begin().await.unwrap();
    for stmt in repair {
        sqlx::query(generated(stmt.clone()))
            .execute(&mut *transaction)
            .await
            .unwrap_or_else(|e| panic!("rerunning {stmt}: {e}"));
    }
    transaction.commit().await.unwrap();
    let mut rerun = Vec::new();
    for table in TABLES {
        rerun.push(scalars(&db.serving, ends(table)).await);
    }
    assert_eq!(
        rerun, upgraded,
        "rerunning the repair changed the repaired rows"
    );
}
