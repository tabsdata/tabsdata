//
// Copyright 2026 Tabsdata Inc.
//

//! Statements tests use to observe or hold the database itself: sessions,
//! locks and triggers, rather than rows.

use sqlx::{Encode, Error, Executor, PgConnection, Postgres, Type};
use td_database::test_utils::generated;

/// A setting's value on `conn`, as `SHOW` prints it.
pub async fn show_setting(conn: &mut PgConnection, setting: &str) -> Result<String, Error> {
    sqlx::query_scalar("SELECT current_setting($1)")
        .bind(setting)
        .fetch_one(conn)
        .await
}

/// Holds `table` against every other reader and writer until `conn`'s
/// transaction ends.
pub async fn lock_table_exclusive(conn: &mut PgConnection, table: &str) -> Result<(), Error> {
    conn.execute(generated(format!(
        "LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"
    )))
    .await
    .map(|_| ())
}

/// Takes `id`'s row of `table` `FOR UPDATE` without waiting: an error when
/// another transaction holds it.
pub async fn lock_row_nowait<'q, I>(
    conn: &mut PgConnection,
    table: &str,
    id: I,
) -> Result<(), Error>
where
    I: 'q + Encode<'q, Postgres> + Type<Postgres> + Send,
{
    sqlx::query(generated(format!(
        "SELECT 1 FROM {table} WHERE id = $1 FOR UPDATE NOWAIT"
    )))
    .bind(id)
    .execute(conn)
    .await
    .map(|_| ())
}

pub async fn backend_pid(conn: &mut PgConnection) -> Result<i32, Error> {
    sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(conn)
        .await
}

pub async fn in_transaction(conn: &mut PgConnection, pid: i32) -> Result<bool, Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND locktype = 'virtualxid')",
    )
    .bind(pid)
    .fetch_one(conn)
    .await
}

/// How many locks other backends hold on `relation`.
pub async fn locks_held_by_others(conn: &mut PgConnection, relation: &str) -> Result<i64, Error> {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks \
         WHERE relation = $1::regclass AND pid <> pg_backend_pid()",
    )
    .bind(relation)
    .fetch_one(conn)
    .await
}

/// `(kind, from, to, allowed)`: a status change a trigger is checked on.
pub type TransitionCase = (String, String, String, bool);

/// Attaches `trigger_fn` to a scratch table and tries every case on it,
/// returning those where the trigger disagrees with `allowed`.
pub async fn trigger_disagreements(
    conn: &mut PgConnection,
    trigger_fn: &str,
    cases: &[TransitionCase],
) -> Result<Vec<String>, Error> {
    conn.execute(
        "CREATE TEMP TABLE transitions_probe (worker_type text, status text) ON COMMIT DROP",
    )
    .await?;
    conn.execute(generated(format!(
        "CREATE TRIGGER transitions_probe BEFORE UPDATE OF status ON transitions_probe \
         FOR EACH ROW EXECUTE FUNCTION {trigger_fn}()"
    )))
    .await?;

    let mut mismatches = Vec::new();
    for (kind, from, to, allowed) in cases {
        conn.execute("DELETE FROM transitions_probe").await?;
        sqlx::query("INSERT INTO transitions_probe VALUES ($1, $2)")
            .bind(kind)
            .bind(from)
            .execute(&mut *conn)
            .await?;
        conn.execute("SAVEPOINT probe").await?;
        let moved = sqlx::query("UPDATE transitions_probe SET status = $1")
            .bind(to)
            .execute(&mut *conn)
            .await
            .is_ok();
        conn.execute("ROLLBACK TO SAVEPOINT probe").await?;
        if moved != *allowed {
            mismatches.push(format!(
                "{kind} {from} -> {to}: trigger {moved}, relation {allowed}"
            ));
        }
    }
    Ok(mismatches)
}
