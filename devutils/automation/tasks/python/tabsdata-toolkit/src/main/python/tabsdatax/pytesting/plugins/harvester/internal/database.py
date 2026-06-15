#
#  Copyright 2026 Tabs Data Inc.
#

"""SQLite-based harvester database for fixture timing and other events.

Uses WAL mode for concurrent reads (monitor) while workers write.
Each process (controller + workers) opens its own connection.

The database lives in the harvester folder as ``harvester.db``.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time

# Thread-local storage for per-thread connections
_local = threading.local()

DB_FILENAME = "_meta_database.db"


def db_path(harvester_folder: str) -> str:
    """Return the full path to the harvester database."""
    return os.path.join(harvester_folder, DB_FILENAME)


def get_connection(harvester_folder: str) -> sqlite3.Connection:
    """Get or create a thread-local SQLite connection with WAL mode.

    Detects when the DB file has been replaced (e.g. by ``shutil.rmtree``
    at session start) and reconnects so readers never use a stale handle.
    """
    path = db_path(harvester_folder)
    conn = getattr(_local, "conn", None)
    conn_path = getattr(_local, "conn_path", None)
    if conn is not None and conn_path == path:
        # Check if the file was replaced (inode changed).
        try:
            current_ino = os.stat(path).st_ino
        except OSError:
            current_ino = None
        cached_ino = getattr(_local, "conn_ino", None)
        if current_ino is not None and current_ino == cached_ino:
            return conn
        # File was replaced or deleted — close stale connection.
        try:
            conn.close()
        except Exception:
            pass
    conn = sqlite3.connect(path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    _local.conn = conn
    _local.conn_path = path
    try:
        _local.conn_ino = os.stat(path).st_ino
    except OSError:
        _local.conn_ino = None
    return conn


def init_db(harvester_folder: str) -> None:
    """Create the database and tables if they don't exist."""
    conn = get_connection(harvester_folder)
    conn.executescript("""
        create table if not exists fixture_events (
            id          integer primary key autoincrement,
            fixture     text    not null,
            scope       text    not null,
            event       text    not null,
            worker      text    not null default '',
            pid         integer not null default 0,
            timestamp   real    not null,
            duration    real
        );

        create index if not exists idx_fixture_events_fixture
            on fixture_events (fixture);

        create index if not exists idx_fixture_events_event
            on fixture_events (event);

        create table if not exists test_completions (
            id          integer primary key autoincrement,
            nodeid      text    not null,
            outcome     text    not null,
            duration    real    not null,
            worker      text    not null default '',
            component   text    not null default '',
            timestamp   real    not null
        );

        create index if not exists idx_test_completions_outcome
            on test_completions (outcome);

        create index if not exists idx_test_completions_component
            on test_completions (component);

        create table if not exists test_records (
            nodeid                    text primary key,
            status                    text not null,
            phase                     text,
            file                      text,
            line                      integer,
            function                  text,
            start                     real,
            stop                      real,
            worker                    text not null default '',
            pid                       integer not null default 0,
            duration_setup            real,
            duration_call             real,
            duration_teardown         real,
            error                     text,
            error_type                text,
            traceback                 text,
            error_location            text,
            skip_reason               text,
            teardown_error            text,
            teardown_error_type       text,
            teardown_traceback        text,
            teardown_error_location   text,
            updated_at                real not null
        );

        create index if not exists idx_test_records_status
            on test_records (status);
    """)
    conn.commit()


def reset_db(harvester_folder: str) -> None:
    """Drop and recreate all tables (called at session start)."""
    conn = get_connection(harvester_folder)
    conn.executescript("""
        drop table if exists fixture_events;
        drop table if exists test_completions;
        drop table if exists test_records;
    """)
    conn.commit()
    init_db(harvester_folder)


TEST_RECORD_COLUMNS = frozenset({
    "status",
    "phase",
    "file",
    "line",
    "function",
    "start",
    "stop",
    "worker",
    "pid",
    "duration_setup",
    "duration_call",
    "duration_teardown",
    "error",
    "error_type",
    "traceback",
    "error_location",
    "skip_reason",
    "teardown_error",
    "teardown_error_type",
    "teardown_traceback",
    "teardown_error_location",
})


# ---------------------------------------------------------------------------
# Write helpers (called by workers and controller)
# ---------------------------------------------------------------------------


def record_fixture_setup_start(
    harvester_folder: str,
    fixture: str,
    scope: str,
    worker: str = "",
) -> int:
    """Record that a fixture setup has started.  Returns the row ID."""
    conn = get_connection(harvester_folder)
    cursor = conn.execute(
        "insert into fixture_events (fixture, scope, event, worker, pid, timestamp)"
        " values (?, ?, 'setup_start', ?, ?, ?)",
        (fixture, scope, worker, os.getpid(), time.time()),
    )
    conn.commit()
    return cursor.lastrowid


def record_fixture_setup_end(
    harvester_folder: str,
    fixture: str,
    scope: str,
    duration: float,
    worker: str = "",
) -> None:
    """Record that a fixture setup has completed."""
    conn = get_connection(harvester_folder)
    conn.execute(
        "insert into fixture_events"
        " (fixture, scope, event, worker, pid,"
        " timestamp, duration)"
        " values (?, ?, 'setup_end', ?, ?, ?, ?)",
        (fixture, scope, worker, os.getpid(), time.time(), duration),
    )
    conn.commit()


def record_fixture_teardown_end(
    harvester_folder: str,
    fixture: str,
    scope: str,
    worker: str = "",
) -> None:
    """Record that a fixture teardown has completed."""
    conn = get_connection(harvester_folder)
    conn.execute(
        "insert into fixture_events (fixture, scope, event, worker, pid, timestamp)"
        " values (?, ?, 'teardown_end', ?, ?, ?)",
        (fixture, scope, worker, os.getpid(), time.time()),
    )
    conn.commit()


def record_test_completion(
    harvester_folder: str,
    nodeid: str,
    outcome: str,
    duration: float,
    worker: str = "",
    component: str = "",
) -> None:
    """Record a completed test with its duration and outcome."""
    conn = get_connection(harvester_folder)
    conn.execute(
        "insert into test_completions"
        " (nodeid, outcome, duration, worker, component, timestamp)"
        " values (?, ?, ?, ?, ?, ?)",
        (nodeid, outcome, duration, worker, component, time.time()),
    )
    conn.commit()


def record_test_running(
    harvester_folder: str,
    nodeid: str,
    file: str,
    line: int,
    function: str,
    status: str,
    start: float,
    worker: str = "",
) -> None:
    """Insert or replace a test_records row with status='Running'."""
    conn = get_connection(harvester_folder)
    conn.execute(
        """
        insert into test_records (
            nodeid, status, file, line, function, start,
            worker, pid, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(nodeid) do update set
            status     = excluded.status,
            file       = excluded.file,
            line       = excluded.line,
            function   = excluded.function,
            start      = excluded.start,
            stop       = null,
            worker     = excluded.worker,
            pid        = excluded.pid,
            phase      = null,
            duration_setup    = null,
            duration_call     = null,
            duration_teardown = null,
            error             = null,
            error_type        = null,
            traceback         = null,
            error_location    = null,
            skip_reason       = null,
            teardown_error             = null,
            teardown_error_type        = null,
            teardown_traceback         = null,
            teardown_error_location    = null,
            updated_at = excluded.updated_at
        """,
        (nodeid, status, file, line, function, start, worker, os.getpid(), time.time()),
    )
    conn.commit()


def update_test_record(
    harvester_folder: str,
    nodeid: str,
    **fields,
) -> None:
    """Apply a partial update to a test_records row by nodeid."""
    columns = {
        key: value for key, value in fields.items() if key in TEST_RECORD_COLUMNS
    }
    if not columns:
        return
    assignments = ", ".join(f"{column} = ?" for column in columns)
    parameters = list(columns.values()) + [time.time(), nodeid]
    conn = get_connection(harvester_folder)
    conn.execute(
        f"update test_records set {assignments}, updated_at = ? where nodeid = ?",
        parameters,
    )
    conn.commit()


def promote_passed_to_teardown_failure(
    harvester_folder: str,
    nodeid: str,
    teardown_error: str,
    teardown_error_type: str,
    teardown_traceback: str,
    teardown_error_location: str | None,
    failed_teardown_status: str,
    teardown_phase: str,
    stop: float,
    duration_teardown: float | None,
) -> bool:
    """Promote a PASSED record to FAILED_TEARDOWN atomically.

    Returns True when the row was promoted (status was previously PASSED).
    """
    conn = get_connection(harvester_folder)
    cursor = conn.execute(
        """
        update test_records set
            status                  = ?,
            phase                   = ?,
            stop                    = ?,
            duration_teardown       = coalesce(?, duration_teardown),
            teardown_error          = ?,
            teardown_error_type     = ?,
            teardown_traceback      = ?,
            teardown_error_location = ?,
            updated_at              = ?
        where nodeid = ? and status = 'Passed'
        """,
        (
            failed_teardown_status,
            teardown_phase,
            stop,
            duration_teardown,
            teardown_error,
            teardown_error_type,
            teardown_traceback,
            teardown_error_location,
            time.time(),
            nodeid,
        ),
    )
    conn.commit()
    return cursor.rowcount > 0


def record_teardown_error_fields(
    harvester_folder: str,
    nodeid: str,
    teardown_error: str,
    teardown_error_type: str,
    teardown_traceback: str,
    teardown_error_location: str | None,
    stop: float,
    duration_teardown: float | None,
) -> None:
    """Attach teardown-error fields to a record without changing its status."""
    conn = get_connection(harvester_folder)
    conn.execute(
        """
        update test_records set
            stop                    = coalesce(?, stop),
            duration_teardown       = coalesce(?, duration_teardown),
            teardown_error          = ?,
            teardown_error_type     = ?,
            teardown_traceback      = ?,
            teardown_error_location = ?,
            updated_at              = ?
        where nodeid = ?
        """,
        (
            stop,
            duration_teardown,
            teardown_error,
            teardown_error_type,
            teardown_traceback,
            teardown_error_location,
            time.time(),
            nodeid,
        ),
    )
    conn.commit()


def upsert_test_final(
    harvester_folder: str,
    nodeid: str,
    status: str,
    **fields,
) -> None:
    """Upsert a row with a final status (PASSED/SKIPPED/FAILED_*/UNKNOWN).

    If the row exists, status and provided fields are updated. If it does not
    exist (e.g. xdist controller synthesis), the row is created with the
    provided fields and sensible defaults for the rest.
    """
    columns = {
        key: value for key, value in fields.items() if key in TEST_RECORD_COLUMNS
    }
    now = time.time()
    pid = os.getpid()
    worker = columns.get("worker", "")
    insert_columns = ["nodeid", "status", "pid", "updated_at"]
    insert_values = [nodeid, status, pid, now]
    if "worker" not in columns:
        insert_columns.append("worker")
        insert_values.append(worker)
    for column, value in columns.items():
        insert_columns.append(column)
        insert_values.append(value)
    placeholders = ", ".join("?" for _ in insert_columns)
    insert_sql = (
        f"insert into test_records ({', '.join(insert_columns)}) "
        f"values ({placeholders})"
    )
    update_pairs = ["status = excluded.status", "updated_at = excluded.updated_at"]
    for column in columns:
        update_pairs.append(f"{column} = excluded.{column}")
    update_sql = f"on conflict(nodeid) do update set {', '.join(update_pairs)}"
    conn = get_connection(harvester_folder)
    conn.execute(f"{insert_sql} {update_sql}", insert_values)
    conn.commit()


def record_exists(harvester_folder: str, nodeid: str) -> bool:
    """Return True if a test_records row exists for the given nodeid."""
    path = db_path(harvester_folder)
    if not os.path.exists(path):
        return False
    conn = get_connection(harvester_folder)
    row = conn.execute(
        "select 1 from test_records where nodeid = ? limit 1",
        (nodeid,),
    ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# Query helpers (called by the monitor)
# ---------------------------------------------------------------------------


def query_fixture_stats(harvester_folder: str) -> list[dict]:
    """Return aggregated fixture setup/teardown stats.

    Returns a list of dicts with keys:
        fixture, scope, setup_count, teardown_count,
        avg_setup_duration, max_setup_duration, total_setup_duration,
        active_setups
    """
    path = db_path(harvester_folder)
    if not os.path.exists(path):
        return []

    try:
        conn = get_connection(harvester_folder)
        rows = conn.execute("""
            select
                fixture,
                scope,
                sum(case when event = 'setup_end' then 1 else 0 end) as setup_count,
                sum(case when event = 'teardown_end'
                    then 1 else 0 end) as teardown_count,
                avg(case when event = 'setup_end'
                    then duration end) as avg_setup_duration,
                max(case when event = 'setup_end'
                    then duration end) as max_setup_duration,
                sum(case when event = 'setup_end'
                    then duration else 0 end)
                    as total_setup_duration,
                sum(case when event = 'setup_start' then 1 else 0 end)
                - sum(case when event = 'setup_end' then 1 else 0 end) as active_setups
            from fixture_events
            group by fixture, scope
            order by fixture
        """).fetchall()
        return [dict(row) for row in rows]
    except (sqlite3.OperationalError, sqlite3.DatabaseError):
        return []


def query_component_stats(harvester_folder: str) -> list[dict]:
    """Return aggregated per-component test stats.

    Returns a list of dicts with keys:
        component, setup_count, teardown_count, avg_duration
    """
    path = db_path(harvester_folder)
    if not os.path.exists(path):
        return []
    try:
        conn = get_connection(harvester_folder)
        rows = conn.execute("""
            select
                component,
                count(*)                                     as setup_count,
                sum(case when outcome in ('passed', 'failed')
                    then 1 else 0 end)                       as teardown_count,
                avg(case when outcome in ('passed', 'failed')
                    then duration end)                       as avg_duration
            from test_completions
            where component != ''
            group by component
            order by component
        """).fetchall()
        return [dict(row) for row in rows]
    except (sqlite3.OperationalError, sqlite3.DatabaseError):
        return []


def query_outcome_counts(harvester_folder: str) -> dict:
    path = db_path(harvester_folder)
    if not os.path.exists(path):
        return {"passed": 0, "failed": 0, "skipped": 0}
    try:
        conn = get_connection(harvester_folder)
        rows = conn.execute(
            "select outcome, count(*) from test_completions group by outcome"
        ).fetchall()
        counts = {"passed": 0, "failed": 0, "skipped": 0}
        for outcome, count in rows:
            if outcome in counts:
                counts[outcome] = int(count)
        return counts
    except (sqlite3.OperationalError, sqlite3.DatabaseError):
        return {"passed": 0, "failed": 0, "skipped": 0}


def query_test_averages(harvester_folder: str) -> dict:
    """Return average durations by outcome.

    Returns a dict with keys: avg_all, avg_passed, avg_failed,
    count_passed, count_failed.
    """
    path = db_path(harvester_folder)
    if not os.path.exists(path):
        return {}
    try:
        conn = get_connection(harvester_folder)
        row = conn.execute("""
            select
                avg(duration) as avg_all,
                avg(case when outcome = 'passed' then duration end) as avg_passed,
                avg(case when outcome = 'failed' then duration end) as avg_failed,
                sum(case when outcome = 'passed' then 1 else 0 end) as count_passed,
                sum(case when outcome = 'failed' then 1 else 0 end) as count_failed
            from test_completions
            where outcome in ('passed', 'failed')
        """).fetchone()
        return dict(row) if row else {}
    except (sqlite3.OperationalError, sqlite3.DatabaseError):
        return {}


def query_tests_per_minute(
    harvester_folder: str, session_start_ts: float | None = None
) -> list[dict]:
    """Return test counts bucketed by minute since session start.

    Each element is a dict with keys:
        minute, total, passed, failed, skipped
    """
    path = db_path(harvester_folder)
    if not os.path.exists(path) or session_start_ts is None:
        return []
    try:
        conn = get_connection(harvester_folder)
        rows = conn.execute(
            """
            select
                cast((timestamp - ?) / 60 as integer) + 1 as minute,
                count(*)                                    as total,
                sum(case when outcome = 'passed'  then 1 else 0 end) as passed,
                sum(case when outcome = 'failed'  then 1 else 0 end) as failed,
                sum(case when outcome = 'skipped' then 1 else 0 end) as skipped
            from test_completions
            group by cast((timestamp - ?) / 60 as integer)
            order by minute
            """,
            (session_start_ts, session_start_ts),
        ).fetchall()
        return [dict(row) for row in rows]
    except (sqlite3.OperationalError, sqlite3.DatabaseError):
        return []


def query_test_durations(harvester_folder: str) -> dict[str, list[float]]:
    """Return test durations by outcome, for distribution rendering.

    Returns ``{"all": [...], "passed": [...], "failed": [...]}``.
    """
    path = db_path(harvester_folder)
    if not os.path.exists(path):
        return {"all": [], "passed": [], "failed": []}
    try:
        conn = get_connection(harvester_folder)
        rows = conn.execute(
            "select outcome, duration from test_completions"
            " where outcome in ('passed', 'failed')"
            " order by duration"
        ).fetchall()
        result: dict[str, list[float]] = {"all": [], "passed": [], "failed": []}
        for outcome, duration in rows:
            result["all"].append(duration)
            result[outcome].append(duration)
        return result
    except (sqlite3.OperationalError, sqlite3.DatabaseError):
        return {"all": [], "passed": [], "failed": []}


def query_test_records(harvester_folder: str) -> list[sqlite3.Row]:
    """Return every row from test_records."""
    path = db_path(harvester_folder)
    if not os.path.exists(path):
        return []
    try:
        conn = get_connection(harvester_folder)
        return list(conn.execute("select * from test_records").fetchall())
    except (sqlite3.OperationalError, sqlite3.DatabaseError):
        return []
