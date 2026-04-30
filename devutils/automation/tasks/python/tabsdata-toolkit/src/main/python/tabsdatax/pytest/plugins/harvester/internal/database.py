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
        CREATE TABLE IF NOT EXISTS fixture_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture     TEXT    NOT NULL,
            scope       TEXT    NOT NULL,
            event       TEXT    NOT NULL,
            -- 'setup_start', 'setup_end', 'teardown_end'
            worker      TEXT    NOT NULL DEFAULT '',
            pid         INTEGER NOT NULL DEFAULT 0,
            timestamp   REAL    NOT NULL,
            duration    REAL
            -- NULL for setup_start, filled for others
        );

        CREATE INDEX IF NOT EXISTS idx_fixture_events_fixture
            ON fixture_events (fixture);

        CREATE INDEX IF NOT EXISTS idx_fixture_events_event
            ON fixture_events (event);

        CREATE TABLE IF NOT EXISTS test_completions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            nodeid      TEXT    NOT NULL,
            outcome     TEXT    NOT NULL,   -- 'passed', 'failed', 'skipped'
            duration    REAL    NOT NULL,
            worker      TEXT    NOT NULL DEFAULT '',
            timestamp   REAL    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_test_completions_outcome
            ON test_completions (outcome);
    """)
    conn.commit()


def reset_db(harvester_folder: str) -> None:
    """Drop and recreate all tables (called at session start)."""
    conn = get_connection(harvester_folder)
    conn.executescript("""
        DROP TABLE IF EXISTS fixture_events;
        DROP TABLE IF EXISTS test_completions;
    """)
    conn.commit()
    init_db(harvester_folder)


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
        "INSERT INTO fixture_events (fixture, scope, event, worker, pid, timestamp)"
        " VALUES (?, ?, 'setup_start', ?, ?, ?)",
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
        "INSERT INTO fixture_events"
        " (fixture, scope, event, worker, pid,"
        " timestamp, duration)"
        " VALUES (?, ?, 'setup_end', ?, ?, ?, ?)",
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
        "INSERT INTO fixture_events (fixture, scope, event, worker, pid, timestamp)"
        " VALUES (?, ?, 'teardown_end', ?, ?, ?)",
        (fixture, scope, worker, os.getpid(), time.time()),
    )
    conn.commit()


def record_test_completion(
    harvester_folder: str,
    nodeid: str,
    outcome: str,
    duration: float,
    worker: str = "",
) -> None:
    """Record a completed test with its duration and outcome."""
    conn = get_connection(harvester_folder)
    conn.execute(
        "INSERT INTO test_completions (nodeid, outcome, duration, worker, timestamp)"
        " VALUES (?, ?, ?, ?, ?)",
        (nodeid, outcome, duration, worker, time.time()),
    )
    conn.commit()


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
            SELECT
                fixture,
                scope,
                SUM(CASE WHEN event = 'setup_end' THEN 1 ELSE 0 END) as setup_count,
                SUM(CASE WHEN event = 'teardown_end'
                    THEN 1 ELSE 0 END) as teardown_count,
                AVG(CASE WHEN event = 'setup_end'
                    THEN duration END) as avg_setup_duration,
                MAX(CASE WHEN event = 'setup_end'
                    THEN duration END) as max_setup_duration,
                SUM(CASE WHEN event = 'setup_end'
                    THEN duration ELSE 0 END)
                    as total_setup_duration,
                SUM(CASE WHEN event = 'setup_start' THEN 1 ELSE 0 END)
                - SUM(CASE WHEN event = 'setup_end' THEN 1 ELSE 0 END) as active_setups
            FROM fixture_events
            GROUP BY fixture, scope
            ORDER BY fixture
        """).fetchall()
        return [dict(row) for row in rows]
    except (sqlite3.OperationalError, sqlite3.DatabaseError):
        return []


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
            SELECT
                AVG(duration) as avg_all,
                AVG(CASE WHEN outcome = 'passed' THEN duration END) as avg_passed,
                AVG(CASE WHEN outcome = 'failed' THEN duration END) as avg_failed,
                SUM(CASE WHEN outcome = 'passed' THEN 1 ELSE 0 END) as count_passed,
                SUM(CASE WHEN outcome = 'failed' THEN 1 ELSE 0 END) as count_failed
            FROM test_completions
            WHERE outcome IN ('passed', 'failed')
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
            "SELECT outcome, duration FROM test_completions"
            " WHERE outcome IN ('passed', 'failed')"
            " ORDER BY duration"
        ).fetchall()
        result: dict[str, list[float]] = {"all": [], "passed": [], "failed": []}
        for outcome, duration in rows:
            result["all"].append(duration)
            result[outcome].append(duration)
        return result
    except (sqlite3.OperationalError, sqlite3.DatabaseError):
        return {"all": [], "passed": [], "failed": []}
