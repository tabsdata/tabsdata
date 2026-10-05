#
# Copyright 2026 Tabsdata Inc.
#

"""``_cdc_impl.py`` -- pure-function coverage for the Db2 CDC
dialect, CD-column matcher, SQL Replication ingester and offset.
Catalog / engine calls (``server_timezone``,
``gather_table_identity``, ``query_table_watermarks``,
``poll_changes``) need a real Db2 fixture and live in
``test__cdc_plugin_integration.py``."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone, tzinfo

import json
import logging
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa
import sqlalchemy.types as sa_types
from ibm_db_sa import LONGVARCHAR
from ibm_db_sa.base import DBCLOB, XML
from sqlalchemy.types import BLOB, CLOB, LargeBinary, Text

from tabsdatak.conn.common.sql.cdc._ingester import CDC_OFFSET_STATE_KEY
from tabsdatak.conn.common.sql.cdc._model import TableObject, TrackedTable
from tabsdatak.conn.common.sql.cdc.constants import DEFAULT_FETCH_SIZE
from tabsdatak.conn.db2 import _cdc_impl
from tabsdatak.conn.db2._cdc_impl import (
    COMMITSEQ_ORIGIN,
    INITIAL_LOAD_ISOLATION_LEVEL,
    CommitSeqOffset,
    Db2Dialect,
    MatchedColumns,
    RegistrationPositions,
    SqlReplicaIngester,
    SqlReplicaInitialLoadSession,
    commitseq_lsn,
    match_cd_columns,
    verify_positions,
)
from tabsdatak.conn.db2.error import Db2ErrorCode
from tabsdatak.error import TabsdataException

# =====================================================================
# Db2Dialect
# =====================================================================


@pytest.fixture
def d() -> Db2Dialect:
    return Db2Dialect()


# --- Identifier handling --------------------------------------------


def test_quotes(d):
    assert d.open_quote == '"'
    assert d.close_quote == '"'


def test_default_case_fold_uppers(d):
    # Db2 folds unquoted identifiers to upper case.
    assert d.default_case_fold("users") == "USERS"
    assert d.default_case_fold("MixedCase") == "MIXEDCASE"


def test_parse_qualified_table_simple(d):
    schema, table = d.parse_qualified_table("APP.USERS")
    assert str(schema) == "APP"
    assert str(table) == "USERS"


def test_parse_qualified_table_double_quoted(d):
    schema, table = d.parse_qualified_table('"MySchema"."Users"')
    assert str(schema) == "MySchema"
    assert str(table) == "Users"
    assert schema.quote is True
    assert table.quote is True


def test_parse_qualified_table_doubled_quote_escape(d):
    schema, table = d.parse_qualified_table('"weird ""schema".USERS')
    assert str(schema) == 'weird "schema'


def test_parse_qualified_table_allows_unqualified(d):
    # Unqualified CD name: schema is None; the ingester fills it from the
    # connection's CURRENT SCHEMA (see default_schema).
    schema, table = d.parse_qualified_table("USERS")
    assert schema is None
    assert str(table) == "USERS"


def test_parse_qualified_table_allows_unqualified_double_quoted(d):
    schema, table = d.parse_qualified_table('"Users"')
    assert schema is None
    assert str(table) == "Users"
    assert table.quote is True


def test_parse_qualified_table_rejects_empty_side(d):
    with pytest.raises(ValueError):
        d.parse_qualified_table(".USERS")
    with pytest.raises(ValueError):
        d.parse_qualified_table("APP.")
    with pytest.raises(ValueError):
        d.parse_qualified_table("")


def test_default_schema_queries_current_schema(d):
    # Db2 must NOT use SQLAlchemy's default_schema_name (the authorization-id
    # schema); it reads CURRENT SCHEMA, which honours the `currentSchema=`
    # connection attribute.
    inspector = MagicMock()
    connection = inspector.bind.connect.return_value.__enter__.return_value
    connection.execute.return_value.scalar.return_value = "  XP_20260719  "
    assert d.default_schema(inspector) == "XP_20260719"
    # the query text is CURRENT SCHEMA, not a reliance on default_schema_name
    sql = str(connection.execute.call_args.args[0]).lower()
    assert "current schema" in sql


# --- Large-column classification ------------------------------------


def test_large_column_kind_object_for_lob_and_xml(d):
    assert d.large_column_kind(BLOB()) == "object"
    assert d.large_column_kind(CLOB()) == "object"
    assert d.large_column_kind(LargeBinary()) == "object"
    assert d.large_column_kind(DBCLOB()) == "object"
    assert d.large_column_kind(XML()) == "object"


def test_large_column_kind_none_for_regular_types(d):
    assert d.large_column_kind(sa_types.Integer()) is None
    assert d.large_column_kind(sa_types.String()) is None
    assert d.large_column_kind(LONGVARCHAR()) is None
    assert d.large_column_kind(Text()) is None


# --- cdc_to_sa ingester dispatch ------------------------------------


def test_cdc_to_sa_unknown_ingester_raises(d):
    with pytest.raises(NotImplementedError):
        d.cdc_to_sa("wal2json", "integer")


def test_cdc_to_sa_sqlreplica_is_passthrough(d):
    # SqlReplicaIngester already returns SA types from CD-table
    # introspection -- no mapping needed, just identity.
    t = sa_types.Integer()
    assert d.cdc_to_sa("sqlreplica", t) is t


# =====================================================================
# match_cd_columns
# =====================================================================


def _col(name: str) -> tuple[str, sa_types.TypeEngine]:
    return (name, sa_types.Integer())


def test_match_cd_columns_pairs_before_and_after_image():
    # X is the before-image prefix; the user columns are id /
    # X_id / name / X_name -- both pairs match cleanly.
    cd_columns = [
        ("IBMSNAP_OPERATION", sa_types.String()),  # system, dropped
        ("IBMSNAP_COMMITSEQ", sa_types.LargeBinary()),  # system, dropped
        ("IBMSNAP_INTENTSEQ", sa_types.LargeBinary()),  # system, dropped
        _col("id"),
        _col("X_id"),
        _col("name"),
        _col("X_name"),
    ]
    out = match_cd_columns(cd_columns, before_column_prefix="X_")
    assert isinstance(out, MatchedColumns)
    assert [c[0] for c in out.after_columns_list] == ["id", "name"]
    assert out.before_columns_map == {"id": "X_id", "name": "X_name"}


def test_match_cd_columns_rejects_empty_prefix():
    with pytest.raises(TabsdataException) as ei:
        match_cd_columns(
            [_col("id")],
            before_column_prefix=None,
            cd_schema="S", cd_table="T",
        )
    assert ei.value.api_error is Db2ErrorCode.DB2_18


def test_match_cd_columns_rejects_unmatched_before_image():
    # X_orphan starts with prefix but has no after-image sibling.
    cd_columns = [_col("id"), _col("X_id"), _col("X_orphan")]
    with pytest.raises(TabsdataException) as ei:
        match_cd_columns(cd_columns, before_column_prefix="X_")
    assert ei.value.api_error is Db2ErrorCode.DB2_19


def test_match_cd_columns_rejects_unmatched_after_image():
    # 'name' has no X_name sibling so it can't be matched.
    cd_columns = [_col("id"), _col("X_id"), _col("name")]
    with pytest.raises(TabsdataException) as ei:
        match_cd_columns(cd_columns, before_column_prefix="X_")
    assert ei.value.api_error is Db2ErrorCode.DB2_20


def test_match_cd_columns_drops_system_columns_before_matching():
    cd_columns = [
        ("IBMSNAP_OPERATION", sa_types.String()),
        ("IBMSNAP_COMMITSEQ", sa_types.LargeBinary()),
        _col("id"),
        _col("X_id"),
    ]
    out = match_cd_columns(cd_columns, before_column_prefix="X_")
    after_names = [c[0] for c in out.after_columns_list]
    assert "IBMSNAP_OPERATION" not in after_names
    assert "IBMSNAP_COMMITSEQ" not in after_names
    assert after_names == ["id"]


# =====================================================================
# CommitSeqOffset
# =====================================================================


def test_commitseq_offset_round_trip():
    raw = {"CDSCHEMA.T1": b"\x01" * 16}
    off = CommitSeqOffset(commits=raw)
    serialised = off.write()
    assert isinstance(serialised, str)
    # Binary commitseqs serialise as hex inside the JSON string.
    assert json.loads(serialised) == {"CDSCHEMA.T1": ("01" * 16)}
    back = CommitSeqOffset.read(serialised)
    assert back.commits == raw


@pytest.mark.parametrize(
    "state", ["", None, "[]", '{"S.T": "zz"}', '{"S.T": "0102"}', '{"S.T": 5}']
)
def test_commitseq_offset_read_rejects_an_unexpected_format(state):
    with pytest.raises(TabsdataException) as ei:
        CommitSeqOffset.read(state)
    assert ei.value.api_error is Db2ErrorCode.DB2_28


def test_commitseq_offset_update_merges_per_table_keys():
    a = CommitSeqOffset(commits={"CDSCHEMA.T1": b"\x01" * 16})
    b = CommitSeqOffset(commits={"CDSCHEMA.T2": b"\x02" * 16})
    a.update(b)
    assert set(a.commits.keys()) == {"CDSCHEMA.T1", "CDSCHEMA.T2"}


def test_commitseq_offset_update_overrides_overlapping_keys():
    a = CommitSeqOffset(commits={"CDSCHEMA.T1": b"\x01" * 16})
    b = CommitSeqOffset(commits={"CDSCHEMA.T1": b"\x09" * 16})
    a.update(b)
    assert a.commits["CDSCHEMA.T1"] == b"\x09" * 16


def test_commitseq_offset_update_rejects_foreign_offset():
    a = CommitSeqOffset(commits={"CDSCHEMA.T1": b"\x01" * 16})

    class _Foreign:
        pass

    with pytest.raises(TabsdataException) as ei:
        a.update(_Foreign())  # type: ignore[arg-type]
    assert ei.value.api_error is Db2ErrorCode.DB2_21


def test_commitseq_offset_copy_is_independent():
    a = CommitSeqOffset(commits={"CDSCHEMA.T1": b"\x01" * 16})
    b = a.copy()
    b.commits["CDSCHEMA.T2"] = b"\x02" * 16
    assert "CDSCHEMA.T2" not in a.commits


# =====================================================================
# SqlReplicaIngester -- pure-function coverage
# =====================================================================


def _ingester_with_tables(qualified: list[str]) -> SqlReplicaIngester:
    conn = MagicMock()
    conn.uri = "db2+ibm_db://h:50000/sample"
    bi = SqlReplicaIngester(
        conn=conn,
        tables=qualified,
        start_from="tail",
    )
    bi._tracked_tables = {}
    for q in qualified:
        cd_schema, cd_table = q.split(".")
        # Use a distinct source-table name to verify ``qualified``
        # uses the *data* table, not the CD table.
        data = TableObject(schema=cd_schema, table=cd_table.replace("_CD", ""))
        cdc = TableObject(schema=cd_schema, table=cd_table)
        bi._tracked_tables[data.qualified] = TrackedTable(
            data_table=data,
            cdc_table=cdc,
        )
        bi._before_img_prefixes[data.qualified] = "X_"
    return bi


def test_ingester_type_is_sqlreplica():
    bi = _ingester_with_tables(["CDSCHEMA.USERS_CD"])
    assert bi.ingester_type == "sqlreplica"


def test_ingester_default_fetch_size():
    """The fetch size is shared with the other CDC connectors rather than
    db2-local, so a deployment tunes one key for all of them."""
    assert DEFAULT_FETCH_SIZE == 100_000


# --- base_offset ----------------------------------------------------


def test_base_offset_is_empty_before_init_reader():
    bi = _ingester_with_tables(["CDSCHEMA.USERS_CD"])
    base = bi.base_offset()
    assert isinstance(base, CommitSeqOffset)
    assert base.commits == {}


# --- COMMITSEQ_ORIGIN -----------------------------------------------


def test_commitseq_origin_is_16_zero_bytes():
    assert COMMITSEQ_ORIGIN == b"\x00" * 16


# =====================================================================
# Offset ownership -- Db2 is Tabsdata-kept: it persists the per-CD-table
# commitseq watermarks in stream state (stage_state) and reconstructs them
# on resume through its offsets keeper.
# =====================================================================


def test_the_keeper_reads_back_a_commitseq_offset():
    bi = _ingester_with_tables(["CDSCHEMA.USERS_CD"])
    offset = bi.offsets.read(
        CommitSeqOffset(commits={"CDSCHEMA.USERS_CD": b"\x01" * 16}).write()
    )
    assert isinstance(offset, CommitSeqOffset)
    assert offset.commits == {"CDSCHEMA.USERS_CD": b"\x01" * 16}


def test_stage_state_persists_commitseqs_in_stream_state():
    bi = _ingester_with_tables(["CDSCHEMA.USERS_CD"])
    offset = CommitSeqOffset(commits={"CDSCHEMA.USERS_CD": b"\x01" * 16})
    spec = bi.stage_state(offset)
    assert isinstance(spec, dict)
    assert spec == {CDC_OFFSET_STATE_KEY: offset.write()}


def test_resume_offset_round_trips_persisted_commitseqs():
    bi = _ingester_with_tables(["CDSCHEMA.USERS_CD"])
    offset = CommitSeqOffset(commits={"CDSCHEMA.USERS_CD": b"\x07" * 16})
    persisted = bi.stage_state(offset)
    resumed = bi.resume_offset(first_stage=False, stream_state=persisted)
    assert isinstance(resumed, CommitSeqOffset)
    assert resumed.commits == {"CDSCHEMA.USERS_CD": b"\x07" * 16}


def test_resume_offset_first_stage_ignores_persisted_state():
    bi = _ingester_with_tables(["CDSCHEMA.USERS_CD"])
    assert (
        bi.resume_offset(
            first_stage=True,
            stream_state={CDC_OFFSET_STATE_KEY: {"CDSCHEMA.USERS_CD": "01" * 16}},
        )
        is None
    )


def test_seed_offset_sets_resume_mark():
    bi = _ingester_with_tables(["CDSCHEMA.USERS_CD"])
    offset = CommitSeqOffset(commits={"CDSCHEMA.USERS_CD": b"\x01" * 16})
    bi.seed_offset(offset)
    assert bi._mark is offset


# =====================================================================
# SqlReplicaInitialLoadSession
# =====================================================================


def _commitseq(flush: int, lsn: int) -> bytes:
    return flush.to_bytes(8, byteorder="big") + lsn.to_bytes(8, byteorder="big")


class _Result:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value[0] if isinstance(self._value, tuple) else self._value

    def fetchone(self):
        return self._value

    def fetchall(self):
        return self._value


class _Connection:
    def __init__(self, engine: "_Engine"):
        self._engine = engine

    def execution_options(self, **options):
        self._engine.log.append(("options", options))
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def commit(self):
        self._engine.log.append(("commit",))

    def execute(self, statement, parameters=None):
        sql = str(statement)
        self._engine.log.append(("execute", sql, parameters))
        for fragment, answer in self._engine.answers.items():
            if fragment in sql:
                value = answer() if callable(answer) else answer
                if isinstance(value, Exception):
                    raise value
                return _Result(value)
        raise AssertionError(f"unexpected statement: {sql}")


class _Engine:
    def __init__(self, answers: dict):
        self.answers = answers
        self.log: list[tuple] = []

    def connect(self):
        return _Connection(self)

    def dispose(self):
        pass

    def executed(self) -> list[str]:
        return [entry[1] for entry in self.log if entry[0] == "execute"]


def _tracked(cd_table: str) -> TrackedTable:
    return TrackedTable(
        data_table=TableObject(schema="SRC", table=cd_table.removesuffix("_CD")),
        cdc_table=TableObject(schema="ASN", table=cd_table),
    )


CAPTURE_START = b"\x00" * 7 + b"\x01" + b"\x00" * 7 + b"\x10"


def _registrations(
    *tables, state="A", start=CAPTURE_START, bound=CAPTURE_START, splits="N"
):
    return [("ASN", table, state, None, start, splits, bound) for table in tables]


SERVER_NOW = datetime(2026, 1, 1, 12, 0, 0)


def _session(
    answers: dict, server_tz: tzinfo | None = timezone(timedelta(hours=2))
) -> tuple[SqlReplicaInitialLoadSession, _Engine]:
    engine = _Engine(
        {
            "cur_commit": "ON",
            "ibmsnap_register r": _registrations("T_CD", "A_CD", "B_CD"),
            "sysibm.sysdummy1": SERVER_NOW,
        }
        | answers
    )
    return SqlReplicaInitialLoadSession(engine=engine, server_tz=server_tz), engine


def _restart_rows(*rows):
    pending = list(rows)
    return lambda: pending.pop(0) if len(pending) > 1 else pending[0]


def test_commitseq_lsn_is_the_second_half_of_the_commit_sequence():
    assert commitseq_lsn(_commitseq(0xAB, 0x0A3024)) == 0x0A3024


def test_the_load_time_is_the_server_time_at_the_start_in_utc():
    session, engine = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(1, 0x100), None),
        "as idx": [],
    })
    list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    assert session.timestamp == datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    executed = engine.executed()
    first_time = next(i for i, sql in enumerate(executed) if "sysdummy1" in sql)
    first_lsn = next(
        i for i, sql in enumerate(executed) if "mon_get_transaction_log" in sql
    )
    assert first_time < first_lsn


def test_the_initial_load_reads_under_cursor_stability():
    session, engine = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(1, 0x100), None),
        "as idx": [],
    })
    list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    assert ("options", {"isolation_level": INITIAL_LOAD_ISOLATION_LEVEL}) in engine.log
    assert INITIAL_LOAD_ISOLATION_LEVEL == "CURSOR STABILITY"


def test_the_pin_takes_no_table_lock():
    session, engine = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(1, 0x100), None),
        "as idx": [],
    })
    list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    assert not [sql for sql in engine.executed() if "lock table" in sql]


def test_the_pin_reads_the_log_position_before_the_first_table():
    session, engine = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(1, 0x100), None),
        "as idx": [],
    })
    tables = session.tables({"ASN.T_CD": _tracked("T_CD")})
    next(tables)
    assert any("mon_get_transaction_log" in sql for sql in engine.executed())
    assert not any("IBMSNAP_RESTART" in sql for sql in engine.executed())
    with pytest.raises(TabsdataException) as ei:
        _ = session.offset
    assert ei.value.api_error is Db2ErrorCode.DB2_25
    list(tables)
    assert session.offset is not None


def test_the_pin_resumes_each_table_from_its_last_commit_before_it():
    before = _commitseq(7, 0x0FF)
    session, _ = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(9, 0x180), None),
        "as idx": [(0, before)],
    })
    list(session.tables({"ASN.A_CD": _tracked("A_CD"), "ASN.B_CD": _tracked("B_CD")}))
    assert session.offset.commits == {
        "ASN.A_CD": before,
        "ASN.B_CD": CAPTURE_START,
    }


def test_the_pin_bounds_the_commit_sequence_strictly_below_it():
    session, engine = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(1, 0x100), None),
        "as idx": [],
    })
    list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    (sql, parameters), _ = [
        (entry[1], entry[2])
        for entry in engine.log
        if entry[0] == "execute" and "as idx" in entry[1]
    ]
    assert "substr(\"IBMSNAP_COMMITSEQ\", 9, 8) < :bound" in sql
    assert parameters == {"bound": (0x100).to_bytes(8, byteorder="big")}


def test_every_table_is_pinned_in_one_statement():
    session, engine = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(1, 0x100), None),
        "as idx": [],
    })
    list(session.tables({"ASN.A_CD": _tracked("A_CD"), "ASN.B_CD": _tracked("B_CD")}))
    start, end = [sql for sql in engine.executed() if "as idx" in sql]
    for sql in (start, end):
        assert '"ASN"."A_CD"' in sql
        assert '"ASN"."B_CD"' in sql
        assert " union all " in sql


def test_the_pin_waits_until_capture_passes_the_log_position(monkeypatch):
    monkeypatch.setattr(_cdc_impl.time, "sleep", lambda seconds: None)
    session, engine = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": _restart_rows(
            (_commitseq(1, 0x050), _commitseq(1, 0x060)),
            (_commitseq(1, 0x050), _commitseq(1, 0x0FF)),
            (_commitseq(1, 0x050), _commitseq(1, 0x100)),
        ),
        "as idx": [],
    })
    list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    # Three waiting for the start, one for the window's end.
    assert len([sql for sql in engine.executed() if "IBMSNAP_RESTART" in sql]) == 4


def test_a_capture_commit_past_the_log_position_is_enough_while_one_is_in_flight():
    session, engine = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(1, 0x120), _commitseq(1, 0x010)),
        "as idx": [],
    })
    list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    # One for the start, one for the window's end.
    assert len([sql for sql in engine.executed() if "IBMSNAP_RESTART" in sql]) == 2


def test_the_pin_fails_when_capture_never_passes_the_log_position(
    monkeypatch,
):
    monkeypatch.setattr(_cdc_impl, "CAPTURE_CATCH_UP_TIMEOUT_SECONDS", 0)
    session, _ = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(1, 0x050), _commitseq(1, 0x050)),
        "as idx": [],
    })
    with pytest.raises(TabsdataException) as ei:
        list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    assert ei.value.api_error is Db2ErrorCode.DB2_26


def test_an_unreadable_log_position_names_the_missing_privilege():
    denied = sa.exc.DBAPIError("select", {}, Exception("SQL0551N not authorized"))
    session, _ = _session({"mon_get_transaction_log": denied})
    with pytest.raises(TabsdataException) as ei:
        list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    assert ei.value.api_error is Db2ErrorCode.DB2_27


@pytest.mark.parametrize("setting", ["DISABLED", "AVAILABLE"])
def test_a_cur_commit_setting_other_than_on_is_warned_about(setting, caplog):
    session, _ = _session({
        "cur_commit": setting,
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(1, 0x100), None),
        "as idx": [],
    })
    with caplog.at_level(logging.WARNING, logger=_cdc_impl.__name__):
        list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    assert "cur_commit" in caplog.text
    assert setting in caplog.text


def test_cur_commit_on_is_not_warned_about(caplog):
    session, _ = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(1, 0x100), None),
        "as idx": [],
    })
    with caplog.at_level(logging.WARNING, logger=_cdc_impl.__name__):
        list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    assert "cur_commit" not in caplog.text


def test_the_window_ends_at_the_log_position_read_after_the_rows():
    before, after = _commitseq(7, 0x0FF), _commitseq(8, 0x1FF)
    positions = [0x100, 0x200]
    pins = [[(0, before)], [(0, after)]]
    session, engine = _session({
        "mon_get_transaction_log": lambda: positions.pop(0),
        "IBMSNAP_RESTART": (_commitseq(9, 0x300), None),
        "as idx": lambda: pins.pop(0),
    })
    list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    assert session.offset.commits == {"ASN.T_CD": before}
    assert session.window_end.commits == {"ASN.T_CD": after}
    _, (sql, parameters) = [
        (entry[1], entry[2])
        for entry in engine.log
        if entry[0] == "execute" and "as idx" in entry[1]
    ]
    assert "substr(\"IBMSNAP_COMMITSEQ\", 9, 8) <= :bound" in sql
    assert parameters == {"bound": (0x200).to_bytes(8, byteorder="big")}


def test_the_window_end_is_unknown_before_the_rows_are_read():
    session, _ = _session({})
    with pytest.raises(TabsdataException) as ei:
        _ = session.window_end
    assert ei.value.api_error is Db2ErrorCode.DB2_25


def test_a_registration_that_splits_updates_fails_the_load():
    session, _ = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(1, 0x100), None),
        "as idx": [],
        "ibmsnap_register r": _registrations("T_CD", splits="Y"),
    })
    with pytest.raises(TabsdataException) as ei:
        list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    assert ei.value.api_error is Db2ErrorCode.DB2_35


def test_the_pin_fails_when_capture_started_after_the_load_position():
    late = _commitseq(1, 0x100)
    session, _ = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(1, 0x100), None),
        "as idx": [],
        "ibmsnap_register r": _registrations("T_CD", start=late, bound=late),
    })
    with pytest.raises(TabsdataException) as ei:
        list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    assert ei.value.api_error is Db2ErrorCode.DB2_34


def test_the_pin_fails_when_a_table_has_no_registration():
    session, _ = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(1, 0x100), None),
        "as idx": [],
        "ibmsnap_register r": [],
    })
    with pytest.raises(TabsdataException) as ei:
        list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    assert ei.value.api_error is Db2ErrorCode.DB2_33


# --- verify_positions ------------------------------------------------


POSITION = _commitseq(5, 0x500)


def _verify(position=POSITION, **registration):
    verify_positions(
        {
            "ASN.T_CD": RegistrationPositions(
                **{
                    "state": "A",
                    "state_info": None,
                    "capture_start": _commitseq(1, 0x100),
                    "prune_bound": _commitseq(1, 0x100),
                }
                | registration
            )
        },
        {"ASN.T_CD": position} if position is not None else {},
        [TableObject(schema="ASN", table="T_CD")],
    )


def test_a_position_capture_still_covers_passes():
    _verify()
    _verify(capture_start=POSITION, prune_bound=POSITION)
    _verify(prune_bound=None)


@pytest.mark.parametrize(
    ("change", "code"),
    [
        ({"state": "I"}, Db2ErrorCode.DB2_30),
        ({"capture_start": None}, Db2ErrorCode.DB2_31),
        ({"capture_start": _commitseq(6, 0x600)}, Db2ErrorCode.DB2_31),
        ({"prune_bound": _commitseq(6, 0x600)}, Db2ErrorCode.DB2_32),
    ],
)
def test_a_position_capture_no_longer_covers_fails(change, code):
    with pytest.raises(TabsdataException) as ei:
        _verify(**change)
    assert ei.value.api_error is code


def test_a_table_without_a_position_fails():
    with pytest.raises(TabsdataException) as ei:
        _verify(position=None)
    assert ei.value.api_error is Db2ErrorCode.DB2_33


def test_a_table_without_a_registration_fails():
    with pytest.raises(TabsdataException) as ei:
        verify_positions(
            {}, {"ASN.T_CD": POSITION}, [TableObject(schema="ASN", table="T_CD")]
        )
    assert ei.value.api_error is Db2ErrorCode.DB2_29


def test_the_floor_is_the_higher_of_the_two_positions():
    registration = RegistrationPositions(
        state="A",
        state_info=None,
        capture_start=_commitseq(1, 0x100),
        prune_bound=_commitseq(2, 0x200),
    )
    assert registration.floor == _commitseq(2, 0x200)


# --- Resume and poll ---------------------------------------------------


class _Session:
    def __init__(self, answers: dict):
        self.engine = _Engine(answers)

    def connect(self):
        return self.engine.connect()


def _resumed(monkeypatch, commits, registrations, captured=None):
    monkeypatch.setattr(_cdc_impl.time, "sleep", lambda seconds: None)
    bi = _ingester_with_tables(["ASN.T_CD"])
    bi.seed_offset(CommitSeqOffset(commits=commits))
    pending = list(registrations)
    session = _Session({
        "ibmsnap_register r": lambda: pending.pop(0) if len(pending) > 1 else pending[0],
        "IBMSNAP_RESTART": (captured,),
    })
    return bi, session


def test_a_resume_capture_still_covers_starts(monkeypatch):
    bi, session = _resumed(monkeypatch, {"ASN.T_CD": POSITION}, [_registrations("T_CD")])
    bi._init_reader(session)
    assert bi.base_offset().commits == {"ASN.T_CD": POSITION}


def test_a_resume_past_a_retention_prune_fails(monkeypatch):
    moved = _commitseq(6, 0x600)
    bi, session = _resumed(
        monkeypatch, {"ASN.T_CD": POSITION}, [_registrations("T_CD", start=moved)]
    )
    with pytest.raises(TabsdataException) as ei:
        bi._init_reader(session)
    assert ei.value.api_error is Db2ErrorCode.DB2_31


def test_a_head_start_begins_at_the_registration_floor(monkeypatch):
    bi = _ingester_with_tables(["ASN.T_CD"])
    bi._start_from = "head"
    bound = _commitseq(2, 0x200)
    session = _Session({"ibmsnap_register r": _registrations("T_CD", bound=bound)})
    bi._init_reader(session)
    assert bi.base_offset().commits == {"ASN.T_CD": bound}


def test_rows_pruned_while_a_poll_reads_them_fail_the_poll(monkeypatch):
    bi, session = _resumed(
        monkeypatch,
        {"ASN.T_CD": POSITION},
        [
            _registrations("T_CD"),
            _registrations("T_CD", bound=_commitseq(6, 0x600)),
        ],
    )
    watermark = _commitseq(9, 0x900)
    monkeypatch.setattr(bi, "_query_table_watermarks", lambda c: {"ASN.T_CD": watermark})
    monkeypatch.setattr(bi, "_extract_raw_schema", lambda *a: ())
    bi._schema_registry = MagicMock()
    read = []
    monkeypatch.setattr(
        bi,
        "_query_table_changes",
        lambda **kwargs: iter([read.append(kwargs["commitseq"]) or "change"]),
    )
    bi._poll_max_secs = 60
    with pytest.raises(TabsdataException) as ei:
        list(bi.poll_changes(session))
    assert ei.value.api_error is Db2ErrorCode.DB2_32
    assert read == [POSITION]


def test_a_bounded_poll_reads_up_to_the_bound_once_and_stands_at_it(monkeypatch):
    end = _commitseq(7, 0x700)
    quiet_end = _commitseq(8, 0x800)
    bi, session = _resumed(
        monkeypatch,
        {"ASN.T_CD": POSITION, "ASN.Q_CD": POSITION},
        [_registrations("T_CD", "Q_CD")],
    )
    bi._tracked_tables = {"SRC.T": _tracked("T_CD"), "SRC.Q": _tracked("Q_CD")}
    bi._before_img_prefixes = {"SRC.T": None, "SRC.Q": None}
    watermark = _commitseq(9, 0x900)
    monkeypatch.setattr(
        bi, "_query_table_watermarks", lambda c: {"ASN.T_CD": watermark}
    )
    monkeypatch.setattr(bi, "_extract_raw_schema", lambda *a: ())
    bi._schema_registry = MagicMock()
    read = []
    monkeypatch.setattr(
        bi,
        "_query_table_changes",
        lambda **kwargs: iter(
            [read.append((kwargs["commitseq"], kwargs["watermark"])) or "change"]
        ),
    )
    bi._poll_max_secs = 60
    bound = CommitSeqOffset(commits={"ASN.T_CD": end, "ASN.Q_CD": quiet_end})
    bi.bound_stream(bound)
    assert not bi.stream_bound_reached()

    assert list(bi.poll_changes(session)) == ["change"]

    assert read == [(POSITION, end)]
    assert bi.stream_bound_reached()
    assert bi._mark.commits == {"ASN.T_CD": end, "ASN.Q_CD": quiet_end}


def test_a_poll_capture_still_covers_yields_its_changes(monkeypatch):
    bi, session = _resumed(monkeypatch, {"ASN.T_CD": POSITION}, [_registrations("T_CD")])
    monkeypatch.setattr(bi, "_query_table_watermarks", lambda c: {})
    bi._poll_max_secs = 0.05
    assert list(bi.poll_changes(session)) == []
    assert any("ibmsnap_register r" in sql for sql in session.engine.executed())


# =====================================================================
# Quiet tables follow Capture's position
# =====================================================================


def _two_tables(monkeypatch, *, captured, watermarks):
    bi, session = _resumed(
        monkeypatch,
        {"ASN.T_CD": POSITION, "ASN.Q_CD": POSITION},
        [_registrations("T_CD", "Q_CD")],
        captured=captured,
    )
    bi._tracked_tables = {"SRC.T": _tracked("T_CD"), "SRC.Q": _tracked("Q_CD")}
    bi._before_img_prefixes = {"SRC.T": None, "SRC.Q": None}
    monkeypatch.setattr(bi, "_query_table_watermarks", lambda c: watermarks)
    monkeypatch.setattr(bi, "_extract_raw_schema", lambda *a: ())
    bi._schema_registry = MagicMock()
    monkeypatch.setattr(bi, "_query_table_changes", lambda **kwargs: iter(["change"]))
    bi._poll_max_secs = 0.05
    return bi, session


def _quiet_ingester() -> SqlReplicaIngester:
    bi = _ingester_with_tables(["ASN.T_CD", "ASN.Q_CD"])
    bi.seed_offset(
        CommitSeqOffset(commits={"ASN.T_CD": POSITION, "ASN.Q_CD": POSITION})
    )
    return bi


def test_a_table_with_nothing_to_read_moves_on_to_capture_s_position():
    bi = _quiet_ingester()
    captured = _commitseq(9, 0x900)
    bi._advance_quiet_tables(captured, read={"ASN.T_CD"})
    assert bi._mark.commits == {"ASN.T_CD": POSITION, "ASN.Q_CD": captured}
    assert bi._quiet == {"ASN.Q_CD": (POSITION, captured)}


def test_a_quiet_table_keeps_its_last_row_as_it_moves_on():
    bi = _quiet_ingester()
    first, later = _commitseq(8, 0x800), _commitseq(9, 0x900)
    bi._advance_quiet_tables(first, read=set())
    bi._advance_quiet_tables(later, read=set())
    assert bi._quiet["ASN.Q_CD"] == (POSITION, later)


def test_no_table_moves_without_a_capture_position(monkeypatch):
    bi, session = _two_tables(monkeypatch, captured=None, watermarks={})
    list(bi.poll_changes(session))
    assert bi._mark.commits == {"ASN.T_CD": POSITION, "ASN.Q_CD": POSITION}


def test_a_capture_position_behind_a_table_does_not_move_it_back(monkeypatch):
    bi, session = _two_tables(
        monkeypatch, captured=_commitseq(1, 0x100), watermarks={}
    )
    list(bi.poll_changes(session))
    assert bi._mark.commits == {"ASN.T_CD": POSITION, "ASN.Q_CD": POSITION}


def test_capture_s_position_is_read_before_the_tables_are(monkeypatch):
    bi, session = _two_tables(monkeypatch, captured=_commitseq(9, 0x900), watermarks={})
    order = []
    monkeypatch.setattr(
        bi, "_captured_position", lambda c: order.append("captured") or None
    )
    monkeypatch.setattr(
        bi, "_query_table_watermarks", lambda c: order.append("watermarks") or {}
    )
    list(bi.poll_changes(session))
    assert order[:2] == ["captured", "watermarks"]


def test_a_moved_position_is_staged_once_the_last_row_is():
    bi = _ingester_with_tables(["ASN.Q_CD"])
    last_row, moved_to = _commitseq(5, 0x500), _commitseq(9, 0x900)
    bi._quiet = {"ASN.Q_CD": (last_row, moved_to)}
    staged = bi.stage_state(CommitSeqOffset(commits={"ASN.Q_CD": last_row}))
    assert CommitSeqOffset.read(staged["cdc.offset"]).commits == {
        "ASN.Q_CD": moved_to
    }


def test_a_moved_position_waits_for_a_row_not_staged_yet():
    """The last row read from the table is not in this stage: saving the
    moved position would skip it."""
    bi = _ingester_with_tables(["ASN.Q_CD"])
    staged_row = _commitseq(3, 0x300)
    last_row, moved_to = _commitseq(5, 0x500), _commitseq(9, 0x900)
    bi._quiet = {"ASN.Q_CD": (last_row, moved_to)}
    staged = bi.stage_state(CommitSeqOffset(commits={"ASN.Q_CD": staged_row}))
    assert CommitSeqOffset.read(staged["cdc.offset"]).commits == {
        "ASN.Q_CD": staged_row
    }


def test_a_table_read_again_is_no_longer_quiet():
    bi = _quiet_ingester()
    bi._quiet = {"ASN.Q_CD": (POSITION, _commitseq(6, 0x600))}
    bi._advance_quiet_tables(_commitseq(9, 0x900), read={"ASN.Q_CD"})
    assert "ASN.Q_CD" not in bi._quiet


def test_a_poll_moves_a_table_on_once_its_rows_are_read(monkeypatch):
    captured = _commitseq(9, 0x900)
    row = _commitseq(7, 0x700)
    bi, session = _two_tables(
        monkeypatch, captured=captured, watermarks={"ASN.T_CD": row}
    )
    list(bi.poll_changes(session))
    # The first pass reads T up to its row; the next ones find it quiet.
    assert bi._mark.commits == {"ASN.T_CD": captured, "ASN.Q_CD": captured}
    assert bi._quiet["ASN.T_CD"] == (row, captured)
