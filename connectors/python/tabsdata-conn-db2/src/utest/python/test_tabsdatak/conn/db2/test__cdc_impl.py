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
    SqlReplicaIngester,
    SqlReplicaInitialLoadSession,
    commitseq_lsn,
    match_cd_columns,
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


def test_commitseq_offset_read_empty_string():
    assert CommitSeqOffset.read("{}").commits == {}
    assert CommitSeqOffset.read("").commits == {}


def test_commitseq_offset_read_none_is_safe():
    assert CommitSeqOffset.read(None).commits == {}


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


# --- base_offset / commit_offset ------------------------------------


def test_base_offset_is_empty_before_init_reader():
    bi = _ingester_with_tables(["CDSCHEMA.USERS_CD"])
    base = bi.base_offset()
    assert isinstance(base, CommitSeqOffset)
    assert base.commits == {}


def test_commit_offset_accepts_commitseq_offset():
    bi = _ingester_with_tables(["CDSCHEMA.USERS_CD"])
    bi.commit_offset(CommitSeqOffset(commits={"CDSCHEMA.USERS_CD": b"\x01" * 16}))


def test_commit_offset_rejects_foreign_offset():
    bi = _ingester_with_tables(["CDSCHEMA.USERS_CD"])

    class _Foreign:
        pass

    with pytest.raises(TabsdataException) as ei:
        bi.commit_offset(_Foreign())  # type: ignore[arg-type]
    assert ei.value.api_error is Db2ErrorCode.DB2_21


# --- COMMITSEQ_ORIGIN -----------------------------------------------


def test_commitseq_origin_is_16_zero_bytes():
    assert COMMITSEQ_ORIGIN == b"\x00" * 16


# =====================================================================
# Offset ownership -- Db2 is Tabsdata-owned: it persists the per-CD-table
# commitseq watermarks in stream state (default commit_state) and
# reconstructs them on resume via offset_from_state.
# =====================================================================


def test_offset_from_state_reconstructs_commitseq_offset():
    bi = _ingester_with_tables(["CDSCHEMA.USERS_CD"])
    offset = bi.offset_from_state(
        CommitSeqOffset(commits={"CDSCHEMA.USERS_CD": b"\x01" * 16}).write()
    )
    assert isinstance(offset, CommitSeqOffset)
    assert offset.commits == {"CDSCHEMA.USERS_CD": b"\x01" * 16}


def test_commit_state_persists_commitseqs_in_stream_state():
    bi = _ingester_with_tables(["CDSCHEMA.USERS_CD"])
    offset = CommitSeqOffset(commits={"CDSCHEMA.USERS_CD": b"\x01" * 16})
    spec = bi.commit_state(offset)
    assert isinstance(spec, dict)
    assert spec == {CDC_OFFSET_STATE_KEY: offset.write()}


def test_resume_offset_round_trips_persisted_commitseqs():
    bi = _ingester_with_tables(["CDSCHEMA.USERS_CD"])
    offset = CommitSeqOffset(commits={"CDSCHEMA.USERS_CD": b"\x07" * 16})
    persisted = bi.commit_state(offset)
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


def _session(answers: dict) -> tuple[SqlReplicaInitialLoadSession, _Engine]:
    engine = _Engine({"cur_commit": "ON"} | answers)
    return SqlReplicaInitialLoadSession(engine=engine), engine


def _restart_rows(*rows):
    pending = list(rows)
    return lambda: pending.pop(0) if len(pending) > 1 else pending[0]


def test_commitseq_lsn_is_the_second_half_of_the_commit_sequence():
    assert commitseq_lsn(_commitseq(0xAB, 0x0A3024)) == 0x0A3024


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
        "ASN.B_CD": COMMITSEQ_ORIGIN,
    }


def test_the_pin_bounds_the_commit_sequence_strictly_below_it():
    session, engine = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(1, 0x100), None),
        "as idx": [],
    })
    list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    [(sql, parameters)] = [
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
    [sql] = [sql for sql in engine.executed() if "as idx" in sql]
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
    assert len([sql for sql in engine.executed() if "IBMSNAP_RESTART" in sql]) == 3


def test_a_capture_commit_past_the_log_position_is_enough_while_one_is_in_flight():
    session, engine = _session({
        "mon_get_transaction_log": 0x100,
        "IBMSNAP_RESTART": (_commitseq(1, 0x120), _commitseq(1, 0x010)),
        "as idx": [],
    })
    list(session.tables({"ASN.T_CD": _tracked("T_CD")}))
    assert len([sql for sql in engine.executed() if "IBMSNAP_RESTART" in sql]) == 1


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
