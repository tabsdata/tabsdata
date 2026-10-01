#
# Copyright 2026 Tabsdata Inc.
#

"""``_cdc_impl.py`` -- pure-function coverage for the MySQL CDC
dialect, binlog ingester and offset. Catalog / engine calls
(``server_timezone``, ``get_catalog_columns``, stream init,
``poll_changes``) need a real MySQL CDC domain and live in the
integration tests (``test_plugin_cdc.py``)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import sqlalchemy.types as sa_types
from pymysqlreplication.event import GtidEvent
from sqlalchemy.dialects.mysql import (
    DATETIME,
    ENUM,
    LONGBLOB,
    LONGTEXT,
    MEDIUMBLOB,
    MEDIUMTEXT,
    SET,
    TIME,
    TIMESTAMP,
    TINYBLOB,
    TINYINT,
    TINYTEXT,
)
from sqlalchemy.types import LargeBinary, Text

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql.cdc._ingester import CDC_OFFSET_STATE_KEY
from tabsdatak.conn.common.sql.cdc._model import TableObject, TrackedTable
from tabsdatak.conn.common.sql.cdc.constants import CdcOperation
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mysql import _cdc_impl
from tabsdatak.conn.mysql._cdc_impl import (
    INITIAL_LOAD_LOCK_WAIT_TIMEOUT_SECONDS,
    BinlogIngester,
    BinlogInitialLoadSession,
    GtidOffset,
    MySQLDialect,
)
from tabsdatak.conn.mysql.error import MysqlErrorCode
from tabsdatak.error import TabsdataException


# =====================================================================
# MySQLDialect
# =====================================================================


@pytest.fixture
def d() -> MySQLDialect:
    return MySQLDialect()


# --- Identifier handling ---------------------------------------------


def test_quotes(d):
    assert d.open_quote == "`"
    assert d.close_quote == "`"


def test_default_case_fold_passes_through(d):
    assert d.default_case_fold("Users") == "Users"


def test_parse_qualified_table_simple(d):
    schema, table = d.parse_qualified_table("app.users")
    assert str(schema) == "app"
    assert str(table) == "users"


def test_parse_qualified_table_backticked(d):
    schema, table = d.parse_qualified_table("`my_app`.`users`")
    assert str(schema) == "my_app"
    assert str(table) == "users"
    assert schema.quote is True
    assert table.quote is True


def test_parse_qualified_table_allows_unqualified(d):
    # Unqualified name: schema is None; the ingester fills it from the
    # connection's default database (see default_schema).
    schema, table = d.parse_qualified_table("users")
    assert schema is None
    assert str(table) == "users"


def test_parse_qualified_table_allows_unqualified_backticked(d):
    schema, table = d.parse_qualified_table("`users`")
    assert schema is None
    assert str(table) == "users"
    assert table.quote is True


def test_parse_qualified_table_rejects_empty_side(d):
    with pytest.raises(ValueError):
        d.parse_qualified_table(".users")
    with pytest.raises(ValueError):
        d.parse_qualified_table("app.")
    with pytest.raises(ValueError):
        d.parse_qualified_table("")


def test_default_schema_uses_inspector_default(d):
    # MySQL's effective schema for an unqualified name is the current database,
    # which is what SQLAlchemy reports as default_schema_name.
    inspector = MagicMock()
    inspector.default_schema_name = "app_db"
    assert d.default_schema(inspector) == "app_db"


# --- Large-column classification -------------------------------------


def test_large_column_kind_object_for_blob_family(d):
    assert d.large_column_kind(LargeBinary()) == "object"
    assert d.large_column_kind(TINYBLOB()) == "object"
    assert d.large_column_kind(MEDIUMBLOB()) == "object"
    assert d.large_column_kind(LONGBLOB()) == "object"


def test_large_column_kind_text_for_text_family_and_json(d):
    assert d.large_column_kind(Text()) == "text"
    assert d.large_column_kind(TINYTEXT()) == "text"
    assert d.large_column_kind(MEDIUMTEXT()) == "text"
    assert d.large_column_kind(LONGTEXT()) == "text"
    assert d.large_column_kind(sa_types.JSON()) == "text"


def test_large_column_kind_none_for_regular_types(d):
    assert d.large_column_kind(sa_types.Integer()) is None
    assert d.large_column_kind(sa_types.String()) is None
    assert d.large_column_kind(sa_types.String(length=65_535)) is None


# --- cdc_to_sa ingester dispatch ------------------------------------


def test_cdc_to_sa_unknown_ingester_raises(d):
    with pytest.raises(NotImplementedError):
        d.cdc_to_sa("wal2json", object())


# --- cdc_to_sa for binlog: type-by-type coverage --------------------


class _Col:
    """Minimal stand-in for a pymysqlreplication ``Column``."""

    def __init__(self, type_code: int, **kw):
        self.type = type_code
        for k, v in kw.items():
            setattr(self, k, v)


def test_binlog_integer_family(d):
    import pymysqlreplication.constants.FIELD_TYPE as FT
    assert isinstance(d.cdc_to_sa("binlog", _Col(FT.LONG)), sa_types.Integer)
    assert isinstance(d.cdc_to_sa("binlog", _Col(FT.SHORT)), sa_types.SmallInteger)
    assert isinstance(d.cdc_to_sa("binlog", _Col(FT.LONGLONG)), sa_types.BigInteger)
    assert isinstance(d.cdc_to_sa("binlog", _Col(FT.TINY)), TINYINT)


def test_binlog_numeric_carries_precision_and_scale(d):
    import pymysqlreplication.constants.FIELD_TYPE as FT
    t = d.cdc_to_sa("binlog", _Col(FT.NEWDECIMAL, precision=10, decimals=2))
    assert isinstance(t, sa_types.Numeric)
    assert t.precision == 10
    assert t.scale == 2


def test_binlog_string_uses_max_length(d):
    import pymysqlreplication.constants.FIELD_TYPE as FT
    t = d.cdc_to_sa("binlog", _Col(FT.VARCHAR, max_length=255))
    assert isinstance(t, sa_types.String)
    assert t.length == 255


def test_binlog_datetime_carries_fsp(d):
    import pymysqlreplication.constants.FIELD_TYPE as FT
    t = d.cdc_to_sa("binlog", _Col(FT.DATETIME, fsp=3))
    assert isinstance(t, DATETIME)
    assert t.fsp == 3


def test_binlog_time_and_timestamp(d):
    import pymysqlreplication.constants.FIELD_TYPE as FT
    assert isinstance(d.cdc_to_sa("binlog", _Col(FT.TIME)), TIME)
    assert isinstance(d.cdc_to_sa("binlog", _Col(FT.TIMESTAMP)), TIMESTAMP)


def test_binlog_enum_and_set(d):
    import pymysqlreplication.constants.FIELD_TYPE as FT
    e = d.cdc_to_sa("binlog", _Col(FT.ENUM, enum_values=["a", "b"]))
    assert isinstance(e, ENUM)
    s = d.cdc_to_sa("binlog", _Col(FT.SET, set_values=["x", "y"]))
    assert isinstance(s, SET)


@pytest.mark.parametrize(
    "length_size,binary_type,text_type",
    [
        (1, TINYBLOB, TINYTEXT),
        (2, LargeBinary, Text),
        (3, MEDIUMBLOB, MEDIUMTEXT),
        (4, LONGBLOB, LONGTEXT),
    ],
)
def test_binlog_blob_variant_from_length_prefix_and_charset(
    d, length_size, binary_type, text_type
):
    import pymysqlreplication.constants.FIELD_TYPE as FT

    binary = d.cdc_to_sa(
        "binlog",
        _Col(FT.BLOB, length_size=length_size, character_set_name="binary"),
    )
    assert type(binary) is binary_type
    textual = d.cdc_to_sa(
        "binlog",
        _Col(FT.BLOB, length_size=length_size, character_set_name="utf8mb4"),
    )
    assert type(textual) is text_type


def test_binlog_blob_without_charset_is_binary(d):
    import pymysqlreplication.constants.FIELD_TYPE as FT

    assert type(d.cdc_to_sa("binlog", _Col(FT.BLOB, length_size=4))) is LONGBLOB


def test_binlog_unknown_field_type_raises(d):
    with pytest.raises(ValueError):
        d.cdc_to_sa("binlog", _Col(type_code=99999))


# =====================================================================
# GtidOffset
# =====================================================================


def test_gtid_offset_round_trip():
    a = GtidOffset(gtid="uuid:42")
    serialised = a.write()
    assert isinstance(serialised, str)
    assert GtidOffset.read(serialised).gtid == "uuid:42"


def test_gtid_offset_read_missing_gtid_defaults_to_empty():
    assert GtidOffset.read("{}").gtid == ""
    assert GtidOffset.read("").gtid == ""


def test_gtid_offset_update_with_another_gtid_offset():
    a = GtidOffset(gtid="uuid:1")
    a.update(GtidOffset(gtid="uuid:99"))
    assert a.gtid == "uuid:99"


def test_gtid_offset_update_rejects_foreign_offset():
    a = GtidOffset(gtid="uuid:1")

    class _Foreign:
        pass

    with pytest.raises(TabsdataException) as ei:
        a.update(_Foreign())  # type: ignore[arg-type]
    assert ei.value.api_error is MysqlErrorCode.MYSQL_17


def test_gtid_offset_copy_is_independent():
    a = GtidOffset(gtid="uuid:1")
    b = a.copy()
    b.gtid = "other"
    assert a.gtid == "uuid:1"


# =====================================================================
# BinlogIngester -- pure-function coverage
# =====================================================================

# --- as_gtid_set translation -----------------------------------------


def test_as_gtid_set_translates_single_to_range():
    assert BinlogIngester._as_gtid_set("uuid:42") == "uuid:1-42"


def test_as_gtid_set_passes_range_through():
    assert BinlogIngester._as_gtid_set("uuid:5-42") == "uuid:5-42"


def test_as_gtid_set_passes_empty_through():
    assert BinlogIngester._as_gtid_set("") == ""


def test_as_gtid_set_no_colon_passes_through():
    # Defensive: malformed GTID returns as-is rather than raising;
    # pymysqlreplication will surface the format error if it cares.
    assert BinlogIngester._as_gtid_set("garbage") == "garbage"


# --- schema_table_filter -------------------------------------------


def _ingester_with_tables(qualified: list[str]) -> BinlogIngester:
    conn = MagicMock()
    conn.uri = "mysql://h:3306/d"
    bi = BinlogIngester(
        conn=conn,
        tables=qualified,
        start_from="tail",
        server_id=42,
    )
    bi._tracked_tables = {
        q: TrackedTable(
            data_table=TableObject(schema=q.split(".")[0], table=q.split(".")[1]),
            cdc_table=TableObject(schema=q.split(".")[0], table=q.split(".")[1]),
        )
        for q in qualified
    }
    return bi


# --- _resolve_url / _stream_connection_settings ----------------------


def test_resolve_url_injects_mysqlconnector_driver():
    conn = MagicMock()
    conn._uri.return_value = "mysql://h:3306/d"
    conn.credentials = UserPassword(user=Secret("etl"), password=Secret("hunter2"))
    bi = BinlogIngester(conn=conn, tables=[], start_from="tail", server_id=42)
    url = bi._resolve_url()
    assert url.drivername == "mysql+mysqlconnector"
    assert url.username == "etl"
    settings = bi._stream_connection_settings()
    assert settings["host"] == "h"
    assert settings["port"] == 3306
    assert settings["user"] == "etl"
    assert settings["db"] == "d"


def test_resolve_url_preserves_explicit_driver():
    conn = MagicMock()
    conn._uri.return_value = "mysql+pymysql://h/d"
    conn.credentials = UserPassword(user=Secret("etl"), password=Secret("hunter2"))
    bi = BinlogIngester(conn=conn, tables=[], start_from="tail", server_id=42)
    assert bi._resolve_url().drivername == "mysql+pymysql"


# --- truncate ---------------------------------------------------------


def _truncate_ready(qualified: str = "app.users") -> BinlogIngester:
    bi = _ingester_with_tables([qualified])
    bi.init_schema_registry(CdcFormat())
    return bi


def _query_event(query: str, schema: str = "app") -> MagicMock:
    event = MagicMock()
    event.query = query
    event.schema = schema.encode()
    event.timestamp = 1767268800
    return event


@pytest.mark.parametrize(
    "statement",
    [
        "truncate table users",
        "TRUNCATE TABLE users",
        "truncate users",
        "truncate table app.users",
        "truncate table `app`.`users`",
        "truncate table users;",
    ],
)
def test_truncate_statement_forms_are_recognized(statement):
    """A MySQL truncate reaches the binlog as statement text, so every
    spelling of it has to resolve to the same tracked table."""
    bi = _truncate_ready()

    change = bi._truncate_change(_query_event(statement), "uuid:7")

    assert change is not None
    assert change.table == "app.users"
    assert change.operation is CdcOperation.TRUNCATE


@pytest.mark.parametrize(
    "statement",
    ["BEGIN", "COMMIT", "insert into users values (1)", "truncatex table users"],
)
def test_non_truncate_statements_are_ignored(statement):
    bi = _truncate_ready()

    assert bi._truncate_change(_query_event(statement), "uuid:7") is None


def test_truncate_is_emitted_with_the_known_table_definition():
    bi = _truncate_ready()
    bi._schema_registry.register_from_log(
        "app.users", (("id", "int", _Col(3)), ("name", "str", _Col(15, max_length=50)))
    )

    change = bi._truncate_change(_query_event("truncate table users"), "uuid:7")

    assert [column.name for column in change.schema.columns] == ["id", "name"]
    assert change.new_values == {"id": None, "name": None}


def test_truncate_of_a_never_captured_table_is_still_emitted():
    """Dropping it would strand rows in the destination, so it carries the
    unknown definition rather than being skipped."""
    bi = _truncate_ready()

    change = bi._truncate_change(_query_event("truncate table users"), "uuid:7")

    assert change is not None
    assert change.schema.columns == ()
    assert change.new_values == {}


def test_truncate_stands_alone_in_its_own_transaction():
    """MySQL truncates commit implicitly, so the event arrives outside any
    surrounding transaction and carries that transaction's own GTID."""
    bi = _truncate_ready()

    change = bi._truncate_change(_query_event("truncate table users"), "uuid:42")

    assert change.transaction == "uuid:42"
    assert change.offset.gtid == "uuid:42"
    assert change.segment == 42


def test_truncate_of_an_untracked_table_is_ignored():
    bi = _truncate_ready()

    assert bi._truncate_change(_query_event("truncate table other"), "uuid:7") is None


def test_schema_table_filter_empty_when_no_tables():
    conn = MagicMock()
    bi = BinlogIngester(conn=conn, tables=[], start_from="tail", server_id=42)
    schemas, tables = bi._schema_table_filter()
    assert schemas is None
    assert tables is None


def test_schema_table_filter_dedupes_schemas():
    bi = _ingester_with_tables(["app.users", "app.orders", "logs.events"])
    schemas, tables = bi._schema_table_filter()
    assert set(schemas) == {"app", "logs"}
    assert set(tables) == {"users", "orders", "events"}


# --- _as_qualified_table ---------------------------------------------


def test_as_qualified_table_known():
    bi = _ingester_with_tables(["app.users"])
    assert bi._as_qualified_table("app", "users") == "app.users"


def test_as_qualified_table_unknown_returns_none():
    bi = _ingester_with_tables(["app.users"])
    assert bi._as_qualified_table("other", "users") is None


# --- extract_raw_schema ---------------------------------------------


def test_extract_raw_schema_composes_type_string():
    class _RawCol:
        def __init__(self, **kw):
            for k, v in kw.items():
                setattr(self, k, v)

    class _Event:
        table = "users"
        columns = [
            _RawCol(name="id", type="LONG", unsigned=False),
            _RawCol(name="age", type="LONG", unsigned=True),
            _RawCol(name="name", type="VARCHAR", max_length=255),
            _RawCol(name="price", type="NEWDECIMAL", precision=10, decimals=2),
        ]

    bi = _ingester_with_tables(["app.users"])
    rs = bi._extract_raw_schema(_Event())
    names = [t[0] for t in rs]
    types = [t[1] for t in rs]
    assert names == ["id", "age", "name", "price"]
    assert types == [
        "LONG",
        "LONG:unsigned",
        "VARCHAR:len=255",
        "NEWDECIMAL:prec=10:dec=2",
    ]


def test_extract_raw_schema_rejects_anonymous_column():
    class _RawCol:
        name = None

    class _Event:
        table = "users"
        columns = [_RawCol()]

    bi = _ingester_with_tables(["app.users"])
    with pytest.raises(TabsdataException) as ei:
        bi._extract_raw_schema(_Event())
    assert ei.value.api_error is MysqlErrorCode.MYSQL_16


# --- base_offset / commit_offset ------------------------------------


def test_base_offset_is_empty_before_init_stream():
    bi = _ingester_with_tables(["app.users"])
    base = bi.base_offset()
    assert isinstance(base, GtidOffset)
    assert base.gtid == ""


def test_commit_offset_accepts_gtid_offset():
    bi = _ingester_with_tables(["app.users"])
    bi.commit_offset(GtidOffset(gtid="uuid:1"))


def test_commit_offset_rejects_foreign_offset():
    bi = _ingester_with_tables(["app.users"])

    class _Foreign:
        pass

    with pytest.raises(TabsdataException) as ei:
        bi.commit_offset(_Foreign())  # type: ignore[arg-type]
    assert ei.value.api_error is MysqlErrorCode.MYSQL_17


# =====================================================================
# Offset ownership -- MySQL is Tabsdata-owned: it persists the GTID in
# stream state (default commit_state) and reconstructs it on resume via
# offset_from_state.
# =====================================================================


def test_offset_from_state_reconstructs_gtid_offset():
    bi = _ingester_with_tables(["app.users"])
    offset = bi.offset_from_state(GtidOffset(gtid="uuid:42").write())
    assert isinstance(offset, GtidOffset)
    assert offset.gtid == "uuid:42"


def test_commit_state_persists_gtid_in_stream_state():
    bi = _ingester_with_tables(["app.users"])
    spec = bi.commit_state(GtidOffset(gtid="uuid:7"))
    assert isinstance(spec, dict)
    assert spec == {CDC_OFFSET_STATE_KEY: GtidOffset(gtid="uuid:7").write()}


def test_resume_offset_round_trips_persisted_gtid():
    bi = _ingester_with_tables(["app.users"])
    persisted = bi.commit_state(GtidOffset(gtid="uuid:7"))
    resumed = bi.resume_offset(first_stage=False, stream_state=persisted)
    assert isinstance(resumed, GtidOffset)
    assert resumed.gtid == "uuid:7"


def test_resume_offset_first_stage_ignores_persisted_state():
    bi = _ingester_with_tables(["app.users"])
    assert (
        bi.resume_offset(
            first_stage=True,
            stream_state={CDC_OFFSET_STATE_KEY: {"gtid": "uuid:7"}},
        )
        is None
    )


def test_seed_offset_sets_resume_mark():
    bi = _ingester_with_tables(["app.users"])
    bi.seed_offset(GtidOffset(gtid="uuid:7"))
    assert bi._mark.gtid == "uuid:7"


# --- poll deadline at a transaction boundary -------------------------


def test_poll_carries_the_gtid_of_a_transaction_it_ran_out_of_time_on():
    from pymysqlreplication.row_event import WriteRowsEvent

    bi = _ingester_with_tables(["app.users"])
    bi._blocking_timeout_seconds = 0
    opening = MagicMock(spec=GtidEvent)
    opening.gtid = "uuid:9475"
    bi._stream = iter([opening, MagicMock(spec=WriteRowsEvent)])

    assert list(bi._poll_changes_buffered(session=None)) == []
    assert bi._carry_gtid == "uuid:9475"


def test_poll_adopts_a_carried_gtid_instead_of_tagging_segment_zero():
    bi = _ingester_with_tables(["app.users"])
    bi._carry_gtid = "uuid:9475"
    bi._stream = iter([])

    list(bi._poll_changes_buffered(session=None))

    # Consumed by the poll that adopted it, so it is not replayed again.
    assert bi._carry_gtid is None


def test_closing_the_session_drops_a_carried_gtid():
    bi = _ingester_with_tables(["app.users"])
    bi._carry_gtid = "uuid:9475"
    bi._stream = MagicMock()

    bi.close_session(None)

    assert bi._carry_gtid is None


# =====================================================================
# BinlogInitialLoadSession
# =====================================================================


class _Cursor:
    def __init__(self, log: list[str]):
        self._log = log
        self._row = None

    def execute(self, sql: str) -> None:
        self._log.append(sql)
        if sql == "select @@gtid_mode":
            self._row = ("ON",)
        elif sql == "select @@global.gtid_executed":
            self._row = ("uuid:1-5",)

    def fetchone(self):
        return self._row


class _PymysqlConnection:
    def __init__(self, log: list[str]):
        self._log = log

    def cursor(self) -> _Cursor:
        return _Cursor(self._log)

    def close(self) -> None:
        self._log.append("close")


class _WarmedConnection:
    def __init__(self, log: list[str]):
        self._log = log

    def close(self) -> None:
        self._log.append("engine connection returned")


class _Engine:
    def __init__(self, log: list[str]):
        self._log = log

    def connect(self) -> _WarmedConnection:
        self._log.append("engine connection opened")
        return _WarmedConnection(self._log)

    def dispose(self) -> None:
        self._log.append("dispose")


@pytest.fixture
def initial_load_log(monkeypatch) -> list[str]:
    log: list[str] = []
    monkeypatch.setattr(
        _cdc_impl.pymysql, "connect", lambda **settings: _PymysqlConnection(log)
    )
    monkeypatch.setattr(
        _cdc_impl.sqlalchemy_utils,
        "create_engine",
        lambda *args, **kwargs: _Engine(log),
    )
    return log


def test_the_engine_is_initialised_before_the_snapshot_opens(initial_load_log):
    BinlogInitialLoadSession(connection_settings={})
    log = initial_load_log
    assert log.index("engine connection returned") < log.index(
        "start transaction with consistent snapshot, read only"
    )


def test_the_snapshot_is_opened_under_repeatable_read(initial_load_log):
    BinlogInitialLoadSession(connection_settings={})
    log = initial_load_log
    assert log.index("set session transaction isolation level repeatable read") < (
        log.index("start transaction with consistent snapshot, read only")
    )


def test_the_global_read_lock_waits_as_long_as_the_server_allows(initial_load_log):
    BinlogInitialLoadSession(connection_settings={})
    log = initial_load_log
    setting = f"set session lock_wait_timeout = {INITIAL_LOAD_LOCK_WAIT_TIMEOUT_SECONDS}"
    assert INITIAL_LOAD_LOCK_WAIT_TIMEOUT_SECONDS == 31536000
    assert log.index(setting) < log.index("flush tables with read lock")


def test_the_position_is_read_under_the_global_read_lock(initial_load_log):
    session = BinlogInitialLoadSession(connection_settings={})
    log = initial_load_log
    assert (
        log.index("flush tables with read lock")
        < log.index("start transaction with consistent snapshot, read only")
        < log.index("select @@global.gtid_executed")
        < log.index("unlock tables")
    )
    assert session.offset.gtid == "uuid:1-5"
