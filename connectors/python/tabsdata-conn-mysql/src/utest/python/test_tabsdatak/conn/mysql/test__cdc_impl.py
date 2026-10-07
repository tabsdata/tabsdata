#
# Copyright 2026 Tabsdata Inc.
#

"""``_cdc_impl.py`` -- pure-function coverage for the MySQL CDC
dialect, binlog ingester and offset. Catalog / engine calls
(``server_timezone``, ``get_catalog_columns``, stream init,
``poll_changes``) need a real MySQL CDC domain and live in the
integration tests (``test_plugin_cdc.py``)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from unittest.mock import MagicMock

import pytest
import sqlalchemy.types as sa_types
from pymysqlreplication.event import GtidEvent, HeartbeatLogEvent
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
    BINLOG_START_POSITION,
    INITIAL_LOAD_LOCK_WAIT_TIMEOUT_SECONDS,
    BinlogIngester,
    BinlogInitialLoadSession,
    GtidOffset,
    GtidSet,
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


@pytest.mark.parametrize("unsigned", [False, True], ids=["signed", "unsigned"])
def test_binlog_integers_keep_their_unsigned_flag(d, unsigned):
    import pymysqlreplication.constants.FIELD_TYPE as FT

    for code in (FT.TINY, FT.SHORT, FT.INT24, FT.LONG, FT.LONGLONG):
        assert d.cdc_to_sa("binlog", _Col(code, unsigned=unsigned)).unsigned is unsigned


@pytest.mark.parametrize(
    ("code", "catalog"),
    [
        ("TINY", "TINYINT"),
        ("SHORT", "SMALLINT"),
        ("INT24", "MEDIUMINT"),
        ("LONG", "INTEGER"),
        ("LONGLONG", "BIGINT"),
    ],
)
@pytest.mark.parametrize("unsigned", [False, True], ids=["signed", "unsigned"])
def test_binlog_and_catalog_integers_normalize_alike(d, code, catalog, unsigned):
    """The load reads a column's type from the catalog and the stream from
    the binlog: both must give it the same Arrow type, or the two schema
    hashes differ and an unsigned value past the signed range overflows."""
    import pymysqlreplication.constants.FIELD_TYPE as FT
    from sqlalchemy.dialects import mysql

    from tabsdatak.conn.common.sql.cdc._dtype import sqlalchemy_to_arrow

    logged = d.cdc_to_sa("binlog", _Col(getattr(FT, code), unsigned=unsigned))
    reflected = getattr(mysql, catalog)(unsigned=unsigned)
    assert sqlalchemy_to_arrow(logged) == sqlalchemy_to_arrow(reflected)


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


@pytest.mark.parametrize(
    ("column", "reflected"),
    [
        (
            {"code": "FLOAT", "size": 4},
            lambda mysql: mysql.FLOAT(),
        ),
        (
            {"code": "FLOAT", "size": 4},
            lambda mysql: mysql.FLOAT(precision=7, scale=4),
        ),
        (
            {"code": "DOUBLE", "size": 8},
            lambda mysql: mysql.DOUBLE(),
        ),
        (
            {"code": "VARCHAR", "max_length": 8, "character_set_name": "binary"},
            lambda mysql: mysql.VARBINARY(length=8),
        ),
        (
            {"code": "STRING", "max_length": 3, "character_set_name": "binary"},
            lambda mysql: mysql.BINARY(length=3),
        ),
        (
            {"code": "VARCHAR", "max_length": 80, "character_set_name": "utf8mb4"},
            lambda mysql: mysql.VARCHAR(length=20),
        ),
        (
            {"code": "STRING", "max_length": 20, "character_set_name": "utf8mb4"},
            lambda mysql: mysql.CHAR(length=5),
        ),
    ],
    ids=[
        "float",
        "float-with-precision",
        "double",
        "varbinary",
        "binary",
        "varchar",
        "char",
    ],
)
def test_binlog_and_catalog_columns_normalize_alike(d, column, reflected):
    import pymysqlreplication.constants.FIELD_TYPE as FT
    from sqlalchemy.dialects import mysql

    from tabsdatak.conn.common.sql.cdc._dtype import sqlalchemy_to_arrow

    attributes = dict(column)
    code = getattr(FT, attributes.pop("code"))
    logged = d.cdc_to_sa("binlog", _Col(code, **attributes))
    assert sqlalchemy_to_arrow(logged) == sqlalchemy_to_arrow(reflected(mysql))


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


@pytest.mark.parametrize("state", ["", "{}", '{"gtid": null}', '{"gtid": 5}', "x"])
def test_gtid_offset_read_rejects_an_unexpected_format(state):
    with pytest.raises(TabsdataException) as ei:
        GtidOffset.read(state)
    assert ei.value.api_error is MysqlErrorCode.MYSQL_24


def test_gtid_offset_reads_an_empty_position():
    offset = GtidOffset.read('{"gtid": "", "source": null}')
    assert (offset.gtid, offset.source) == ("", None)


def test_gtid_offset_round_trips_its_source():
    offset = GtidOffset.read(GtidOffset(gtid="u:1-5", source="u").write())
    assert (offset.gtid, offset.source) == ("u:1-5", "u")


def test_gtid_offset_update_keeps_a_recorded_source():
    offset = GtidOffset(gtid="u:1-5", source="u")
    offset.update(GtidOffset(gtid="u:1-6"))
    assert (offset.gtid, offset.source) == ("u:1-6", "u")


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


def test_a_timestamp_reading_becomes_the_server_clock_and_a_datetime_stays():
    """The binlog reader hands a TIMESTAMP as a naive UTC reading while a batch
    read of the row gives the server zone's clock; a DATETIME is that clock
    in both. The stream publishes both as the batch read does."""
    import pymysqlreplication.constants.FIELD_TYPE as FT

    bi = _ingester_with_tables(["app.users"])
    bi._server_tz = timezone(timedelta(hours=2))
    event = _event(
        object,
        columns=[_Col(FT.TIMESTAMP2, name="at"), _Col(FT.DATETIME2, name="when")],
    )
    values = {
        "at": datetime(2026, 1, 1, 10, 0),
        "when": datetime(2026, 1, 1, 10, 0),
        "n": 1,
    }

    bi._to_server_clock(values, bi._timestamp_columns(event))

    assert values == {
        "at": datetime(2026, 1, 1, 12, 0),
        "when": datetime(2026, 1, 1, 10, 0),
        "n": 1,
    }
    assert values["at"].tzinfo is None


def test_without_a_known_server_zone_a_timestamp_stays_utc_and_is_warned_once(
    caplog,
):
    import pymysqlreplication.constants.FIELD_TYPE as FT

    bi = _ingester_with_tables(["app.users"])
    assert bi._server_tz is None
    event = _event(object, columns=[_Col(FT.TIMESTAMP, name="at")])
    stamps = bi._timestamp_columns(event)
    values = {"at": datetime(2026, 1, 1, 10, 0)}

    with caplog.at_level("WARNING"):
        bi._to_server_clock(values, stamps)
        bi._to_server_clock(values, stamps)

    assert values == {"at": datetime(2026, 1, 1, 10, 0)}
    assert caplog.text.count("server zone is unknown") == 1


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

    change = bi._truncate_change(_query_event(statement), "uuid:7", GtidOffset(gtid="uuid:1-7"))

    assert change is not None
    assert change.table == "app.users"
    assert change.operation is CdcOperation.TRUNCATE


@pytest.mark.parametrize(
    "statement",
    ["BEGIN", "COMMIT", "insert into users values (1)", "truncatex table users"],
)
def test_non_truncate_statements_are_ignored(statement):
    bi = _truncate_ready()

    assert bi._truncate_change(_query_event(statement), "uuid:7", GtidOffset(gtid="uuid:1-7")) is None


def test_truncate_is_emitted_with_the_known_table_definition():
    bi = _truncate_ready()
    bi._schema_registry.register_from_log(
        "app.users", (("id", "int", _Col(3)), ("name", "str", _Col(15, max_length=50)))
    )

    change = bi._truncate_change(_query_event("truncate table users"), "uuid:7", GtidOffset(gtid="uuid:1-7"))

    assert [column.name for column in change.schema.columns] == ["id", "name"]
    assert change.new_values == {"id": None, "name": None}


def test_truncate_of_a_never_captured_table_is_still_emitted():
    """Dropping it would strand rows in the destination, so it carries the
    unknown definition rather than being skipped."""
    bi = _truncate_ready()

    change = bi._truncate_change(_query_event("truncate table users"), "uuid:7", GtidOffset(gtid="uuid:1-7"))

    assert change is not None
    assert change.schema.columns == ()
    assert change.new_values == {}


def test_truncate_stands_alone_in_its_own_transaction():
    """MySQL truncates commit implicitly, so the event arrives outside any
    surrounding transaction and carries that transaction's own GTID."""
    bi = _truncate_ready()

    change = bi._truncate_change(_query_event("truncate table users"), "uuid:42", GtidOffset(gtid="uuid:1-42"))

    assert change.transaction == "uuid:42"
    assert change.offset.gtid == "uuid:1-42"
    assert change.segment == 42


def test_truncate_of_an_untracked_table_is_ignored():
    bi = _truncate_ready()

    assert bi._truncate_change(_query_event("truncate table other"), "uuid:7", GtidOffset(gtid="uuid:1-7")) is None


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


# --- base_offset ----------------------------------------------------


def test_base_offset_is_empty_before_init_stream():
    bi = _ingester_with_tables(["app.users"])
    base = bi.base_offset()
    assert isinstance(base, GtidOffset)
    assert base.gtid == ""


# =====================================================================
# Offset ownership -- MySQL is Tabsdata-kept: it persists the GTID in
# stream state (stage_state) and reconstructs it on resume through its
# offsets keeper.
# =====================================================================


def test_the_keeper_reads_back_a_gtid_offset():
    bi = _ingester_with_tables(["app.users"])
    offset = bi.offsets.read(GtidOffset(gtid="uuid:42").write())
    assert isinstance(offset, GtidOffset)
    assert offset.gtid == "uuid:42"


def test_stage_state_persists_gtid_in_stream_state():
    bi = _ingester_with_tables(["app.users"])
    spec = bi.stage_state(GtidOffset(gtid="uuid:7"))
    assert isinstance(spec, dict)
    assert spec == {CDC_OFFSET_STATE_KEY: GtidOffset(gtid="uuid:7").write()}


def test_resume_offset_round_trips_persisted_gtid():
    bi = _ingester_with_tables(["app.users"])
    persisted = bi.stage_state(GtidOffset(gtid="uuid:7"))
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
    bi._poll_max_secs = 0
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
        elif sql == "select utc_timestamp(6)":
            self._row = (datetime(2026, 1, 1, 12, 0, 0),)

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


def test_the_load_time_is_the_server_time_in_utc_once_the_snapshot_is_open(
    initial_load_log,
):
    session = BinlogInitialLoadSession(connection_settings={})
    log = initial_load_log
    assert session.timestamp == datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert log.index("select utc_timestamp(6)") > log.index(
        "start transaction with consistent snapshot, read only"
    )


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


def test_the_catalog_is_not_read_through_the_snapshot_connection(
    initial_load_log, monkeypatch
):
    """A connection handed back to its pool is rolled back, which on the
    snapshot's connection would end the snapshot before the tables are read,
    so the catalog is read through connections of its own."""
    engines = []

    def create_engine(*args, **kwargs):
        engine = _Engine(initial_load_log)
        engine.creator = kwargs["creator"]
        engines.append(engine)
        return engine

    monkeypatch.setattr(_cdc_impl.sqlalchemy_utils, "create_engine", create_engine)
    session = BinlogInitialLoadSession(connection_settings={})
    snapshot = session._connection
    (reader,) = [engine for engine in engines if engine is not session.engine]

    assert reader.creator() is snapshot
    assert session.engine.creator() is not snapshot


def test_closing_the_load_disposes_both_engines(initial_load_log):
    BinlogInitialLoadSession(connection_settings={}).close()
    assert initial_load_log.count("dispose") == 2


# =====================================================================
# Resume continuity
# =====================================================================


class _GtidServer:
    def __init__(self, row):
        self.row = row
        self.queries = []

    def cursor(self):
        server = self

        class _Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *exc_info):
                return False

            def execute(self, sql, parameters=None):
                server.queries.append((sql, parameters))

            def fetchone(self):
                return server.row

        return _Cursor()

    def close(self):
        pass


def _gtid_ingester(monkeypatch, row) -> tuple[BinlogIngester, _GtidServer]:
    bi = BinlogIngester(
        conn=MagicMock(),
        tables=["db.t"],
        start_from="head",
        server_id=1,
    )
    server = _GtidServer(row)
    monkeypatch.setattr(_cdc_impl.pymysql, "connect", lambda **settings: server)
    monkeypatch.setattr(
        bi,
        "_stream_connection_settings",
        lambda: {"host": "h", "port": 1, "user": "u", "passwd": "p", "db": "d"},
    )
    return bi, server


def test_a_resume_the_server_can_serve_passes(monkeypatch):
    bi, server = _gtid_ingester(monkeypatch, ("u:1-5", "u:1-90", 1, 1))
    bi._verify_gtids("u:1-40")
    sql, parameters = server.queries[0]
    assert "gtid_subset(@@global.gtid_purged, %s)" in sql
    assert parameters == ("u:1-40", "u:1-40")


def test_a_resume_after_unread_transactions_were_purged_fails(monkeypatch):
    bi, _ = _gtid_ingester(monkeypatch, ("u:1-60", "u:1-90", 0, 1))
    with pytest.raises(TabsdataException) as ei:
        bi._verify_gtids("u:1-40")
    assert ei.value.api_error is MysqlErrorCode.MYSQL_22


def test_a_resume_on_a_server_missing_consumed_transactions_fails(monkeypatch):
    bi, _ = _gtid_ingester(monkeypatch, ("u:1-5", "u:1-30", 1, 0))
    with pytest.raises(TabsdataException) as ei:
        bi._verify_gtids("u:1-40")
    assert ei.value.api_error is MysqlErrorCode.MYSQL_23


# =====================================================================
# GtidSet and the offset of every transaction read
# =====================================================================


def test_a_gtid_set_parses_and_prints_canonically():
    parsed = GtidSet.parse("B:3,\n a:7:1-5 , a:6")
    assert str(parsed) == "a:1-7,b:3"


def test_a_gtid_set_keeps_tagged_sources_apart():
    parsed = GtidSet.parse("u:1-3:tag:1-2")
    parsed.add("u:tag:3")
    parsed.add("u:9")
    assert str(parsed) == "u:1-3:9,u:tag:1-3"


def test_a_gtid_set_adds_up_to_a_gtid():
    gtid_set = GtidSet.parse("u:5-6")
    gtid_set.add_up_to("u:9")
    assert str(gtid_set) == "u:1-9"


def _event(kind, **attributes):
    event = MagicMock(spec=kind)
    for name, value in attributes.items():
        setattr(event, name, value)
    return event


def _transactions(*gtids_and_bodies):
    from pymysqlreplication.event import QueryEvent, XidEvent
    from pymysqlreplication.row_event import WriteRowsEvent

    events = []
    for gtid, body in gtids_and_bodies:
        events.append(_event(GtidEvent, gtid=gtid))
        if body == "dml":
            events.append(_event(QueryEvent, query="BEGIN", timestamp=0))
            events.append(_event(WriteRowsEvent, schema="other", table="t", rows=[]))
            events.append(_event(XidEvent, timestamp=0))
        else:
            events.append(_event(QueryEvent, query=body, timestamp=0))
    return events


@pytest.mark.parametrize("loop", ["_poll_changes_buffered", "_poll_changes_unbuffered"])
def test_every_transaction_read_enters_the_offset(loop):
    bi = _ingester_with_tables(["app.users"])
    bi._poll_max_secs = 60
    bi._executed = GtidSet.parse("u:1-10")
    bi._stream = iter(
        _transactions(
            ("u:11", "dml"),
            ("u:12", "create table other.x (id int)"),
            ("u:13", "dml"),
        )
    )
    list(getattr(bi, loop)(session=None))
    assert str(bi._executed) == "u:1-13"
    assert (bi._mark.gtid, bi._mark.source) == ("u:1-13", "u")


@pytest.mark.parametrize("loop", ["_poll_changes_buffered", "_poll_changes_unbuffered"])
def test_a_transaction_from_another_source_fails_the_poll(loop):
    bi = _ingester_with_tables(["app.users"])
    bi._poll_max_secs = 60
    bi._executed = GtidSet.parse("u:1-10")
    bi._stream = iter(_transactions(("u:11", "dml"), ("v:1", "dml")))
    with pytest.raises(TabsdataException) as ei:
        list(getattr(bi, loop)(session=None))
    assert ei.value.api_error is MysqlErrorCode.MYSQL_25
    assert str(bi._executed) == "u:1-11"


@pytest.mark.parametrize("loop", ["_poll_changes_buffered", "_poll_changes_unbuffered"])
def test_a_poll_ending_on_a_heartbeat_carries_its_open_transaction(loop):
    bi = _ingester_with_tables(["app.users"])
    bi._poll_max_secs = 0
    bi._carry_gtid = "u:5"
    bi._stream = iter([_event(HeartbeatLogEvent)])
    list(getattr(bi, loop)(session=None))
    assert bi._carry_gtid == "u:5"


def _init_stream(monkeypatch, bi):
    captured = {}
    monkeypatch.setattr(
        _cdc_impl, "BinLogStreamReader", lambda **kwargs: captured.update(kwargs)
    )
    monkeypatch.setattr(bi, "_stream_connection_settings", lambda: {})
    monkeypatch.setattr(bi, "_schema_table_filter", lambda: (None, None))
    monkeypatch.setattr(bi, "_verify_gtids", lambda consumed: None)
    monkeypatch.setattr(bi, "_verify_server_settings", lambda: None)
    monkeypatch.setattr(bi, "_first_binlog_file", lambda: ("binlog.000003", "u:1-40"))
    monkeypatch.setattr(bi, "_binlog_begins_at", lambda file: FIRST_BINLOG_AT)
    monkeypatch.setattr(
        bi, "_last_binlog_position", lambda: ("binlog.000009", 812, "u:1-90,v:1-4")
    )
    bi.init_stream()
    return captured


FIRST_BINLOG_AT = 1_700_000_000


def test_a_start_by_timestamp_reads_from_the_oldest_binlog_and_skips_before_it(
    monkeypatch,
):
    from tabsdatak.conn.common.sql.cdc import TimestampPosition

    since = datetime.fromtimestamp(FIRST_BINLOG_AT + 3_600, tz=timezone.utc)
    bi = _ingester_with_tables(["app.users"])
    bi._start_from = TimestampPosition(ts=since)
    captured = _init_stream(monkeypatch, bi)
    assert captured["log_file"] == "binlog.000003"
    assert captured["log_pos"] == BINLOG_START_POSITION
    assert captured["skip_to_timestamp"] == FIRST_BINLOG_AT + 3_600
    assert "auto_position" not in captured


def test_a_start_by_timestamp_before_the_retained_binlogs_fails(monkeypatch):
    from tabsdatak.conn.common.sql.cdc import TimestampPosition

    since = datetime.fromtimestamp(FIRST_BINLOG_AT - 1, tz=timezone.utc)
    bi = _ingester_with_tables(["app.users"])
    bi._start_from = TimestampPosition(ts=since)
    with pytest.raises(TabsdataException) as ei:
        _init_stream(monkeypatch, bi)
    assert ei.value.api_error is MysqlErrorCode.MYSQL_29


def test_a_resume_asks_for_everything_after_the_full_set(monkeypatch):
    bi = _ingester_with_tables(["app.users"])
    bi.seed_offset(GtidOffset(gtid="u:1-50,v:1-7"))
    captured = _init_stream(monkeypatch, bi)
    assert captured["auto_position"] == "u:1-50,v:1-7"
    assert bi.base_offset().gtid == "u:1-50,v:1-7"


@pytest.mark.parametrize(
    ("start_from", "seed", "log_file"),
    [
        ("tail", "u:1-90,v:1-4", "binlog.000009"),
        ("head", "u:1-40", "binlog.000003"),
    ],
)
def test_a_start_by_position_seeds_the_transactions_before_it(
    monkeypatch, start_from, seed, log_file
):
    bi = _ingester_with_tables(["app.users"])
    bi._start_from = start_from
    captured = _init_stream(monkeypatch, bi)
    assert captured["log_file"] == log_file
    assert bi.base_offset().gtid == seed


def test_a_start_by_binlog_position_counts_each_sources_earlier_transactions(
    monkeypatch,
):
    from tabsdatak.conn.mysql import BinlogPosition

    bi = _ingester_with_tables(["app.users"])
    bi._start_from = BinlogPosition(file="binlog.000004", pos=4)
    _init_stream(monkeypatch, bi)
    assert bi._begin_transaction("u:10").gtid == "u:1-10"
    bi._complete_transaction("u:10")
    assert bi._begin_transaction("u:11").gtid == "u:1-11"


def test_a_resume_follows_the_source_it_recorded(monkeypatch):
    bi = _ingester_with_tables(["app.users"])
    bi.seed_offset(GtidOffset(gtid="u:1-50,w:1-9", source="u"))
    _init_stream(monkeypatch, bi)
    assert bi._begin_transaction("u:51").gtid == "u:1-51,w:1-9"
    with pytest.raises(TabsdataException) as ei:
        bi._begin_transaction("w:10")
    assert ei.value.api_error is MysqlErrorCode.MYSQL_25


def test_a_reload_keeps_the_source_the_previous_stream_recorded(monkeypatch):
    bi = _ingester_with_tables(["app.users"])
    bi.seed_offset(GtidOffset(gtid="u:1-50", source="u"))
    bi.seed_offset(GtidOffset(gtid="u:1-60,v:1-2"))
    _init_stream(monkeypatch, bi)
    assert bi.base_offset().source == "u"
    with pytest.raises(TabsdataException) as ei:
        bi._begin_transaction("v:3")
    assert ei.value.api_error is MysqlErrorCode.MYSQL_25


def test_a_tag_is_a_source_of_its_own():
    bi = _ingester_with_tables(["app.users"])
    bi._begin_transaction("u:5")
    with pytest.raises(TabsdataException) as ei:
        bi._begin_transaction("u:batch:1")
    assert ei.value.api_error is MysqlErrorCode.MYSQL_25


HEALTHY_BINLOG = [
    ("log_bin", "ON"),
    ("binlog_format", "ROW"),
    ("binlog_row_image", "FULL"),
    ("gtid_mode", "ON"),
]


@pytest.mark.parametrize(
    ("variables", "fails"),
    [
        ([("replica_parallel_workers", "4"), ("replica_preserve_commit_order", "ON")], False),
        ([("replica_parallel_workers", "4"), ("replica_preserve_commit_order", "OFF")], True),
        ([("replica_parallel_workers", "1"), ("replica_preserve_commit_order", "OFF")], False),
        ([("slave_parallel_workers", "8"), ("slave_preserve_commit_order", "OFF")], True),
        ([], False),
    ],
)
def test_parallel_apply_without_commit_order_is_refused(monkeypatch, variables, fails):
    bi, server = _gtid_ingester(monkeypatch, None)
    server.cursor = lambda: _Rows(HEALTHY_BINLOG + variables)
    if fails:
        with pytest.raises(TabsdataException) as ei:
            bi._verify_server_settings()
        assert ei.value.api_error is MysqlErrorCode.MYSQL_26
    else:
        bi._verify_server_settings()


@pytest.mark.parametrize(
    ("setting", "value"),
    [
        ("log_bin", "OFF"),
        ("binlog_format", "STATEMENT"),
        ("binlog_format", "MIXED"),
        ("binlog_row_image", "MINIMAL"),
        ("binlog_row_image", "NOBLOB"),
        ("gtid_mode", "OFF"),
        ("gtid_mode", "ON_PERMISSIVE"),
    ],
)
def test_a_binlog_the_stream_cannot_rely_on_is_refused(monkeypatch, setting, value):
    bi, server = _gtid_ingester(monkeypatch, None)
    variables = [
        (name, value if name == setting else good) for name, good in HEALTHY_BINLOG
    ]
    server.cursor = lambda: _Rows(variables)
    with pytest.raises(TabsdataException) as ei:
        bi._verify_server_settings()
    assert ei.value.api_error is MysqlErrorCode.MYSQL_27
    assert f"{setting} {value}" in str(ei.value)


def test_a_missing_binlog_setting_is_refused(monkeypatch):
    bi, server = _gtid_ingester(monkeypatch, None)
    variables = [row for row in HEALTHY_BINLOG if row[0] != "gtid_mode"]
    server.cursor = lambda: _Rows(variables)
    with pytest.raises(TabsdataException) as ei:
        bi._verify_server_settings()
    assert "gtid_mode unset" in str(ei.value)


@pytest.mark.parametrize(
    ("variable", "value", "channels", "fails"),
    [
        ("log_replica_updates", "OFF", [("source",)], True),
        ("log_slave_updates", "OFF", [("source",)], True),
        ("log_replica_updates", "ON", [("source",)], False),
        ("log_replica_updates", "OFF", [], False),
    ],
    ids=[
        "replica-not-logging",
        "older-name-not-logging",
        "replica-logging",
        "no-replica",
    ],
)
def test_a_replica_that_does_not_log_what_it_replicates_is_refused(
    monkeypatch, variable, value, channels, fails
):
    bi, server = _gtid_ingester(monkeypatch, None)
    rows = HEALTHY_BINLOG + [(variable, value)]
    server.cursor = lambda: _Rows(rows, channels)
    if fails:
        with pytest.raises(TabsdataException) as ei:
            bi._verify_server_settings()
        assert ei.value.api_error is MysqlErrorCode.MYSQL_28
        assert value in str(ei.value)
    else:
        bi._verify_server_settings()


def test_the_replica_status_has_its_older_name_before_8_0_22():
    class OldServer:
        def __init__(self):
            self.statements = []

        def execute(self, sql):
            self.statements.append(sql)
            if sql == "show replica status":
                raise _cdc_impl.pymysql.err.ProgrammingError(1064, "syntax")

        def fetchall(self):
            return [("source",)]

    cursor = OldServer()
    assert BinlogIngester._is_replica(cursor) is True
    assert cursor.statements == ["show replica status", "show slave status"]


def test_log_bin_reported_as_one_is_accepted(monkeypatch):
    bi, server = _gtid_ingester(monkeypatch, None)
    server.cursor = lambda: _Rows([("log_bin", "1")] + HEALTHY_BINLOG[1:])
    bi._verify_server_settings()


class _Rows:
    """The server's global variables, and its replica status: a row per
    replication channel it has configured, none when it is no replica."""

    def __init__(self, rows, channels=()):
        self._rows = rows
        self._channels = list(channels)
        self._last = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql, parameters=None):
        assert "replica_preserve_commit_order" in sql or sql == "show replica status"
        self._last = sql

    def fetchall(self):
        return self._channels if self._last == "show replica status" else self._rows


def test_a_user_gtid_set_is_not_flattened():
    assert BinlogIngester._as_gtid_set("u:1-5,v:7") == "u:1-5,v:7"
    assert BinlogIngester._as_gtid_set("u:42") == "u:1-42"
