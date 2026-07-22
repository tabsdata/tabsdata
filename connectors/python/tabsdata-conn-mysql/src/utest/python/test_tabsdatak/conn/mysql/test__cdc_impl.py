#
# Copyright 2026 Tabsdata Inc.
#

"""``_cdc_impl.py`` -- pure-function coverage for the MySQL CDC
dialect, binlog ingester and offset. Catalog / engine calls
(``server_timezone``, ``get_catalog_columns``, stream init,
``poll_changes``) need a real MySQL fixture and live in
``test__cdc_plugin_integration.py``."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import sqlalchemy.types as sa_types
from sqlalchemy.dialects.mysql import (
    DATETIME,
    ENUM,
    LONGTEXT,
    SET,
    TIME,
    TIMESTAMP,
    TINYBLOB,
    TINYINT,
)
from sqlalchemy.types import LargeBinary, Text

from tabsdatak.conn.common.sql.cdc._ingester import CDC_OFFSET_STATE_KEY
from tabsdatak.conn.common.sql.cdc._model import TableObject, TrackedTable
from tabsdatak.conn.mysql._cdc_impl import (
    BinlogIngester,
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


def test_parse_qualified_table_rejects_unqualified(d):
    with pytest.raises(ValueError):
        d.parse_qualified_table("users")


def test_parse_qualified_table_rejects_empty_side(d):
    with pytest.raises(ValueError):
        d.parse_qualified_table(".users")
    with pytest.raises(ValueError):
        d.parse_qualified_table("app.")


# --- Large-column detection ------------------------------------------


def test_is_large_column_blob_family(d):
    assert d.is_large_column(LargeBinary())
    assert d.is_large_column(TINYBLOB())
    assert d.is_large_column(LONGTEXT())
    assert d.is_large_column(Text())


def test_is_large_column_false_for_regular_types(d):
    assert not d.is_large_column(sa_types.Integer())
    assert not d.is_large_column(sa_types.String())


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
