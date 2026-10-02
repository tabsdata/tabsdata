#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import typing

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import CfgSpec, EntityWhitelistSpec, UserPassword
from tabsdatak.conn.mysql import (
    DEFAULT_SERVER_ID,
    BinlogPosition,
    CredentialsSpec,
    DestCfgSpec,
    GtidPosition,
    IfTableExistsSpec,
    MySQLCdcStreamSrc,
    MySQLCdcStreamSrcConn,
    MySQLDest,
    MySQLDestConn,
    MySQLSrc,
    MySQLSrcConn,
    MySQLTableNamesSpec,
    QueriesSpec,
    SrcCfgSpec,
    TimestampPosition,
    UriSpec,
)

# noinspection PyProtectedMember
from tabsdatak.conn.mysql.error import (
    MysqlErrorCode,
    MySQLValidateException,
)
from tabsdatak.error import TabsdataException

pytestmark = pytest.mark.mysql


# ---------------------------------------------------------------------------
# MySQLConn
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", [MySQLSrcConn, MySQLDestConn])
def test_mysql_conn_round_trip(cls):
    c = cls(
        uri="mysql://mysql.example.com:3306/analytics",
        credentials=UserPassword(
            user=Secret("etl"),
            password=Secret("hunter2"),
        ),
        conn_cfg={"mysql.timeout": 30},
    )
    loaded = cls.from_json(c.to_json())
    # Serializing always dehydrates, so secret values do not survive the
    # round-trip -- the credentials come back dehydrated.
    assert loaded.credentials == UserPassword(user=Secret(), password=Secret())
    assert loaded.uri == "mysql://mysql.example.com:3306/analytics"
    assert loaded.conn_cfg == {"mysql.timeout": 30}


@pytest.mark.parametrize("cls", [MySQLSrcConn, MySQLDestConn])
def test_mysql_conn_rejects_bad_uri(cls):
    # The ``uri`` field is a Union of ``str | Secret``, each
    # carrying the same regex. A bad value fails *every* branch of the
    # Union -- pydantic surfaces whichever raised; assert on the
    # ``TabsdataException`` umbrella rather than the specific code.
    with pytest.raises(TabsdataException):
        cls(
            uri="ftp://host/db",
            credentials=UserPassword(
                user=Secret("u"),
                password=Secret("p"),
            ),
        )


def test_mysql_src_dest_conn_are_distinct_classes():
    # Source and destination use distinct connection classes (told apart
    # on the wire by the envelope ``type``), though both now share the
    # uniform ``connectionDef`` kind.
    assert MySQLSrcConn is not MySQLDestConn
    assert MySQLSrcConn.__kind__ == MySQLDestConn.__kind__ == "connectionDef"


# ---------------------------------------------------------------------------
# MySQLSrc
# ---------------------------------------------------------------------------


def test_mysql_src_full_round_trip():
    src = MySQLSrc(
        queries=["SELECT * FROM users", "SELECT * FROM orders"],
        transactional=False,
        schema_overrides=[None, None],
        initial_values={"x": 1},
        src_cfg={"mysql.logging_level": 20},
    )
    loaded = MySQLSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.queries == ["SELECT * FROM users", "SELECT * FROM orders"]
    assert loaded.transactional is False
    assert loaded.schema_overrides == [None, None]
    assert loaded.initial_values == {"x": 1}
    assert loaded.src_cfg == {"mysql.logging_level": 20}


def test_mysql_src_validate_rejects_schema_overrides_length_mismatch():
    with pytest.raises(MySQLValidateException) as ei:
        MySQLSrc(
            queries=["SELECT 1", "SELECT 2"],
            schema_overrides=[None],
        )
    assert ei.value.api_error is MysqlErrorCode.MYSQL_3


def test_mysql_src_validate_accepts_matching_schema_overrides_length():
    src = MySQLSrc(
        queries=["SELECT 1", "SELECT 2"],
        schema_overrides=[None, None],
    )
    assert len(src.schema_overrides) == len(src.queries)


# ---------------------------------------------------------------------------
# MySQLDest
# ---------------------------------------------------------------------------


def test_mysql_dest_full_round_trip():
    dest = MySQLDest(
        tables=["analytics.users", "analytics.orders"],
        if_table_exists="replace",
        transactional=False,
        dest_cfg={"mysql.logging_level": 20},
    )
    loaded = MySQLDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["analytics.users", "analytics.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.transactional is False
    assert loaded.dest_cfg == {"mysql.logging_level": 20}


def test_mysql_dest_defaults_if_table_exists_to_append():
    dest = MySQLDest(tables=["analytics.users"])
    assert dest.if_table_exists == "append"
    assert dest.transactional is True
    assert dest.dest_cfg is None


# ---------------------------------------------------------------------------
# Field-spec wiring -- assert each dataclass field carries the expected
# type alias. The aliases themselves (UriSpec, MySQLTableNamesSpec, ...)
# are exercised in test__validation.py and the conn-common test suite; here
# we only check the wiring, so the dataclasses can't silently drift away
# from their declared constraints.
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


@pytest.mark.parametrize("cls", [MySQLSrcConn, MySQLDestConn])
def test_mysql_conn_fields_wire_expected_specs(cls):
    assert _hint(cls, "uri") == UriSpec
    assert _hint(cls, "credentials") == CredentialsSpec
    assert _hint(cls, "entity_whitelist") == EntityWhitelistSpec | None
    assert _hint(cls, "conn_cfg") == CfgSpec | None


def test_mysql_src_fields_wire_expected_specs():
    assert _hint(MySQLSrc, "queries") == QueriesSpec
    assert _hint(MySQLSrc, "transactional") is bool
    assert _hint(MySQLSrc, "schema_overrides") == SchemaOverrideSpec | None
    assert _hint(MySQLSrc, "src_cfg") == SrcCfgSpec | None


def test_mysql_dest_fields_wire_expected_specs():
    assert _hint(MySQLDest, "tables") == MySQLTableNamesSpec
    assert _hint(MySQLDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(MySQLDest, "transactional") is bool
    assert _hint(MySQLDest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# src_cfg / dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_mysql_src_rejects_unknown_src_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        MySQLSrc(
            queries=["SELECT 1"],
            src_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is MysqlErrorCode.MYSQL_2


def test_mysql_src_accepts_known_src_cfg_keys():
    src = MySQLSrc(
        queries=["SELECT 1"],
        src_cfg={
            "mysql.logging_level": 20,
            "tabsdata.sql.read_engine": "polarsSqlAlchemyRead",
            "tabsdata.sql.chunk_size": 100,
        },
    )
    loaded = MySQLSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.src_cfg == {
        "mysql.logging_level": 20,
        "tabsdata.sql.read_engine": "polarsSqlAlchemyRead",
        "tabsdata.sql.chunk_size": 100,
    }


def test_mysql_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        MySQLDest(
            tables=["analytics.users"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is MysqlErrorCode.MYSQL_4


def test_mysql_dest_accepts_known_dest_cfg_keys():
    dest = MySQLDest(
        tables=["analytics.users"],
        dest_cfg={
            "mysql.logging_level": 20,
            "tabsdata.sql.write_engine": "polarsSqlAlchemyWrite",
            "tabsdata.sql.chunk_size": 100,
        },
    )
    loaded = MySQLDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.dest_cfg == {
        "mysql.logging_level": 20,
        "tabsdata.sql.write_engine": "polarsSqlAlchemyWrite",
        "tabsdata.sql.chunk_size": 100,
    }


# =====================================================================
# CDC public primitives -- GtidPosition / BinlogPosition /
# TimestampPosition / MySQLCdcStreamSrcConn / MySQLCdcStreamSrc
# =====================================================================
#
# These types live on the public mysql surface (``__init__.py``)
# so users build CDC sources without reaching into private
# modules. The plugin (``_cdc_plugin.py``) converts the mysql-local
# format / TimestampPosition onto the common runner types -- see
# ``test__cdc_plugin.py`` for that conversion.

from datetime import datetime, timezone  # noqa: E402

# --- Position primitives --------------------------------------------


def test_gtid_position_round_trip():
    g = GtidPosition(gtid="uuid-1:42")
    assert g.gtid == "uuid-1:42"


def test_gtid_position_rejects_non_string():
    with pytest.raises(ConnInitException) as ei:
        GtidPosition(gtid=42)  # type: ignore[arg-type]
    assert ei.value.api_error is MysqlErrorCode.MYSQL_12


def test_binlog_position_round_trip():
    b = BinlogPosition(file="mysql-bin.000001", pos=100)
    assert b.file == "mysql-bin.000001"
    assert b.pos == 100


def test_binlog_position_negative_pos_is_lenient():
    # The validator just type-checks ``int``; negative positions
    # are syntactically valid but semantically meaningless.
    b = BinlogPosition(file="x", pos=-1)
    assert b.pos == -1


def test_timestamp_position_is_mysql_local():
    ts = TimestampPosition(ts=datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert ts.ts.year == 2026


def test_cdc_typing_classes_are_mysql_local_not_common_reexports():
    # Pins the boundary: the user-visible CDC dataclasses must be
    # mysql-local so a knob added on a future Postgres CDC source
    # cannot accidentally widen the mysql schema.
    from tabsdatak.conn.common.sql.cdc.typing import (
        TimestampPosition as _CommonTimestampPosition,
    )

    assert TimestampPosition is not _CommonTimestampPosition


# --- MySQLCdcStreamSrcConn ---------------------------------------------------


def _cdc_creds():
    return UserPassword(user=Secret("u"), password=Secret("p"))


def test_mysql_cdc_conn_default_server_id():
    conn = MySQLCdcStreamSrcConn(
        uri="mysql://host:3306/db",
        credentials=_cdc_creds(),
    )
    assert conn.server_id == DEFAULT_SERVER_ID
    assert conn.uri == "mysql://host:3306/db"


def test_mysql_cdc_conn_explicit_server_id():
    conn = MySQLCdcStreamSrcConn(
        uri="mysql://host/db",
        credentials=_cdc_creds(),
        server_id=999,
    )
    assert conn.server_id == 999


def test_mysql_cdc_conn_is_not_a_mysql_conn_subclass():
    # MySQLCdcStreamSrcConn deliberately does not subclass the batch
    # SQL conns; the connector treats them as siblings with overlapping
    # fields.
    assert not issubclass(MySQLCdcStreamSrcConn, MySQLSrcConn)
    assert not issubclass(MySQLCdcStreamSrcConn, MySQLDestConn)
    assert not issubclass(MySQLSrcConn, MySQLCdcStreamSrcConn)
    assert not issubclass(MySQLDestConn, MySQLCdcStreamSrcConn)


# --- MySQLCdcStreamSrc ----------------------------------------------


def test_mysql_cdc_stream_src_minimal_construction():
    src = MySQLCdcStreamSrc(
        tables=["app.users"],
        start_from="tail",
    )
    assert src.tables == ["app.users"]
    assert src.start_from == "tail"
    assert src.columns_format == "columns"
    assert src.flatten_columns is True


def test_mysql_cdc_stream_src_accepts_explicit_format_fields():
    src = MySQLCdcStreamSrc(
        tables=["app.users"],
        start_from="head",
        columns_format="map",
        flatten_columns=False,
    )
    assert src.columns_format == "map"
    assert src.flatten_columns is False


def test_mysql_cdc_stream_src_rejects_unknown_columns_format():
    with pytest.raises(ConnInitException) as ei:
        MySQLCdcStreamSrc(
            tables=["app.users"],
            start_from="tail",
            columns_format="bogus",  # type: ignore[arg-type]
        )
    assert ei.value.api_error is MysqlErrorCode.MYSQL_11


def test_mysql_cdc_stream_src_accepts_gtid_start_from():
    src = MySQLCdcStreamSrc(
        tables=["app.users"],
        start_from=GtidPosition(gtid="uuid:1"),
    )
    assert isinstance(src.start_from, GtidPosition)


def test_mysql_cdc_stream_src_accepts_binlog_start_from():
    src = MySQLCdcStreamSrc(
        tables=["app.users"],
        start_from=BinlogPosition(file="bin.000001", pos=4),
    )
    assert isinstance(src.start_from, BinlogPosition)


def test_mysql_cdc_stream_src_accepts_timestamp_start_from():
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    src = MySQLCdcStreamSrc(
        tables=["app.users"],
        start_from=TimestampPosition(ts=ts),
    )
    assert isinstance(src.start_from, TimestampPosition)
    assert src.start_from.ts == ts


def test_mysql_cdc_stream_src_initial_load_defaults_to_false():
    src = MySQLCdcStreamSrc(tables=["app.users"], start_from="tail")
    assert src.initial_load is False


def test_mysql_cdc_stream_src_accepts_initial_load_without_start_from():
    src = MySQLCdcStreamSrc(tables=["app.users"], initial_load=True)
    assert src.initial_load is True
    assert src.start_from is None


def test_mysql_cdc_stream_src_rejects_initial_load_with_start_from():
    with pytest.raises(ConnInitException) as ei:
        MySQLCdcStreamSrc(tables=["app.users"], start_from="tail", initial_load=True)
    assert ei.value.api_error is MysqlErrorCode.MYSQL_11


def test_mysql_cdc_stream_src_requires_start_from_or_initial_load():
    with pytest.raises(ConnInitException) as ei:
        MySQLCdcStreamSrc(tables=["app.users"])
    assert ei.value.api_error is MysqlErrorCode.MYSQL_11


def test_mysql_cdc_stream_src_rejects_unknown_src_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        MySQLCdcStreamSrc(
            tables=["app.users"],
            start_from="tail",
            src_cfg={"bogus.key": 1},  # type: ignore[dict-item]
        )
    assert ei.value.api_error is MysqlErrorCode.MYSQL_11


def test_mysql_cdc_stream_src_accepts_known_src_cfg_keys():
    src = MySQLCdcStreamSrc(
        tables=["app.users"],
        start_from="head",
        src_cfg={
            "tabsdata.cdc.poll.max_secs": 5,
            "mysql.cdc.logging_level": 20,
            "tabsdata.cdc.buffer.max_rows": 1000,
            "tabsdata.cdc.poll.idle_secs": 2,
        },
    )
    assert src.src_cfg["tabsdata.cdc.buffer.max_rows"] == 1000
    assert src.src_cfg["mysql.cdc.logging_level"] == 20


def test_mysql_cdc_stream_src_json_round_trip():
    src = MySQLCdcStreamSrc(
        tables=["app.users", "app.orders"],
        start_from=GtidPosition(gtid="uuid-x:42"),
        columns_format="struct",
        flatten_columns=False,
    )
    loaded = MySQLCdcStreamSrc.from_json(src.to_json())
    assert loaded == src


# ---------------------------------------------------------------------------
# Conn / Src / Dest init validation error codes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", [MySQLSrcConn, MySQLDestConn])
def test_mysql_conn_rejects_missing_uri(cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        cls(  # type: ignore[call-arg]
            credentials=UserPassword(
                user=Secret("etl"),
                password=Secret("hunter2"),
            ),
        )
    assert ei.value.api_error is MysqlErrorCode.MYSQL_1


@pytest.mark.parametrize("cls", [MySQLSrcConn, MySQLDestConn])
def test_mysql_conn_rejects_missing_credentials(cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        cls(uri="mysql://h:3306/d")  # type: ignore[call-arg]
    assert ei.value.api_error is MysqlErrorCode.MYSQL_1
