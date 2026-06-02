from __future__ import annotations

import typing

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.error import TabsdataException
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import CfgSpec, UserPassword
from tabsdatak.conn.db2 import (
    CommitSeqPosition,
    CredentialsSpec,
    Db2CdcStreamSrc,
    Db2CdcStreamSrcConn,
    Db2Dest,
    Db2DestConn,
    Db2Src,
    Db2SrcConn,
    Db2TableNamesSpec,
    DestCfgSpec,
    DestConnSpec,
    IfTableExistsSpec,
    QueriesSpec,
    SrcCfgSpec,
    SrcConnSpec,
    TableCommitSeqPosition,
    TimestampPosition,
    UriSpec,
)
# noinspection PyProtectedMember
from tabsdatak.conn.db2.error import (
    Db2ValidateException,
    Db2ErrorCode,
)


pytestmark = pytest.mark.db2


def _src_conn() -> Db2SrcConn:
    return Db2SrcConn(
        uri="db2://db2.example.com:50000/SAMPLE",
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        conn_cfg={"db2.timeout": 30},
    )


def _dest_conn() -> Db2DestConn:
    return Db2DestConn(
        uri="db2://db2.example.com:50000/SAMPLE",
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        conn_cfg={"db2.timeout": 30},
    )


# ---------------------------------------------------------------------------
# Db2SrcConn / Db2DestConn
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", [Db2SrcConn, Db2DestConn])
def test_db2_conn_round_trip(cls):
    c = cls(
        uri="db2://db2.example.com:50000/SAMPLE",
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        conn_cfg={"db2.timeout": 30},
    )
    loaded = cls.from_json(c.to_json())
    assert loaded == c
    assert loaded.uri == "db2://db2.example.com:50000/SAMPLE"
    assert loaded.conn_cfg == {"db2.timeout": 30}


@pytest.mark.parametrize("cls", [Db2SrcConn, Db2DestConn])
def test_db2_conn_rejects_bad_uri(cls):
    with pytest.raises(TabsdataException):
        cls(
            uri="ftp://host/db",
            credentials=UserPassword(
                user=Secret.lit("u"),
                password=Secret.lit("p"),
            ),
        )


def test_db2_src_dest_conn_have_distinct_kinds():
    assert Db2SrcConn.__kind__ != Db2DestConn.__kind__
    assert "Src" in Db2SrcConn.__kind__
    assert "Dest" in Db2DestConn.__kind__


# ---------------------------------------------------------------------------
# Db2Src
# ---------------------------------------------------------------------------


def test_db2_src_full_round_trip():
    src = Db2Src(
        conn=_src_conn(),
        queries=["SELECT * FROM USERS", "SELECT * FROM ORDERS"],
        transactional=False,
        schema_overrides=[None, None],
        initial_values={"x": 1},
        src_cfg={"db2.logging_level": 20},
    )
    loaded = Db2Src.from_json(src.to_json())
    assert loaded == src
    assert loaded.queries == ["SELECT * FROM USERS", "SELECT * FROM ORDERS"]
    assert loaded.transactional is False
    assert loaded.schema_overrides == [None, None]
    assert loaded.initial_values == {"x": 1}
    assert loaded.src_cfg == {"db2.logging_level": 20}


def test_db2_src_round_trip_with_named_conn():
    src = Db2Src(
        conn=NamedConn(name="prod_db2"),
        queries=["SELECT 1 FROM SYSIBM.SYSDUMMY1"],
    )
    loaded = Db2Src.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_db2"


def test_db2_src_validate_rejects_schema_overrides_length_mismatch():
    with pytest.raises(Db2ValidateException) as ei:
        Db2Src(
            conn=_src_conn(),
            queries=[
                "SELECT 1 FROM SYSIBM.SYSDUMMY1",
                "SELECT 2 FROM SYSIBM.SYSDUMMY1",
            ],
            schema_overrides=[None],
        )
    assert ei.value.api_error is Db2ErrorCode.DB2_3


def test_db2_src_validate_accepts_matching_schema_overrides_length():
    src = Db2Src(
        conn=_src_conn(),
        queries=[
            "SELECT 1 FROM SYSIBM.SYSDUMMY1",
            "SELECT 2 FROM SYSIBM.SYSDUMMY1",
        ],
        schema_overrides=[None, None],
    )
    assert len(src.schema_overrides) == len(src.queries)


# ---------------------------------------------------------------------------
# Db2Dest
# ---------------------------------------------------------------------------


def test_db2_dest_full_round_trip():
    dest = Db2Dest(
        conn=_dest_conn(),
        tables=["ANALYTICS.USERS", "ANALYTICS.ORDERS"],
        if_table_exists="replace",
        transactional=False,
        dest_cfg={"db2.logging_level": 20},
    )
    loaded = Db2Dest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["ANALYTICS.USERS", "ANALYTICS.ORDERS"]
    assert loaded.if_table_exists == "replace"
    assert loaded.transactional is False
    assert loaded.dest_cfg == {"db2.logging_level": 20}


def test_db2_dest_round_trip_with_named_conn():
    dest = Db2Dest(
        conn=NamedConn(name="prod_db2"),
        tables=["ANALYTICS.USERS"],
    )
    loaded = Db2Dest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_db2"


def test_db2_dest_defaults_if_table_exists_to_append():
    dest = Db2Dest(conn=_dest_conn(), tables=["ANALYTICS.USERS"])
    assert dest.if_table_exists == "append"
    assert dest.transactional is True
    assert dest.dest_cfg is None


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


@pytest.mark.parametrize("cls", [Db2SrcConn, Db2DestConn])
def test_db2_conn_fields_wire_expected_specs(cls):
    assert _hint(cls, "uri") == UriSpec
    assert _hint(cls, "credentials") == CredentialsSpec
    assert _hint(cls, "conn_cfg") == CfgSpec | None


def test_db2_src_fields_wire_expected_specs():
    assert _hint(Db2Src, "conn") == SrcConnSpec
    assert _hint(Db2Src, "queries") == QueriesSpec
    assert _hint(Db2Src, "transactional") is bool
    assert _hint(Db2Src, "schema_overrides") == SchemaOverrideSpec | None
    assert _hint(Db2Src, "src_cfg") == SrcCfgSpec | None


def test_db2_dest_fields_wire_expected_specs():
    assert _hint(Db2Dest, "conn") == DestConnSpec
    assert _hint(Db2Dest, "tables") == Db2TableNamesSpec
    assert _hint(Db2Dest, "if_table_exists") == IfTableExistsSpec
    assert _hint(Db2Dest, "transactional") is bool
    assert _hint(Db2Dest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# src_cfg / dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_db2_src_rejects_unknown_src_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        Db2Src(
            conn=_src_conn(),
            queries=["SELECT 1"],
            src_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is Db2ErrorCode.DB2_2


def test_db2_src_accepts_known_src_cfg_keys():
    src = Db2Src(
        conn=_src_conn(),
        queries=["SELECT 1"],
        src_cfg={
            "db2.logging_level": 20,
            "tabsdata.sql.sql.readEngine": "polarsSqlAlchemyRead",
            "tabsdata.sql.chunkSize": 100,
        },
    )
    loaded = Db2Src.from_json(src.to_json())
    assert loaded == src


def test_db2_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        Db2Dest(
            conn=_dest_conn(),
            tables=["analytics.users"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is Db2ErrorCode.DB2_4


def test_db2_dest_accepts_known_dest_cfg_keys():
    dest = Db2Dest(
        conn=_dest_conn(),
        tables=["analytics.users"],
        dest_cfg={
            "db2.logging_level": 20,
            "tabsdata.sql.sql.writeEngine": "polarsSqlAlchemyWrite",
            "tabsdata.sql.chunkSize": 100,
        },
    )
    loaded = Db2Dest.from_json(dest.to_json())
    assert loaded == dest


# =====================================================================
# CDC public primitives -- CommitSeqPosition / TableCommitSeqPosition /
# TimestampPosition / Db2CdcStreamSrcConn / Db2CdcStreamSrc
# =====================================================================
#
# These types live on the public db2 surface (``__init__.py``)
# so users build CDC sources without reaching into private
# modules. The plugin (``_cdc_plugin.py``) converts ``columns_format``
# / ``flatten_columns`` / the db2-local ``TimestampPosition`` onto
# the common runner types -- see ``test__cdc_plugin.py`` for that
# conversion.

from datetime import datetime, timezone  # noqa: E402


def _cdc_creds():
    return UserPassword(user=Secret.lit("u"), password=Secret.lit("p"))


# --- Position primitives --------------------------------------------


def test_commitseq_position_round_trip():
    p = CommitSeqPosition(seq="00" * 16)
    assert p.seq == "00" * 16


def test_commitseq_position_rejects_non_string():
    with pytest.raises(ConnInitException) as ei:
        CommitSeqPosition(seq=42)  # type: ignore[arg-type]
    assert ei.value.api_error is Db2ErrorCode.DB2_12


def test_table_commitseq_position_round_trip():
    p = TableCommitSeqPosition(
        seqs={"CDSCHEMA.CDTABLE1": "ab" * 16, "CDSCHEMA.CDTABLE2": "cd" * 16},
    )
    assert p.seqs["CDSCHEMA.CDTABLE1"] == "ab" * 16
    assert p.seqs["CDSCHEMA.CDTABLE2"] == "cd" * 16


def test_table_commitseq_position_rejects_non_string_value():
    with pytest.raises(ConnInitException) as ei:
        TableCommitSeqPosition(seqs={"CDSCHEMA.CDTABLE": 42})  # type: ignore[dict-item]
    assert ei.value.api_error is Db2ErrorCode.DB2_13


def test_timestamp_position_round_trip():
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    p = TimestampPosition(ts=ts)
    assert p.ts == ts


def test_cdc_typing_classes_are_db2_local_not_common_reexports():
    # Pins the boundary: the user-visible CDC dataclasses must be
    # db2-local so a knob added on a future Oracle CDC source
    # cannot accidentally widen the db2 schema.
    from tabsdatak.conn.common.sql.cdc.typing import (
        TimestampPosition as _CommonTimestampPosition,
    )

    assert TimestampPosition is not _CommonTimestampPosition


# --- Db2CdcStreamSrcConn -----------------------------------------------------


def test_db2_cdc_conn_minimal_construction():
    conn = Db2CdcStreamSrcConn(
        uri="db2+ibm_db://host:50000/sample",
        credentials=_cdc_creds(),
    )
    assert conn.uri == "db2+ibm_db://host:50000/sample"


def test_db2_cdc_conn_is_not_a_db2_conn_subclass():
    # Db2CdcStreamSrcConn deliberately does not subclass the batch Db2 conns.
    assert not issubclass(Db2CdcStreamSrcConn, Db2SrcConn)
    assert not issubclass(Db2CdcStreamSrcConn, Db2DestConn)
    assert not issubclass(Db2SrcConn, Db2CdcStreamSrcConn)
    assert not issubclass(Db2DestConn, Db2CdcStreamSrcConn)


# --- Db2CdcStreamSrc ------------------------------------------------


def _cdc_stream_src(**overrides) -> Db2CdcStreamSrc:
    kw = dict(
        conn=Db2CdcStreamSrcConn(
            uri="db2+ibm_db://h:50000/sample",
            credentials=_cdc_creds(),
        ),
        tables=["CDSCHEMA.USERS_CD"],
        start_from="tail",
    )
    kw.update(overrides)
    return Db2CdcStreamSrc(**kw)


def test_db2_cdc_stream_src_minimal_construction():
    src = _cdc_stream_src()
    assert src.tables == ["CDSCHEMA.USERS_CD"]
    assert src.start_from == "tail"
    assert src.columns_format == "columns"
    assert src.flatten_columns is True


def test_db2_cdc_stream_src_accepts_explicit_format_fields():
    src = _cdc_stream_src(columns_format="map", flatten_columns=False)
    assert src.columns_format == "map"
    assert src.flatten_columns is False


def test_db2_cdc_stream_src_rejects_unknown_columns_format():
    with pytest.raises(ConnInitException) as ei:
        _cdc_stream_src(columns_format="bogus")  # type: ignore[arg-type]
    assert ei.value.api_error is Db2ErrorCode.DB2_11


def test_db2_cdc_stream_src_accepts_commitseq_start_from():
    src = _cdc_stream_src(start_from=CommitSeqPosition(seq="ab" * 16))
    assert isinstance(src.start_from, CommitSeqPosition)


def test_db2_cdc_stream_src_accepts_table_commitseq_start_from():
    src = _cdc_stream_src(
        start_from=TableCommitSeqPosition(
            seqs={"CDSCHEMA.USERS_CD": "ab" * 16},
        ),
    )
    assert isinstance(src.start_from, TableCommitSeqPosition)


def test_db2_cdc_stream_src_accepts_timestamp_start_from():
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    src = _cdc_stream_src(start_from=TimestampPosition(ts=ts))
    assert isinstance(src.start_from, TimestampPosition)
    assert src.start_from.ts == ts


def test_db2_cdc_stream_src_rejects_unknown_src_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        _cdc_stream_src(src_cfg={"bogus.key": 1})  # type: ignore[dict-item]
    assert ei.value.api_error is Db2ErrorCode.DB2_11


def test_db2_cdc_stream_src_accepts_known_src_cfg_keys():
    src = _cdc_stream_src(
        src_cfg={
            "db2.cdc.blocking_timeout_seconds": 5,
            "db2.cdc.fetch_size": 5000,
            "db2.cdc.logging_level": 20,
            "tabsdata.cdc.buffer.max_rows": 1000,
            "tabsdata.cdc.poll_interval_seconds": 2,
        },
    )
    assert src.src_cfg["tabsdata.cdc.buffer.max_rows"] == 1000
    assert src.src_cfg["db2.cdc.logging_level"] == 20


def test_db2_cdc_stream_src_json_round_trip():
    src = _cdc_stream_src(
        tables=["CDSCHEMA.USERS_CD", "CDSCHEMA.ORDERS_CD"],
        start_from=CommitSeqPosition(seq="ab" * 16),
        columns_format="struct",
        flatten_columns=False,
    )
    loaded = Db2CdcStreamSrc.from_json(src.to_json())
    assert loaded == src


# ---------------------------------------------------------------------------
# Db2Conn init validation error code
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", [Db2SrcConn, Db2DestConn])
def test_db2_conn_rejects_missing_uri(cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        cls(  # type: ignore[call-arg]
            credentials=UserPassword(
                user=Secret.lit("u"),
                password=Secret.lit("p"),
            ),
        )
    assert ei.value.api_error is Db2ErrorCode.DB2_1


@pytest.mark.parametrize("cls", [Db2SrcConn, Db2DestConn])
def test_db2_conn_rejects_missing_credentials(cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        cls(uri="db2+ibm_db://h:50000/db")  # type: ignore[call-arg]
    assert ei.value.api_error is Db2ErrorCode.DB2_1
