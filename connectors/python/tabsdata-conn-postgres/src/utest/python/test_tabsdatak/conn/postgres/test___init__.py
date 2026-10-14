#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import typing

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import CfgSpec, UserPassword
from tabsdatak.conn.postgres import (
    CredentialsSpec,
    DestCfgSpec,
    IfTableExistsSpec,
    LsnPosition,
    PostgresCdcStreamSrc,
    PostgresCdcStreamSrcConn,
    PostgresDest,
    PostgresDestConn,
    PostgresSrc,
    PostgresSrcConn,
    PostgresTableNamesSpec,
    QueriesSpec,
    SrcCfgSpec,
    UriSpec,
)

# noinspection PyProtectedMember
from tabsdatak.conn.postgres.error import (
    PostgresErrorCode,
    PostgresValidateException,
)
from tabsdatak.error import TabsdataException

pytestmark = pytest.mark.postgres


# ---------------------------------------------------------------------------
# PostgresConn
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", [PostgresSrcConn, PostgresDestConn])
def test_postgres_conn_round_trip(cls):
    c = cls(
        uri="postgresql://postgres.example.com:5432/analytics",
        credentials=UserPassword(
            user=Secret("etl"),
            password=Secret("hunter2"),
        ),
        conn_cfg={"postgres.timeout": 30},
    )
    loaded = cls.from_json(c.to_json())
    # Serializing always dehydrates, so secret values do not survive the
    # round-trip -- the credentials come back dehydrated.
    assert loaded.credentials == UserPassword(user=Secret(), password=Secret())
    assert loaded.uri == "postgresql://postgres.example.com:5432/analytics"
    assert loaded.conn_cfg == {"postgres.timeout": 30}


def test_postgres_src_dest_conn_are_distinct_classes():
    # Source and destination use distinct connection classes (told apart
    # on the wire by the envelope ``type``), though both now share the
    # uniform ``connectionDef`` kind.
    assert PostgresSrcConn is not PostgresDestConn
    assert PostgresSrcConn.__kind__ == PostgresDestConn.__kind__ == "connectionDef"


@pytest.mark.parametrize("cls", [PostgresSrcConn, PostgresDestConn])
def test_postgres_conn_rejects_bad_uri(cls):
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


# ---------------------------------------------------------------------------
# PostgresSrc
# ---------------------------------------------------------------------------


def test_postgres_src_full_round_trip():
    src = PostgresSrc(
        queries=["SELECT * FROM users", "SELECT * FROM orders"],
        transactional=False,
        schema_overrides=[None, None],
        initial_values={"x": 1},
        src_cfg={"postgres.logging_level": 20},
    )
    loaded = PostgresSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.queries == ["SELECT * FROM users", "SELECT * FROM orders"]
    assert loaded.transactional is False
    assert loaded.schema_overrides == [None, None]
    assert loaded.initial_values == {"x": 1}
    assert loaded.src_cfg == {"postgres.logging_level": 20}


def test_postgres_src_validate_rejects_schema_overrides_length_mismatch():
    with pytest.raises(PostgresValidateException) as ei:
        PostgresSrc(
            queries=["SELECT 1", "SELECT 2"],
            schema_overrides=[None],
        )
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_3


def test_postgres_src_validate_accepts_matching_schema_overrides_length():
    src = PostgresSrc(
        queries=["SELECT 1", "SELECT 2"],
        schema_overrides=[None, None],
    )
    assert len(src.schema_overrides) == len(src.queries)


# ---------------------------------------------------------------------------
# PostgresDest
# ---------------------------------------------------------------------------


def test_postgres_dest_full_round_trip():
    dest = PostgresDest(
        tables=["analytics.users", "analytics.orders"],
        if_table_exists="replace",
        transactional=False,
        dest_cfg={"postgres.logging_level": 20},
    )
    loaded = PostgresDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["analytics.users", "analytics.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.transactional is False
    assert loaded.dest_cfg == {"postgres.logging_level": 20}


def test_postgres_dest_defaults_if_table_exists_to_append():
    dest = PostgresDest(tables=["analytics.users"])
    assert dest.if_table_exists == "append"
    assert dest.transactional is True
    assert dest.dest_cfg is None


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


@pytest.mark.parametrize("cls", [PostgresSrcConn, PostgresDestConn])
def test_postgres_conn_fields_wire_expected_specs(cls):
    assert _hint(cls, "uri") == UriSpec
    assert _hint(cls, "credentials") == CredentialsSpec
    assert _hint(cls, "conn_cfg") == CfgSpec | None


def test_postgres_src_fields_wire_expected_specs():
    assert _hint(PostgresSrc, "queries") == QueriesSpec
    assert _hint(PostgresSrc, "transactional") is bool
    assert _hint(PostgresSrc, "schema_overrides") == SchemaOverrideSpec | None
    assert _hint(PostgresSrc, "src_cfg") == SrcCfgSpec | None


def test_postgres_dest_fields_wire_expected_specs():
    assert _hint(PostgresDest, "tables") == PostgresTableNamesSpec
    assert _hint(PostgresDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(PostgresDest, "transactional") is bool
    assert _hint(PostgresDest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# src_cfg / dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_postgres_src_rejects_unknown_src_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        PostgresSrc(
            queries=["SELECT 1"],
            src_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_2


def test_postgres_src_accepts_known_src_cfg_keys():
    src = PostgresSrc(
        queries=["SELECT 1"],
        src_cfg={
            "postgres.logging_level": 20,
            "tabsdata.sql.read_engine": "polarsSqlAlchemyRead",
            "tabsdata.sql.chunk_size": 100,
        },
    )
    loaded = PostgresSrc.from_json(src.to_json())
    assert loaded == src


def test_postgres_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        PostgresDest(
            tables=["analytics.users"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_4


def test_postgres_dest_accepts_known_dest_cfg_keys():
    dest = PostgresDest(
        tables=["analytics.users"],
        dest_cfg={
            "postgres.logging_level": 20,
            "tabsdata.sql.write_engine": "polarsSqlAlchemyWrite",
            "tabsdata.sql.chunk_size": 100,
        },
    )
    loaded = PostgresDest.from_json(dest.to_json())
    assert loaded == dest


# =====================================================================
# CDC public primitives -- LsnPosition / PostgresCdcStreamSrcConn /
# PostgresCdcStreamSrc
# =====================================================================
#
# These types live on the public postgres surface (``__init__.py``)
# so users build CDC sources without reaching into private
# modules. The plugin (``_cdc_plugin.py``) converts ``columns_format``
# / ``flatten_columns`` onto the common runner ``CdcFormat`` -- see
# ``test__cdc_plugin.py`` for that conversion.


def _cdc_creds():
    return UserPassword(user=Secret("u"), password=Secret("p"))


# --- LsnPosition ----------------------------------------------------


def test_lsn_position_round_trip():
    p = LsnPosition(lsn=12345)
    assert p.lsn == 12345


def test_lsn_position_rejects_non_int():
    with pytest.raises(ConnInitException) as ei:
        LsnPosition(lsn="abc")  # type: ignore[arg-type]
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_12


def test_lsn_position_negative_is_lenient():
    # ``int`` accepts negatives; semantically meaningless for an
    # LSN but the validator does not enforce that.
    p = LsnPosition(lsn=-1)
    assert p.lsn == -1


# --- PostgresCdcStreamSrcConn ------------------------------------------------


def test_postgres_cdc_conn_minimal_construction():
    conn = PostgresCdcStreamSrcConn(
        uri="postgresql://host:5432/db",
        credentials=_cdc_creds(),
    )
    assert conn.uri == "postgresql://host:5432/db"


def test_postgres_cdc_conn_is_not_a_postgres_conn_subclass():
    # PostgresCdcStreamSrcConn deliberately does not subclass the batch
    # SQL conns; siblings with overlapping fields.
    assert not issubclass(PostgresCdcStreamSrcConn, PostgresSrcConn)
    assert not issubclass(PostgresCdcStreamSrcConn, PostgresDestConn)
    assert not issubclass(PostgresSrcConn, PostgresCdcStreamSrcConn)
    assert not issubclass(PostgresDestConn, PostgresCdcStreamSrcConn)


# --- PostgresCdcStreamSrc -------------------------------------------


def _cdc_stream_src(**overrides) -> PostgresCdcStreamSrc:
    kw = dict(
        tables=["public.users"],
        replication_slot="tdcdc_slot",
        start_from="tail",
    )
    kw.update(overrides)
    return PostgresCdcStreamSrc(**kw)


def test_postgres_cdc_stream_src_minimal_construction():
    src = _cdc_stream_src()
    assert src.tables == ["public.users"]
    assert src.replication_slot == "tdcdc_slot"
    assert src.start_from == "tail"
    # Flat format-field defaults.
    assert src.columns_format == "columns"
    assert src.flatten_columns is True
    # Slot behavior default.
    assert src.replication_slot_behavior == "reuse"
    assert src.publication_name is None


def test_postgres_cdc_stream_src_accepts_explicit_format_fields():
    src = _cdc_stream_src(
        columns_format="map",
        flatten_columns=False,
    )
    assert src.columns_format == "map"
    assert src.flatten_columns is False


def test_postgres_cdc_stream_src_rejects_unknown_columns_format():
    with pytest.raises(ConnInitException) as ei:
        _cdc_stream_src(columns_format="bogus")  # type: ignore[arg-type]
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_11


def test_postgres_cdc_stream_src_rejects_unknown_slot_behavior():
    with pytest.raises(ConnInitException) as ei:
        _cdc_stream_src(replication_slot_behavior="bogus")  # type: ignore[arg-type]
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_11


def test_postgres_cdc_stream_src_accepts_lsn_start_from():
    src = _cdc_stream_src(start_from=LsnPosition(lsn=42))
    assert isinstance(src.start_from, LsnPosition)
    assert src.start_from.lsn == 42


def test_postgres_cdc_stream_src_accepts_head_and_tail_start_from():
    head = _cdc_stream_src(start_from="head")
    assert head.start_from == "head"
    tail = _cdc_stream_src(start_from="tail")
    assert tail.start_from == "tail"


def test_postgres_cdc_stream_src_accepts_a_full_start():
    assert _cdc_stream_src(start_from="full").start_from == "full"


@pytest.mark.parametrize("missing", [{"start_from": None}, {}], ids=["none", "omitted"])
def test_postgres_cdc_stream_src_requires_start_from(missing):
    kw = {"tables": ["public.users"], "replication_slot": "tdcdc_slot", **missing}
    with pytest.raises(ConnInitException) as ei:
        PostgresCdcStreamSrc(**kw)
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_11


def test_postgres_cdc_stream_src_accepts_publication_name():
    src = _cdc_stream_src(publication_name="pub_users")
    assert src.publication_name == "pub_users"


def test_postgres_cdc_stream_src_rejects_unknown_src_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        _cdc_stream_src(src_cfg={"bogus.key": 1})  # type: ignore[dict-item]
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_11


def test_postgres_cdc_stream_src_accepts_known_src_cfg_keys():
    src = _cdc_stream_src(
        src_cfg={
            "tabsdata.cdc.poll.max_secs": 5,
            "postgres.cdc.logging_level": 20,
            "postgres.cdc.wal2json_format_version": 2,
            "tabsdata.cdc.buffer.max_rows": 1000,
            "tabsdata.cdc.poll.idle_secs": 2,
        },
    )
    assert src.src_cfg["tabsdata.cdc.buffer.max_rows"] == 1000
    assert src.src_cfg["postgres.cdc.logging_level"] == 20


def test_postgres_cdc_stream_src_json_round_trip():
    src = _cdc_stream_src(
        tables=["public.users", "public.orders"],
        start_from=LsnPosition(lsn=12345),
        columns_format="struct",
        flatten_columns=False,
        publication_name="pub_users",
        replication_slot_behavior="create",
    )
    loaded = PostgresCdcStreamSrc.from_json(src.to_json())
    assert loaded == src


# ---------------------------------------------------------------------------
# PostgresConn init validation error code
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", [PostgresSrcConn, PostgresDestConn])
def test_postgres_conn_rejects_missing_uri(cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        cls(  # type: ignore[call-arg]
            credentials=UserPassword(
                user=Secret("u"),
                password=Secret("p"),
            ),
        )
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_1


@pytest.mark.parametrize("cls", [PostgresSrcConn, PostgresDestConn])
def test_postgres_conn_rejects_missing_credentials(cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        cls(uri="postgresql://h:5432/d")  # type: ignore[call-arg]
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_1
