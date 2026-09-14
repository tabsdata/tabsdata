#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import typing

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.sql.error import (
    SqlCommonErrorCode,
    SqlCommonValidateException,
)
from tabsdatak.conn.common.types import CfgSpec, UserPassword

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge import MergeCfg as SqlMergeCfg
from tabsdatak.conn.snowflake import (
    AccountSpec,
    CredentialsSpec,
    DestCfgSpec,
    IdentifierSpec,
    IfTableExistsSpec,
    MergeCfg,
    MergeCfgSpec,
    MergeColumnSpec,
    MergeColumnsSpec,
    NameSpec,
    SchemaEvolutionSpec,
    SnowflakeDestConn,
    SnowflakeDest,
    SnowflakeTableNamesSpec,
)
from tabsdatak.conn.snowflake.error import (
    SnowflakeErrorCode,
    SnowflakeValidateException,
)


pytestmark = pytest.mark.snowflake


# ---------------------------------------------------------------------------
# SnowflakeDestConn / SnowflakeDest round-trip
# ---------------------------------------------------------------------------


def test_snowflake_dest_full_round_trip():
    dest = SnowflakeDest(
        tables=["ANALYTICS.PUBLIC.USERS", "ANALYTICS.PUBLIC.ORDERS"],
        if_table_exists="replace",
        dest_cfg={"snowflake.logging_level": 20},
    )
    loaded = SnowflakeDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["ANALYTICS.PUBLIC.USERS", "ANALYTICS.PUBLIC.ORDERS"]
    assert loaded.if_table_exists == "replace"
    assert loaded.dest_cfg == {"snowflake.logging_level": 20}


def test_snowflake_conn_validate_rejects_schema_without_database():
    with pytest.raises(SnowflakeValidateException) as ei:
        SnowflakeDestConn(
            account="ACME-MAIN",
            credentials=UserPassword(
                user=Secret("etl"),
                password=Secret("hunter2"),
            ),
            schema="PUBLIC",
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_2


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


def test_snowflake_conn_fields_wire_expected_specs():
    assert _hint(SnowflakeDestConn, "account") == AccountSpec
    assert _hint(SnowflakeDestConn, "credentials") == CredentialsSpec
    assert _hint(SnowflakeDestConn, "role") == NameSpec | None
    assert _hint(SnowflakeDestConn, "database") == NameSpec | None
    assert _hint(SnowflakeDestConn, "schema") == NameSpec | None
    assert _hint(SnowflakeDestConn, "warehouse") == NameSpec | None
    assert _hint(SnowflakeDestConn, "stage") == IdentifierSpec | None
    assert _hint(SnowflakeDestConn, "conn_cfg") == CfgSpec | None


def test_snowflake_dest_fields_wire_expected_specs():
    assert _hint(SnowflakeDest, "tables") == SnowflakeTableNamesSpec
    assert _hint(SnowflakeDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(SnowflakeDest, "schema_evolution") == SchemaEvolutionSpec
    assert _hint(SnowflakeDest, "merge_cfg") == MergeCfgSpec | None
    assert _hint(SnowflakeDest, "dest_cfg") == DestCfgSpec | None


def test_merge_cfg_fields_wire_expected_specs():
    assert _hint(MergeCfg, "on") == MergeColumnsSpec
    assert _hint(MergeCfg, "op_column") == MergeColumnSpec


# ---------------------------------------------------------------------------
# dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_snowflake_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        SnowflakeDest(
            tables=["analytics.users"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_3


def test_snowflake_dest_accepts_known_dest_cfg_keys():
    dest = SnowflakeDest(
        tables=["analytics.users"],
        dest_cfg={
            "snowflake.logging_level": 20,
            "snowflake.schema_evolution_engine": "iceberg",
        },
    )
    loaded = SnowflakeDest.from_json(dest.to_json())
    assert loaded == dest


# ---------------------------------------------------------------------------
# schema_evolution_engine
#
# A MERGE does not read ENABLE_SCHEMA_EVOLUTION, so the only evolution it can
# have is the Iceberg pass -- which the write has to ask for rather than get by
# default. Append and replace keep native evolution unless they opt out.
# ---------------------------------------------------------------------------


def test_snowflake_dest_requires_iceberg_for_a_merge():
    with pytest.raises(SnowflakeValidateException) as ei:
        SnowflakeDest(
            tables=["DB.S.T"],
            if_table_exists="merge",
            merge_cfg=_merge_cfg(),
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_22


def test_snowflake_dest_rejects_native_evolution_for_a_merge():
    with pytest.raises(SnowflakeValidateException) as ei:
        SnowflakeDest(
            tables=["DB.S.T"],
            if_table_exists="merge",
            merge_cfg=_merge_cfg(),
            dest_cfg={"snowflake.schema_evolution_engine": "native"},
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_22


def test_a_missing_merge_cfg_is_reported_before_the_engine():
    """Both are wrong here; the merge_cfg is the more basic thing to fix, and
    the engine flag would read as noise beside it."""
    with pytest.raises(SnowflakeValidateException) as ei:
        SnowflakeDest(tables=["DB.S.T"], if_table_exists="merge")
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_12


@pytest.mark.parametrize("if_table_exists", ["append", "replace"])
@pytest.mark.parametrize("engine", ["native", "iceberg", None])
def test_a_bulk_load_takes_any_engine(if_table_exists, engine):
    """Only a merge constrains the engine -- append and replace can do either,
    and unset means native."""
    dest = SnowflakeDest(
        tables=["DB.S.T"],
        if_table_exists=if_table_exists,
        dest_cfg=(
            None if engine is None else {"snowflake.schema_evolution_engine": engine}
        ),
    )
    assert dest.if_table_exists == if_table_exists


# ---------------------------------------------------------------------------
# SnowflakeDestConn init validation error code
# ---------------------------------------------------------------------------


def test_snowflake_conn_rejects_missing_account():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        SnowflakeDestConn(  # type: ignore[call-arg]
            credentials=UserPassword(
                user=Secret("u"),
                password=Secret("p"),
            ),
            database="DB",
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_1


def test_snowflake_conn_rejects_missing_credentials():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        SnowflakeDestConn(account="ACME", database="DB")  # type: ignore[call-arg]
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_1


# ---------------------------------------------------------------------------
# MergeCfg -- round trip, column folding
# ---------------------------------------------------------------------------


# A MERGE does not read ENABLE_SCHEMA_EVOLUTION, so the connector evolves its
# target with ALTER statements or not at all -- and the write has to say so.
_ICEBERG = {"snowflake.schema_evolution_engine": "iceberg"}


def _merge_cfg(**overrides) -> MergeCfg:
    kwargs: dict = dict(on=["order_id"], op_column="op")
    kwargs.update(overrides)
    return MergeCfg(**kwargs)


def test_merge_cfg_round_trip():
    cfg = _merge_cfg(
        op_column="op",
    )
    loaded = MergeCfg.from_json(cfg.to_json())
    assert loaded == cfg


def test_snowflake_dest_round_trips_a_merge():
    dest = SnowflakeDest(
        tables=["ANALYTICS.PUBLIC.ORDERS"],
        if_table_exists="merge",
        merge_cfg=_merge_cfg(op_column="op"),
        dest_cfg=_ICEBERG,
    )
    loaded = SnowflakeDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.merge_cfg.op_column == "op"


def test_snowflake_dest_round_trips_a_merge_cfg_per_table():
    dest = SnowflakeDest(
        tables=["DB.S.ORDERS", "DB.S.ITEMS"],
        if_table_exists="merge",
        merge_cfg=[_merge_cfg(), _merge_cfg(on=["item_id"])],
        dest_cfg=_ICEBERG,
    )
    loaded = SnowflakeDest.from_json(dest.to_json())
    assert loaded == dest
    assert [c.on for c in loaded.merge_cfg] == [["order_id"], ["item_id"]]


def test_merge_cfgs_broadcasts_a_single_cfg_to_every_table():
    cfg = _merge_cfg()
    dest = SnowflakeDest(
        tables=["DB.S.A", "DB.S.B", "DB.S.C"],
        if_table_exists="merge",
        merge_cfg=cfg,
        dest_cfg=_ICEBERG,
    )
    assert dest._merge_cfgs() == [cfg, cfg, cfg]


def test_merge_cfgs_is_none_outside_merge_mode():
    assert SnowflakeDest(tables=["DB.S.A"])._merge_cfgs() is None


def test_merge_cfg_folds_an_unquoted_column_the_way_snowflake_stores_it():
    # The connector templates its tables from ``infer_schema(ignore_case =>
    # true)``, which stores columns upper-folded whatever case the parquet
    # used -- so a lower-case name has to fold onto the upper-case column.
    cfg = _merge_cfg(op_column="op")
    assert cfg._keys() == ["ORDER_ID"]
    assert cfg._op_column() == "OP"


def test_merge_cfg_keeps_a_quoted_column_case_sensitive():
    assert _merge_cfg(on=['"myCol"'])._keys() == ["myCol"]


def test_merge_cfg_rejects_a_dotted_column():
    with pytest.raises(ConnInitException) as ei:
        _merge_cfg(on=["orders.id"])
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_11


def test_merge_cfg_rejects_an_empty_on():
    # Structural invariant, so it is the shared `MergeCfg` that reports it.
    with pytest.raises(SqlCommonValidateException) as ei:
        _merge_cfg(on=[])
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_5


def test_merge_cfg_is_the_shared_merge_cfg():
    """The Snowflake spelling of the shared descriptor: the structure and its
    invariants come from common-sql, the identifier folding from here."""
    assert issubclass(MergeCfg, SqlMergeCfg)
    assert MergeCfg.__kind__ == "snowflakeMergeCfg"



# ---------------------------------------------------------------------------
# SnowflakeDest -- merge_cfg and if_table_exists require each other
# ---------------------------------------------------------------------------


def test_snowflake_dest_rejects_merge_without_a_merge_cfg():
    with pytest.raises(SnowflakeValidateException) as ei:
        SnowflakeDest(tables=["DB.S.T"], if_table_exists="merge")
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_12


@pytest.mark.parametrize("if_table_exists", ["append", "replace"])
def test_snowflake_dest_rejects_a_merge_cfg_it_would_ignore(if_table_exists):
    with pytest.raises(SnowflakeValidateException) as ei:
        SnowflakeDest(
            tables=["DB.S.T"],
            if_table_exists=if_table_exists,
            merge_cfg=_merge_cfg(),
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_13
    assert repr(if_table_exists) in str(ei.value)


def test_snowflake_dest_rejects_a_merge_cfg_list_that_misses_a_table():
    with pytest.raises(SnowflakeValidateException) as ei:
        SnowflakeDest(
            tables=["DB.S.A", "DB.S.B"],
            if_table_exists="merge",
            merge_cfg=[_merge_cfg()],
        )
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_14


def test_snowflake_dest_rejects_an_unknown_if_table_exists():
    with pytest.raises(ConnInitException) as ei:
        SnowflakeDest(tables=["DB.S.T"], if_table_exists="upsert")
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_3


# ---------------------------------------------------------------------------
# schema_evolution
#
# Opt-in rather than opt-out: the target's schema is pinned by default, and
# "update" is what lets a widening frame carry its new columns onto it.
# Independent of the disposition either way.
# ---------------------------------------------------------------------------


def test_snowflake_dest_defaults_to_strict_schema_evolution():
    assert SnowflakeDest(tables=["DB.S.T"]).schema_evolution == "strict"


@pytest.mark.parametrize("schema_evolution", ["strict", "update"])
def test_snowflake_dest_round_trips_schema_evolution(schema_evolution):
    dest = SnowflakeDest(
        tables=["DB.S.T"], schema_evolution=schema_evolution
    )
    loaded = SnowflakeDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.schema_evolution == schema_evolution


def test_snowflake_dest_rejects_an_unknown_schema_evolution():
    with pytest.raises(ConnInitException) as ei:
        SnowflakeDest(tables=["DB.S.T"], schema_evolution="evolve")
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_3


@pytest.mark.parametrize("if_table_exists", ["append", "replace", "merge"])
def test_schema_evolution_is_independent_of_the_disposition(if_table_exists):
    # The two settings do not constrain each other: evolution says whether the
    # target's columns may change, the disposition says what happens to its rows.
    merge_cfg = MergeCfg(on=["id"], op_column="op") if if_table_exists == "merge" else None
    dest = SnowflakeDest(
        tables=["DB.S.T"],
        if_table_exists=if_table_exists,
        schema_evolution="update",
        merge_cfg=merge_cfg,
        dest_cfg=_ICEBERG if if_table_exists == "merge" else None,
    )
    assert dest.schema_evolution == "update"


# ---------------------------------------------------------------------------
# MergeCfg.op_column
#
# Without it the merge is a plain upsert. With it, each incoming row's marker
# decides which clause claims it.
# ---------------------------------------------------------------------------


def test_merge_cfg_folds_the_op_column_like_any_other():
    assert _merge_cfg(op_column="op")._op_column() == "OP"
    assert _merge_cfg(op_column='"opCol"')._op_column() == "opCol"


def test_merge_cfg_round_trips_the_op_column():
    cfg = _merge_cfg(op_column="op")
    assert MergeCfg.from_json(cfg.to_json()).op_column == "op"


def test_merge_cfg_rejects_a_dotted_op_column():
    with pytest.raises(ConnInitException) as ei:
        _merge_cfg(op_column="orders.op")
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_11


def test_merge_cfg_rejects_a_key_as_the_op_column():
    # A key is what rows are matched by; it cannot also carry the marker that
    # says what to do with the row it matched.
    with pytest.raises(SqlCommonValidateException) as ei:
        _merge_cfg(on=["order_id"], op_column="ORDER_ID")
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_6


def test_merge_cfg_rejects_a_key_as_the_op_column_across_case():
    with pytest.raises(SqlCommonValidateException) as ei:
        _merge_cfg(on=["order_id"], op_column="order_id")
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_6


def test_merge_cfg_rejects_a_quoted_key_as_the_op_column():
    """The collision check runs on folded names, so the Snowflake fold this
    subclass supplies still catches a marker that only differs by quoting."""
    with pytest.raises(SqlCommonValidateException) as ei:
        _merge_cfg(on=['"OP"'], op_column="op")
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_6


def test_merge_cfg_requires_an_op_column():
    """Every row is claimed by the clause its marker names, so there is no merge
    without the column that carries it."""
    with pytest.raises(ConnInitException) as ei:
        MergeCfg(on=["order_id"])
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_11
