#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import typing

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.bigquery import (
    BigQueryDestConn,
    BigQueryDest,
    BigQueryTableNamesSpec,
    CredentialsSpec,
    DatasetSpec,
    DestCfgSpec,
    IfTableExistsSpec,
    MergeCfg,
    MergeCfgSpec,
    MergeColumnSpec,
    MergeColumnsSpec,
    ProjectSpec,
    SchemaEvolutionSpec,
)
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.bigquery.error import (
    BigQueryValidateException,
    BigQueryErrorCode,
)
from tabsdatak.conn.bigquery import GCSLocation

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge import MergeCfg as SqlMergeCfg
from tabsdatak.conn.common.sql.error import (
    SqlCommonErrorCode,
    SqlCommonValidateException,
)
from tabsdatak.conn.common.types import CfgSpec, GCPJson


pytestmark = pytest.mark.bigquery


def _staging():
    return GCSLocation(
        bucket="staging-bucket",
        credentials=GCPJson(json=Secret()),
        base_path="/staging",
    )


def test_bigquery_dest_full_round_trip():
    dest = BigQueryDest(
        tables=["`my-project.analytics.users`", "`my-project.analytics.orders`"],
        if_table_exists="replace",
        schema_evolution="strict",
        dest_cfg={"bigquery.logging_level": 20},
    )
    loaded = BigQueryDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == [
        "`my-project.analytics.users`",
        "`my-project.analytics.orders`",
    ]
    assert loaded.if_table_exists == "replace"
    assert loaded.schema_evolution == "strict"
    assert loaded.dest_cfg == {"bigquery.logging_level": 20}


def test_bigquery_conn_validate_rejects_dataset_without_project():
    with pytest.raises(BigQueryValidateException) as ei:
        BigQueryDestConn(
            staging=_staging(),
            credentials=GCPJson(json=Secret()),
            dataset="analytics",
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_2


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


def test_bigquery_conn_fields_wire_expected_specs():
    assert _hint(BigQueryDestConn, "staging") == GCSLocation
    assert _hint(BigQueryDestConn, "credentials") == CredentialsSpec
    assert _hint(BigQueryDestConn, "project") == ProjectSpec | None
    assert _hint(BigQueryDestConn, "dataset") == DatasetSpec | None
    assert _hint(BigQueryDestConn, "conn_cfg") == CfgSpec | None


def test_bigquery_dest_fields_wire_expected_specs():
    assert _hint(BigQueryDest, "tables") == BigQueryTableNamesSpec
    assert _hint(BigQueryDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(BigQueryDest, "schema_evolution") == SchemaEvolutionSpec
    assert _hint(BigQueryDest, "merge_cfg") == MergeCfgSpec | None
    assert _hint(BigQueryDest, "dest_cfg") == DestCfgSpec | None


def test_bigquery_dest_defaults_to_strict_schema_evolution():
    """Opt-in rather than opt-out: the target's schema is pinned unless the
    flow asks for it to move."""
    assert BigQueryDest(tables=["`p.ds.t`"]).schema_evolution == "strict"


# ---------------------------------------------------------------------------
# dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_bigquery_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        BigQueryDest(
            tables=["`myproject.mydataset.users`"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_3


def test_bigquery_dest_accepts_known_dest_cfg_keys():
    dest = BigQueryDest(
        tables=["`myproject.mydataset.users`"],
        dest_cfg={"bigquery.logging_level": 20},
    )
    assert dest.dest_cfg == {"bigquery.logging_level": 20}


def test_bigquery_conn_rejects_missing_staging():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        BigQueryDestConn(  # type: ignore[call-arg]
            credentials=GCPJson(json=Secret()),
            project="my-project",
            dataset="analytics",
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_1


# ---------------------------------------------------------------------------
# MergeCfg -- the BigQuery spelling of the shared descriptor
# ---------------------------------------------------------------------------


def _merge_cfg(**overrides) -> MergeCfg:
    kwargs: dict = dict(on=["order_id"], op_column="op")
    kwargs.update(overrides)
    return MergeCfg(**kwargs)


def test_merge_cfg_is_the_shared_merge_cfg():
    """The structure and its invariants come from common-sql; only the column
    folding is BigQuery's."""
    assert issubclass(MergeCfg, SqlMergeCfg)
    assert MergeCfg.__kind__ == "bigQueryMergeCfg"


def test_merge_cfg_round_trip():
    cfg = _merge_cfg(on=["order_id", "customer_id"])
    assert MergeCfg.from_json(cfg.to_json()) == cfg


def test_merge_cfg_fields_wire_expected_specs():
    assert _hint(MergeCfg, "on") == MergeColumnsSpec
    assert _hint(MergeCfg, "op_column") == MergeColumnSpec


def test_merge_cfg_folds_columns_case_insensitively():
    # BigQuery resolves column names case-insensitively, so any spelling of a
    # name has to reach the same column.
    assert _merge_cfg(on=["ORDER_ID"])._keys() == ["order_id"]
    assert _merge_cfg(op_column="OP")._op_column() == "op"


def test_merge_cfg_strips_backticks_without_making_a_name_case_sensitive():
    """Unlike Snowflake, a quoted BigQuery name is not case-sensitive -- the
    backticks are only there for a flexible column name."""
    assert _merge_cfg(on=["`OrderId`"])._keys() == ["orderid"]
    assert _merge_cfg(on=["`weird col`"])._keys() == ["weird col"]


def test_merge_cfg_rejects_a_dotted_column():
    with pytest.raises(ConnInitException) as ei:
        _merge_cfg(on=["orders.id"])
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_11


def test_merge_cfg_rejects_a_dotted_op_column():
    with pytest.raises(ConnInitException) as ei:
        _merge_cfg(op_column="orders.op")
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_11


def test_merge_cfg_requires_an_op_column():
    with pytest.raises(ConnInitException) as ei:
        MergeCfg(on=["order_id"])
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_11


def test_merge_cfg_rejects_an_empty_on():
    # Structural invariant, so the shared MergeCfg reports it.
    with pytest.raises(SqlCommonValidateException) as ei:
        _merge_cfg(on=[])
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_5


def test_merge_cfg_rejects_a_key_as_the_op_column():
    with pytest.raises(SqlCommonValidateException) as ei:
        _merge_cfg(on=["order_id", "op"], op_column="op")
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_6


def test_merge_cfg_rejects_a_key_as_the_op_column_across_case():
    with pytest.raises(SqlCommonValidateException) as ei:
        _merge_cfg(on=["OP"], op_column="op")
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_6


def test_merge_cfg_rejects_a_quoted_key_as_the_op_column():
    """The collision check runs on folded names, so quoting cannot smuggle the
    marker past it."""
    with pytest.raises(SqlCommonValidateException) as ei:
        _merge_cfg(on=["`op`"], op_column="OP")
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_6


# ---------------------------------------------------------------------------
# BigQueryDest -- merge_cfg and if_table_exists require each other
# ---------------------------------------------------------------------------


def test_bigquery_dest_round_trips_a_merge():
    dest = BigQueryDest(
        tables=["`my-project.analytics.orders`"],
        if_table_exists="merge",
        merge_cfg=_merge_cfg(),
    )
    loaded = BigQueryDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.merge_cfg.op_column == "op"


def test_bigquery_dest_round_trips_a_merge_cfg_per_table():
    dest = BigQueryDest(
        tables=["`p.ds.orders`", "`p.ds.items`"],
        if_table_exists="merge",
        merge_cfg=[_merge_cfg(), _merge_cfg(on=["item_id"])],
    )
    loaded = BigQueryDest.from_json(dest.to_json())
    assert [c.on for c in loaded.merge_cfg] == [["order_id"], ["item_id"]]


def test_bigquery_dest_broadcasts_a_single_cfg_to_every_table():
    cfg = _merge_cfg()
    dest = BigQueryDest(
        tables=["`p.ds.a`", "`p.ds.b`", "`p.ds.c`"],
        if_table_exists="merge",
        merge_cfg=cfg,
    )
    assert dest._merge_cfgs() == [cfg, cfg, cfg]


def test_bigquery_dest_merge_cfgs_is_none_outside_merge_mode():
    assert BigQueryDest(tables=["`p.ds.a`"])._merge_cfgs() is None


def test_bigquery_dest_keeps_a_list_of_cfgs_in_order():
    first, second = _merge_cfg(), _merge_cfg(on=["item_id"])
    dest = BigQueryDest(
        tables=["`p.ds.a`", "`p.ds.b`"],
        if_table_exists="merge",
        merge_cfg=[first, second],
    )
    assert dest._merge_cfgs() == [first, second]


def test_bigquery_dest_rejects_merge_without_a_merge_cfg():
    with pytest.raises(BigQueryValidateException) as ei:
        BigQueryDest(tables=["`p.ds.a`"], if_table_exists="merge")
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_12
    assert "requires merge_cfg" in str(ei.value)


@pytest.mark.parametrize("if_table_exists", ["append", "replace"])
def test_bigquery_dest_rejects_a_merge_cfg_outside_merge_mode(if_table_exists):
    with pytest.raises(BigQueryValidateException) as ei:
        BigQueryDest(
            tables=["`p.ds.a`"],
            if_table_exists=if_table_exists,
            merge_cfg=_merge_cfg(),
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_13
    assert if_table_exists in str(ei.value)


def test_bigquery_dest_rejects_a_cfg_list_that_does_not_match_tables():
    with pytest.raises(BigQueryValidateException) as ei:
        BigQueryDest(
            tables=["`p.ds.a`", "`p.ds.b`"],
            if_table_exists="merge",
            merge_cfg=[_merge_cfg()],
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_14
    assert "1 entries but tables has 2" in str(ei.value)
