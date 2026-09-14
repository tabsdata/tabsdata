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


# A MERGE takes no load-job options, so the connector evolves its target with
# ALTER statements or not at all -- and the write has to say so.
_ICEBERG = {"bigquery.schema_evolution_engine": "iceberg"}


def _merge_cfg(**overrides) -> MergeCfg:
    kwargs: dict = dict(on=["order_id"], op_column="op")
    kwargs.update(overrides)
    return MergeCfg(**kwargs)


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


def test_a_dotted_merge_key_is_refused_at_the_field():
    """The column spec gates the value, so a qualified reference never reaches
    the merge. The column code and the offending position both survive into the
    message."""
    with pytest.raises(ConnInitException) as ei:
        _merge_cfg(on=["orders.id"])
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_11
    assert "BIGQUERY-25" in str(ei.value)
    assert "on.0" in str(ei.value)


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
        merge_cfg=[_merge_cfg()], dest_cfg=_ICEBERG,
    )
    loaded = BigQueryDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.merge_cfg[0].op_column == "op"


def test_bigquery_dest_round_trips_a_merge_cfg_per_table():
    dest = BigQueryDest(
        tables=["`p.ds.orders`", "`p.ds.items`"],
        if_table_exists="merge",
        merge_cfg=[_merge_cfg(), _merge_cfg(on=["item_id"])], dest_cfg=_ICEBERG,
    )
    loaded = BigQueryDest.from_json(dest.to_json())
    assert [c.on for c in loaded.merge_cfg] == [["order_id"], ["item_id"]]


def test_bigquery_dest_takes_one_merge_cfg_per_table():
    cfgs = [_merge_cfg(), _merge_cfg(on=["item_id"])]
    dest = BigQueryDest(
        tables=["`p.ds.a`", "`p.ds.b`"],
        if_table_exists="merge",
        merge_cfg=cfgs, dest_cfg=_ICEBERG,
    )
    assert dest._merge_cfgs() == cfgs


def test_bigquery_dest_takes_a_one_entry_list_for_a_single_table():
    cfg = _merge_cfg()
    dest = BigQueryDest(
        tables=["`p.ds.a`"],
        if_table_exists="merge",
        merge_cfg=[cfg], dest_cfg=_ICEBERG,
    )
    assert dest._merge_cfgs() == [cfg]


def test_bigquery_dest_refuses_a_bare_merge_cfg():
    # A list even for one table, so reading a merge config off a destination
    # never depends on how many tables it happens to write.
    with pytest.raises(ConnInitException) as ei:
        BigQueryDest(
            tables=["`p.ds.a`"],
            if_table_exists="merge",
            merge_cfg=_merge_cfg(), dest_cfg=_ICEBERG,
        )
    assert "valid list" in str(ei.value)


def test_bigquery_dest_merge_cfgs_is_none_outside_merge_mode():
    assert BigQueryDest(tables=["`p.ds.a`"])._merge_cfgs() is None


def test_bigquery_dest_keeps_a_list_of_cfgs_in_order():
    first, second = _merge_cfg(), _merge_cfg(on=["item_id"])
    dest = BigQueryDest(
        tables=["`p.ds.a`", "`p.ds.b`"],
        if_table_exists="merge",
        merge_cfg=[first, second], dest_cfg=_ICEBERG,
    )
    assert dest._merge_cfgs() == [first, second]


def test_bigquery_dest_rejects_merge_without_a_merge_cfg():
    with pytest.raises(BigQueryValidateException) as ei:
        BigQueryDest(tables=["`p.ds.a`"], if_table_exists="merge", dest_cfg=_ICEBERG)
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_12
    assert "requires merge_cfg" in str(ei.value)


@pytest.mark.parametrize("if_table_exists", ["append", "replace"])
def test_bigquery_dest_rejects_a_merge_cfg_outside_merge_mode(if_table_exists):
    with pytest.raises(BigQueryValidateException) as ei:
        BigQueryDest(
            tables=["`p.ds.a`"],
            if_table_exists=if_table_exists,
            merge_cfg=[_merge_cfg()],
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_13
    assert if_table_exists in str(ei.value)


def test_bigquery_dest_rejects_a_cfg_list_that_does_not_match_tables():
    with pytest.raises(BigQueryValidateException) as ei:
        BigQueryDest(
            tables=["`p.ds.a`", "`p.ds.b`"],
            if_table_exists="merge",
            merge_cfg=[_merge_cfg()], dest_cfg=_ICEBERG,
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_14
    assert "1 entries but tables has 2" in str(ei.value)


# ---------------------------------------------------------------------------
# watermark_column
#
# The idempotency watermark belongs to the write, not to the way one table's
# rows are matched, so it is named on the destination and stamped onto every
# merge config on the way past.
# ---------------------------------------------------------------------------

_WATERMARK = "wm"


def test_bigquery_dest_wires_the_watermark_column_spec():
    assert _hint(BigQueryDest, "watermark_column") == MergeColumnSpec | None


def test_bigquery_dest_accepts_a_watermark_on_an_append():
    dest = BigQueryDest(
        tables=["`p.ds.t`"],
        if_table_exists="append",
        watermark_column=_WATERMARK,
    )
    assert dest.watermark_column == _WATERMARK
    assert dest._merge_cfgs() is None


def test_bigquery_dest_rejects_a_watermark_on_a_replace():
    # A replace rewrites the target wholesale, so a rerun already leaves the
    # same rows behind. There is nothing here to make idempotent.
    with pytest.raises(BigQueryValidateException) as ei:
        BigQueryDest(
            tables=["`p.ds.t`"],
            if_table_exists="replace",
            watermark_column=_WATERMARK,
        )
    assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_24


def test_bigquery_dest_watermark_named_on_the_merge_cfg_does_not_arrive():
    # MergeCfg has no such field any more, and the API dataclass drops a kwarg
    # it does not declare -- so the name is gone before validate sees it and the
    # destination is the only place it can be named.
    dest = BigQueryDest(
        tables=["`p.ds.t`"],
        if_table_exists="merge",
        merge_cfg=[_merge_cfg(watermark_column=_WATERMARK)],
            dest_cfg=_ICEBERG,
    )
    assert dest._watermark_column() is None
    assert not hasattr(dest._merge_cfgs()[0], "watermark_column")


def test_bigquery_dest_hands_its_merge_cfgs_over_untouched():
    # The watermark travels to the driver as a value of its own now, so the
    # configs are broadcast rather than rewritten.
    a, b = _merge_cfg(), _merge_cfg(on=["item_id"])
    dest = BigQueryDest(
        tables=["`p.ds.t`", "`p.ds.t`"],
        if_table_exists="merge",
        merge_cfg=[a, b],
        watermark_column=_WATERMARK,
            dest_cfg=_ICEBERG,
    )
    assert dest._merge_cfgs() == [a, b]


def test_bigquery_dest_folds_the_watermark_column_to_its_stored_form():
    dest = BigQueryDest(
        tables=["`p.ds.t`"],
        if_table_exists="append",
        watermark_column="WM",
    )
    assert dest._watermark_column() == "wm"


def test_bigquery_dest_reports_no_watermark_column_when_unset():
    dest = BigQueryDest(tables=["`p.ds.t`"], if_table_exists="append")
    assert dest._watermark_column() is None


def test_bigquery_dest_rejects_a_watermark_colliding_with_a_key():
    # The collision checks live on the shared MergeCfg, so they only see the
    # watermark once the destination has stamped it on.
    with pytest.raises(SqlCommonValidateException) as ei:
        BigQueryDest(
            tables=["`p.ds.t`"],
            if_table_exists="merge",
            merge_cfg=[_merge_cfg()],
            watermark_column="order_id",
            dest_cfg=_ICEBERG,
        )
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_7


def test_bigquery_dest_rejects_a_quoted_watermark_colliding_with_a_quoted_key():
    # Backticks quote a special-char name without making it case-sensitive,
    # so both names fold to `mycol` and collide. Unlike Snowflake's, this
    # folding is idempotent, so it holds however often it is applied.
    with pytest.raises(SqlCommonValidateException) as ei:
        BigQueryDest(
            tables=["`p.ds.t`"],
            if_table_exists="merge",
            merge_cfg=[_merge_cfg(on=['`MyCol`'])],
            watermark_column='`MYCOL`',
            dest_cfg=_ICEBERG,
        )
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_7


def test_bigquery_dest_rejects_a_watermark_colliding_with_the_op_column():
    with pytest.raises(SqlCommonValidateException) as ei:
        BigQueryDest(
            tables=["`p.ds.t`"],
            if_table_exists="merge",
            merge_cfg=[_merge_cfg()],
            watermark_column="op",
            dest_cfg=_ICEBERG,
        )
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_8


def test_bigquery_dest_rejects_a_quoted_watermark_colliding_with_a_quoted_op_column():
    with pytest.raises(SqlCommonValidateException) as ei:
        BigQueryDest(
            tables=["`p.ds.t`"],
            if_table_exists="merge",
            merge_cfg=[_merge_cfg(op_column='`OpCol`')],
            watermark_column='`OPCOL`',
            dest_cfg=_ICEBERG,
        )
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_8


def test_bigquery_merge_cfg_has_no_watermark_of_its_own():
    # Named on the destination, and gone from the shared base too.
    assert "watermark_column" not in MergeCfg.__annotations__
    assert not hasattr(_merge_cfg(), "watermark_column")
