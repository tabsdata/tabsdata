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
from tabsdatak.conn.common.types import CfgSpec, EntityWhitelistSpec

from tabsdatak.conn.databricks import (
    CredentialsSpec,
    DatabricksDestConn,
    DatabricksSrcConn,
    DatabricksDest,
    DatabricksSrc,
    DatabricksTableNamesSpec,
    DestCfgSpec,
    HostUrlSpec,
    IdentifierSpec,
    IfTableExistsSpec,
    MergeCfg,
    MergeCfgSpec,
    MergeColumnSpec,
    MergeColumnsSpec,
    QueriesSpec,
    SchemaEvolutionSpec,
    SrcCfgSpec,
    VolumeSpec,
    WarehouseIdSpec,
    WarehouseSpec,
)
from tabsdatak.conn.databricks.error import (
    DatabricksErrorCode,
    DatabricksValidateException,
)


pytestmark = pytest.mark.databricks


def test_databricks_src_full_round_trip():
    src = DatabricksSrc(
        queries=["SELECT * FROM main.analytics.users", "SELECT * FROM main.analytics.orders"],
        initial_values={"x": 1},
        src_cfg={"databricks.logging_level": 20},
    )
    loaded = DatabricksSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.queries == [
        "SELECT * FROM main.analytics.users",
        "SELECT * FROM main.analytics.orders",
    ]
    assert loaded.initial_values == {"x": 1}
    assert loaded.src_cfg == {"databricks.logging_level": 20}


def test_databricks_dest_full_round_trip():
    dest = DatabricksDest(
        tables=["main.analytics.users", "main.analytics.orders"],
        if_table_exists="replace",
        schema_evolution="strict",
        dest_cfg={"databricks.logging_level": 20},
    )
    loaded = DatabricksDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["main.analytics.users", "main.analytics.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.schema_evolution == "strict"
    assert loaded.dest_cfg == {"databricks.logging_level": 20}


@pytest.mark.parametrize("cls", [DatabricksSrcConn, DatabricksDestConn])
def test_databricks_conn_validate_rejects_warehouse_and_warehouse_id(cls):
    with pytest.raises(DatabricksValidateException) as ei:
        cls(
            host_url=Secret("https://my-workspace.cloud.databricks.com"),
            token=Secret("dapi-secret-token"),
            volume="main.staging.my_volume",
            warehouse="MY_WAREHOUSE",
            warehouse_id="0123456789abcdef",
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_2


@pytest.mark.parametrize("cls", [DatabricksSrcConn, DatabricksDestConn])
def test_databricks_conn_validate_rejects_schema_without_catalog(cls):
    with pytest.raises(DatabricksValidateException) as ei:
        cls(
            host_url=Secret("https://my-workspace.cloud.databricks.com"),
            token=Secret("dapi-secret-token"),
            volume="main.staging.my_volume",
            schema="analytics",
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_2


def test_databricks_src_dest_conn_are_distinct_classes():
    # Source and destination use distinct connection classes (told apart
    # on the wire by the envelope ``type``), though both now share the
    # uniform ``connectionDef`` kind.
    assert DatabricksSrcConn is not DatabricksDestConn
    assert DatabricksSrcConn.__kind__ == DatabricksDestConn.__kind__ == "connectionDef"


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


@pytest.mark.parametrize("cls", [DatabricksSrcConn, DatabricksDestConn])
def test_databricks_conn_fields_wire_expected_specs(cls):
    assert _hint(cls, "host_url") == HostUrlSpec
    assert _hint(cls, "token") == CredentialsSpec
    assert _hint(cls, "volume") == VolumeSpec
    assert _hint(cls, "warehouse") == WarehouseSpec | None
    assert _hint(cls, "warehouse_id") == WarehouseIdSpec | None
    assert _hint(cls, "catalog") == IdentifierSpec | None
    assert _hint(cls, "schema") == IdentifierSpec | None
    assert _hint(cls, "entity_whitelist") == EntityWhitelistSpec | None
    assert _hint(cls, "conn_cfg") == CfgSpec | None


def test_databricks_src_fields_wire_expected_specs():
    from tabsdatak.api import BasicDictSpec

    assert _hint(DatabricksSrc, "queries") == QueriesSpec
    assert _hint(DatabricksSrc, "initial_values") == BasicDictSpec | None
    assert _hint(DatabricksSrc, "src_cfg") == SrcCfgSpec | None


def test_databricks_dest_fields_wire_expected_specs():
    assert _hint(DatabricksDest, "tables") == DatabricksTableNamesSpec
    assert _hint(DatabricksDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(DatabricksDest, "schema_evolution") == SchemaEvolutionSpec
    assert _hint(DatabricksDest, "dest_cfg") == DestCfgSpec | None


def test_databricks_dest_defaults_to_strict_schema_evolution():
    """Opt-in rather than opt-out: the target's schema is pinned unless the
    flow asks for it to move."""
    assert DatabricksDest(tables=["c.s.t"]).schema_evolution == "strict"


# ---------------------------------------------------------------------------
# src_cfg / dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_databricks_src_rejects_unknown_src_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        DatabricksSrc(
            queries=["SELECT 1"],
            src_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_3


def test_databricks_src_accepts_known_src_cfg_keys():
    src = DatabricksSrc(
        queries=["SELECT 1"],
        src_cfg={"databricks.logging_level": 20},
    )
    loaded = DatabricksSrc.from_json(src.to_json())
    assert loaded == src


def test_databricks_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        DatabricksDest(
            tables=["main.analytics.users"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_4


def test_databricks_dest_accepts_known_dest_cfg_keys():
    dest = DatabricksDest(
        tables=["main.analytics.users"],
        dest_cfg={"databricks.logging_level": 20},
    )
    loaded = DatabricksDest.from_json(dest.to_json())
    assert loaded == dest


@pytest.mark.parametrize("cls", [DatabricksSrcConn, DatabricksDestConn])
def test_databricks_conn_rejects_missing_required_fields(cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        cls(  # type: ignore[call-arg]
            token=Secret("dapi-..."),
            volume="main.staging.v",
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_1


# ---------------------------------------------------------------------------
# MergeCfg -- the Databricks spelling of the shared descriptor
# ---------------------------------------------------------------------------


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
    # Databricks resolves identifiers case-insensitively, so any spelling of a
    # name has to reach the same column.
    assert _merge_cfg(on=["ORDER_ID"])._keys() == ["order_id"]
    assert _merge_cfg(op_column="OP")._op_column() == "op"


def test_merge_cfg_strips_backticks_without_making_a_name_case_sensitive():
    """Unlike Snowflake, a quoted Databricks name is not case-sensitive -- the
    backticks are only there for special characters."""
    assert _merge_cfg(on=["`OrderId`"])._keys() == ["orderid"]
    assert _merge_cfg(on=["`weird col`"])._keys() == ["weird col"]


def test_merge_cfg_rejects_a_dotted_column():
    with pytest.raises(ConnInitException) as ei:
        _merge_cfg(on=["orders.id"])
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_18


def test_merge_cfg_rejects_a_dotted_op_column():
    with pytest.raises(ConnInitException) as ei:
        _merge_cfg(op_column="orders.op")
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_18


def test_merge_cfg_requires_an_op_column():
    with pytest.raises(ConnInitException) as ei:
        MergeCfg(on=["order_id"])
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_18


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
    the merge. The identifier code and the offending position both survive into
    the message."""
    with pytest.raises(ConnInitException) as ei:
        _merge_cfg(on=["orders.id"])
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_18
    assert "DATABRICKS-14" in str(ei.value)
    assert "on.0" in str(ei.value)


def test_merge_cfg_rejects_a_quoted_key_as_the_op_column():
    """The collision check runs on folded names, so quoting cannot smuggle the
    marker past it."""
    with pytest.raises(SqlCommonValidateException) as ei:
        _merge_cfg(on=["`op`"], op_column="OP")
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_6


# ---------------------------------------------------------------------------
# DatabricksDest -- merge_cfg and if_table_exists require each other
# ---------------------------------------------------------------------------


def test_databricks_dest_wires_the_merge_cfg_spec():
    assert _hint(DatabricksDest, "merge_cfg") == MergeCfgSpec | None


def test_databricks_dest_round_trips_a_merge():
    dest = DatabricksDest(
        tables=["main.analytics.orders"],
        if_table_exists="merge",
        merge_cfg=[_merge_cfg()],
    )
    loaded = DatabricksDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.merge_cfg[0].op_column == "op"


def test_databricks_dest_round_trips_a_merge_cfg_per_table():
    dest = DatabricksDest(
        tables=["c.s.orders", "c.s.items"],
        if_table_exists="merge",
        merge_cfg=[_merge_cfg(), _merge_cfg(on=["item_id"])],
    )
    loaded = DatabricksDest.from_json(dest.to_json())
    assert [c.on for c in loaded.merge_cfg] == [["order_id"], ["item_id"]]


def test_databricks_dest_takes_one_merge_cfg_per_table():
    cfgs = [_merge_cfg(), _merge_cfg(on=["item_id"])]
    dest = DatabricksDest(
        tables=["c.s.a", "c.s.b"],
        if_table_exists="merge",
        merge_cfg=cfgs,
    )
    assert dest._merge_cfgs() == cfgs


def test_databricks_dest_takes_a_one_entry_list_for_a_single_table():
    cfg = _merge_cfg()
    dest = DatabricksDest(
        tables=["c.s.a"],
        if_table_exists="merge",
        merge_cfg=[cfg],
    )
    assert dest._merge_cfgs() == [cfg]


def test_databricks_dest_refuses_a_bare_merge_cfg():
    # A list even for one table, so reading a merge config off a destination
    # never depends on how many tables it happens to write.
    with pytest.raises(ConnInitException) as ei:
        DatabricksDest(
            tables=["c.s.a"],
            if_table_exists="merge",
            merge_cfg=_merge_cfg(),
        )
    assert "valid list" in str(ei.value)


def test_databricks_dest_merge_cfgs_is_none_outside_merge_mode():
    assert DatabricksDest(tables=["c.s.a"])._merge_cfgs() is None


def test_databricks_dest_keeps_a_list_of_cfgs_in_order():
    first, second = _merge_cfg(), _merge_cfg(on=["item_id"])
    dest = DatabricksDest(
        tables=["c.s.a", "c.s.b"],
        if_table_exists="merge",
        merge_cfg=[first, second],
    )
    assert dest._merge_cfgs() == [first, second]


def test_databricks_dest_rejects_merge_without_a_merge_cfg():
    with pytest.raises(DatabricksValidateException) as ei:
        DatabricksDest(tables=["c.s.a"], if_table_exists="merge")
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_2
    assert "requires merge_cfg" in str(ei.value)


@pytest.mark.parametrize("if_table_exists", ["append", "replace"])
def test_databricks_dest_rejects_a_merge_cfg_outside_merge_mode(if_table_exists):
    with pytest.raises(DatabricksValidateException) as ei:
        DatabricksDest(
            tables=["c.s.a"],
            if_table_exists=if_table_exists,
            merge_cfg=[_merge_cfg()],
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_2
    assert if_table_exists in str(ei.value)


def test_databricks_dest_rejects_a_cfg_list_that_does_not_match_tables():
    with pytest.raises(DatabricksValidateException) as ei:
        DatabricksDest(
            tables=["c.s.a", "c.s.b"],
            if_table_exists="merge",
            merge_cfg=[_merge_cfg()],
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_2
    assert "1 entries but tables has 2" in str(ei.value)


# ---------------------------------------------------------------------------
# watermark_column
#
# The idempotency watermark belongs to the write, not to the way one table's
# rows are matched, so it is named on the destination and stamped onto every
# merge config on the way past.
# ---------------------------------------------------------------------------

_WATERMARK = "wm"


def test_databricks_dest_wires_the_watermark_column_spec():
    assert _hint(DatabricksDest, "watermark_column") == MergeColumnSpec | None


def test_databricks_dest_accepts_a_watermark_on_an_append():
    dest = DatabricksDest(
        tables=["c.s.t"], if_table_exists="append", watermark_column=_WATERMARK
    )
    assert dest.watermark_column == _WATERMARK
    assert dest._merge_cfgs() is None


def test_databricks_dest_rejects_a_watermark_on_a_replace():
    # A replace rewrites the target wholesale, so a rerun already leaves the
    # same rows behind. There is nothing here to make idempotent.
    with pytest.raises(DatabricksValidateException) as ei:
        DatabricksDest(
            tables=["c.s.t"], if_table_exists="replace", watermark_column=_WATERMARK
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_2
    assert "nothing for a watermark to make idempotent" in str(ei.value)


def test_databricks_dest_watermark_named_on_the_merge_cfg_does_not_arrive():
    # MergeCfg has no such field any more, and the API dataclass drops a kwarg
    # it does not declare -- so the name is gone before validate sees it and the
    # destination is the only place it can be named.
    dest = DatabricksDest(
        tables=["c.s.t"],
        if_table_exists="merge",
        merge_cfg=[_merge_cfg(watermark_column=_WATERMARK)],
    )
    assert dest._watermark_column() is None
    assert not hasattr(dest._merge_cfgs()[0], "watermark_column")


def test_databricks_dest_hands_its_merge_cfgs_over_untouched():
    # The watermark travels to the driver as a value of its own now, so the
    # configs are broadcast rather than rewritten.
    a, b = _merge_cfg(), _merge_cfg(on=["item_id"])
    dest = DatabricksDest(
        tables=["c.s.t", "c.s.t"],
        if_table_exists="merge",
        merge_cfg=[a, b],
        watermark_column=_WATERMARK,
    )
    assert dest._merge_cfgs() == [a, b]


def test_databricks_dest_folds_the_watermark_column_to_its_stored_form():
    dest = DatabricksDest(
        tables=["c.s.t"], if_table_exists="append", watermark_column="WM"
    )
    assert dest._watermark_column() == "wm"


def test_databricks_dest_reports_no_watermark_column_when_unset():
    dest = DatabricksDest(tables=["c.s.t"], if_table_exists="append")
    assert dest._watermark_column() is None


def test_databricks_dest_rejects_a_watermark_colliding_with_a_key():
    # The collision checks live on the shared MergeCfg, so they only see the
    # watermark once the destination has stamped it on.
    with pytest.raises(SqlCommonValidateException) as ei:
        DatabricksDest(
            tables=["c.s.t"],
            if_table_exists="merge",
            merge_cfg=[_merge_cfg()],
            watermark_column="order_id",
        )
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_7


def test_databricks_dest_rejects_a_quoted_watermark_colliding_with_a_quoted_key():
    # Backticks quote a special-char name without making it case-sensitive,
    # so both names fold to `mycol` and collide. Unlike Snowflake's, this
    # folding is idempotent, so it holds however often it is applied.
    with pytest.raises(SqlCommonValidateException) as ei:
        DatabricksDest(
            tables=["c.s.t"],
            if_table_exists="merge",
            merge_cfg=[_merge_cfg(on=['`MyCol`'])],
            watermark_column='`MYCOL`',
        )
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_7


def test_databricks_dest_rejects_a_watermark_colliding_with_the_op_column():
    with pytest.raises(SqlCommonValidateException) as ei:
        DatabricksDest(
            tables=["c.s.t"],
            if_table_exists="merge",
            merge_cfg=[_merge_cfg()],
            watermark_column="op",
        )
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_8


def test_databricks_dest_rejects_a_quoted_watermark_colliding_with_a_quoted_op_column():
    with pytest.raises(SqlCommonValidateException) as ei:
        DatabricksDest(
            tables=["c.s.t"],
            if_table_exists="merge",
            merge_cfg=[_merge_cfg(op_column='`OpCol`')],
            watermark_column='`OPCOL`',
        )
    assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_8


def test_databricks_merge_cfg_has_no_watermark_of_its_own():
    # Named on the destination, and gone from the shared base too.
    assert "watermark_column" not in MergeCfg.__annotations__
    assert not hasattr(_merge_cfg(), "watermark_column")
