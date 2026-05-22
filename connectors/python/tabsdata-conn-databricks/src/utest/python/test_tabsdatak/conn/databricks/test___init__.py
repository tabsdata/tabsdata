from __future__ import annotations

import typing

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.types import CfgSpec, EntityWhitelistSpec
from tabsdatak.conn.databricks import (
    DestConnSpec,
    SrcConnSpec,
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


def _src_conn() -> DatabricksSrcConn:
    return DatabricksSrcConn(
        host_url=Secret.lit("https://my-workspace.cloud.databricks.com"),
        token=Secret.lit("dapi-secret-token"),
        volume="main.staging.my_volume",
        warehouse="MY_WAREHOUSE",
        catalog="main",
        schema="analytics",
        entity_whitelist=["main.analytics.users", "main.analytics.orders"],
        conn_cfg={"databricks.timeout": 30},
    )


def _dest_conn() -> DatabricksDestConn:
    return DatabricksDestConn(
        host_url=Secret.lit("https://my-workspace.cloud.databricks.com"),
        token=Secret.lit("dapi-secret-token"),
        volume="main.staging.my_volume",
        warehouse="MY_WAREHOUSE",
        catalog="main",
        schema="analytics",
        entity_whitelist=["main.analytics.users", "main.analytics.orders"],
        conn_cfg={"databricks.timeout": 30},
    )


def test_databricks_src_full_round_trip():
    src = DatabricksSrc(
        conn=_src_conn(),
        queries=["SELECT * FROM main.analytics.users", "SELECT * FROM main.analytics.orders"],
        initial_values={"x": 1},
        src_cfg={"databricks.logging_level": 20},
    )
    loaded = DatabricksSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.conn.warehouse.value() == "MY_WAREHOUSE"
    assert loaded.conn.catalog.value() == "main"
    assert loaded.conn.schema.value() == "analytics"
    assert loaded.conn.entity_whitelist == ["main.analytics.users", "main.analytics.orders"]
    assert loaded.conn.conn_cfg == {"databricks.timeout": 30}
    assert loaded.queries == [
        "SELECT * FROM main.analytics.users",
        "SELECT * FROM main.analytics.orders",
    ]
    assert loaded.initial_values == {"x": 1}
    assert loaded.src_cfg == {"databricks.logging_level": 20}


def test_databricks_dest_full_round_trip():
    dest = DatabricksDest(
        conn=_dest_conn(),
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


def test_databricks_src_round_trip_with_named_conn():
    src = DatabricksSrc(conn=NamedConn(name="prod_databricks"), queries=["SELECT 1"])
    loaded = DatabricksSrc.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_databricks"


def test_databricks_dest_round_trip_with_named_conn():
    dest = DatabricksDest(
        conn=NamedConn(name="prod_databricks"),
        tables=["main.analytics.users"],
    )
    loaded = DatabricksDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_databricks"


@pytest.mark.parametrize("cls", [DatabricksSrcConn, DatabricksDestConn])
def test_databricks_conn_validate_rejects_warehouse_and_warehouse_id(cls):
    with pytest.raises(DatabricksValidateException) as ei:
        cls(
            host_url=Secret.lit("https://my-workspace.cloud.databricks.com"),
            token=Secret.lit("dapi-secret-token"),
            volume="main.staging.my_volume",
            warehouse="MY_WAREHOUSE",
            warehouse_id="0123456789abcdef",
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_2


@pytest.mark.parametrize("cls", [DatabricksSrcConn, DatabricksDestConn])
def test_databricks_conn_validate_rejects_schema_without_catalog(cls):
    with pytest.raises(DatabricksValidateException) as ei:
        cls(
            host_url=Secret.lit("https://my-workspace.cloud.databricks.com"),
            token=Secret.lit("dapi-secret-token"),
            volume="main.staging.my_volume",
            schema="analytics",
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_2


def test_databricks_src_dest_conn_have_distinct_kinds():
    assert DatabricksSrcConn.__kind__ != DatabricksDestConn.__kind__


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
    assert _hint(DatabricksSrc, "conn") == SrcConnSpec
    assert _hint(DatabricksSrc, "queries") == QueriesSpec
    assert _hint(DatabricksSrc, "initial_values") == dict | None
    assert _hint(DatabricksSrc, "src_cfg") == SrcCfgSpec | None


def test_databricks_dest_fields_wire_expected_specs():
    assert _hint(DatabricksDest, "conn") == DestConnSpec
    assert _hint(DatabricksDest, "tables") == DatabricksTableNamesSpec
    assert _hint(DatabricksDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(DatabricksDest, "schema_evolution") == SchemaEvolutionSpec
    assert _hint(DatabricksDest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# src_cfg / dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_databricks_src_rejects_unknown_src_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        DatabricksSrc(
            conn=_src_conn(),
            queries=["SELECT 1"],
            src_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_3


def test_databricks_src_accepts_known_src_cfg_keys():
    src = DatabricksSrc(
        conn=_src_conn(),
        queries=["SELECT 1"],
        src_cfg={"databricks.logging_level": 20},
    )
    loaded = DatabricksSrc.from_json(src.to_json())
    assert loaded == src


def test_databricks_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        DatabricksDest(
            conn=_dest_conn(),
            tables=["main.analytics.users"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_4


def test_databricks_dest_accepts_known_dest_cfg_keys():
    dest = DatabricksDest(
        conn=_dest_conn(),
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
            token=Secret.lit("dapi-..."),
            volume="main.staging.v",
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_1
