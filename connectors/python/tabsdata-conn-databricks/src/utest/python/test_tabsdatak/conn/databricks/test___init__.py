
import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.databricks.error import DatabricksValidateException
from tabsdatak.conn.databricks import DatabricksConn, DatabricksDest, DatabricksSrc
from tabsdatak.conn.databricks.error import DatabricksErrorCode


pytestmark = pytest.mark.databricks


def _conn():
    return DatabricksConn(
        host_url=Secret.lit("https://my-workspace.cloud.databricks.com"),
        token=Secret.lit("dapi-secret-token"),
        volume="main.staging.my_volume",
        warehouse="MY_WAREHOUSE",
        catalog="main",
        schema="analytics",
        conn_cfg={"databricks.timeout": 30},
    )


def test_databricks_src_full_round_trip():
    src = DatabricksSrc(
        conn=_conn(),
        queries=["SELECT * FROM main.analytics.users", "SELECT * FROM main.analytics.orders"],
        initial_values={"x": 1},
        entity_whitelist=["main.analytics.users", "main.analytics.orders"],
        src_cfg={"chunkSize": 1024},
    )
    loaded = DatabricksSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.conn.warehouse.value() == "MY_WAREHOUSE"
    assert loaded.conn.catalog.value() == "main"
    assert loaded.conn.schema.value() == "analytics"
    assert loaded.conn.conn_cfg == {"databricks.timeout": 30}
    assert loaded.queries == [
        "SELECT * FROM main.analytics.users",
        "SELECT * FROM main.analytics.orders",
    ]
    assert loaded.initial_values == {"x": 1}
    assert loaded.entity_whitelist == ["main.analytics.users", "main.analytics.orders"]
    assert loaded.src_cfg == {"chunkSize": 1024}


def test_databricks_dest_full_round_trip():
    dest = DatabricksDest(
        conn=_conn(),
        tables=["main.analytics.users", "main.analytics.orders"],
        if_table_exists="replace",
        schema_evolution="strict",
        dest_cfg={"chunkSize": 2048},
    )
    loaded = DatabricksDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["main.analytics.users", "main.analytics.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.schema_evolution == "strict"
    assert loaded.dest_cfg == {"chunkSize": 2048}


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


def test_databricks_conn_validate_rejects_warehouse_and_warehouse_id():
    with pytest.raises(DatabricksValidateException) as ei:
        DatabricksConn(
            host_url=Secret.lit("https://my-workspace.cloud.databricks.com"),
            token=Secret.lit("dapi-secret-token"),
            volume="main.staging.my_volume",
            warehouse="MY_WAREHOUSE",
            warehouse_id="0123456789abcdef",
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_2


def test_databricks_conn_validate_rejects_schema_without_catalog():
    with pytest.raises(DatabricksValidateException) as ei:
        DatabricksConn(
            host_url=Secret.lit("https://my-workspace.cloud.databricks.com"),
            token=Secret.lit("dapi-secret-token"),
            volume="main.staging.my_volume",
            schema="analytics",
        )
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_2
