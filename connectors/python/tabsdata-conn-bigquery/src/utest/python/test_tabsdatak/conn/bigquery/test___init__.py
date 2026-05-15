
import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.bigquery.error import BigQueryValidateException
from tabsdatak.conn.common.types import GCPJson, GCSLocation
from tabsdatak.conn.bigquery import BigQueryConn, BigQueryDest
from tabsdatak.conn.bigquery.error import BigqueryErrorCode


pytestmark = pytest.mark.bigquery


def _staging():
    return GCSLocation(
        bucket="staging-bucket",
        credentials=GCPJson(json=Secret("gcp-sa#KEY")),
        base_path="staging/",
    )


def _conn():
    return BigQueryConn(
        staging=_staging(),
        credentials=GCPJson(json=Secret("gcp-sa#KEY")),
        project="my-project",
        dataset="analytics",
        conn_cfg={"bigquery.timeout": 30},
    )


def test_bigquery_dest_full_round_trip():
    dest = BigQueryDest(
        conn=_conn(),
        tables=["my-project.analytics.users", "my-project.analytics.orders"],
        if_table_exists="replace",
        schema_evolution="strict",
        dest_cfg={"chunkSize": 2048},
    )
    loaded = BigQueryDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.conn.project.value() == "my-project"
    assert loaded.conn.dataset.value() == "analytics"
    assert loaded.conn.conn_cfg == {"bigquery.timeout": 30}
    assert loaded.tables == [
        "my-project.analytics.users",
        "my-project.analytics.orders",
    ]
    assert loaded.if_table_exists == "replace"
    assert loaded.schema_evolution == "strict"
    assert loaded.dest_cfg == {"chunkSize": 2048}


def test_bigquery_dest_round_trip_with_named_conn():
    dest = BigQueryDest(conn=NamedConn(name="prod_bigquery"))
    loaded = BigQueryDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_bigquery"


def test_bigquery_conn_validate_rejects_dataset_without_project():
    with pytest.raises(BigQueryValidateException) as ei:
        BigQueryConn(
            staging=_staging(),
            credentials=GCPJson(json=Secret("gcp-sa#KEY")),
            dataset="analytics",
        )
    assert ei.value.api_error is BigqueryErrorCode.BIGQUERY_2
