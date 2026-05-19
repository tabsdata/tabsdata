from __future__ import annotations

import typing

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.bigquery import (
    BigQueryConn,
    BigQueryDest,
    BigQueryTableNamesSpec,
    ConnSpec,
    CredentialsSpec,
    DatasetSpec,
    IfTableExistsSpec,
    ProjectSpec,
    SchemaEvolutionSpec,
)
from tabsdatak.conn.bigquery.error import (
    BigQueryValidateException,
    BigqueryErrorCode,
)
from tabsdatak.conn.common.types import CfgSpec, GCPJson, GCSLocation


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
        tables=["`my-project.analytics.users`", "`my-project.analytics.orders`"],
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
        "`my-project.analytics.users`",
        "`my-project.analytics.orders`",
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


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


def test_bigquery_conn_fields_wire_expected_specs():
    assert _hint(BigQueryConn, "staging") == GCSLocation
    assert _hint(BigQueryConn, "credentials") == CredentialsSpec
    assert _hint(BigQueryConn, "project") == ProjectSpec | None
    assert _hint(BigQueryConn, "dataset") == DatasetSpec | None
    assert _hint(BigQueryConn, "conn_cfg") == CfgSpec | None


def test_bigquery_dest_fields_wire_expected_specs():
    assert _hint(BigQueryDest, "conn") == ConnSpec
    assert _hint(BigQueryDest, "tables") == BigQueryTableNamesSpec | None
    assert _hint(BigQueryDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(BigQueryDest, "schema_evolution") == SchemaEvolutionSpec
    assert _hint(BigQueryDest, "dest_cfg") == CfgSpec | None
