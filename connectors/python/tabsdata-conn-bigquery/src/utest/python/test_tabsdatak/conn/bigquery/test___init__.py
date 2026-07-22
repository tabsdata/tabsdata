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
    ProjectSpec,
    SchemaEvolutionSpec,
)
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.bigquery.error import (
    BigQueryValidateException,
    BigqueryErrorCode,
)
from tabsdatak.conn.bigquery import GCSLocation
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
    assert ei.value.api_error is BigqueryErrorCode.BIGQUERY_2


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
    assert _hint(BigQueryDest, "tables") == BigQueryTableNamesSpec | None
    assert _hint(BigQueryDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(BigQueryDest, "schema_evolution") == SchemaEvolutionSpec
    assert _hint(BigQueryDest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_bigquery_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        BigQueryDest(
            tables=["`myproject.mydataset.users`"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is BigqueryErrorCode.BIGQUERY_3


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
    assert ei.value.api_error is BigqueryErrorCode.BIGQUERY_1
