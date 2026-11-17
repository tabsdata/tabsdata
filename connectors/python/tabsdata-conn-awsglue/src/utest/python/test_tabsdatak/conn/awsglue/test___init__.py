#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import typing

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.awsglue import (
    AwsGlueDest,
    AwsGlueDestConn,
    AwsGlueTableNamesSpec,
    CatalogIdSpec,
    CredentialsSpec,
    DatabaseSpec,
    DestCfgSpec,
    IfTableExistsSpec,
    SchemaEvolutionSpec,
)
from tabsdatak.conn.awsglue.error import AwsGlueErrorCode
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AwsRegionSpec,
    CfgSpec,
    CloudRelativePathSpec,
    EntityWhitelistSpec,
    S3BucketSpec,
)

pytestmark = pytest.mark.conn_awsglue


def _credentials() -> AwsAccessSecretKey:
    return AwsAccessSecretKey(access_key_id=Secret(), secret_access_key=Secret())


def _conn(**overrides) -> AwsGlueDestConn:
    kwargs: dict = dict(
        region="us-east-1", credentials=_credentials(), bucket="my-lake"
    )
    kwargs.update(overrides)
    return AwsGlueDestConn(**kwargs)


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


# --- Connection ---------------------------------------------------------


def test_conn_full_round_trip():
    conn = _conn(
        base_path="/warehouse",
        database="sales",
        catalog_id="123456789012",
        conn_cfg={"pyiceberg.glue.max-retries": 3, "obstore.endpoint": "http://x"},
    )
    assert AwsGlueDestConn.from_json(conn.to_json()) == conn


def test_conn_defaults():
    conn = _conn()
    assert conn._base_path() == "/"
    assert conn._database() is None
    assert conn._catalog_id() is None
    assert conn.conn_cfg is None


def test_conn_reads_its_fields_as_text():
    conn = _conn(base_path="/warehouse", database="sales", catalog_id="123456789012")
    assert conn._region() == "us-east-1"
    assert conn._bucket() == "my-lake"
    assert conn._base_path() == "/warehouse"
    assert conn._database() == "sales"
    assert conn._catalog_id() == "123456789012"


def test_conn_fields_wire_expected_specs():
    assert _hint(AwsGlueDestConn, "region") == AwsRegionSpec
    assert _hint(AwsGlueDestConn, "credentials") == CredentialsSpec
    assert _hint(AwsGlueDestConn, "bucket") == S3BucketSpec
    assert _hint(AwsGlueDestConn, "base_path") == CloudRelativePathSpec
    assert _hint(AwsGlueDestConn, "database") == DatabaseSpec | None
    assert _hint(AwsGlueDestConn, "catalog_id") == CatalogIdSpec | None
    assert _hint(AwsGlueDestConn, "entity_whitelist") == EntityWhitelistSpec | None
    assert _hint(AwsGlueDestConn, "conn_cfg") == CfgSpec | None


def test_conn_takes_the_credentials_the_s3_connector_takes():
    assert CredentialsSpec is AwsAccessSecretKey


@pytest.mark.parametrize("missing", ["region", "credentials", "bucket"])
def test_conn_rejects_a_missing_required_field(missing):
    kwargs: dict = dict(
        region="us-east-1", credentials=_credentials(), bucket="my-lake"
    )
    del kwargs[missing]
    with pytest.raises(ConnInitException) as raised:
        AwsGlueDestConn(**kwargs)
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_1


@pytest.mark.parametrize(
    "field,value,code",
    [
        ("region", "nowhere", "CONN-"),
        ("bucket", "My_Bucket", "CONN-"),
        ("base_path", "warehouse/", "CONN-"),
        ("database", "Sales", "AWSGLUE-3"),
        ("database", "sales-eu", "AWSGLUE-3"),
        ("catalog_id", "1234", "AWSGLUE-5"),
    ],
)
def test_conn_rejects_an_invalid_field(field, value, code):
    with pytest.raises(ConnInitException) as raised:
        _conn(**{field: value})
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_1
    assert code in str(raised.value)


def test_conn_rejects_an_entity_whitelist_entry_that_is_not_a_regex():
    with pytest.raises(ConnInitException) as raised:
        _conn(entity_whitelist=["orders_(.*"])
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_1


def test_conn_takes_a_secret_for_a_string_field():
    conn = _conn(region=Secret(), bucket=Secret(), database=Secret())
    assert AwsGlueDestConn.from_json(conn.to_json()) == conn


def test_conn_rejects_a_conn_cfg_value_that_is_not_json_like():
    with pytest.raises(ConnInitException) as raised:
        _conn(conn_cfg={"pyiceberg.glue.max-retries": object()})
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_1


# --- Destination --------------------------------------------------------


def test_dest_full_round_trip():
    dest = AwsGlueDest(
        tables=["sales.orders", "customers"],
        if_table_exists="replace",
        schema_evolution="update",
        dest_cfg={"awsglue.logging_level": 20},
    )
    loaded = AwsGlueDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["sales.orders", "customers"]


def test_dest_defaults_to_a_strict_append():
    dest = AwsGlueDest(tables=["orders"])
    assert dest.if_table_exists == "append"
    assert dest.schema_evolution == "strict"
    assert dest.dest_cfg is None


def test_dest_fields_wire_expected_specs():
    assert _hint(AwsGlueDest, "tables") == AwsGlueTableNamesSpec
    assert _hint(AwsGlueDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(AwsGlueDest, "schema_evolution") == SchemaEvolutionSpec
    assert _hint(AwsGlueDest, "dest_cfg") == DestCfgSpec | None


def test_dest_offers_append_and_replace_only():
    assert typing.get_args(IfTableExistsSpec) == ("append", "replace")


def test_dest_rejects_a_merge():
    with pytest.raises(ConnInitException) as raised:
        AwsGlueDest(tables=["orders"], if_table_exists="merge")
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_2


def test_dest_has_no_merge_fields():
    fields = set(typing.get_type_hints(AwsGlueDest))
    assert not {"merge_cfg", "watermark_column"} & fields


@pytest.mark.parametrize(
    "table", ["Orders", "sales.Orders", "glue.sales.orders", "`orders`", "my-orders"]
)
def test_dest_rejects_an_invalid_table_name(table):
    with pytest.raises(ConnInitException) as raised:
        AwsGlueDest(tables=[table])
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_2
    assert "AWSGLUE-4" in str(raised.value)


@pytest.mark.parametrize(
    "tables", [["orders", "orders"], ["sales.orders", "sales.orders"]]
)
def test_dest_rejects_a_table_listed_twice(tables):
    with pytest.raises(ConnInitException) as raised:
        AwsGlueDest(tables=tables)
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_2


def test_dest_keeps_the_same_table_name_in_two_databases():
    dest = AwsGlueDest(tables=["sales.orders", "returns.orders"])
    assert dest.tables == ["sales.orders", "returns.orders"]


def test_dest_rejects_an_unknown_schema_evolution():
    with pytest.raises(ConnInitException) as raised:
        AwsGlueDest(tables=["orders"], schema_evolution="relaxed")
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_2


def test_dest_rejects_an_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as raised:
        AwsGlueDest(tables=["orders"], dest_cfg={"bogus.key": 1})
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_2


def test_dest_accepts_the_known_dest_cfg_key():
    dest = AwsGlueDest(tables=["orders"], dest_cfg={"awsglue.logging_level": 20})
    assert dest.dest_cfg == {"awsglue.logging_level": 20}


# --- Help ---------------------------------------------------------------


@pytest.mark.parametrize("described", [AwsGlueDest, AwsGlueDestConn])
def test_the_help_says_the_tables_can_be_queried_with_athena(described):
    assert "Amazon Athena" in (described.__doc__ or "")


def test_the_module_help_says_the_tables_can_be_queried_with_athena():
    import tabsdatak.conn.awsglue as module

    assert "Amazon Athena" in (module.__doc__ or "")
