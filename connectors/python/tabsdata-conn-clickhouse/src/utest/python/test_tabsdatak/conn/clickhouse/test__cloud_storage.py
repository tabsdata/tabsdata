#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the cloud-storage layer focused on the credential
surface and the ``s3()`` / ``azureBlobStorage()`` table-function
strings each ``ClickHouseCloudStorage`` emits.

The driver-touching call paths (``upload`` / ``delete``) talk to
``boto3`` / ``google-cloud-storage`` / ``azure-storage-blob`` and
are exercised end-to-end in the itest ``test_plugin_dest.py``; here
we cover the deterministic surface that doesn't need a network.
"""

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._cloud_storage import (
    AzureClickHouseStorage,
    GCSClickHouseStorage,
    S3ClickHouseStorage,
    _make_key,
)
# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._storage_config import (
    AzureConfig,
    GCSConfig,
    S3Config,
)


pytestmark = pytest.mark.clickhouse


# ---------------------------------------------------------------------------
# _make_key
# ---------------------------------------------------------------------------


class TestMakeKey:
    def test_full_path(self) -> None:
        assert _make_key("staging", "ABCD", "in.parquet") == (
            "staging/ABCD/in.parquet"
        )

    def test_empty_base_path_no_leading_slash(self) -> None:
        # Regression: empty base_path used to emit a leading slash
        # that tripped GCS / S3 path resolution.
        assert _make_key("", "ABCD", "in.parquet") == (
            "ABCD/in.parquet"
        )

    def test_trims_surrounding_slashes(self) -> None:
        assert _make_key("/staging/", "ABCD", "in.parquet") == (
            "staging/ABCD/in.parquet"
        )

    def test_nested_base_path(self) -> None:
        assert _make_key("a/b/c", "ABCD", "f") == "a/b/c/ABCD/f"


# ---------------------------------------------------------------------------
# S3ClickHouseStorage.clickhouse_table_function
# ---------------------------------------------------------------------------


def test_s3_table_function_inlines_access_secret() -> None:
    storage = S3ClickHouseStorage(
        S3Config(
            bucket="b",
            region="us-east-1",
            base_path="staging",
            access_key_id="AKIA-CREDS",
            secret_access_key="S3-SECRET",
        )
    )
    fn = storage.clickhouse_table_function(
        "https://b.s3.us-east-1.amazonaws.com/staging/UID/x.parquet"
    )
    assert fn == (
        "s3('https://b.s3.us-east-1.amazonaws.com/"
        "staging/UID/x.parquet', "
        "'AKIA-CREDS', 'S3-SECRET', 'Parquet')"
    )


# ---------------------------------------------------------------------------
# GCSClickHouseStorage.clickhouse_table_function
# ---------------------------------------------------------------------------


def test_gcs_table_function_inlines_hmac_only() -> None:
    """ClickHouse reads GCS through the S3-compatible endpoint;
    only the HMAC keys belong in the ``s3()`` table function -- the
    service-account JSON is for the upload path only and must not
    leak into the SQL string."""
    storage = GCSClickHouseStorage(
        GCSConfig(
            bucket="b",
            base_path="staging",
            service_account_json='{"type":"service_account"}',
            hmac_access_key="GOOG-AK",
            hmac_secret_key="GOOG-SK",
        )
    )
    fn = storage.clickhouse_table_function(
        "https://storage.googleapis.com/b/staging/UID/x.parquet"
    )
    assert fn == (
        "s3('https://storage.googleapis.com/b/"
        "staging/UID/x.parquet', "
        "'GOOG-AK', 'GOOG-SK', 'Parquet')"
    )
    assert "service_account" not in fn


# ---------------------------------------------------------------------------
# AzureClickHouseStorage.clickhouse_table_function
# ---------------------------------------------------------------------------


def test_azure_table_function_builds_connection_string() -> None:
    storage = AzureClickHouseStorage(
        AzureConfig(
            container="staging-container",
            base_path="staging",
            account_name="ACCT",
            account_key="KEY==",
        )
    )
    fn = storage.clickhouse_table_function(
        "staging/UID/x.parquet"
    )
    assert fn == (
        "azureBlobStorage("
        "'DefaultEndpointsProtocol=https;"
        "AccountName=ACCT;"
        "AccountKey=KEY==;"
        "EndpointSuffix=core.windows.net', "
        "'staging-container', "
        "'staging/UID/x.parquet', "
        "'Parquet')"
    )
