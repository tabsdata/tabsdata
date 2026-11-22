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


def test_azure_table_function_passes_the_key_as_its_own_argument() -> None:
    """ClickHouse hides a key passed as its own argument in its query log;
    one inside a connection string is logged in clear."""
    storage = AzureClickHouseStorage(
        AzureConfig(
            container="staging-container",
            base_path="staging",
            account_name="ACCT",
            account_key="KEY==",
        )
    )
    fn = storage.clickhouse_table_function("staging/UID/x.parquet")
    assert fn == (
        "azureBlobStorage("
        "'https://ACCT.blob.core.windows.net', "
        "'staging-container', "
        "'staging/UID/x.parquet', "
        "'ACCT', "
        "'KEY==', "
        "'Parquet')"
    )
    assert "AccountKey" not in fn


def test_table_function_arguments_are_escaped() -> None:
    from tabsdatak.conn.clickhouse._cloud_storage import table_function

    assert table_function("s3", "a\\b", "it's") == "s3('a\\\\b', 'it\\'s')"


# ---------------------------------------------------------------------------
# obstore.* options: the upload store and the address ClickHouse reads from
# ---------------------------------------------------------------------------


def _s3(**options) -> S3ClickHouseStorage:
    return S3ClickHouseStorage(
        S3Config(
            bucket="b",
            region="eu-west-1",
            base_path="staging",
            access_key_id="AK",
            secret_access_key="SK",
            options=options,
        )
    )


def _azure(**options) -> AzureClickHouseStorage:
    return AzureClickHouseStorage(
        AzureConfig(
            container="c",
            base_path="staging",
            account_name="ACCT",
            account_key="KEY",
            options=options,
        )
    )


def test_s3_reads_from_aws_without_an_endpoint():
    assert _s3().staged_ref("k/x.parquet") == (
        "https://b.s3.eu-west-1.amazonaws.com/k/x.parquet"
    )


def test_s3_reads_from_a_custom_endpoint_path_style():
    storage = _s3(aws_endpoint="http://minio:9000/", aws_allow_http="true")
    assert storage.staged_ref("k/x.parquet") == "http://minio:9000/b/k/x.parquet"
    assert storage.store().options["aws_endpoint"] == "http://minio:9000/"
    assert storage.store().options["aws_allow_http"] == "true"


def test_s3_reads_from_a_custom_endpoint_virtual_hosted():
    storage = _s3(
        aws_endpoint="https://storage.example.com",
        aws_virtual_hosted_style_request="true",
    )
    assert storage.staged_ref("k/x.parquet") == (
        "https://b.storage.example.com/k/x.parquet"
    )


def test_azure_reads_from_a_custom_endpoint():
    storage = _azure(azure_storage_endpoint="http://azurite:10000/devstoreaccount1")
    assert storage.clickhouse_table_function("k/x.parquet").startswith(
        "azureBlobStorage('http://azurite:10000/devstoreaccount1', 'c', "
    )
    assert storage.store().options["azure_storage_endpoint"] == (
        "http://azurite:10000/devstoreaccount1"
    )


def test_cfg_obstore_keys_reach_the_storage():
    from tabsdatak.api import Secret
    from tabsdatak.conn.clickhouse import S3Location
    from tabsdatak.conn.clickhouse._impl import build_cloud_storage
    from tabsdatak.conn.common.types import AwsAccessSecretKey

    storage = build_cloud_storage(
        S3Location(
            bucket="my-bucket",
            region="eu-west-1",
            credentials=AwsAccessSecretKey(
                access_key_id=Secret("AK"), secret_access_key=Secret("SK")
            ),
            cfg={"obstore.endpoint": "http://minio:9000", "ignored": 1},
        )
    )
    assert storage.staged_ref("k") == "http://minio:9000/my-bucket/k"


def test_the_cleanup_deletes_only_its_own_folder(tmp_path):
    import polars as pl

    from tabsdatak.conn.common.file._store import FileStore

    class _Local(S3ClickHouseStorage):
        def store(self):
            return FileStore(tmp_path.as_uri())

    storage = _Local(
        S3Config(
            bucket="b",
            region="r",
            base_path="staging",
            access_key_id="AK",
            secret_access_key="SK",
        )
    )
    frame = pl.LazyFrame({"id": [1]})
    storage.upload("fn1", [("t1_0.parquet", frame), ("t1_1.parquet", frame)])
    storage.upload("fn10", [("t2_0.parquet", frame)])
    storage.upload("fn2", [("t3_0.parquet", frame)])
    storage.delete("fn1")
    left = sorted(
        path.relative_to(tmp_path).as_posix()
        for path in tmp_path.rglob("*")
        if path.is_file()
    )
    assert left == ["staging/fn10/t2_0.parquet", "staging/fn2/t3_0.parquet"]
