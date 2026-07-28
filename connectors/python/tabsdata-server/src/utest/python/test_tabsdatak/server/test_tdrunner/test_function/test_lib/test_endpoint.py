#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest
from pydantic import ValidationError

from tabsdatak.server._tdrunner.function._lib.endpoint import (
    AzureConfigs,
    AzureEndpoint,
    EnvRef,
    GCSConfigs,
    GCSEndpoint,
    LocalFileEndpoint,
    S3Configs,
    S3Endpoint,
    UnsupportedSchemeError,
    endpoint_from_location,
)
from tabsdatak.server._tdrunner.function._lib.request import Location

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- per-scheme resolution ---


def test_s3_uri_resolves_to_s3_endpoint_with_aws_creds():
    ep = endpoint_from_location(
        Location(uri="s3://bucket/path/file.parquet", env_prefix=None)
    )
    assert isinstance(ep, S3Endpoint)
    assert ep.url == "s3://bucket/path/file.parquet"
    assert ep.configs.access_key == EnvRef(name="AWS_ACCESS_KEY_ID")
    assert ep.configs.secret_key == EnvRef(name="AWS_SECRET_ACCESS_KEY")
    assert ep.configs.region == EnvRef(name="AWS_REGION")


def test_s3_uri_with_env_prefix_namespaces_creds():
    ep = endpoint_from_location(
        Location(uri="s3://bucket/x", env_prefix="PROD_AWS")
    )
    assert isinstance(ep, S3Endpoint)
    assert ep.configs.access_key == EnvRef(name="PROD_AWS_AWS_ACCESS_KEY_ID")
    assert ep.configs.secret_key == EnvRef(name="PROD_AWS_AWS_SECRET_ACCESS_KEY")
    assert ep.configs.region == EnvRef(name="PROD_AWS_AWS_REGION")


@pytest.mark.parametrize("scheme", ["az", "abfs", "abfss"])
def test_azure_schemes_resolve_to_azure_endpoint(scheme):
    ep = endpoint_from_location(
        Location(uri=f"{scheme}://container/file.parquet", env_prefix=None)
    )
    assert isinstance(ep, AzureEndpoint)
    assert ep.url == f"{scheme}://container/file.parquet"
    assert ep.configs.account_name == EnvRef(name="AZURE_ACCOUNT_NAME")
    assert ep.configs.account_key == EnvRef(name="AZURE_ACCOUNT_KEY")


def test_azure_with_env_prefix():
    ep = endpoint_from_location(
        Location(uri="abfss://container/x", env_prefix="STAGE")
    )
    assert isinstance(ep, AzureEndpoint)
    assert ep.configs.account_name == EnvRef(name="STAGE_AZURE_ACCOUNT_NAME")
    assert ep.configs.account_key == EnvRef(name="STAGE_AZURE_ACCOUNT_KEY")


def test_gs_uri_resolves_to_gcs_endpoint_with_gcp_creds():
    ep = endpoint_from_location(
        Location(uri="gs://bucket/file.parquet", env_prefix=None)
    )
    assert isinstance(ep, GCSEndpoint)
    assert ep.url == "gs://bucket/file.parquet"
    assert ep.configs.service_account_key == EnvRef(name="GCP_SERVICE_ACCOUNT_KEY")


def test_gs_with_env_prefix():
    ep = endpoint_from_location(Location(uri="gs://bucket/x", env_prefix="PROD"))
    assert isinstance(ep, GCSEndpoint)
    assert ep.configs.service_account_key == EnvRef(
        name="PROD_GCP_SERVICE_ACCOUNT_KEY"
    )


def test_file_uri_resolves_to_local_endpoint_without_creds():
    ep = endpoint_from_location(Location(uri="file:///tmp/x.parquet"))
    assert isinstance(ep, LocalFileEndpoint)
    assert ep.url == "file:///tmp/x.parquet"


# --- unsupported schemes ---


@pytest.mark.parametrize(
    "uri",
    [
        "http://example.com/x",
        "ftp://host/x",
        "hdfs://nn/x",
        "no-scheme-at-all",
        "",
    ],
)
def test_unsupported_scheme_raises(uri):
    with pytest.raises(UnsupportedSchemeError, match="unsupported URI scheme"):
        endpoint_from_location(Location(uri=uri))


# --- type-level invariants ---


def test_s3_endpoint_default_configs_is_empty():
    ep = S3Endpoint(url="s3://b/x")
    assert ep.configs == S3Configs()
    assert ep.configs.access_key is None
    assert ep.configs.extra_configs == {}


def test_azure_endpoint_default_configs_is_empty():
    ep = AzureEndpoint(url="az://c/x")
    assert ep.configs == AzureConfigs()
    assert ep.configs.account_name is None


def test_gcs_endpoint_default_configs_is_empty():
    ep = GCSEndpoint(url="gs://b/x")
    assert ep.configs == GCSConfigs()
    assert ep.configs.service_account_key is None


def test_endpoint_is_frozen():
    ep = S3Endpoint(url="s3://b/x")
    with pytest.raises(ValidationError):
        ep.url = "s3://other/y"  # type: ignore[misc]


def test_configs_is_frozen():
    c = S3Configs()
    with pytest.raises(ValidationError):
        c.access_key = EnvRef(name="x")  # type: ignore[misc]


def test_extra_field_on_endpoint_is_forbidden():
    with pytest.raises(ValidationError, match="surprise"):
        S3Endpoint(url="s3://b/x", surprise="x")  # type: ignore[call-arg]


def test_extra_field_on_configs_is_forbidden():
    with pytest.raises(ValidationError, match="surprise"):
        S3Configs(surprise="x")  # type: ignore[call-arg]
