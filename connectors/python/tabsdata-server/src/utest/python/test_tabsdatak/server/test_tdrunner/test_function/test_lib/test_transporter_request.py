#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import Any

import pytest
import yaml

from tabsdatak.common._transporter.handler.handler1 import (
    AzureConfigs,
    AzureEndpoint,
    CopyPair,
    EnvRef,
    GCSConfigs,
    GCSEndpoint,
    LiteralRef,
    LocalFileEndpoint,
    S3Configs,
    S3Endpoint,
    TransporterCopyRequest,
    UnsupportedSchemeError,
    make_dumper,
)
from tabsdatak.server._tdrunner.function._commons.document.request import (
    Info,
    Location,
    Request,
)
from tabsdatak.server._tdrunner.function._lib.transporter1 import (
    build_bundle_fetch_request,
    endpoint_for,
    write_transporter_request,
)


def dump_transporter_request(request: TransporterCopyRequest) -> str:
    return yaml.dump(request, Dumper=make_dumper(), sort_keys=False)


pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def _info(**overrides: Any) -> Info:
    fields: dict[str, Any] = {
        "project_id": "P-1",
        "project": "p",
        "collection_id": 1,
        "collection": "c",
        "function_id": "F-1",
        "function": "fn",
        "function_version_id": "FV-1",
        "function_defined_on": 1780000000000,
        "function_bundle": Location(uri="file:///bundle.tar.zst"),
        "function_stage": Location(uri="file:///stage"),
        "execution_id": 2,
        "execution": None,
        "transaction_id": 3,
        "logical_function_run_id": 4,
        "physical_function_run_id": 4,
        "triggered_on": 1780000000100,
        "function_data": Location(uri="file:///data"),
        "function_scratch": Location(uri="file:///data/s"),
        "scheduled_on": 1780000000200,
    }
    fields.update(overrides)
    return Info(**fields)


def _request(*, info: Info | None = None) -> Request:
    return Request(info=info or _info())


# --- endpoint mapping by scheme ---


@pytest.mark.parametrize("uri", ["s3://b/x.parquet"])
def test_s3_endpoint(uri):
    src = endpoint_for(Location(uri=uri))
    assert isinstance(src, S3Endpoint)
    assert src.url == uri
    assert src.configs.access_key == EnvRef(name="AWS_ACCESS_KEY_ID")
    assert src.configs.secret_key == EnvRef(name="AWS_SECRET_ACCESS_KEY")
    assert src.configs.region == EnvRef(name="AWS_REGION")


@pytest.mark.parametrize(
    "uri",
    ["az://acct/cont/x.parquet", "abfs://c/x", "abfss://c/x"],
)
def test_azure_endpoint(uri):
    src = endpoint_for(Location(uri=uri))
    assert isinstance(src, AzureEndpoint)
    assert src.url == uri
    assert src.configs.account_name == EnvRef(name="AZURE_ACCOUNT_NAME")
    assert src.configs.account_key == EnvRef(name="AZURE_ACCOUNT_KEY")


def test_gcs_endpoint():
    src = endpoint_for(Location(uri="gs://bucket/x.parquet"))
    assert isinstance(src, GCSEndpoint)
    assert src.configs.service_account_key == EnvRef(name="GCP_SERVICE_ACCOUNT_KEY")


def test_local_file_endpoint():
    src = endpoint_for(Location(uri="file:///foo/x.parquet"))
    assert isinstance(src, LocalFileEndpoint)
    assert src.url == "file:///foo/x.parquet"


def test_unsupported_scheme_raises():
    with pytest.raises(UnsupportedSchemeError, match="ftp"):
        endpoint_for(Location(uri="ftp://x/y.parquet"))


# --- env_prefix handling ---


def test_env_prefix_canonical_when_none():
    src = endpoint_for(Location(uri="s3://b/x.parquet", env_prefix=None))
    assert src.configs.access_key == EnvRef(name="AWS_ACCESS_KEY_ID")


def test_env_prefix_prepended():
    src = endpoint_for(Location(uri="s3://b/x.parquet", env_prefix="STAGE"))
    assert src.configs.access_key == EnvRef(name="STAGE_AWS_ACCESS_KEY_ID")
    assert src.configs.secret_key == EnvRef(name="STAGE_AWS_SECRET_ACCESS_KEY")
    assert src.configs.region == EnvRef(name="STAGE_AWS_REGION")


# --- bundle fetch ---


def test_build_bundle_fetch_request_local(tmp_path):
    """Default _request() helper uses a file:// bundle URI."""
    r = _request()
    target = tmp_path / "bundle.tar.zst"
    out = build_bundle_fetch_request(r, target)
    assert len(out.pairs) == 1
    assert isinstance(out.pairs[0].source, LocalFileEndpoint)
    assert out.pairs[0].source.url == "file:///bundle.tar.zst"
    assert isinstance(out.pairs[0].target, LocalFileEndpoint)
    assert "bundle.tar.zst" in out.pairs[0].target.url


def test_build_bundle_fetch_request_s3(tmp_path):
    r = _request(
        info=_info(
            function_bundle=Location(uri="s3://b/bundle.tar.zst", env_prefix="STAGE"),
        ),
    )
    target = tmp_path / "bundle.tar.zst"
    out = build_bundle_fetch_request(r, target)
    src = out.pairs[0].source
    assert isinstance(src, S3Endpoint)
    assert src.url == "s3://b/bundle.tar.zst"
    assert src.configs.access_key == EnvRef(name="STAGE_AWS_ACCESS_KEY_ID")


# --- YAML output (golden checks) ---


def test_yaml_emits_copy_v1_tag(tmp_path):
    r = _request(info=_info(function_bundle=Location(uri="s3://b/bundle.tar.zst")))
    req = build_bundle_fetch_request(r, tmp_path / "bundle.tar.zst")
    text = dump_transporter_request(req)
    assert text.startswith("!CopyV1")
    assert "!S3" in text
    assert "!LocalFile" in text
    assert "!Env" in text


def test_yaml_round_loadable_with_safe_load_unknown_tags_disabled(tmp_path):
    """yaml.safe_load (no constructors) refuses our custom tags — proves we
    really emit them rather than plain mappings."""
    req = TransporterCopyRequest(
        pairs=(
            CopyPair(
                source=S3Endpoint(
                    url="s3://b/x.parquet",
                    configs=S3Configs(
                        access_key=EnvRef(name="AWS_ACCESS_KEY_ID"),
                        secret_key=EnvRef(name="AWS_SECRET_ACCESS_KEY"),
                        region=EnvRef(name="AWS_REGION"),
                    ),
                ),
                target=LocalFileEndpoint(url="file:///tmp/x.parquet"),
            ),
        )
    )
    text = dump_transporter_request(req)
    with pytest.raises(yaml.YAMLError):
        yaml.safe_load(text)


def test_yaml_minimal_request_golden(tmp_path):
    """Empty request renders to !CopyV1 with empty pairs and null parallelism."""
    req = TransporterCopyRequest(pairs=())
    text = dump_transporter_request(req)
    assert text == ("!CopyV1\nsource_target_pairs: []\nparallelism: null\n")


def test_yaml_single_pair_golden(tmp_path):
    req = TransporterCopyRequest(
        pairs=(
            CopyPair(
                source=S3Endpoint(
                    url="s3://b/x.parquet",
                    configs=S3Configs(
                        access_key=EnvRef(name="AWS_ACCESS_KEY_ID"),
                        secret_key=EnvRef(name="AWS_SECRET_ACCESS_KEY"),
                        region=EnvRef(name="AWS_REGION"),
                    ),
                ),
                target=LocalFileEndpoint(url="file:///tmp/x.parquet"),
            ),
        )
    )
    text = dump_transporter_request(req)
    expected = (
        "!CopyV1\n"
        "source_target_pairs:\n"
        "- - !S3\n"
        "    url: s3://b/x.parquet\n"
        "    configs:\n"
        "      access_key: !Env 'AWS_ACCESS_KEY_ID'\n"
        "      secret_key: !Env 'AWS_SECRET_ACCESS_KEY'\n"
        "      region: !Env 'AWS_REGION'\n"
        "      extra_configs: {}\n"
        "  - !LocalFile\n"
        "    url: file:///tmp/x.parquet\n"
        "parallelism: null\n"
    )
    assert text == expected


def test_yaml_emits_literal_ref_tag():
    req = TransporterCopyRequest(
        pairs=(
            CopyPair(
                source=S3Endpoint(
                    url="s3://b/x",
                    configs=S3Configs(
                        access_key=LiteralRef(value="AKIA..."),
                        secret_key=LiteralRef(value="..."),
                        region=LiteralRef(value="us-east-1"),
                    ),
                ),
                target=LocalFileEndpoint(url="file:///tmp/x"),
            ),
        )
    )
    text = dump_transporter_request(req)
    assert "!Literal" in text
    assert "AKIA..." in text


def test_yaml_emits_azure_and_gcs_tags():
    req = TransporterCopyRequest(
        pairs=(
            CopyPair(
                source=AzureEndpoint(
                    url="az://c/x",
                    configs=AzureConfigs(
                        account_name=EnvRef(name="AZURE_ACCOUNT_NAME"),
                        account_key=EnvRef(name="AZURE_ACCOUNT_KEY"),
                    ),
                ),
                target=LocalFileEndpoint(url="file:///t"),
            ),
            CopyPair(
                source=GCSEndpoint(
                    url="gs://b/x",
                    configs=GCSConfigs(
                        service_account_key=EnvRef(name="GCP_SERVICE_ACCOUNT_KEY"),
                    ),
                ),
                target=LocalFileEndpoint(url="file:///t2"),
            ),
        )
    )
    text = dump_transporter_request(req)
    assert "!Azure" in text
    assert "!GCS" in text


# --- write_transporter_request ---


def test_write_transporter_request(tmp_path):
    req = TransporterCopyRequest(pairs=())
    out = tmp_path / "transporter.yaml"
    write_transporter_request(req, out)
    assert out.read_text().startswith("!CopyV1")


def test_write_creates_missing_parent_dirs(tmp_path):
    # Under the publisher init-container chain, prep-* commands write into
    # `shared/shuttle/request/` which starts empty (overlaid by an emptyDir);
    # the writer creates the chain on demand rather than raising OSError.
    req = TransporterCopyRequest(pairs=())
    out = tmp_path / "no" / "such" / "x.yaml"
    write_transporter_request(req, out)
    assert out.read_text().startswith("!CopyV1")


# --- frozen ---


def test_request_is_frozen():
    req = TransporterCopyRequest(pairs=())
    with pytest.raises(Exception):
        req.parallelism = 4  # type: ignore[misc]


def test_endpoint_is_frozen():
    ep = S3Endpoint(url="s3://b/x")
    with pytest.raises(Exception):
        ep.url = "s3://other"  # type: ignore[misc]


def test_configs_is_frozen():
    cfg = S3Configs(access_key=EnvRef(name="X"))
    with pytest.raises(Exception):
        cfg.access_key = EnvRef(name="Y")  # type: ignore[misc]
