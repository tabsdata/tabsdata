#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import pytest
import yaml

from tabsdatak.server._tdrunner.function._lib.endpoint import (
    AzureConfigs,
    AzureEndpoint,
    EnvRef,
    GcsConfigs,
    GcsEndpoint,
    LiteralRef,
    LocalFileEndpoint,
    S3Configs,
    S3Endpoint,
    UnsupportedSchemeError,
)
from tabsdatak.server._tdrunner.function._lib.request import (
    Info,
    Location,
    Request,
    Table,
)
from tabsdatak.server._tdrunner.function._lib.transporter_request import (
    CopyPair,
    TransporterCopyRequest,
    build_bundle_fetch_request,
    build_localize_state_request,
    build_upload_state_request,
    dump_transporter_request,
    write_transporter_request,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def _request(
    *,
    inputs: tuple[Table, ...] = (),
    outputs: tuple[Table, ...] = (),
    system_input: tuple[Table, ...] = (),
    system_output: tuple[Table, ...] = (),
) -> Request:
    return Request(
        info=Info(
            collection="c",
            collection_id=1,
            execution_id=2,
            transaction_id=3,
            function_run_id=4,
            function_bundle=Location(uri="file:///bundle.tgz"),
        ),
        input=inputs,
        output=outputs,
        system_input=system_input,
        system_output=system_output,
    )


# --- builders: shape ---


def test_localize_empty_request_yields_no_pairs(tmp_path):
    r = _request()
    out = build_localize_state_request(r, tmp_path / "in", tmp_path / "state")
    assert out.pairs == ()


def test_upload_empty_request_yields_no_pairs(tmp_path):
    r = _request()
    out = build_upload_state_request(r, tmp_path / "state")
    assert out.pairs == ()


def test_localize_skips_system_input_table_without_location(tmp_path):
    r = _request(
        system_input=(
            Table(name="t1", location=Location(uri="s3://b/t1.parquet")),
            Table(name="t2", location=None),
        )
    )
    out = build_localize_state_request(r, tmp_path / "in", tmp_path / "state")
    assert len(out.pairs) == 1
    assert isinstance(out.pairs[0].source, S3Endpoint)


def test_localize_skips_input_table_without_location(tmp_path):
    r = _request(
        inputs=(
            Table(name="i1", location=None),
            Table(name="i2", location=Location(uri="s3://b/i2.parquet")),
        )
    )
    out = build_localize_state_request(r, tmp_path / "in", tmp_path / "state")
    assert len(out.pairs) == 1
    assert "in/i2.parquet" in out.pairs[0].target.url


def test_upload_skips_table_without_location(tmp_path):
    r = _request(
        system_output=(
            Table(name="t1", location=Location(uri="s3://b/t1.parquet")),
            Table(name="t2", location=None),
        )
    )
    out = build_upload_state_request(r, tmp_path / "state")
    assert len(out.pairs) == 1


def test_localize_handles_input_and_system_input_in_order(tmp_path):
    r = _request(
        inputs=(Table(name="i1", location=Location(uri="s3://b/i1.parquet")),),
        system_input=(Table(name="s1", location=Location(uri="s3://b/s1.parquet")),),
    )
    out = build_localize_state_request(r, tmp_path / "in", tmp_path / "state")
    assert len(out.pairs) == 2
    assert "in/i1.parquet" in out.pairs[0].target.url
    assert "state/s1.parquet" in out.pairs[1].target.url


# --- endpoint mapping by scheme ---


@pytest.mark.parametrize("uri", ["s3://b/x.parquet"])
def test_s3_endpoint(uri, tmp_path):
    r = _request(system_input=(Table(name="t", location=Location(uri=uri)),))
    out = build_localize_state_request(r, tmp_path / "in", tmp_path / "state")
    src = out.pairs[0].source
    assert isinstance(src, S3Endpoint)
    assert src.url == uri
    assert src.configs.access_key == EnvRef(name="AWS_ACCESS_KEY_ID")
    assert src.configs.secret_key == EnvRef(name="AWS_SECRET_ACCESS_KEY")
    assert src.configs.region == EnvRef(name="AWS_REGION")


@pytest.mark.parametrize(
    "uri",
    ["az://acct/cont/x.parquet", "abfs://c/x", "abfss://c/x"],
)
def test_azure_endpoint(uri, tmp_path):
    r = _request(system_input=(Table(name="t", location=Location(uri=uri)),))
    out = build_localize_state_request(r, tmp_path / "in", tmp_path / "state")
    src = out.pairs[0].source
    assert isinstance(src, AzureEndpoint)
    assert src.url == uri
    assert src.configs.account_name == EnvRef(name="AZURE_ACCOUNT_NAME")
    assert src.configs.account_key == EnvRef(name="AZURE_ACCOUNT_KEY")


def test_gcs_endpoint(tmp_path):
    r = _request(
        system_input=(Table(name="t", location=Location(uri="gs://bucket/x.parquet")),)
    )
    out = build_localize_state_request(r, tmp_path / "in", tmp_path / "state")
    src = out.pairs[0].source
    assert isinstance(src, GcsEndpoint)
    assert src.configs.service_account_key == EnvRef(name="GCP_SERVICE_ACCOUNT_KEY")


def test_local_file_endpoint(tmp_path):
    r = _request(
        system_input=(Table(name="t", location=Location(uri="file:///foo/x.parquet")),)
    )
    out = build_localize_state_request(r, tmp_path / "in", tmp_path / "state")
    src = out.pairs[0].source
    assert isinstance(src, LocalFileEndpoint)
    assert src.url == "file:///foo/x.parquet"


def test_unsupported_scheme_raises(tmp_path):
    r = _request(
        system_input=(Table(name="t", location=Location(uri="ftp://x/y.parquet")),)
    )
    with pytest.raises(UnsupportedSchemeError, match="ftp"):
        build_localize_state_request(r, tmp_path / "in", tmp_path / "state")


# --- env_prefix handling ---


def test_env_prefix_canonical_when_none(tmp_path):
    r = _request(
        system_input=(
            Table(
                name="t",
                location=Location(uri="s3://b/x.parquet", env_prefix=None),
            ),
        )
    )
    out = build_localize_state_request(r, tmp_path / "in", tmp_path / "state")
    src = out.pairs[0].source
    assert src.configs.access_key == EnvRef(name="AWS_ACCESS_KEY_ID")


def test_env_prefix_prepended(tmp_path):
    r = _request(
        system_input=(
            Table(
                name="t",
                location=Location(uri="s3://b/x.parquet", env_prefix="STAGE"),
            ),
        )
    )
    out = build_localize_state_request(r, tmp_path / "in", tmp_path / "state")
    src = out.pairs[0].source
    assert src.configs.access_key == EnvRef(name="STAGE_AWS_ACCESS_KEY_ID")
    assert src.configs.secret_key == EnvRef(name="STAGE_AWS_SECRET_ACCESS_KEY")
    assert src.configs.region == EnvRef(name="STAGE_AWS_REGION")


# --- bundle fetch ---


def test_build_bundle_fetch_request_local(tmp_path):
    """Default _request() helper uses a file:// bundle URI."""
    r = _request()
    target = tmp_path / "bundle.tar.gz"
    out = build_bundle_fetch_request(r, target)
    assert len(out.pairs) == 1
    assert isinstance(out.pairs[0].source, LocalFileEndpoint)
    assert out.pairs[0].source.url == "file:///bundle.tgz"
    assert isinstance(out.pairs[0].target, LocalFileEndpoint)
    assert "bundle.tar.gz" in out.pairs[0].target.url


def test_build_bundle_fetch_request_s3(tmp_path):
    r = Request(
        info=Info(
            collection="c",
            collection_id=1,
            execution_id=2,
            transaction_id=3,
            function_run_id=4,
            function_bundle=Location(uri="s3://b/bundle.tgz", env_prefix="STAGE"),
        ),
        input=(),
        output=(),
        system_input=(),
        system_output=(),
    )
    target = tmp_path / "bundle.tar.gz"
    out = build_bundle_fetch_request(r, target)
    src = out.pairs[0].source
    assert isinstance(src, S3Endpoint)
    assert src.url == "s3://b/bundle.tgz"
    assert src.configs.access_key == EnvRef(name="STAGE_AWS_ACCESS_KEY_ID")


# --- upload semantics ---


def test_upload_uses_system_output(tmp_path):
    r = _request(
        system_output=(
            Table(
                name="td-initial-values",
                location=Location(uri="s3://b/state/out.parquet"),
            ),
        )
    )
    out = build_upload_state_request(r, tmp_path / "state")
    pair = out.pairs[0]
    assert isinstance(pair.source, LocalFileEndpoint)
    assert "state/td-initial-values.parquet" in pair.source.url
    assert isinstance(pair.target, S3Endpoint)
    assert pair.target.url == "s3://b/state/out.parquet"


# --- YAML output (golden checks) ---


def test_yaml_emits_copy_v1_tag(tmp_path):
    r = _request(
        system_input=(Table(name="t", location=Location(uri="s3://b/t.parquet")),)
    )
    req = build_localize_state_request(r, tmp_path / "in", tmp_path / "state")
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
                source=GcsEndpoint(
                    url="gs://b/x",
                    configs=GcsConfigs(
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


def test_write_to_missing_parent_raises_oserror(tmp_path):
    req = TransporterCopyRequest(pairs=())
    with pytest.raises(OSError):
        write_transporter_request(req, tmp_path / "no" / "such" / "x.yaml")


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
