#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tabsdatak.common._storage import mounts
from tabsdatak.common._storage.mounts import (
    AnonymousMountDeclaration,
    MountError,
    NamedMountDeclaration,
)
from tabsdatak.common._storage.transfer import (
    S3_SIGNATURE_REQUIREMENT,
    StoreKind,
    TransferError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

PLACEHOLDER = "${{secret}}"
OPTIONAL_PLACEHOLDER = "${{secret?}}"


def storage_config(folder: Path, *declared: dict) -> Path:
    path = folder / "apiserver.storage.yaml"
    path.write_text(yaml.safe_dump({"storage": {"mounts": list(declared)}}))
    return path


def option_variable(mount: str, key: str) -> str:
    return mounts.secret_variable("storage", "mounts", mount, "options", key)


@pytest.mark.parametrize(
    "segment, encoded",
    [
        ("region", "REGION"),
        ("azure_storage_account_key", "AZURE_STORAGE_ACCOUNT_KEY"),
        ("my-key.name", "MY_KEY_NAME"),
        ("0day", "_0DAY"),
    ],
)
def test_segments_encode_like_the_kubernetes_secret_keys(segment: str, encoded: str):
    assert mounts.encode_segment(segment) == encoded


def test_variables_follow_the_secret_naming():
    assert mounts.secret_variable("storage", "mounts", "IMAGES", "uri") == (
        "SECRETS__STORAGE__MOUNTS__IMAGES__URI"
    )
    assert option_variable("IMAGES", "region") == (
        "SECRETS__STORAGE__MOUNTS__IMAGES__OPTIONS__REGION"
    )


@pytest.mark.parametrize(
    "text",
    ["${{secret}}", "${{secret?}}", "${{secret:name}}", "${{secretAuto:password}}"],
)
def test_placeholders_follow_the_kubernetes_grammar(text: str):
    assert mounts.PLACEHOLDER_PATTERN.match(text)


@pytest.mark.parametrize(
    "text",
    ["${{secret", "secret}}", "${{other}}", "prefix ${{secret}}", "s3://bucket"],
)
def test_other_values_are_literals(text: str):
    assert mounts.resolve(text, "storage", "mounts", "ROOT", "uri") == text


def test_read_returns_the_declared_mount(tmp_path: Path):
    config = storage_config(
        tmp_path,
        {"id": "ROOT", "uri": "s3://root"},
        {"id": "IMAGES", "uri": PLACEHOLDER, "options": {"region": "eu-west-1"}},
    )
    assert NamedMountDeclaration.read(config, "IMAGES") == NamedMountDeclaration(
        id="IMAGES",
        uri=PLACEHOLDER,
        options={"region": "eu-west-1"},
    )


def test_read_all_returns_every_identified_mount(tmp_path: Path):
    config = storage_config(
        tmp_path,
        {"id": "ROOT", "uri": "s3://root"},
        {"uri": "s3://anonymous"},
        {"id": "IMAGES", "uri": PLACEHOLDER, "options": {"region": "eu-west-1"}},
    )
    assert NamedMountDeclaration.read_all(config) == [
        NamedMountDeclaration(id="ROOT", uri="s3://root"),
        NamedMountDeclaration(
            id="IMAGES", uri=PLACEHOLDER, options={"region": "eu-west-1"}
        ),
    ]


def test_read_of_an_undeclared_mount_fails(tmp_path: Path):
    config = storage_config(tmp_path, {"id": "IMAGES", "uri": "s3://images"})
    with pytest.raises(MountError, match="not declared"):
        NamedMountDeclaration.read(config, "LOGS")


def test_read_of_an_unreadable_configuration_fails(tmp_path: Path):
    with pytest.raises(MountError, match="Cannot read"):
        NamedMountDeclaration.read(tmp_path / "missing.yaml", "IMAGES")


def test_literal_uri_is_used_as_written():
    declaration = NamedMountDeclaration(id="IMAGES", uri="s3://images/prod")
    assert declaration.resolved_uri() == "s3://images/prod"


def test_placeholder_uri_comes_from_its_variable(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SECRETS__STORAGE__MOUNTS__BUNDLES__URI", "az://bundles/one")
    declaration = NamedMountDeclaration(id="BUNDLES", uri=PLACEHOLDER)
    assert declaration.resolved_uri() == "az://bundles/one"


def test_placeholder_uri_without_variable_fails(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("SECRETS__STORAGE__MOUNTS__BUNDLES__URI", raising=False)
    declaration = NamedMountDeclaration(id="BUNDLES", uri=PLACEHOLDER)
    with pytest.raises(MountError, match="storage.mounts.BUNDLES.uri"):
        declaration.resolved_uri()


def test_mount_without_uri_fails():
    with pytest.raises(MountError, match="declares no uri"):
        NamedMountDeclaration(id="IMAGES", uri="").resolved_uri()


@pytest.mark.parametrize(
    "location, kind",
    [
        ("s3://bucket/key", StoreKind.S3),
        ("az://container/key", StoreKind.AZURE),
        ("abfss://container@account/key", StoreKind.AZURE),
        ("gs://bucket/key", StoreKind.GCS),
        ("file:///tmp/key", StoreKind.LOCAL),
    ],
)
def test_store_kind_comes_from_the_scheme(location: str, kind: StoreKind):
    assert StoreKind.of(location) == kind


def test_unsupported_scheme_fails():
    with pytest.raises(TransferError, match="unsupported scheme"):
        StoreKind.of("ftp://host/key")


def test_a_mount_on_an_unsupported_scheme_fails():
    with pytest.raises(MountError, match="unsupported scheme"):
        NamedMountDeclaration(id="ROOT", uri="ftp://host").storage_options(
            "ftp://host/key"
        )


def test_friendly_and_prefixed_keys_become_canonical():
    assert StoreKind.S3.canonical("region") == "aws_region"
    assert StoreKind.S3.canonical("anonymous") == "aws_skip_signature"
    assert StoreKind.AZURE.canonical("azure_endpoint") == "azure_endpoint"
    assert StoreKind.GCS.canonical("allow_http") == "google_allow_http"


def test_unknown_key_fails():
    with pytest.raises(TransferError, match="not a known s3 option"):
        StoreKind.S3.canonical("colour")


def test_a_mount_with_an_unknown_key_fails():
    declaration = NamedMountDeclaration(
        id="ROOT", uri="s3://root", options={"colour": "blue"}
    )
    with pytest.raises(MountError, match="not a known s3 option"):
        declaration.storage_options("s3://root/key")


def test_options_mix_placeholders_and_literals(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(option_variable("IMAGES", "access_key_id"), "identity")
    monkeypatch.setenv(option_variable("IMAGES", "secret_access_key"), "secret")
    declaration = NamedMountDeclaration(
        id="IMAGES",
        uri="s3://images",
        options={
            "access_key_id": PLACEHOLDER,
            "secret_access_key": PLACEHOLDER,
            "region": "eu-west-1",
        },
    )
    assert declaration.storage_options("s3://images/python/image.tar.zst") == {
        "aws_access_key_id": "identity",
        "aws_secret_access_key": "secret",
        "aws_region": "eu-west-1",
    }


def test_unset_optional_placeholder_drops_the_option(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv(option_variable("ROOT", "endpoint"), raising=False)
    declaration = NamedMountDeclaration(
        id="ROOT",
        uri="s3://root",
        options={
            "anonymous": "true",
            "region": "us-east-1",
            "endpoint": OPTIONAL_PLACEHOLDER,
        },
    )
    assert declaration.storage_options("s3://root/key") == {
        "aws_skip_signature": "true",
        "aws_region": "us-east-1",
    }


def test_unset_mandatory_placeholder_fails(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv(option_variable("IMAGES", "secret_access_key"), raising=False)
    declaration = NamedMountDeclaration(
        id="IMAGES",
        uri="s3://images",
        options={"secret_access_key": PLACEHOLDER},
    )
    with pytest.raises(MountError, match="IMAGES.options.secret_access_key"):
        declaration.storage_options("s3://images/key")


@pytest.mark.parametrize(
    "location, declared, requirement",
    [
        ("s3://bucket/key", {"region": "eu-west-1"}, "access_key_id"),
        (
            "s3://bucket/key",
            {"access_key_id": "identity", "secret_access_key": "secret"},
            "region or endpoint",
        ),
        ("az://container/key", {"account_key": "key"}, "account_name"),
        ("az://container/key", {"account_name": "account"}, "account_key"),
        ("gs://bucket/key", {"allow_http": "true"}, "service_account_key"),
    ],
)
def test_missing_credentials_fail(location: str, declared: dict, requirement: str):
    declaration = NamedMountDeclaration(id="ROOT", uri=location, options=declared)
    with pytest.raises(MountError, match=requirement):
        declaration.storage_options(location)


def test_local_mount_needs_no_options(tmp_path: Path):
    declaration = NamedMountDeclaration(id="ROOT", uri=tmp_path.as_uri())
    assert declaration.storage_options((tmp_path / "key").as_uri()) == {}


@pytest.mark.parametrize(
    "location",
    ["s3://bucket/key", "az://container/key", "gs://bucket/key", "file:///data/key"],
)
def test_an_anonymous_mount_contributes_no_options(location: str):
    assert AnonymousMountDeclaration().storage_options(location) == {}


def test_read_turns_scalars_into_text(tmp_path: Path):
    config = storage_config(
        tmp_path,
        {"id": "ROOT", "uri": "s3://root", "options": {"allow_http": True, "port": 9}},
    )
    assert NamedMountDeclaration.read(config, "ROOT").options == {
        "allow_http": "true",
        "port": "9",
    }


def test_the_scan_finds_every_option_of_a_mount():
    environment = {
        option_variable("ROOT", "aws_access_key_id"): "k",
        option_variable("ROOT", "aws_endpoint_url"): "http://127.0.0.1:9000",
        option_variable("BUNDLES", "aws_region"): "eu-west-1",
        mounts.secret_variable("storage", "mounts", "ROOT", "uri"): "s3://b",
        "AWS_REGION": "us-east-1",
    }
    assert mounts.mount_option_variables("ROOT", environment) == {
        "aws_access_key_id": option_variable("ROOT", "aws_access_key_id"),
        "aws_endpoint_url": option_variable("ROOT", "aws_endpoint_url"),
    }


def test_the_scan_encodes_the_mount_like_the_secret_keys():
    environment = {option_variable("my-mount", "region"): "eu-west-1"}
    assert mounts.mount_option_variables("my-mount", environment) == {
        "region": "SECRETS__STORAGE__MOUNTS__MY_MOUNT__OPTIONS__REGION"
    }


def test_a_mount_without_variables_has_no_options():
    assert mounts.mount_option_variables("ROOT", {}) == {}


def test_an_environment_mount_resolves_canonical_options(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(option_variable("ROOT", "access_key_id"), " k ")
    monkeypatch.setenv(option_variable("ROOT", "aws_secret_access_key"), "s")
    monkeypatch.setenv(option_variable("ROOT", "endpoint"), "http://127.0.0.1:9000")
    declaration = mounts.EnvironmentMountDeclaration(id="ROOT")
    assert declaration.storage_options("s3://b/x") == {
        "aws_access_key_id": "k",
        "aws_secret_access_key": "s",
        "aws_endpoint": "http://127.0.0.1:9000",
    }


def test_an_environment_mount_without_credentials_fails(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(option_variable("ROOT", "aws_region"), "eu-west-1")
    with pytest.raises(MountError, match=S3_SIGNATURE_REQUIREMENT):
        mounts.EnvironmentMountDeclaration(id="ROOT").storage_options("s3://b/x")


def test_a_local_environment_mount_needs_no_options():
    declaration = mounts.EnvironmentMountDeclaration(id="ROOT")
    assert declaration.storage_options("file:///data/x") == {}
