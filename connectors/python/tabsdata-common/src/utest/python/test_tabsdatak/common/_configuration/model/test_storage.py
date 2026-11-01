#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatak.common._configuration.document.loader import load
from tabsdatak.common._configuration.model.storage import StorageConfiguration
from tabsdatak.common._storage.mounts import MountError, NamedMountDeclaration
from test_tabsdatak.bootest import ROOT_FOLDER

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

KPROFILE_CONFIGS = Path(ROOT_FOLDER) / "variant" / "resources" / "kprofile" / "configs"

DOCUMENT = """
storage:
  mounts:
  - id: ROOT
    path: /
    uri: ${{secret}}
    options:
      access_key_id: ${{secret}}
      secret_access_key: ${{secret}}
      region: eu-west-1
      allow_http: true
      endpoint: ${{secret?}}
  - id: IMAGES
    path: /images
    uri: file:///images
"""


@pytest.fixture
def configuration(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("SECRETS__STORAGE__MOUNTS__ROOT__URI", "s3://root/prefix")
    monkeypatch.setenv(
        "SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__ACCESS_KEY_ID",
        "identity",
    )
    monkeypatch.setenv(
        "SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__SECRET_ACCESS_KEY",
        "secret",
    )
    monkeypatch.delenv(
        "SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__ENDPOINT", raising=False
    )
    path = tmp_path / "apiserver.storage.yaml"
    path.write_text(DOCUMENT)
    return path


def test_declaration_carries_resolved_text_options(configuration: Path):
    root = load(configuration, StorageConfiguration).mount("ROOT")
    assert root.declaration() == NamedMountDeclaration(
        id="ROOT",
        uri="s3://root/prefix",
        options={
            "access_key_id": "identity",
            "secret_access_key": "secret",
            "region": "eu-west-1",
            "allow_http": "true",
        },
    )


def test_typed_and_standalone_readers_agree(configuration: Path):
    typed = load(configuration, StorageConfiguration).mount("ROOT").declaration()
    standalone = NamedMountDeclaration.read(configuration, "ROOT")
    location = "s3://root/prefix/python/image.tar.zst"
    assert typed.resolved_uri() == standalone.resolved_uri()
    assert typed.storage_options(location) == standalone.storage_options(location)


def test_undeclared_mount_fails(configuration: Path):
    with pytest.raises(MountError, match="not declared"):
        load(configuration, StorageConfiguration).mount("LOGS")


@pytest.mark.parametrize(
    "path",
    sorted(KPROFILE_CONFIGS.glob("apiserver.storage_*.yaml")),
    ids=lambda path: path.name,
)
def test_every_shipped_storage_configuration_fits_the_model(path: Path):
    load(path, StorageConfiguration, strict=True, resolve_secrets=False)
