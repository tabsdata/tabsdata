#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Location,
)
from tabsdatak.server._tdrunner.function._commons.transporter import transporter
from tabsdatak.server._tdrunner.function._commons.transporter.transporter import (
    Transporter,
)
from tabsdatak.server._tdrunner.function._commons.transporter.transporter1 import (
    Transporter1,
)
from tabsdatak.server._tdrunner.function._commons.transporter.transporter2 import (
    Transporter2,
)
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadStorageConfigError,
    CommandErrorCode,
    UnexpectedError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

STORAGE = """\
storage:
  mounts:
    - id: ROOT
      uri: s3://bucket/root
      options:
        access_key_id: ${{secret:storage.mounts.ROOT.options.access_key_id}}
        secret_access_key: ${{secret:storage.mounts.ROOT.options.secret_access_key}}
"""


def storage_configuration(folder: Path) -> Path:
    path = folder / "storage.yaml"
    path.write_text(STORAGE)
    return path


def test_the_legacy_mode_is_the_first_transporter(tmp_path, monkeypatch):
    monkeypatch.setattr(transporter, "TRANSPORTER_MODE", 1)
    assert isinstance(Transporter.of(tmp_path / "unused.yaml"), Transporter1)


def test_the_current_mode_reads_the_mount_option_keys(tmp_path, monkeypatch):
    monkeypatch.setattr(transporter, "TRANSPORTER_MODE", 2)
    chosen = Transporter.of(storage_configuration(tmp_path))
    assert isinstance(chosen, Transporter2)
    assert chosen.option_keys == {"ROOT": ["access_key_id", "secret_access_key"]}


def test_an_unknown_mode_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(transporter, "TRANSPORTER_MODE", 7)
    with pytest.raises(UnexpectedError) as raised:
        Transporter.of(tmp_path / "unused.yaml")
    assert raised.value.api_error is CommandErrorCode.CMD_5


def test_an_unreadable_storage_configuration_is_refused(tmp_path):
    with pytest.raises(BadStorageConfigError) as raised:
        Transporter2.of(tmp_path / "missing.yaml")
    assert raised.value.api_error is CommandErrorCode.CMD_3


def test_a_mounted_location_references_its_option_secrets():
    location = Transporter2({"ROOT": ["access_key_id"]}).location(
        Location(uri="s3://bucket/root/x.parquet", env_prefix="ROOT")
    )
    assert location.uri == "s3://bucket/root/x.parquet"
    assert location.configs == {
        "access_key_id": "env:SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__ACCESS_KEY_ID"
    }


def test_a_location_on_an_undeclared_mount_is_refused():
    with pytest.raises(BadStorageConfigError) as raised:
        Transporter2({}).location(Location(uri="s3://bucket/x", env_prefix="OTHER"))
    assert raised.value.api_error is CommandErrorCode.CMD_4
