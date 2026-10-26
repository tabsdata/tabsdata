#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from tabsdatax.itest.fixture.backends.gcs import GCSDomain
from tabsdatax.itest.fixture.backends.s3 import S3Domain

from tabsdatak.common._storage import mounts
from tabsdatak.common._storage.mounts import NamedMountDeclaration

MOUNT = "ROOT"
PLACEHOLDER = "${{secret}}"


def declared_options(domain) -> dict[str, str]:
    if domain.storage_options:
        options = dict(domain.storage_options)
    elif isinstance(domain, S3Domain):
        options = {
            "access_key_id": domain.access_key,
            "secret_access_key": domain.secret_key,
            "region": domain.region,
        }
    elif isinstance(domain, GCSDomain):
        options = {"service_account_key": domain.service_account_key}
    else:
        options = {
            "account_name": domain.account_name,
            "account_key": domain.account_key,
        }
    return options


def mounted(domain, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    options = declared_options(domain)
    config = tmp_path / "apiserver.storage.yaml"
    mount = {
        "id": MOUNT,
        "uri": PLACEHOLDER,
        "options": {key: PLACEHOLDER for key in options},
    }
    config.write_text(yaml.safe_dump({"storage": {"mounts": [mount]}}))
    monkeypatch.setenv(
        mounts.secret_variable("storage", "mounts", MOUNT, "uri"),
        domain.uri,
    )
    for key, value in options.items():
        monkeypatch.setenv(
            mounts.secret_variable("storage", "mounts", MOUNT, "options", key),
            value,
        )
    return config


def declaration(
    domain,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> NamedMountDeclaration:
    return mounts.read(mounted(domain, tmp_path, monkeypatch), MOUNT)
