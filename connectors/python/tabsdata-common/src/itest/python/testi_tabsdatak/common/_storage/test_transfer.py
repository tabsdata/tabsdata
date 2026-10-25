#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import uuid
from pathlib import Path

import obstore
import pytest
import yaml
from tabsdatax.itest.fixture.backends.azureblob import AzureblobDomain, azureblob_domain
from tabsdatax.itest.fixture.backends.azurebloblocal import (
    AzureBlobLocalDomain,
    azurebloblocal_domain,
)
from tabsdatax.itest.fixture.backends.gcs import GCSDomain, gcs_domain
from tabsdatax.itest.fixture.backends.gcslocal import GcsLocalDomain, gcslocal_domain
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.backends.s3local import S3LocalDomain, s3local_domain

from tabsdatak.common._storage import mounts, transfer
from tabsdatak.common._storage.transfer import StorageLocation

pytestmark = [pytest.mark.integration, pytest.mark.tabsdatak]

MOUNT = "ROOT"
PLACEHOLDER = "${{secret}}"
SIZE = 3 * transfer.MEBIBYTE + 17
RANGED_THRESHOLD = transfer.MEBIBYTE
RANGED_SIZE = 256 * 1024


def declared_options(domain) -> dict[str, str]:
    if domain.storage_options:
        return dict(domain.storage_options)
    if isinstance(domain, S3Domain):
        return {
            "access_key_id": domain.access_key,
            "secret_access_key": domain.secret_key,
            "region": domain.region,
        }
    if isinstance(domain, GCSDomain):
        return {"service_account_key": domain.service_account_key}
    return {
        "account_name": domain.account_name,
        "account_key": domain.account_key,
    }


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


def round_trip(domain, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    declaration = mounts.read(mounted(domain, tmp_path, monkeypatch), MOUNT)
    root = mounts.uri(declaration).rstrip("/")
    uri = f"{root}/transfer-{uuid.uuid4().hex}/object.bin"
    location = StorageLocation(uri=uri, options=mounts.options(declaration, uri))
    source = tmp_path / "source.bin"
    source.write_bytes(os.urandom(SIZE))
    expected = source.read_bytes()
    try:
        assert transfer.upload(source, location) == SIZE
        streamed = tmp_path / "streamed.bin"
        assert transfer.download(location, streamed) == SIZE
        assert streamed.read_bytes() == expected
        monkeypatch.setattr(transfer, "STREAM_THRESHOLD", RANGED_THRESHOLD)
        monkeypatch.setattr(transfer, "RANGE_SIZE", RANGED_SIZE)
        ranged = tmp_path / "ranged.bin"
        assert transfer.download(location, ranged) == SIZE
        assert ranged.read_bytes() == expected
        assert b"".join(transfer.chunks(location)) == expected
    finally:
        store_root, name = transfer.split(uri)
        obstore.delete(transfer.store(store_root, location.options), name)


@s3local_domain("s3local")
def test_s3local_round_trip(
    domain_s3local: S3LocalDomain,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    round_trip(domain_s3local, tmp_path, monkeypatch)


@gcslocal_domain("gcslocal")
def test_gcslocal_round_trip(
    domain_gcslocal: GcsLocalDomain,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    round_trip(domain_gcslocal, tmp_path, monkeypatch)


@azurebloblocal_domain("azurebloblocal")
def test_azurebloblocal_round_trip(
    domain_azurebloblocal: AzureBlobLocalDomain,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    round_trip(domain_azurebloblocal, tmp_path, monkeypatch)


@pytest.mark.s3
@pytest.mark.requires_internet
@s3_domain("s3")
def test_s3_round_trip(
    domain_s3: S3Domain,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    round_trip(domain_s3, tmp_path, monkeypatch)


@pytest.mark.gcs
@pytest.mark.requires_internet
@gcs_domain("gcs")
def test_gcs_round_trip(
    domain_gcs: GCSDomain,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    round_trip(domain_gcs, tmp_path, monkeypatch)


@pytest.mark.azure
@pytest.mark.requires_internet
@azureblob_domain("azureblob")
def test_azureblob_round_trip(
    domain_azureblob: AzureblobDomain,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    round_trip(domain_azureblob, tmp_path, monkeypatch)
