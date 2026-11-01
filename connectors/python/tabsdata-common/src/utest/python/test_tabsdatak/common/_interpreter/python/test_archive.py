#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
import yaml

from tabsdatak.common._configuration.document.error import DocumentException
from tabsdatak.common._interpreter.python import archive
from tabsdatak.common._storage import mounts, transfer
from tabsdatak.common._storage.mounts import MountError

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

PLACEHOLDER = "${{secret}}"
IMAGE_NAME = "tabsdata-td.tar.zst"


def storage_config(folder: Path, *declared: dict) -> Path:
    path = folder / "apiserver.storage.yaml"
    path.write_text(yaml.safe_dump({"storage": {"mounts": list(declared)}}))
    return path


def images(uri: str, options: dict | None = None) -> dict:
    return {"id": archive.IMAGES_MOUNT_ID, "uri": uri, "options": options or {}}


@pytest.fixture
def image(tmp_path: Path) -> Path:
    path = tmp_path / "build" / IMAGE_NAME
    path.parent.mkdir()
    path.write_bytes(os.urandom(4096))
    return path


@pytest.mark.parametrize(
    "mount_uri, subfolder, expected",
    [
        ("s3://bucket/images", "python", "s3://bucket/images/python/image"),
        ("s3://bucket/images/", "/python/", "s3://bucket/images/python/image"),
        ("s3://bucket/images", "", "s3://bucket/images/image"),
    ],
)
def test_target_uri_joins_the_mount_the_subfolder_and_the_name(
    mount_uri: str, subfolder: str, expected: str
):
    assert archive.target_uri(mount_uri, subfolder, "image") == expected


def test_image_lands_under_the_mount_subfolder(tmp_path: Path, image: Path):
    root = tmp_path / "storage"
    config = storage_config(tmp_path, images(root.as_uri()))
    assert archive.send_image(image, config) == image.stat().st_size
    sent = root / archive.IMAGES_SUBFOLDER / IMAGE_NAME
    assert sent.read_bytes() == image.read_bytes()


def test_placeholder_uri_resolves_from_the_environment(
    tmp_path: Path, image: Path, monkeypatch: pytest.MonkeyPatch
):
    root = tmp_path / "storage"
    monkeypatch.setenv(
        mounts.secret_variable("storage", "mounts", archive.IMAGES_MOUNT_ID, "uri"),
        root.as_uri(),
    )
    config = storage_config(tmp_path, images(PLACEHOLDER))
    archive.send_image(image, config)
    assert (root / archive.IMAGES_SUBFOLDER / IMAGE_NAME).is_file()


def test_unset_placeholder_uri_fails(tmp_path: Path, image: Path):
    config = storage_config(tmp_path, images(PLACEHOLDER))
    variable = mounts.secret_variable(
        "storage", "mounts", archive.IMAGES_MOUNT_ID, "uri"
    )
    with pytest.raises(DocumentException, match=variable):
        archive.send_image(image, config)


def test_cloud_target_carries_the_mount_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    for key, value in (("access_key_id", "key"), ("secret_access_key", "secret")):
        monkeypatch.setenv(
            mounts.secret_variable(
                "storage", "mounts", archive.IMAGES_MOUNT_ID, "options", key
            ),
            value,
        )
    config = storage_config(
        tmp_path,
        images(
            "s3://bucket/images",
            {
                "access_key_id": PLACEHOLDER,
                "secret_access_key": PLACEHOLDER,
                "region": "eu-west-1",
            },
        ),
    )
    target = archive.target_location(
        config, archive.IMAGES_MOUNT_ID, archive.IMAGES_SUBFOLDER, IMAGE_NAME
    )
    assert target.uri == f"s3://bucket/images/python/{IMAGE_NAME}"
    assert target.options == {
        transfer.AWS_ACCESS_KEY_ID_OPTION: "key",
        transfer.AWS_SECRET_ACCESS_KEY_OPTION: "secret",
        transfer.AWS_REGION_OPTION: "eu-west-1",
    }


def test_cloud_target_without_credentials_fails(tmp_path: Path):
    config = storage_config(
        tmp_path, images("s3://bucket/images", {"region": "eu-west-1"})
    )
    with pytest.raises(MountError, match=transfer.S3_SIGNATURE_REQUIREMENT):
        archive.target_location(
            config, archive.IMAGES_MOUNT_ID, archive.IMAGES_SUBFOLDER, IMAGE_NAME
        )


def test_undeclared_mount_fails(tmp_path: Path, image: Path):
    config = storage_config(tmp_path, {"id": "ROOT", "uri": tmp_path.as_uri()})
    with pytest.raises(MountError, match=archive.IMAGES_MOUNT_ID):
        archive.send_image(image, config)


def test_unreadable_storage_configuration_fails(tmp_path: Path, image: Path):
    with pytest.raises(DocumentException):
        archive.send_image(image, tmp_path / "missing.yaml")


def test_missing_image_fails(tmp_path: Path):
    config = storage_config(tmp_path, images((tmp_path / "storage").as_uri()))
    with pytest.raises(FileNotFoundError):
        archive.send_image(tmp_path / IMAGE_NAME, config)


def test_main_reports_a_failed_send(
    tmp_path: Path, image: Path, monkeypatch: pytest.MonkeyPatch
):
    config = storage_config(tmp_path, {"id": "ROOT", "uri": tmp_path.as_uri()})
    monkeypatch.setattr(
        sys,
        "argv",
        ["archive", "--image", str(image), "--storage-config", str(config)],
    )
    assert archive.main() == 1


def test_main_rejects_the_transporter_flags(
    tmp_path: Path, image: Path, monkeypatch: pytest.MonkeyPatch
):
    config = storage_config(tmp_path, images((tmp_path / "storage").as_uri()))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "archive",
            "--image",
            str(image),
            "--storage-config",
            str(config),
            "--transporter-request",
            str(tmp_path / "request.yaml"),
        ],
    )
    with pytest.raises(SystemExit):
        archive.main()
