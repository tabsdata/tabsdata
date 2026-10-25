#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import sys
import tarfile
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml
import zstandard

from tabsdatak.server._tdrunner.function.commands.get_bundle import main
from tabsdatak.server._tdrunner.function.commands.get_bundle.error import (
    BadRequestError,
    DownloadFailedError,
    ExtractFailedError,
    StorageConfigurationError,
)
from test_tabsdatak.server.test_tdrunner.test_function.conftest import info_payload

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

MOUNT = "ROOT"


@dataclass
class Paths:
    request: Path
    bundle_dir: Path
    bundle_archive: Path
    storage_config: Path
    store: Path
    liveness: Path


@pytest.fixture
def paths(tmp_path: Path) -> Paths:
    store = tmp_path / "store"
    store.mkdir()
    storage_config = tmp_path / "config" / "apiserver.storage.yaml"
    storage_config.parent.mkdir()
    storage_config.write_text(
        yaml.safe_dump({"storage": {"mounts": [{"id": MOUNT, "uri": store.as_uri()}]}}),
    )
    return Paths(
        request=tmp_path / "request.yaml",
        bundle_dir=tmp_path / "bundle",
        bundle_archive=tmp_path / "stage" / "bundle.tar.zst",
        storage_config=storage_config,
        store=store,
        liveness=tmp_path / "live",
    )


def set_arguments(monkeypatch: pytest.MonkeyPatch, paths: Paths, responder_url: str):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "get-bundle",
            "--request",
            str(paths.request),
            "--bundle-dir",
            str(paths.bundle_dir),
            "--bundle-archive",
            str(paths.bundle_archive),
            "--liveness",
            str(paths.liveness),
            "--storage-config",
            str(paths.storage_config),
            "--responder-url",
            responder_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer wtok",
            "--journal-file",
            "/tmp/em.yaml",
        ],
    )


def write_request(path: Path, bundle: Path, mount: str | None = MOUNT) -> None:
    info = info_payload(
        function_bundle={"uri": bundle.as_uri(), "env_prefix": mount},
    )
    path.write_text(yaml.safe_dump({"Function": {"info": info}}))


def make_tarball(path: Path, files: dict[str, bytes]) -> Path:
    compressor = zstandard.ZstdCompressor()
    with open(path, "wb") as raw:
        with compressor.stream_writer(raw, closefd=False) as stream:
            with tarfile.open(fileobj=stream, mode="w|") as tar:
                for name, content in files.items():
                    info = tarfile.TarInfo(name=name)
                    info.size = len(content)
                    tar.addfile(info, io.BytesIO(content))
    return path


def test_main_localizes_a_mounted_bundle(monkeypatch, paths, http_ok_url):
    bundle = make_tarball(paths.store / "bundle.tar.zst", {"a.py": b"print('hi')"})
    write_request(paths.request, bundle)
    set_arguments(monkeypatch, paths, http_ok_url)
    assert main() == 0
    assert (paths.bundle_dir / "a.py").read_bytes() == b"print('hi')"
    assert paths.bundle_archive.read_bytes() == bundle.read_bytes()
    assert paths.liveness.exists()


def test_main_localizes_a_bundle_without_mount(monkeypatch, paths, http_ok_url):
    bundle = make_tarball(paths.store / "bundle.tar.zst", {"b.py": b"pass"})
    write_request(paths.request, bundle, mount=None)
    set_arguments(monkeypatch, paths, http_ok_url)
    assert main() == 0
    assert (paths.bundle_dir / "b.py").read_bytes() == b"pass"


def test_main_missing_request_returns_bad_request(monkeypatch, paths, http_ok_url):
    set_arguments(monkeypatch, paths, http_ok_url)
    assert main() == BadRequestError.exit_code


def test_main_malformed_request_returns_bad_request(monkeypatch, paths, http_ok_url):
    paths.request.write_text("info: { unclosed")
    set_arguments(monkeypatch, paths, http_ok_url)
    assert main() == BadRequestError.exit_code


def test_main_undeclared_mount_returns_storage_configuration_error(
    monkeypatch,
    paths,
    http_ok_url,
):
    bundle = make_tarball(paths.store / "bundle.tar.zst", {"a.py": b"pass"})
    write_request(paths.request, bundle, mount="BUNDLES")
    set_arguments(monkeypatch, paths, http_ok_url)
    assert main() == StorageConfigurationError.exit_code


def test_main_unreadable_storage_configuration_returns_storage_configuration_error(
    monkeypatch,
    paths,
    http_ok_url,
):
    bundle = make_tarball(paths.store / "bundle.tar.zst", {"a.py": b"pass"})
    write_request(paths.request, bundle)
    paths.storage_config.write_text("storage: { unclosed")
    set_arguments(monkeypatch, paths, http_ok_url)
    assert main() == StorageConfigurationError.exit_code


def test_main_unset_mount_secret_returns_storage_configuration_error(
    monkeypatch,
    paths,
    http_ok_url,
):
    bundle = make_tarball(paths.store / "bundle.tar.zst", {"a.py": b"pass"})
    write_request(paths.request, bundle)
    paths.storage_config.write_text(
        yaml.safe_dump({"storage": {"mounts": [{"id": MOUNT, "uri": "${{secret}}"}]}}),
    )
    set_arguments(monkeypatch, paths, http_ok_url)
    assert main() == StorageConfigurationError.exit_code


def test_main_missing_bundle_returns_download_failed(monkeypatch, paths, http_ok_url):
    write_request(paths.request, paths.store / "missing.tar.zst")
    set_arguments(monkeypatch, paths, http_ok_url)
    assert main() == DownloadFailedError.exit_code
    assert not paths.bundle_archive.exists()


def test_main_corrupt_bundle_returns_extract_failed(monkeypatch, paths, http_ok_url):
    bundle = paths.store / "bundle.tar.zst"
    bundle.write_bytes(b"not a tar")
    write_request(paths.request, bundle)
    set_arguments(monkeypatch, paths, http_ok_url)
    assert main() == ExtractFailedError.exit_code


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["get-bundle", "--request", "x"])
    assert main() == 1


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_get_bundle"
