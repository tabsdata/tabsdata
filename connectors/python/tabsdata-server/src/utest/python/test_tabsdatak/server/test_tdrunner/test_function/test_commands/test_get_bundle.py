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
    bundle_folder: Path
    bundle_archive: Path
    store: Path
    liveness: Path


@pytest.fixture
def paths(tmp_path: Path) -> Paths:
    store = tmp_path / "store"
    store.mkdir()
    return Paths(
        request=tmp_path / "request.yaml",
        bundle_folder=tmp_path / "bundle",
        bundle_archive=tmp_path / "stage" / "bundle.tar.zst",
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
            "--bundle-folder",
            str(paths.bundle_folder),
            "--bundle-archive",
            str(paths.bundle_archive),
            "--liveness",
            str(paths.liveness),
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


def write_request(path: Path, bundle: Path | str, mount: str | None = MOUNT) -> None:
    uri = bundle.as_uri() if isinstance(bundle, Path) else bundle
    info = info_payload(
        function_bundle={"uri": uri, "env_prefix": mount},
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
    assert (paths.bundle_folder / "a.py").read_bytes() == b"print('hi')"
    assert paths.bundle_archive.read_bytes() == bundle.read_bytes()
    assert paths.liveness.exists()


def test_main_localizes_a_bundle_without_mount(monkeypatch, paths, http_ok_url):
    bundle = make_tarball(paths.store / "bundle.tar.zst", {"b.py": b"pass"})
    write_request(paths.request, bundle, mount=None)
    set_arguments(monkeypatch, paths, http_ok_url)
    assert main() == 0
    assert (paths.bundle_folder / "b.py").read_bytes() == b"pass"


def test_main_missing_request_returns_bad_request(monkeypatch, paths, http_ok_url):
    set_arguments(monkeypatch, paths, http_ok_url)
    assert main() == BadRequestError.exit_code


def test_main_malformed_request_returns_bad_request(monkeypatch, paths, http_ok_url):
    paths.request.write_text("info: { unclosed")
    set_arguments(monkeypatch, paths, http_ok_url)
    assert main() == BadRequestError.exit_code


def test_main_cloud_bundle_without_mount_options_returns_storage_configuration_error(
    monkeypatch,
    paths,
    http_ok_url,
):
    write_request(paths.request, "s3://bucket/bundle.tar.zst")
    set_arguments(monkeypatch, paths, http_ok_url)
    assert main() == StorageConfigurationError.exit_code


def test_main_unknown_mount_option_returns_storage_configuration_error(
    monkeypatch,
    paths,
    http_ok_url,
):
    bundle = make_tarball(paths.store / "bundle.tar.zst", {"a.py": b"pass"})
    write_request(paths.request, bundle)
    monkeypatch.setenv(f"SECRETS__STORAGE__MOUNTS__{MOUNT}__OPTIONS__BOGUS", "x")
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
