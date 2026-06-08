#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import io
import sys
import tarfile
from pathlib import Path

import pytest
import yaml
import zstandard

from tabsdatak.server._tdrunner.function._lib.transporter_proc import (
    TransporterNotFoundError,
)
from tabsdatak.server._tdrunner.function.commands.localize_fn_bundle import main
from tabsdatak.server._tdrunner.function.commands.localize_fn_bundle.error import (
    BadRequestError,
    ExtractFailedError,
    TransporterFailedError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


_MOD = "tabsdatak.server._tdrunner.function.commands.localize_fn_bundle"


def _set_argv(
    monkeypatch,
    *,
    request_path,
    bundle_dir,
    bundle_archive,
    transporter_request,
    transporter_response,
    liveness,
    responder_url,
):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "localize-fn-bundle",
            "--request",
            str(request_path),
            "--bundle-dir",
            str(bundle_dir),
            "--bundle-archive",
            str(bundle_archive),
            "--transporter-request",
            str(transporter_request),
            "--transporter-response",
            str(transporter_response),
            "--liveness",
            str(liveness),
            "--responder-url",
            responder_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer wtok",
        ],
    )


@pytest.fixture
def paths(tmp_path):
    """Standard set of paths the command needs, all under tmp_path."""

    class _Paths:
        request = tmp_path / "request.yaml"
        bundle_dir = tmp_path / "bundle"
        bundle_archive = tmp_path / "stage" / "bundle.tar.zst"
        transporter_request = tmp_path / "stage" / "transporter.yaml"
        transporter_response = tmp_path / "stage" / "transporter-response.yaml"
        liveness = tmp_path / "live"

    _Paths.bundle_archive.parent.mkdir(parents=True, exist_ok=True)
    return _Paths


def _write_request(path: Path, bundle_uri: str = "file:///bundle.tgz") -> Path:
    path.write_text(
        yaml.safe_dump({
            "FunctionV2": {
                "info": {
                    "collection": "c",
                    "collection_id": 1,
                    "function_version_id": "FV-1",
                    "function_defined_on": 1780000000000,
                    "function": "fn",
                    "function_run_id": 4,
                    "function_bundle": {"uri": bundle_uri, "env_prefix": "ROOT"},
                    "function_stage": {
                        "uri": "file:///stage",
                        "env_prefix": "ROOT",
                    },
                    "triggered_on": 1780000000100,
                    "transaction_id": 3,
                    "execution_id": 2,
                    "function_data": {"uri": "file:///data", "env_prefix": "ROOT"},
                    "scheduled_on": 1780000000200,
                },
            }
        })
    )
    return path


def _make_tarball(path: Path, files: dict[str, bytes] | None = None) -> None:
    """Write a `.tar.zst` (zstandard-compressed tar) containing `files`,
    matching the bundle wire format produced by ``build_bundle``."""
    files = files or {"hello.txt": b"hi"}
    compressor = zstandard.ZstdCompressor()
    with open(path, "wb") as raw:
        with compressor.stream_writer(raw, closefd=False) as stream:
            with tarfile.open(fileobj=stream, mode="w|") as tar:
                for name, content in files.items():
                    info = tarfile.TarInfo(name=name)
                    info.size = len(content)
                    tar.addfile(info, io.BytesIO(content))


def _stub_transporter_writes_archive(
    monkeypatch, bundle_archive: Path, files: dict[str, bytes] | None = None
):
    """Replace run_transporter so it writes a real .tar.zst at the archive path."""

    def fake(transporter_yaml, report, liveness=None):
        _make_tarball(bundle_archive, files)
        return 0

    monkeypatch.setattr(f"{_MOD}.run_transporter", fake)


# --- happy path ---


def test_main_happy_path(monkeypatch, paths, http_ok_url):
    _write_request(paths.request)
    _stub_transporter_writes_archive(
        monkeypatch,
        paths.bundle_archive,
        files={"a.py": b"print('hi')"},
    )
    _set_argv(
        monkeypatch,
        request_path=paths.request,
        bundle_dir=paths.bundle_dir,
        bundle_archive=paths.bundle_archive,
        transporter_request=paths.transporter_request,
        transporter_response=paths.transporter_response,
        liveness=paths.liveness,
        responder_url=http_ok_url,
    )
    rc = main()
    assert rc == 0
    assert (paths.bundle_dir / "a.py").read_bytes() == b"print('hi')"
    assert paths.liveness.exists()
    assert paths.bundle_archive.exists()


# --- error paths ---


def test_main_missing_request_returns_bad_request(monkeypatch, paths, http_ok_url):
    _set_argv(
        monkeypatch,
        request_path=paths.request,
        bundle_dir=paths.bundle_dir,
        bundle_archive=paths.bundle_archive,
        transporter_request=paths.transporter_request,
        transporter_response=paths.transporter_response,
        liveness=paths.liveness,
        responder_url=http_ok_url,
    )
    assert main() == BadRequestError.exit_code


def test_main_malformed_request_returns_bad_request(monkeypatch, paths, http_ok_url):
    paths.request.write_text("info: { unclosed")
    _set_argv(
        monkeypatch,
        request_path=paths.request,
        bundle_dir=paths.bundle_dir,
        bundle_archive=paths.bundle_archive,
        transporter_request=paths.transporter_request,
        transporter_response=paths.transporter_response,
        liveness=paths.liveness,
        responder_url=http_ok_url,
    )
    assert main() == BadRequestError.exit_code


def test_main_transporter_not_found_returns_transporter_failed(
    monkeypatch, paths, http_ok_url
):
    _write_request(paths.request)

    def fake(yaml_path, report, liveness=None):
        raise TransporterNotFoundError("no binary")

    monkeypatch.setattr(f"{_MOD}.run_transporter", fake)
    _set_argv(
        monkeypatch,
        request_path=paths.request,
        bundle_dir=paths.bundle_dir,
        bundle_archive=paths.bundle_archive,
        transporter_request=paths.transporter_request,
        transporter_response=paths.transporter_response,
        liveness=paths.liveness,
        responder_url=http_ok_url,
    )
    assert main() == TransporterFailedError.exit_code


def test_main_transporter_nonzero_returns_transporter_failed(
    monkeypatch, paths, http_ok_url
):
    _write_request(paths.request)
    monkeypatch.setattr(
        f"{_MOD}.run_transporter", lambda yaml_path, report, liveness=None: 7
    )
    _set_argv(
        monkeypatch,
        request_path=paths.request,
        bundle_dir=paths.bundle_dir,
        bundle_archive=paths.bundle_archive,
        transporter_request=paths.transporter_request,
        transporter_response=paths.transporter_response,
        liveness=paths.liveness,
        responder_url=http_ok_url,
    )
    assert main() == TransporterFailedError.exit_code


def test_main_corrupt_bundle_returns_extract_failed(monkeypatch, paths, http_ok_url):
    _write_request(paths.request)

    def fake(transporter_yaml, report, liveness=None):
        paths.bundle_archive.write_bytes(b"not a tar")
        return 0

    monkeypatch.setattr(f"{_MOD}.run_transporter", fake)
    _set_argv(
        monkeypatch,
        request_path=paths.request,
        bundle_dir=paths.bundle_dir,
        bundle_archive=paths.bundle_archive,
        transporter_request=paths.transporter_request,
        transporter_response=paths.transporter_response,
        liveness=paths.liveness,
        responder_url=http_ok_url,
    )
    assert main() == ExtractFailedError.exit_code


def test_main_missing_bundle_after_transporter_returns_extract_failed(
    monkeypatch, paths, http_ok_url
):
    """Transporter says success but doesn't actually create the file."""
    _write_request(paths.request)
    monkeypatch.setattr(
        f"{_MOD}.run_transporter", lambda yaml_path, report, liveness=None: 0
    )
    _set_argv(
        monkeypatch,
        request_path=paths.request,
        bundle_dir=paths.bundle_dir,
        bundle_archive=paths.bundle_archive,
        transporter_request=paths.transporter_request,
        transporter_response=paths.transporter_response,
        liveness=paths.liveness,
        responder_url=http_ok_url,
    )
    assert main() == ExtractFailedError.exit_code


def test_main_invalid_options_returns_1(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["localize-fn-bundle", "--request", "x"])
    assert main() == 1


# --- decorator metadata ---


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_localize_fn_bundle"
