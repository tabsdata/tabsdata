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

from tabsdatak._tdrunner.function._lib.transporter_proc import (
    TransporterNotFoundError,
)
from tabsdatak._tdrunner.function.commands.localize_fn_bundle import (
    EXIT_BAD_REQUEST,
    EXIT_BAD_RUN_ENV,
    EXIT_EXTRACT_FAILED,
    EXIT_TRANSPORTER_FAILED,
    main,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


_MOD = "tabsdatak._tdrunner.function.commands.localize_fn_bundle"


def _set_argv(monkeypatch, request_path, run_env_path, dest_dir, liveness):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "localize-fn-bundle",
            "--request",
            str(request_path),
            "--run-env",
            str(run_env_path),
            "--dir",
            str(dest_dir),
            "--liveness",
            str(liveness),
        ],
    )


def _write_request(path: Path, bundle_uri: str = "file:///bundle.tgz") -> Path:
    path.write_text(
        yaml.safe_dump({
            "info": {
                "collection": "c",
                "collection_id": 1,
                "execution_id": 2,
                "transaction_id": 3,
                "function_run_id": 4,
                "function_bundle": {"uri": bundle_uri},
            },
        })
    )
    return path


def _write_run_env(path: Path) -> Path:
    path.write_text(
        yaml.safe_dump({
            "python_bin": "/v/bin/python",
            "venv_dir": "/v",
            "requirements_digest": "sha256:abc",
        })
    )
    return path


def _make_tarball(path: Path, files: dict[str, bytes] | None = None) -> None:
    files = files or {"hello.txt": b"hi"}
    with tarfile.open(path, "w:gz") as tar:
        for name, content in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))


def _stub_transporter_returns(monkeypatch, tar_target_files=None, returncode=0):
    """Replace run_transporter so it pretends to download a bundle.

    Reads the transporter request YAML to find the target's local URL
    (it's a `file://...` URL we generated), then writes a real .tar.gz
    there before returning `returncode`.
    """
    import re

    def fake(transporter_yaml, liveness=None):
        text = transporter_yaml.read_text()
        # Find the LocalFileEndpoint target's url. The transporter YAML has
        # custom tags so safe_load won't parse it; pull out the second
        # `url:` (target follows source) via regex.
        urls = re.findall(r"url:\s*(file://\S+)", text)
        # source is first, target is second
        target_url = urls[1] if len(urls) >= 2 else urls[0]
        target_path = Path(target_url.replace("file://", "", 1))
        target_path.parent.mkdir(parents=True, exist_ok=True)
        _make_tarball(target_path, tar_target_files)
        return returncode

    monkeypatch.setattr(f"{_MOD}.run_transporter", fake)


# --- happy path ---


def test_main_happy_path(monkeypatch, tmp_path):
    request_yaml = _write_request(tmp_path / "request.yaml")
    run_env_yaml = _write_run_env(tmp_path / "run-env.yaml")
    dest = tmp_path / "bundle"
    liveness = tmp_path / "live"

    _stub_transporter_returns(monkeypatch, tar_target_files={"a.py": b"print('hi')"})

    _set_argv(monkeypatch, request_yaml, run_env_yaml, dest, liveness)
    rc = main()
    assert rc == 0
    assert (dest / "a.py").read_bytes() == b"print('hi')"
    assert liveness.exists()


# --- error paths ---


def test_main_missing_request_returns_bad_request(monkeypatch, tmp_path):
    run_env_yaml = _write_run_env(tmp_path / "run-env.yaml")
    _set_argv(
        monkeypatch,
        tmp_path / "no-such-request.yaml",
        run_env_yaml,
        tmp_path / "bundle",
        tmp_path / "live",
    )
    assert main() == EXIT_BAD_REQUEST


def test_main_malformed_request_returns_bad_request(monkeypatch, tmp_path):
    bad = tmp_path / "request.yaml"
    bad.write_text("info: { unclosed")
    run_env_yaml = _write_run_env(tmp_path / "run-env.yaml")
    _set_argv(monkeypatch, bad, run_env_yaml, tmp_path / "bundle", tmp_path / "live")
    assert main() == EXIT_BAD_REQUEST


def test_main_missing_run_env_returns_bad_run_env(monkeypatch, tmp_path):
    request_yaml = _write_request(tmp_path / "request.yaml")
    _set_argv(
        monkeypatch,
        request_yaml,
        tmp_path / "no-run-env.yaml",
        tmp_path / "bundle",
        tmp_path / "live",
    )
    assert main() == EXIT_BAD_RUN_ENV


def test_main_malformed_run_env_returns_bad_run_env(monkeypatch, tmp_path):
    request_yaml = _write_request(tmp_path / "request.yaml")
    bad = tmp_path / "run-env.yaml"
    bad.write_text("python_bin: { unclosed")
    _set_argv(monkeypatch, request_yaml, bad, tmp_path / "bundle", tmp_path / "live")
    assert main() == EXIT_BAD_RUN_ENV


def test_main_transporter_not_found_returns_transporter_failed(monkeypatch, tmp_path):
    request_yaml = _write_request(tmp_path / "request.yaml")
    run_env_yaml = _write_run_env(tmp_path / "run-env.yaml")

    def fake(yaml_path, liveness=None):
        raise TransporterNotFoundError("no binary")

    monkeypatch.setattr(f"{_MOD}.run_transporter", fake)
    _set_argv(
        monkeypatch,
        request_yaml,
        run_env_yaml,
        tmp_path / "bundle",
        tmp_path / "live",
    )
    assert main() == EXIT_TRANSPORTER_FAILED


def test_main_transporter_nonzero_returns_transporter_failed(monkeypatch, tmp_path):
    request_yaml = _write_request(tmp_path / "request.yaml")
    run_env_yaml = _write_run_env(tmp_path / "run-env.yaml")
    monkeypatch.setattr(f"{_MOD}.run_transporter", lambda yaml_path, liveness=None: 7)
    _set_argv(
        monkeypatch,
        request_yaml,
        run_env_yaml,
        tmp_path / "bundle",
        tmp_path / "live",
    )
    assert main() == EXIT_TRANSPORTER_FAILED


def test_main_corrupt_bundle_returns_extract_failed(monkeypatch, tmp_path):
    request_yaml = _write_request(tmp_path / "request.yaml")
    run_env_yaml = _write_run_env(tmp_path / "run-env.yaml")

    def fake(transporter_yaml, liveness=None):
        # Find the target path and write garbage instead of a real tar.
        import re

        text = transporter_yaml.read_text()
        urls = re.findall(r"url:\s*(file://\S+)", text)
        target = Path(urls[1].replace("file://", "", 1))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"not a tar")
        return 0

    monkeypatch.setattr(f"{_MOD}.run_transporter", fake)
    _set_argv(
        monkeypatch,
        request_yaml,
        run_env_yaml,
        tmp_path / "bundle",
        tmp_path / "live",
    )
    assert main() == EXIT_EXTRACT_FAILED


def test_main_missing_bundle_after_transporter_returns_extract_failed(
    monkeypatch, tmp_path
):
    """Transporter says success but doesn't actually create the file."""
    request_yaml = _write_request(tmp_path / "request.yaml")
    run_env_yaml = _write_run_env(tmp_path / "run-env.yaml")
    monkeypatch.setattr(f"{_MOD}.run_transporter", lambda yaml_path, liveness=None: 0)
    _set_argv(
        monkeypatch,
        request_yaml,
        run_env_yaml,
        tmp_path / "bundle",
        tmp_path / "live",
    )
    assert main() == EXIT_EXTRACT_FAILED


def test_main_invalid_options_returns_1(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "argv", ["localize-fn-bundle", "--request", "x"])
    assert main() == 1


# --- decorator metadata ---


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_localize_fn_bundle"
