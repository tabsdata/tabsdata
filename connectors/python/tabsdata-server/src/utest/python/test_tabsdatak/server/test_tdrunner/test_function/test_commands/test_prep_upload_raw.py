#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

from tabsdatak.server._tdrunner.function.commands.prep_upload_raw import main
from tabsdatak.server._tdrunner.function.commands.prep_upload_raw.error import (
    BadManifestError,
    BadRequestError,
)
from test_tabsdatak.server.test_tdrunner.test_function.conftest import (
    info_payload as _info,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

RAW_URI = "s3://bucket/c/1/w/4/r"


def _journal(folder: Path) -> Path:
    path = folder / "journal.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}\n")
    return path


def _set_argv(
    monkeypatch, request_path, manifest_path, out, liveness, *, responder_url
):
    storage_config = Path(out).parent / "storage.yaml"
    storage_config.parent.mkdir(parents=True, exist_ok=True)
    storage_config.write_text(
        "storage:\n  mounts:\n    - id: ROOT\n      options: {}\n"
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prep-upload-raw",
            "--request",
            str(request_path),
            "--inputs-manifest",
            str(manifest_path),
            "--out",
            str(out),
            "--liveness",
            str(liveness),
            "--storage-config",
            str(storage_config),
            "--responder-url",
            responder_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer wtok",
            "--journal-file",
            str(_journal(Path(out).parent)),
        ],
    )


def _write_request(path: Path, *, raw_uri: str | None = RAW_URI) -> Path:
    info = _info()
    if raw_uri is not None:
        info["function_raw"] = {"uri": raw_uri, "env_prefix": "ROOT"}
    path.write_text(yaml.safe_dump({"Function": {"info": info}}))
    return path


def _write_manifest(path: Path, entries: list) -> Path:
    path.write_text(yaml.safe_dump({"inputs": entries}))
    return path


def _slot(name: str, tables: list[dict]) -> dict:
    return {"name": name, "table_pos": 0, "tables": tables}


def main_with_args(
    monkeypatch, request_path, manifest_path, out, liveness, *, responder_url
):
    _set_argv(
        monkeypatch,
        request_path,
        manifest_path,
        out,
        liveness,
        responder_url=responder_url,
    )
    return main()


# --- happy path -----------------------------------------------------------


def test_main_pairs_each_stamped_parquet_with_its_index(
    monkeypatch, tmp_path, http_ok_url
):
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(
        tmp_path / "enriched.yaml",
        [
            _slot(
                "a",
                [
                    {"file": "/shared/rich/slot0/a.parquet", "input_idx": 0},
                    {"file": "/shared/rich/slot0/b.parquet", "input_idx": 1},
                ],
            ),
            _slot("c", [{"file": "/shared/rich/slot1/c.parquet", "input_idx": 2}]),
        ],
    )
    out = tmp_path / "transporter.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        manifest,
        out,
        tmp_path / "liveness",
        responder_url=http_ok_url,
    )
    assert rc == 0
    written = yaml.safe_load(out.read_text())
    body = yaml.safe_dump(written)
    # One target per input index, named by the index the manifest recorded.
    for index in (0, 1, 2):
        assert f"{RAW_URI}/{index}.t" in body, f"index {index} missing: {body}"


def test_main_writes_an_empty_request_when_nothing_was_ingested(
    monkeypatch, tmp_path, http_ok_url
):
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(tmp_path / "enriched.yaml", [])
    out = tmp_path / "transporter.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        manifest,
        out,
        tmp_path / "liveness",
        responder_url=http_ok_url,
    )
    assert rc == 0
    assert out.exists(), "an empty request must still be written for run-transporter"


def test_main_skips_a_function_with_no_raw_location(monkeypatch, tmp_path, http_ok_url):
    # Transformers and subscribers ingest nothing, so the apiserver mints them
    # no raw location; the step must be a no-op rather than a failure.
    request_yaml = _write_request(tmp_path / "request.yaml", raw_uri=None)
    manifest = _write_manifest(
        tmp_path / "enriched.yaml",
        [_slot("a", [{"file": "/shared/rich/slot0/a.parquet", "input_idx": 0}])],
    )
    out = tmp_path / "transporter.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        manifest,
        out,
        tmp_path / "liveness",
        responder_url=http_ok_url,
    )
    assert rc == 0
    assert RAW_URI not in out.read_text()


def test_main_ignores_a_record_with_no_file(monkeypatch, tmp_path, http_ok_url):
    # A version member with no physical data carries file: null; it has nothing
    # to persist and must not produce a pair.
    request_yaml = _write_request(tmp_path / "request.yaml")
    manifest = _write_manifest(
        tmp_path / "enriched.yaml",
        [
            _slot(
                "a",
                [
                    {"file": None, "input_idx": 0},
                    {"file": "/shared/rich/slot0/b.parquet", "input_idx": 1},
                ],
            )
        ],
    )
    out = tmp_path / "transporter.yaml"
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        manifest,
        out,
        tmp_path / "liveness",
        responder_url=http_ok_url,
    )
    assert rc == 0
    body = out.read_text()
    assert f"{RAW_URI}/1.t" in body
    assert f"{RAW_URI}/0.t" not in body


# --- failures -------------------------------------------------------------


def test_main_missing_request_returns_bad_request(monkeypatch, tmp_path, http_ok_url):
    manifest = _write_manifest(tmp_path / "enriched.yaml", [])
    rc = main_with_args(
        monkeypatch,
        tmp_path / "absent.yaml",
        manifest,
        tmp_path / "transporter.yaml",
        tmp_path / "liveness",
        responder_url=http_ok_url,
    )
    assert rc == BadRequestError.exit_code


def test_main_missing_manifest_returns_bad_manifest(monkeypatch, tmp_path, http_ok_url):
    request_yaml = _write_request(tmp_path / "request.yaml")
    rc = main_with_args(
        monkeypatch,
        request_yaml,
        tmp_path / "absent.yaml",
        tmp_path / "transporter.yaml",
        tmp_path / "liveness",
        responder_url=http_ok_url,
    )
    assert rc == BadManifestError.exit_code


def test_command_is_registered_under_its_console_name():
    assert getattr(main, "tabsdata_command_name", None) == "td_prep_upload_raw"
