#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for `tabsdatak._tdrunner.function._lib.state_manifest`.

Schema parallels `inputs_manifest` (same `{name, files}` per entry)
but lives under the distinct top-level `state:` key. Tests mirror the
inputs-manifest tests where the semantics overlap.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tabsdatak._tdrunner.function._lib.state_manifest import (
    MANIFEST_FILES_KEY,
    MANIFEST_NAME_KEY,
    InvalidStateManifestError,
    load_state_manifest,
    write_state_manifest,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- constants -----------------------------------------------------------


def test_public_constants_match_yaml_schema():
    assert MANIFEST_NAME_KEY == "name"
    assert MANIFEST_FILES_KEY == "files"


# --- load_state_manifest -------------------------------------------------


def _write_yaml(path: Path, data) -> Path:
    path.write_text(yaml.safe_dump(data))
    return path


def test_load_happy_path(tmp_path):
    p = _write_yaml(
        tmp_path / "m.yaml",
        {"state": [{"name": "s1", "files": ["/a.parquet"]}]},
    )
    assert load_state_manifest(p) == [{"name": "s1", "files": ["/a.parquet"]}]


def test_load_empty_state_list(tmp_path):
    p = _write_yaml(tmp_path / "m.yaml", {"state": []})
    assert load_state_manifest(p) == []


def test_load_multiple_entries_preserves_order(tmp_path):
    entries = [
        {"name": "s3", "files": ["/c.parquet"]},
        {"name": "s1", "files": ["/a.parquet"]},
        {"name": "s2", "files": []},
    ]
    p = _write_yaml(tmp_path / "m.yaml", {"state": entries})
    assert load_state_manifest(p) == entries


def test_load_missing_file_raises_filenotfound(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_state_manifest(tmp_path / "nope.yaml")


def test_load_malformed_yaml(tmp_path):
    p = tmp_path / "m.yaml"
    p.write_text("state: [{unclosed")
    with pytest.raises(InvalidStateManifestError, match="malformed YAML"):
        load_state_manifest(p)


def test_load_top_level_not_mapping(tmp_path):
    p = tmp_path / "m.yaml"
    p.write_text("- a\n- b\n")
    with pytest.raises(InvalidStateManifestError, match="state"):
        load_state_manifest(p)


def test_load_missing_state_key(tmp_path):
    p = _write_yaml(tmp_path / "m.yaml", {"inputs": []})
    with pytest.raises(InvalidStateManifestError, match="state"):
        load_state_manifest(p)


def test_load_state_not_a_list(tmp_path):
    p = _write_yaml(tmp_path / "m.yaml", {"state": "not-a-list"})
    with pytest.raises(InvalidStateManifestError, match="must be a list"):
        load_state_manifest(p)


# --- write_state_manifest ------------------------------------------------


def test_write_then_load_roundtrip(tmp_path):
    entries = [{"name": "s1", "files": ["/x.parquet"]}]
    p = tmp_path / "m.yaml"
    write_state_manifest(entries, p)
    assert load_state_manifest(p) == entries


def test_write_empty_entry_files_roundtrips(tmp_path):
    entries = [{"name": "s1", "files": []}]
    p = tmp_path / "m.yaml"
    write_state_manifest(entries, p)
    assert load_state_manifest(p) == entries


def test_write_preserves_entry_order(tmp_path):
    entries = [
        {"name": "s2", "files": ["/b.parquet"]},
        {"name": "s1", "files": ["/a.parquet"]},
    ]
    p = tmp_path / "m.yaml"
    write_state_manifest(entries, p)
    data = yaml.safe_load(p.read_text())
    assert [e["name"] for e in data["state"]] == ["s2", "s1"]


def test_write_wraps_under_state_top_level_key(tmp_path):
    p = tmp_path / "m.yaml"
    write_state_manifest([], p)
    data = yaml.safe_load(p.read_text())
    assert "state" in data
    assert data["state"] == []
    # And explicitly NOT the inputs key — keeps the manifests distinct
    # on the wire even though per-entry shapes are identical.
    assert "inputs" not in data


def test_write_creates_missing_parent_dir(tmp_path):
    nested = tmp_path / "a" / "b" / "m.yaml"
    write_state_manifest([{"name": "x", "files": []}], nested)
    assert nested.is_file()


def test_write_to_directory_path_raises_oserror(tmp_path):
    d = tmp_path / "i-am-a-dir"
    d.mkdir()
    with pytest.raises(OSError):
        write_state_manifest([], d)


# --- cross-manifest distinctness ----------------------------------------


def test_load_state_rejects_inputs_manifest_file(tmp_path):
    """An inputs-manifest file (top-level `inputs:`) must not load as
    a state manifest — they share entry shape but the wire-level
    distinction is the top-level key."""
    p = _write_yaml(
        tmp_path / "m.yaml",
        {"inputs": [{"name": "x", "files": []}]},
    )
    with pytest.raises(InvalidStateManifestError):
        load_state_manifest(p)
