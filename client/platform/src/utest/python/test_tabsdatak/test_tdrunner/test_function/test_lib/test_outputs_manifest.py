#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for `tabsdatak._tdrunner.function._lib.outputs_manifest`.

Mirrors `test_inputs_manifest.py` — the shapes are deliberately
parallel, with `file:` (singular) replacing `files:` (list) and `null`
being a meaningful value (= "not modified this run").
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tabsdatak._tdrunner.function._lib.outputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_NAME_KEY,
    InvalidOutputsManifestError,
    load_outputs_manifest,
    write_outputs_manifest,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- constants -----------------------------------------------------------


def test_public_constants_match_yaml_schema():
    assert MANIFEST_NAME_KEY == "name"
    assert MANIFEST_FILE_KEY == "file"


# --- load_outputs_manifest -----------------------------------------------


def _write_yaml(path: Path, data) -> Path:
    path.write_text(yaml.safe_dump(data))
    return path


def test_load_happy_path(tmp_path):
    p = _write_yaml(
        tmp_path / "m.yaml",
        {"outputs": [{"name": "t1", "file": "/a.parquet"}]},
    )
    assert load_outputs_manifest(p) == [{"name": "t1", "file": "/a.parquet"}]


def test_load_null_file_means_not_modified(tmp_path):
    p = _write_yaml(
        tmp_path / "m.yaml",
        {"outputs": [{"name": "t1", "file": None}]},
    )
    assert load_outputs_manifest(p) == [{"name": "t1", "file": None}]


def test_load_mixed_some_files_some_null(tmp_path):
    entries = [
        {"name": "t1", "file": "/a.parquet"},
        {"name": "t2", "file": None},
        {"name": "t3", "file": "/c.parquet"},
    ]
    p = _write_yaml(tmp_path / "m.yaml", {"outputs": entries})
    assert load_outputs_manifest(p) == entries


def test_load_empty_outputs_list(tmp_path):
    p = _write_yaml(tmp_path / "m.yaml", {"outputs": []})
    assert load_outputs_manifest(p) == []


def test_load_missing_file_raises_filenotfound(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_outputs_manifest(tmp_path / "nope.yaml")


def test_load_malformed_yaml(tmp_path):
    p = tmp_path / "m.yaml"
    p.write_text("outputs: [{unclosed")
    with pytest.raises(InvalidOutputsManifestError, match="malformed YAML"):
        load_outputs_manifest(p)


def test_load_top_level_not_mapping(tmp_path):
    """A YAML document whose root isn't a mapping (e.g. a list)
    surfaces the 'top-level must be a mapping' error, not the
    'missing outputs key' one. The two cases are reported separately
    so failures point at the actual shape problem."""
    p = tmp_path / "m.yaml"
    p.write_text("- a\n- b\n")
    with pytest.raises(
        InvalidOutputsManifestError, match="top-level must be a mapping"
    ):
        load_outputs_manifest(p)


def test_load_top_level_null(tmp_path):
    """An empty YAML file parses to None -- that's a top-level shape
    failure (None is not a mapping), distinct from missing-key."""
    p = tmp_path / "m.yaml"
    p.write_text("")
    with pytest.raises(
        InvalidOutputsManifestError, match="top-level must be a mapping"
    ):
        load_outputs_manifest(p)


def test_load_missing_outputs_key(tmp_path):
    """A mapping that doesn't contain ``outputs`` surfaces the
    missing-key error specifically (not the top-level one)."""
    p = _write_yaml(tmp_path / "m.yaml", {"other": []})
    with pytest.raises(InvalidOutputsManifestError, match="missing 'outputs' key"):
        load_outputs_manifest(p)


def test_load_outputs_not_a_list(tmp_path):
    p = _write_yaml(tmp_path / "m.yaml", {"outputs": "not-a-list"})
    with pytest.raises(InvalidOutputsManifestError, match="must be a list"):
        load_outputs_manifest(p)


# --- write_outputs_manifest ----------------------------------------------


def test_write_then_load_roundtrip(tmp_path):
    entries = [{"name": "t1", "file": "/x.parquet"}]
    p = tmp_path / "m.yaml"
    write_outputs_manifest(entries, p)
    assert load_outputs_manifest(p) == entries


def test_write_null_file_roundtrips(tmp_path):
    entries = [{"name": "t1", "file": None}]
    p = tmp_path / "m.yaml"
    write_outputs_manifest(entries, p)
    text = p.read_text()
    # Explicit `null` (YAML's null literal) should appear in the file.
    assert "null" in text
    assert load_outputs_manifest(p) == entries


def test_write_preserves_entry_order(tmp_path):
    entries = [
        {"name": "t2", "file": "/b.parquet"},
        {"name": "t1", "file": "/a.parquet"},
    ]
    p = tmp_path / "m.yaml"
    write_outputs_manifest(entries, p)
    data = yaml.safe_load(p.read_text())
    assert [e["name"] for e in data["outputs"]] == ["t2", "t1"]


def test_write_wraps_under_outputs_top_level_key(tmp_path):
    p = tmp_path / "m.yaml"
    write_outputs_manifest([], p)
    data = yaml.safe_load(p.read_text())
    assert "outputs" in data
    assert data["outputs"] == []


def test_write_creates_missing_parent_dir(tmp_path):
    nested = tmp_path / "a" / "b" / "m.yaml"
    write_outputs_manifest([{"name": "x", "file": None}], nested)
    assert nested.is_file()


def test_write_to_directory_path_raises_oserror(tmp_path):
    d = tmp_path / "i-am-a-dir"
    d.mkdir()
    with pytest.raises(OSError):
        write_outputs_manifest([], d)
