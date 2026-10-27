#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest
import yaml

from tabsdatak.server._tdrunner.function._commons.manifests.state import (
    StateEntry,
    StateManifest,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def test_write_produces_the_state_document(tmp_path):
    path = tmp_path / "state.yaml"
    StateManifest(
        state=(
            StateEntry(name="a", files=("/a.parquet",)),
            StateEntry(name="b"),
        )
    ).write(path)
    assert yaml.safe_load(path.read_text()) == {
        "state": [
            {"name": "a", "files": ["/a.parquet"]},
            {"name": "b", "files": []},
        ]
    }


def test_load_keeps_an_entry_without_files(tmp_path):
    path = tmp_path / "state.yaml"
    path.write_text("state: [{name: a, files: []}, {name: b}]\n")
    assert StateManifest.load(path).state == (
        StateEntry(name="a"),
        StateEntry(name="b"),
    )
