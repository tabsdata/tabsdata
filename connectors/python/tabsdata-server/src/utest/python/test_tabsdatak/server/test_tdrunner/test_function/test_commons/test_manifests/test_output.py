#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest
import yaml

from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputEntry,
    OutputManifest,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def test_write_produces_the_outputs_document(tmp_path):
    path = tmp_path / "outputs.yaml"
    OutputManifest(
        outputs=(
            OutputEntry(name="a", file="/a.parquet"),
            OutputEntry(name="b"),
        )
    ).write(path)
    assert yaml.safe_load(path.read_text()) == {
        "outputs": [
            {"name": "a", "file": "/a.parquet"},
            {"name": "b", "file": None},
        ]
    }


def test_load_keeps_a_null_file_as_not_modified(tmp_path):
    path = tmp_path / "outputs.yaml"
    path.write_text("outputs: [{name: a, file: null}]\n")
    assert OutputManifest.load(path).outputs == (OutputEntry(name="a"),)


def test_load_keeps_files_and_null_files_apart(tmp_path):
    path = tmp_path / "outputs.yaml"
    path.write_text(
        "outputs: [{name: a, file: /a.parquet}, {name: b, file: null}, {name: c}]\n"
    )
    assert OutputManifest.load(path).outputs == (
        OutputEntry(name="a", file="/a.parquet"),
        OutputEntry(name="b"),
        OutputEntry(name="c"),
    )
