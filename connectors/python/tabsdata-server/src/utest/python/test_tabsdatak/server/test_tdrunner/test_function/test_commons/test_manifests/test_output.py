#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest
import yaml

from tabsdatak.server._tdrunner.function._commons.manifests.manifest import (
    ManifestError,
)
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputEntry,
    OutputManifest,
    TableStats,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

STATS = TableStats(column_count=2, row_count=3, schema_hash="sha256:abc", data_bytes=64)
STATS_DOCUMENT = {
    "column_count": 2,
    "row_count": 3,
    "schema_hash": "sha256:abc",
    "data_bytes": 64,
}


def test_write_produces_the_outputs_document(tmp_path):
    path = tmp_path / "outputs.yaml"
    OutputManifest(
        outputs=(
            OutputEntry(name="a", uri="s3://b/a.t", stats=STATS),
            OutputEntry(name="b"),
        )
    ).write(path)
    assert yaml.safe_load(path.read_text()) == {
        "outputs": [
            {"name": "a", "uri": "s3://b/a.t", "stats": STATS_DOCUMENT},
            {"name": "b", "uri": None, "stats": None},
        ],
        "state": None,
    }


def test_round_trip_keeps_the_state(tmp_path):
    path = tmp_path / "outputs.yaml"
    manifest = OutputManifest(
        outputs=(),
        state=OutputEntry(name="td.fn_state__f", uri="s3://b/s.t", stats=STATS),
    )
    manifest.write(path)
    assert OutputManifest.load(path) == manifest


def test_load_without_state_has_none(tmp_path):
    path = tmp_path / "outputs.yaml"
    path.write_text("outputs: []\n")
    assert OutputManifest.load(path).state is None


def test_load_keeps_an_entry_without_data_as_not_modified(tmp_path):
    path = tmp_path / "outputs.yaml"
    path.write_text("outputs: [{name: a, uri: null, stats: null}, {name: b}]\n")
    assert OutputManifest.load(path).outputs == (
        OutputEntry(name="a"),
        OutputEntry(name="b"),
    )


def test_load_reads_the_stats(tmp_path):
    path = tmp_path / "outputs.yaml"
    path.write_text(
        yaml.safe_dump({
            "outputs": [{"name": "a", "uri": "s3://b/a.t", "stats": STATS_DOCUMENT}]
        })
    )
    assert OutputManifest.load(path).outputs == (
        OutputEntry(name="a", uri="s3://b/a.t", stats=STATS),
    )


@pytest.mark.parametrize("missing", sorted(STATS_DOCUMENT))
def test_load_rejects_incomplete_stats(tmp_path, missing):
    stats = {key: value for key, value in STATS_DOCUMENT.items() if key != missing}
    path = tmp_path / "outputs.yaml"
    path.write_text(
        yaml.safe_dump({
            "outputs": [{"name": "a", "uri": "s3://b/a.t", "stats": stats}]
        })
    )
    with pytest.raises(ManifestError, match=missing):
        OutputManifest.load(path)
