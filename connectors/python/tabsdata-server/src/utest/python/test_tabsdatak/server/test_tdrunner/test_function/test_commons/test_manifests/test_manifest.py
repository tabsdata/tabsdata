#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass, fields

import pytest
import yaml

from tabsdatak.server._tdrunner.function._commons.manifests.input import (
    InputEntry,
    InputManifest,
    InputRecord,
)
from tabsdatak.server._tdrunner.function._commons.manifests.manifest import (
    Manifest,
    ManifestError,
)
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputEntry,
    OutputManifest,
    TableStats,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


@dataclass(frozen=True)
class Sample:
    manifest: Manifest
    entry_without_required_field: dict

    @property
    def key(self) -> str:
        return fields(self.manifest)[0].name

    @property
    def entries(self) -> tuple:
        return getattr(self.manifest, self.key)

    def with_entries(self, entries: tuple) -> Manifest:
        return type(self.manifest)(**{self.key: entries})


SAMPLES = {
    "inputs": Sample(
        manifest=InputManifest(
            inputs=(
                InputEntry(
                    name="a",
                    table_pos=0,
                    tables=(InputRecord(file="/a.parquet", input_idx=0),),
                ),
                InputEntry(name="b", table_pos=1),
            )
        ),
        entry_without_required_field={"name": "a"},
    ),
    "outputs": Sample(
        manifest=OutputManifest(
            outputs=(
                OutputEntry(
                    name="a",
                    uri="s3://b/a.t",
                    stats=TableStats(
                        column_count=1,
                        row_count=2,
                        schema_hash="sha256:abc",
                        data_bytes=64,
                    ),
                ),
                OutputEntry(name="b"),
            )
        ),
        entry_without_required_field={"uri": "s3://b/a.t"},
    ),
}


@pytest.fixture(params=sorted(SAMPLES))
def sample(request) -> Sample:
    return SAMPLES[request.param]


def manifest_class(sample: Sample) -> type[Manifest]:
    return type(sample.manifest)


def written(tmp_path, content: str):
    path = tmp_path / "manifest.yaml"
    path.write_text(content)
    return path


def test_round_trip(tmp_path, sample):
    path = tmp_path / "manifest.yaml"
    sample.manifest.write(path)
    assert manifest_class(sample).load(path) == sample.manifest


def test_round_trip_without_entries(tmp_path, sample):
    path = tmp_path / "manifest.yaml"
    empty = sample.with_entries(())
    empty.write(path)
    assert manifest_class(sample).load(path) == empty


def test_write_keeps_the_entry_order(tmp_path, sample):
    path = tmp_path / "manifest.yaml"
    reversed_manifest = sample.with_entries(tuple(reversed(sample.entries)))
    reversed_manifest.write(path)
    document = yaml.safe_load(path.read_text())
    assert [entry["name"] for entry in document[sample.key]] == ["b", "a"]


def test_write_puts_the_entries_under_the_manifest_key(tmp_path, sample):
    path = tmp_path / "manifest.yaml"
    sample.manifest.write(path)
    assert list(yaml.safe_load(path.read_text()))[0] == sample.key


def test_write_creates_the_parent_folder(tmp_path, sample):
    path = tmp_path / "a" / "b" / "manifest.yaml"
    sample.manifest.write(path)
    assert path.is_file()


def test_write_to_a_folder_raises(tmp_path, sample):
    with pytest.raises(OSError):
        sample.manifest.write(tmp_path)


def test_load_ignores_unknown_keys(tmp_path, sample):
    path = tmp_path / "manifest.yaml"
    sample.manifest.write(path)
    document = yaml.safe_load(path.read_text())
    for entry in document[sample.key]:
        entry["unknown"] = 1
    path.write_text(yaml.safe_dump(document))
    assert manifest_class(sample).load(path) == sample.manifest


def test_load_rejects_a_missing_file(tmp_path, sample):
    with pytest.raises(ManifestError):
        manifest_class(sample).load(tmp_path / "missing.yaml")


@pytest.mark.parametrize(
    "content",
    [
        "{key}: [{{unclosed",
        "- a\n- b\n",
        "",
        "null\n",
        "other: []\n",
        "{key}: not-a-list\n",
        "{key}: [not-a-mapping]\n",
    ],
    ids=[
        "malformed-yaml",
        "top-level-list",
        "empty",
        "top-level-null",
        "missing-key",
        "not-a-list",
        "entry-not-a-mapping",
    ],
)
def test_load_rejects_a_malformed_manifest(tmp_path, sample, content):
    path = written(tmp_path, content.format(key=sample.key))
    with pytest.raises(ManifestError, match=str(path)):
        manifest_class(sample).load(path)


def test_load_rejects_an_entry_without_a_required_field(tmp_path, sample):
    path = written(
        tmp_path,
        yaml.safe_dump({sample.key: [sample.entry_without_required_field]}),
    )
    with pytest.raises(ManifestError):
        manifest_class(sample).load(path)


@pytest.mark.parametrize(
    ("kind", "other"),
    [
        (kind, other)
        for kind in sorted(SAMPLES)
        for other in sorted(SAMPLES)
        if kind != other
    ],
)
def test_load_rejects_another_manifest(tmp_path, kind, other):
    path = tmp_path / "manifest.yaml"
    SAMPLES[other].manifest.write(path)
    with pytest.raises(ManifestError):
        manifest_class(SAMPLES[kind]).load(path)
