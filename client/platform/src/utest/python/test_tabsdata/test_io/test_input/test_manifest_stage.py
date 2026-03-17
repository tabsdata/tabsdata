#
# Copyright 2026 Tabs Data Inc.
#

from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from tabsdata._io.inputs.stage_input import (
    STAGE_MANIFEST_FILE_EXTENSION,
    STAGE_MANIFEST_LATEST_VERSION,
    StageManifest,
    StageManifestEntry,
    ManifestStage,
    _build_manifest_azure_source,
    _build_manifest_gcs_source,
    _build_manifest_local_source,
    _build_manifest_s3_source,
    _deserialize_manifest,
    _max_nullable_datetime,
    _parse_utc_datetime,
    _read_manifest_file,
    _serialize_manifest,
    _write_manifest_file,
)
from tabsdata._io.plugin import SourcePlugin

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _write_manifest(path: Path, manifest: StageManifest):
    """Write a manifest YAML file at *path* from a Manifest dataclass."""
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_manifest_file(str(path), _serialize_manifest(manifest))


# ===================================================================
# Tests for _parse_utc_datetime
# ===================================================================


class TestParseUtcDatetime:

    def test_z_suffix(self):
        dt = _parse_utc_datetime("2024-03-09T16:00:00Z")
        assert dt == datetime(2024, 3, 9, 16, 0, 0, tzinfo=timezone.utc)
        assert dt.tzinfo is not None

    def test_plus_zero_suffix(self):
        dt = _parse_utc_datetime("2024-03-09T16:00:00+00:00")
        assert dt == datetime(2024, 3, 9, 16, 0, 0, tzinfo=timezone.utc)
        assert dt.tzinfo is not None

    def test_z_and_plus_zero_are_equal(self):
        dt_z = _parse_utc_datetime("2024-03-09T16:00:00Z")
        dt_plus = _parse_utc_datetime("2024-03-09T16:00:00+00:00")
        assert dt_z == dt_plus

    def test_with_microseconds(self):
        dt = _parse_utc_datetime("2024-03-09T16:00:00.123456+00:00")
        assert dt.microsecond == 123456

    def test_naive_string_returns_naive_datetime(self):
        dt = _parse_utc_datetime("2024-03-09T16:00:00")
        assert dt.tzinfo is None


# ===================================================================
# Tests for _max_nullable_datetime
# ===================================================================


class TestMaxNullableDatetime:

    DT_EARLY = datetime(2024, 1, 1, tzinfo=timezone.utc)
    DT_LATE = datetime(2024, 6, 1, tzinfo=timezone.utc)

    def test_both_present_returns_max(self):
        assert _max_nullable_datetime(self.DT_EARLY, self.DT_LATE) == self.DT_LATE
        assert _max_nullable_datetime(self.DT_LATE, self.DT_EARLY) == self.DT_LATE

    def test_first_none(self):
        assert _max_nullable_datetime(None, self.DT_LATE) == self.DT_LATE

    def test_second_none(self):
        assert _max_nullable_datetime(self.DT_EARLY, None) == self.DT_EARLY

    def test_both_none(self):
        assert _max_nullable_datetime(None, None) is None

    def test_equal_values(self):
        assert _max_nullable_datetime(self.DT_EARLY, self.DT_EARLY) == self.DT_EARLY


# ===================================================================
# Tests for _read_manifest_file / _write_manifest_file
# ===================================================================


class TestManifestFileIO:

    def test_read_returns_string(self, tmp_path):
        path = tmp_path / f"test{STAGE_MANIFEST_FILE_EXTENSION}"
        path.write_text("version: 1\n")
        result = _read_manifest_file(str(path))
        assert isinstance(result, str)
        assert "version: 1" in result

    def test_write_creates_file(self, tmp_path):
        path = tmp_path / f"test{STAGE_MANIFEST_FILE_EXTENSION}"
        _write_manifest_file(str(path), "version: 1\n")
        assert path.read_text() == "version: 1\n"

    def test_read_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError):
            _read_manifest_file(f"/nonexistent/path/test{STAGE_MANIFEST_FILE_EXTENSION}")


# ===================================================================
# Tests for _serialize_manifest / _deserialize_manifest
# ===================================================================


class TestManifestSerialization:

    def test_roundtrip(self):
        original = StageManifest(entries=[
            StageManifestEntry(table="schema.table_a", files=["a1.parquet", "a2.parquet"]),
            StageManifestEntry(table="schema.table_b", files=["b1.parquet"]),
        ])
        content = _serialize_manifest(original)
        loaded = _deserialize_manifest(content)
        assert loaded.version == STAGE_MANIFEST_LATEST_VERSION
        assert len(loaded.entries) == len(original.entries)
        for orig, read in zip(original.entries, loaded.entries):
            assert read.table == orig.table
            assert read.files == orig.files

    def test_deserialize_two_groups(self, tmp_path):
        path = tmp_path / f"test{STAGE_MANIFEST_FILE_EXTENSION}"
        _write_manifest(path, StageManifest(entries=[
            StageManifestEntry(table="s.t1", files=["path/to/file.parquet"]),
            StageManifestEntry(table="s.t2", files=["second/file.parquet", "third/file.parquet"]),
        ]))
        content = _read_manifest_file(str(path))
        result = _deserialize_manifest(content)
        assert result.version == STAGE_MANIFEST_LATEST_VERSION
        assert len(result.entries) == 2
        assert result.entries[0].table == "s.t1"
        assert result.entries[0].files == ["path/to/file.parquet"]
        assert result.entries[1].table == "s.t2"
        assert result.entries[1].files == ["second/file.parquet", "third/file.parquet"]

    def test_deserialize_empty_content(self):
        result = _deserialize_manifest("")
        assert result.entries == []

    def test_deserialize_single_group_multiple_files(self):
        original = StageManifest(entries=[
            StageManifestEntry(table="s.t1", files=["a.parquet", "b.parquet", "c.parquet"]),
        ])
        content = _serialize_manifest(original)
        result = _deserialize_manifest(content)
        assert len(result.entries) == 1
        assert result.entries[0].files == ["a.parquet", "b.parquet", "c.parquet"]

    def test_serialize_empty_manifest(self):
        content = _serialize_manifest(StageManifest())
        data = yaml.safe_load(content)
        assert data == {"version": STAGE_MANIFEST_LATEST_VERSION, "data": {}}

    def test_serialize_includes_version(self):
        content = _serialize_manifest(StageManifest(entries=[
            StageManifestEntry(table="s.t", files=["f.parquet"]),
        ]))
        data = yaml.safe_load(content)
        assert data["version"] == STAGE_MANIFEST_LATEST_VERSION
        assert "data" in data
        assert 0 in data["data"]


# ===================================================================
# Tests for _build_manifest_*_source helpers
# ===================================================================


class TestBuildManifestSourceHelpers:

    def test_local_source_single_file(self):
        plugin = _build_manifest_local_source(["/tmp/a.parquet"], {})
        assert plugin.path == ["/tmp/a.parquet"]

    def test_local_source_multiple_files(self):
        paths = ["/tmp/a.parquet", "/tmp/b.parquet"]
        plugin = _build_manifest_local_source(paths, {})
        assert plugin.path == paths

    def test_s3_source_raises(self):
        with pytest.raises(NotImplementedError):
            _build_manifest_s3_source(["s3://bucket/file.parquet"], {})

    def test_azure_source_raises(self):
        with pytest.raises(NotImplementedError):
            _build_manifest_azure_source(["az://container/file.parquet"], {})

    def test_gcs_source_raises(self):
        with pytest.raises(NotImplementedError):
            _build_manifest_gcs_source(["gs://bucket/file.parquet"], {})


# ===================================================================
# Tests for ManifestStage construction
# ===================================================================


class TestManifestStageConstruction:

    def test_default_use_existing_data(self):
        ms = ManifestStage()
        assert ms.use_existing_data is True

    def test_use_existing_data_false(self):
        ms = ManifestStage(use_existing_data=False)
        assert ms.use_existing_data is False

    def test_setter(self):
        ms = ManifestStage()
        ms.use_existing_data = False
        assert ms.use_existing_data is False

    def test_stream_require_ec(self):
        ms = ManifestStage()
        assert ms._stream_require_ec is True

    def test_is_source_plugin(self):
        ms = ManifestStage()
        assert isinstance(ms, SourcePlugin)

    def test_repr(self):
        ms = ManifestStage()
        assert repr(ms) == "ManifestStage(use_existing_data=True)"

    def test_repr_false(self):
        ms = ManifestStage(use_existing_data=False)
        assert repr(ms) == "ManifestStage(use_existing_data=False)"
