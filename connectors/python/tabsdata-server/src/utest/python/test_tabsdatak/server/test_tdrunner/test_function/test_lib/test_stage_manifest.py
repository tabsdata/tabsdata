#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for `_lib/stage_manifest.py`.

Covers:
  * filename format / parse / round-trip
  * filename tz handling (tz-aware → naive UTC)
  * filename error paths
  * `load_stage_manifest` / `write_stage_manifest` round-trip + errors
  * `write_stage_manifest` creates `manifests/` subdir
  * `enumerate_pending` globs only under `manifests/`, sorts chronologically,
    filters non-canonical names, tolerates missing dirs
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from tabsdatak.server._tdrunner.function._lib.stage_layout import (
    MANIFESTS_SUBDIR,
)
from tabsdatak.server._tdrunner.function._lib.stage_manifest import (
    MANIFEST_PREFIX,
    MANIFEST_SUFFIX,
    InvalidStageManifestError,
    enumerate_pending,
    external_uid_from_filename,
    format_manifest_filename,
    load_stage_manifest,
    parse_manifest_filename,
    write_stage_manifest,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- filename format / parse ----------------------------------------------


def test_format_manifest_filename_naive_utc():
    """Canonical case: naive datetime, ms precision, hyphenated time."""
    ts = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    assert (
        format_manifest_filename(ts) == "stream-manifest-2026-06-16T10-30-12.345000.json"
    )


def test_format_manifest_filename_tz_aware_normalised_to_utc():
    """Tz-aware inputs get converted to UTC then rendered as naive.

    The manifest-trigger sidecar passes the filename stem to the
    apiserver as ``external_id``; the apiserver's dedup compares
    string-wise, so a tz suffix would break equality across writers
    in different time zones."""
    from datetime import timedelta

    plus_two = timezone(timedelta(hours=2))
    ts = datetime(2026, 6, 16, 12, 30, 12, 345_000, tzinfo=plus_two)
    # 12:30 +02:00 == 10:30 UTC
    assert (
        format_manifest_filename(ts) == "stream-manifest-2026-06-16T10-30-12.345000.json"
    )


def test_parse_manifest_filename_returns_naive_datetime():
    name = "stream-manifest-2026-06-16T10-30-12.345000.json"
    dt = parse_manifest_filename(name)
    assert dt == datetime(2026, 6, 16, 10, 30, 12, 345_000)
    assert dt.tzinfo is None


def test_format_parse_round_trip():
    """format → parse → format is the identity (modulo tz normalisation)."""
    original = datetime(2026, 1, 15, 12, 0, 0, 0)
    name = format_manifest_filename(original)
    assert parse_manifest_filename(name) == original


def test_parse_manifest_filename_rejects_missing_prefix():
    with pytest.raises(InvalidStageManifestError, match="missing 'stream-manifest-'"):
        parse_manifest_filename("not-a-manifest-2026-06-16T10-30-12.345000.json")


def test_parse_manifest_filename_rejects_missing_suffix():
    with pytest.raises(InvalidStageManifestError, match="missing .json"):
        parse_manifest_filename("stream-manifest-2026-06-16T10-30-12.345000.yaml")


def test_parse_manifest_filename_rejects_malformed_ts():
    """Anything between the prefix and suffix that doesn't match the
    `<date>T<h>-<m>-<s>.<ms>` pattern is rejected -- guards against
    callers handing us POSIX timestamps, free-form strings, etc."""
    with pytest.raises(InvalidStageManifestError, match="not in the expected"):
        parse_manifest_filename("stream-manifest-1234567890.json")


def test_parse_manifest_filename_rejects_invalid_calendar_date():
    """Pattern is shape-checked; the actual fromisoformat call catches
    invalid calendar dates (e.g. Feb 30)."""
    with pytest.raises(InvalidStageManifestError):
        parse_manifest_filename("stream-manifest-2026-02-30T10-30-12.345000.json")


# --- external_uid_from_filename --------------------------------------------


def test_external_uid_from_filename_returns_iso_stem():
    """What the manifest-trigger sidecar will pass to the apiserver as
    ``?external_id=...`` -- guard the exact wire shape."""
    assert (
        external_uid_from_filename("stream-manifest-2026-06-16T10-30-12.345000.json")
        == "2026-06-16T10-30-12.345000"
    )


def test_external_uid_from_filename_rejects_non_canonical():
    with pytest.raises(InvalidStageManifestError):
        external_uid_from_filename("not-a-manifest.json")


# --- load_stage_manifest --------------------------------------------------


def test_load_stage_manifest_returns_list_of_lists(tmp_path):
    path = tmp_path / "m.json"
    path.write_text(json.dumps([["a.parquet", "b.parquet"], ["c.parquet"]]))
    assert load_stage_manifest(path) == [["a.parquet", "b.parquet"], ["c.parquet"]]


def test_load_stage_manifest_accepts_empty_outer_list(tmp_path):
    """An empty manifest is structurally valid (zero slots produced
    this batch) -- not a degenerate case."""
    path = tmp_path / "m.json"
    path.write_text("[]")
    assert load_stage_manifest(path) == []


def test_load_stage_manifest_accepts_empty_inner_lists(tmp_path):
    """A slot with zero files this batch is also valid -- happens when
    a stream-publisher's source produced no records for one of its
    output tables in this rollover window."""
    path = tmp_path / "m.json"
    path.write_text(json.dumps([[], ["c.parquet"]]))
    assert load_stage_manifest(path) == [[], ["c.parquet"]]


def test_load_stage_manifest_missing_file_raises_filenotfound(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_stage_manifest(tmp_path / "missing.json")


def test_load_stage_manifest_rejects_malformed_json(tmp_path):
    path = tmp_path / "m.json"
    path.write_text("{not-json}")
    with pytest.raises(InvalidStageManifestError, match="malformed JSON"):
        load_stage_manifest(path)


def test_load_stage_manifest_rejects_wrong_shape_top_level_dict(tmp_path):
    path = tmp_path / "m.json"
    path.write_text(json.dumps({"slots": []}))
    with pytest.raises(InvalidStageManifestError, match="list of lists of strings"):
        load_stage_manifest(path)


def test_load_stage_manifest_rejects_non_string_paths(tmp_path):
    path = tmp_path / "m.json"
    path.write_text(json.dumps([[1, 2], [3]]))
    with pytest.raises(InvalidStageManifestError, match="list of lists of strings"):
        load_stage_manifest(path)


# --- write_stage_manifest -------------------------------------------------


def test_write_stage_manifest_creates_manifests_subdir(tmp_path):
    """The streamer pod's first call creates `<stage_root>/manifests/`
    -- mkdir(parents=True, exist_ok=True) must work even if the stage
    root itself exists but manifests/ doesn't."""
    stage_root = tmp_path / "stage"
    stage_root.mkdir()
    ts = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    path = write_stage_manifest(stage_root, [["files/a.parquet"]], ts=ts)
    assert (stage_root / MANIFESTS_SUBDIR).is_dir()
    assert path == stage_root / MANIFESTS_SUBDIR / (
        "stream-manifest-2026-06-16T10-30-12.345000.json"
    )


def test_write_stage_manifest_creates_stage_root_when_absent(tmp_path):
    """Even when the stage root itself is missing -- the streamer pod
    is the first writer; nobody else creates the directory tree first."""
    stage_root = tmp_path / "fresh-stage"
    ts = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    write_stage_manifest(stage_root, [["files/a.parquet"]], ts=ts)
    assert (stage_root / MANIFESTS_SUBDIR).is_dir()


def test_write_then_load_stage_manifest_round_trip(tmp_path):
    """The pair (write, load) is the integration boundary between the
    streamer (writer) and the publisher / sidecar (readers) -- nail
    down round-trip equality."""
    stage_root = tmp_path / "stage"
    ts = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    original = [
        ["files/people-2026-06-16.parquet", "files/people-2026-06-16-2.parquet"],
        ["files/orders-2026-06-16.parquet"],
    ]
    path = write_stage_manifest(stage_root, original, ts=ts)
    assert load_stage_manifest(path) == original


def test_write_stage_manifest_overwrites_same_filename(tmp_path):
    """Two calls with the same timestamp produce the same filename;
    the second call overwrites. The streamer never legitimately calls
    twice with the same ts (it derives from utcnow()), but make the
    behaviour explicit."""
    stage_root = tmp_path / "stage"
    ts = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    path1 = write_stage_manifest(stage_root, [["files/a.parquet"]], ts=ts)
    path2 = write_stage_manifest(stage_root, [["files/b.parquet"]], ts=ts)
    assert path1 == path2
    assert load_stage_manifest(path1) == [["files/b.parquet"]]


# --- enumerate_pending ----------------------------------------------------


def test_enumerate_pending_returns_empty_when_stage_root_missing(tmp_path):
    """Returns [] -- caller (sidecar) treats this as 'no work yet'."""
    assert enumerate_pending(tmp_path / "does-not-exist") == []


def test_enumerate_pending_returns_empty_when_manifests_subdir_missing(tmp_path):
    """Streamer hasn't called stage() yet; the stage root might exist
    (the publisher mounted it) but manifests/ does not."""
    stage_root = tmp_path / "stage"
    stage_root.mkdir()
    assert enumerate_pending(stage_root) == []


def test_enumerate_pending_returns_chronological_order(tmp_path):
    """Filenames sort lexicographically == chronologically. The
    sidecar relies on this ordering to call fn_trigger in oldest-
    first order so dedups work correctly under concurrent writes."""
    stage_root = tmp_path / "stage"
    timestamps = [
        datetime(2026, 6, 16, 10, 30, 12, 345_000),
        datetime(2026, 6, 16, 10, 30, 11, 999_000),  # earlier
        datetime(2026, 6, 16, 10, 30, 13, 0),  # later
    ]
    # Write in scrambled order
    for ts in timestamps:
        write_stage_manifest(stage_root, [[]], ts=ts)
    results = enumerate_pending(stage_root)
    assert [p.name for p in results] == [
        "stream-manifest-2026-06-16T10-30-11.999000.json",
        "stream-manifest-2026-06-16T10-30-12.345000.json",
        "stream-manifest-2026-06-16T10-30-13.000000.json",
    ]


def test_enumerate_pending_filters_non_manifest_files(tmp_path):
    """Stage root may legitimately contain other things (parquets
    under files/ get a separate subdir, but a random file dropped at
    manifests/ root shouldn't crash the sidecar)."""
    stage_root = tmp_path / "stage"
    ts = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    write_stage_manifest(stage_root, [[]], ts=ts)
    manifests = stage_root / MANIFESTS_SUBDIR
    (manifests / "README.txt").write_text("not a manifest")
    (manifests / "stream-manifest-bogus.json").write_text("[]")  # bad TS
    (manifests / "other-prefix-2026-06-16T10-30-12.345000.json").write_text("[]")
    results = enumerate_pending(stage_root)
    assert [p.name for p in results] == ["stream-manifest-2026-06-16T10-30-12.345000.json"]


def test_enumerate_pending_ignores_subdirectories(tmp_path):
    """`<stage_root>/files/` lives next to `manifests/`; verifying we
    only return files, not dirs, in case anything ever drops a
    confusingly-named subdir into manifests/."""
    stage_root = tmp_path / "stage"
    ts = datetime(2026, 6, 16, 10, 30, 12, 345_000)
    write_stage_manifest(stage_root, [[]], ts=ts)
    nested = stage_root / MANIFESTS_SUBDIR / "stream-manifest-bogus.json"
    nested.mkdir()  # a directory with a manifest-shaped name
    results = enumerate_pending(stage_root)
    assert len(results) == 1
    assert results[0].name == "stream-manifest-2026-06-16T10-30-12.345000.json"


# --- module constants -----------------------------------------------------


def test_module_constants_pinned():
    """These are part of the wire contract between the streamer (writer),
    the manifest-trigger sidecar (reads stems for external_id), and the
    publisher's ManifestStageSrcPlugin (globs by these names)."""
    assert MANIFEST_PREFIX == "stream-manifest-"
    assert MANIFEST_SUFFIX == ".json"
