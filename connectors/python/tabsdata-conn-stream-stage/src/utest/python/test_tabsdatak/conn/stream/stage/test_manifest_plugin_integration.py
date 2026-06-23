#
# Copyright 2026 Tabs Data Inc.
#

"""Integration tests for :class:`ManifestStageSrcPlugin` against the
real ``td_transporter`` binary, exercised over the local-file backend.

Skipped unless ``TD_TRANSPORTER_BIN`` points at the binary::

    export TD_TRANSPORTER_BIN=/path/to/td_transporter
    pytest .../test_tabsdatak/connector/_stream -k integration

``ManifestStageConn`` is the read-side scaffolding; its concrete kwargs
constructor is still being shaped, so these tests drive ``read_in`` with
a duck-typed stand-in carrying the four backend slots (only the
populated backend's ``full_path`` / plugin helpers are touched) plus a
real :class:`LocalFileSrcConn`. Swap the stand-in for a real conn once
it gains its ``@_api.dataclass`` decorator.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.localfile import LocalFileSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.stream.stage import (
    _STREAM_MANIFEST_FILE_WILDCARD,
    ManifestStageSrcPlugin,
)

_TRANSPORTER_BIN = os.environ.get("TD_TRANSPORTER_BIN")

pytestmark = pytest.mark.skipif(
    not _TRANSPORTER_BIN or not Path(_TRANSPORTER_BIN).is_file(),
    reason=(
        "TD_TRANSPORTER_BIN env var is unset or doesn't point at an "
        "existing file; integration tests skipped"
    ),
)


class _Ctx:
    """Minimal SrcPluginCtx stand-in: a work dir + a per-flow scratch store."""

    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir
        self._store: dict[str, object] = {}

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    def get_attr(self, key):
        return self._store.get(key)

    def store_attr(self, key, val):
        self._store[key] = val


def _local_conn(base_path: Path):
    return SimpleNamespace(
        local=LocalFileSrcConn(base_path=str(base_path)),
        s3=None,
        az=None,
        gcs=None,
    )


def _write_parquet(path: Path, rows: list[dict]) -> None:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)


def _make_scenario(tmp_path: Path) -> tuple[Path, Path]:
    data = tmp_path / "data"
    data.mkdir()
    work = tmp_path / "work"
    work.mkdir()
    _write_parquet(data / "t0_a.parquet", [{"id": 1}, {"id": 2}])
    _write_parquet(data / "t1_a.parquet", [{"id": 3}])
    _write_parquet(data / "t1_b.parquet", [{"id": 4}])
    # The new layout (per NU1's ``stage_layout`` module): manifests live
    # under ``<base>/manifests/`` so the plugin's wildcard can target
    # exactly that subdir. File paths inside each manifest stay
    # relative to ``<base>``.
    manifests_dir = data / "manifests"
    manifests_dir.mkdir()
    # One slot with one file, one slot with two; paths relative to base_path.
    manifest = [["t0_a.parquet"], ["t1_a.parquet", "t1_b.parquet"]]
    (manifests_dir / "stream-manifest-0001.json").write_text(json.dumps(manifest))
    return data, work


def test_read_in_materialises_nested_slots(tmp_path: Path):
    data, work = _make_scenario(tmp_path)
    ctx = _Ctx(work)
    plugin = ManifestStageSrcPlugin()

    result = plugin.read_in(ctx, _local_conn(data), SimpleNamespace())

    # Nested structure mirrors the manifest: slot0 -> 1 file, slot1 -> 2 files.
    assert [len(slot) for slot in result] == [1, 2]
    for slot in result:
        for parquet in slot:
            assert Path(parquet).exists()
            pl.read_parquet(parquet)  # readable
    # First slot's single file round-trips its contents.
    assert pl.read_parquet(result[0][0]).sort("id").to_dicts() == [
        {"id": 1},
        {"id": 2},
    ]


def test_read_in_consumes_one_manifest_per_call(tmp_path: Path):
    data, work = _make_scenario(tmp_path)
    ctx = _Ctx(work)
    plugin = ManifestStageSrcPlugin()

    first = plugin.read_in(ctx, _local_conn(data), SimpleNamespace())
    assert first  # produced the manifest's slots
    # The cursor advanced past the only manifest...
    assert ctx.get_attr("stream-manifest.lastmod") is not None
    # ...so a second trigger finds nothing new and produces zero slots.
    assert plugin.read_in(ctx, _local_conn(data), SimpleNamespace()) == []


def test_read_in_picks_up_next_manifest(tmp_path: Path):
    data, work = _make_scenario(tmp_path)
    ctx = _Ctx(work)
    plugin = ManifestStageSrcPlugin()

    assert plugin.read_in(ctx, _local_conn(data), SimpleNamespace())
    assert plugin.read_in(ctx, _local_conn(data), SimpleNamespace()) == []

    # A newer manifest shows up; the next trigger consumes it.
    _write_parquet(data / "t2.parquet", [{"id": 5}])
    (data / "manifests" / "stream-manifest-0002.json").write_text(
        json.dumps([["t2.parquet"]])
    )

    third = plugin.read_in(ctx, _local_conn(data), SimpleNamespace())
    assert [len(slot) for slot in third] == [1]
    assert Path(third[0][0]).name.endswith("t2.parquet")


def test_read_in_requires_exactly_one_backend(tmp_path: Path):
    _, work = _make_scenario(tmp_path)
    plugin = ManifestStageSrcPlugin()
    no_backend = SimpleNamespace(local=None, s3=None, az=None, gcs=None)
    with pytest.raises(ConnInitException):
        plugin.read_in(_Ctx(work), no_backend, SimpleNamespace())


def test_read_in_rejects_malformed_manifest(tmp_path: Path):
    data, work = _make_scenario(tmp_path)
    # Overwrite with a non-list-of-lists payload.
    (data / "manifests" / "stream-manifest-0001.json").write_text(
        json.dumps({"not": "a list"})
    )
    plugin = ManifestStageSrcPlugin()
    with pytest.raises(Exception) as exc_info:
        plugin.read_in(_Ctx(work), _local_conn(data), SimpleNamespace())
    # MANIFEST_STAGE_7: shape validation failure.
    assert "must be a JSON list of lists" in str(exc_info.value)


def test_wildcard_constant_is_glob():
    assert "*" in _STREAM_MANIFEST_FILE_WILDCARD
