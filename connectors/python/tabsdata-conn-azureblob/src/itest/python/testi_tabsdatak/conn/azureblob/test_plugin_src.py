#
# Copyright 2026 Tabsdata Inc.
#

"""AzureBlob source-plugin integration tests using fake context"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.azureblob import AzureblobSystem, azureblob_system
from tabsdatax.itest.fixture.functions.files import ObjectStore, PluginSrcCtx
from testi_tabsdatak.conn.azureblob.backend import run_prefix, src_conn, store
from testi_tabsdatak.conn.common.plugin_ctx import LastModSrcCtx

# noinspection PyProtectedMember
from tabsdatak.conn.azureblob import AzureBlobSrc, _plugin as plugin
from tabsdatak.conn.common.file.typing_ import SrcFileFormat

# Backend-agnostic transporter behaviour (the CSV-option matrix, ragged lines,
# encodings, ...) is exercised exhaustively by the LocalFile suite; here only the
# object-store-meaningful paths are covered.
pytestmark = pytest.mark.azureblob

ROWS = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]

Writer = Callable[[pl.DataFrame, Path], None]


def _parquet(frame: pl.DataFrame, path: Path) -> None:
    frame.write_parquet(path, use_pyarrow=True)


def _stage(
    objects: ObjectStore,
    prefix: str,
    name: str,
    tmp_path: Path,
    rows: list[dict] = ROWS,
    writer: Writer = _parquet,
) -> None:
    """Write ``rows`` locally in the target format and put the raw bytes at
    ``<prefix>/<name>``, so the blob genuinely carries that format."""
    # `name` may be a nested object key; the local staging file stays flat.
    local = tmp_path / f"stage-{uuid.uuid4().hex}-{name.replace('/', '_')}"
    writer(pl.DataFrame(rows), local)
    objects.upload(local, f"{prefix}/{name}")


def _read_back(system: AzureblobSystem, prefix: str, src: AzureBlobSrc, tmp_path: Path):
    work = tmp_path / f"work-src-{uuid.uuid4().hex}"
    work.mkdir()
    return plugin.AzureBlobSrcPlugin().read_in(
        PluginSrcCtx(work), src_conn(system, prefix), src
    )


@azureblob_system("azureblob")
def test_src_wildcard_in_basename_matches_multiple_objects(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    objects = store(system_azureblob)
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    for name in ("a.parquet", "b.parquet", "c.parquet"):
        _stage(objects, prefix, name, tmp_path, rows=[{"name": name}])
    # A non-matching neighbour must not show up.
    _stage(objects, prefix, "skip.csv", tmp_path, rows=[{"name": "skip"}])

    out = _read_back(
        system_azureblob, prefix, AzureBlobSrc(paths=["*.parquet"]), tmp_path
    )
    assert len(out) == 1
    assert len(out[0]) >= 3
    names = sorted(pl.read_parquet(f.file).select("name").item(0, 0) for f in out[0])
    assert names == ["a.parquet", "b.parquet", "c.parquet"]


@azureblob_system("azureblob")
def test_src_wildcard_with_no_matches_returns_empty_slot(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage(store(system_azureblob), prefix, "only.csv", tmp_path, rows=[{"name": "x"}])

    out = _read_back(
        system_azureblob, prefix, AzureBlobSrc(paths=["*.parquet"]), tmp_path
    )
    assert len(out) == 1
    assert out[0] == []


@azureblob_system("azureblob")
def test_src_initial_last_modified_filters_out_older_objects(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    # Blob timestamps are server-side, so the two uploads are deliberately spaced
    # and the cut-off taken between them.
    objects = store(system_azureblob)
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage(objects, prefix, "old.parquet", tmp_path, rows=[{"name": "old"}])
    time.sleep(2)
    cutoff = datetime.now(timezone.utc)
    time.sleep(2)
    _stage(objects, prefix, "new.parquet", tmp_path, rows=[{"name": "new"}])

    out = _read_back(
        system_azureblob,
        prefix,
        AzureBlobSrc(paths=["*.parquet"], initial_last_modified=cutoff),
        tmp_path,
    )
    names = sorted(pl.read_parquet(f.file).select("name").item(0, 0) for f in out[0])
    assert names == ["new"]


@azureblob_system("azureblob")
@pytest.mark.parametrize(
    "name, writer, fmt",
    [
        ("users.csv", lambda df, p: df.write_csv(p), SrcFileFormat.CSV),
        ("users.json", lambda df, p: df.write_ndjson(p), SrcFileFormat.JSON),
        ("users.avro", lambda df, p: df.write_avro(p), SrcFileFormat.AVRO),
        ("users.parquet", _parquet, SrcFileFormat.PARQUET),
    ],
)
def test_src_reads_each_format(
    system_azureblob: AzureblobSystem,
    tmp_path: Path,
    name: str,
    writer: Writer,
    fmt: SrcFileFormat,
):
    # The transporter always emits parquet, so every format is asserted on the
    # parquet it produced.
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage(store(system_azureblob), prefix, name, tmp_path, writer=writer)
    out = _read_back(
        system_azureblob, prefix, AzureBlobSrc(paths=[name], format=fmt), tmp_path
    )
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == ROWS


# A baseline safely older than anything this run uploads: object timestamps are
# server-assigned, so the cursor tests anchor on "an hour ago" rather than trying
# to place a cut-off between two uploads.
def _baseline() -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=1)


def _names(slot) -> list[str]:
    return sorted(pl.read_parquet(f.file).select("name").item(0, 0) for f in slot)


def _read_with(ctx, system: AzureblobSystem, prefix: str, src: AzureBlobSrc):
    return plugin.AzureBlobSrcPlugin().read_in(ctx, src_conn(system, prefix), src)


def _work(tmp_path: Path, name: str) -> Path:
    out = tmp_path / f"work-{name}"
    out.mkdir()
    return out


@azureblob_system("azureblob")
def test_src_one_slot_per_path_in_path_order(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    objects = store(system_azureblob)
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    for name in ("in/a1.parquet", "in/a2.parquet", "out/b1.parquet", "single.parquet"):
        _stage(objects, prefix, name, tmp_path, rows=[{"name": name}])

    src = AzureBlobSrc(paths=["out/*.parquet", "in/*.parquet", "single.parquet"])
    out = _read_back(system_azureblob, prefix, src, tmp_path)

    assert [_names(slot) for slot in out] == [
        ["out/b1.parquet"],
        ["in/a1.parquet", "in/a2.parquet"],
        ["single.parquet"],
    ]


@azureblob_system("azureblob")
def test_src_overlapping_globs_import_the_same_object_in_every_slot(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    objects = store(system_azureblob)
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage(objects, prefix, "shared.parquet", tmp_path, rows=[{"name": "shared"}])

    src = AzureBlobSrc(
        paths=["*.parquet", "sh*.parquet"], initial_last_modified=_baseline()
    )
    ctx = LastModSrcCtx(_work(tmp_path, "first"))

    out = _read_with(ctx, system_azureblob, prefix, src)

    # Each slot keeps its own cursor, so an object both patterns match is imported
    # once per slot rather than deduplicated across them.
    assert [_names(slot) for slot in out] == [["shared"], ["shared"]]
    assert sorted(ctx.attrs) == ["file-lastmod.slot0", "file-lastmod.slot1"]

    resumed = LastModSrcCtx(_work(tmp_path, "resumed"), ctx.attrs)
    assert _read_with(resumed, system_azureblob, prefix, src) == [[], []]


@azureblob_system("azureblob")
def test_src_second_run_resumes_from_the_stored_cursor(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    objects = store(system_azureblob)
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage(objects, prefix, "first.parquet", tmp_path, rows=[{"name": "first"}])

    src = AzureBlobSrc(paths=["*.parquet"], initial_last_modified=_baseline())
    ctx = LastModSrcCtx(_work(tmp_path, "run1"))

    assert _names(_read_with(ctx, system_azureblob, prefix, src)[0]) == ["first"]
    assert sorted(ctx.attrs) == ["file-lastmod.slot0"]

    time.sleep(2)
    _stage(objects, prefix, "second.parquet", tmp_path, rows=[{"name": "second"}])
    resumed = LastModSrcCtx(_work(tmp_path, "run2"), ctx.attrs)

    # The cursor, not the baseline, governs once one is stored: the object the
    # first run consumed stays consumed.
    assert _names(_read_with(resumed, system_azureblob, prefix, src)[0]) == ["second"]


@azureblob_system("azureblob")
def test_src_without_initial_last_modified_keeps_no_cursor(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    # A Src with no baseline is not incremental at all: nothing is stored on the
    # context, so every run re-imports whatever the glob matches.
    objects = store(system_azureblob)
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage(objects, prefix, "only.parquet", tmp_path, rows=[{"name": "only"}])

    src = AzureBlobSrc(paths=["*.parquet"])
    ctx = LastModSrcCtx(_work(tmp_path, "run1"))

    assert _names(_read_with(ctx, system_azureblob, prefix, src)[0]) == ["only"]
    assert ctx.attrs == {}

    again = LastModSrcCtx(_work(tmp_path, "run2"), ctx.attrs)
    assert _names(_read_with(again, system_azureblob, prefix, src)[0]) == ["only"]


@azureblob_system("azureblob")
def test_src_reordering_paths_reimports_because_slots_are_positional(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    objects = store(system_azureblob)
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage(objects, prefix, "in/a.parquet", tmp_path, rows=[{"name": "a"}])
    _stage(objects, prefix, "out/b.parquet", tmp_path, rows=[{"name": "b"}])

    # Distinct basename globs on purpose: a cursor entry is keyed by the basename
    # pattern alone (the transporter splits the path and keys on the part after the
    # last `/`), so two directory globs that share `*.parquet` would share the entry
    # and the swap below would turn on which object is newer.
    baseline = _baseline()
    ordered = AzureBlobSrc(
        paths=["in/a*.parquet", "out/b*.parquet"], initial_last_modified=baseline
    )
    ctx = LastModSrcCtx(_work(tmp_path, "run1"))

    assert [
        _names(slot) for slot in _read_with(ctx, system_azureblob, prefix, ordered)
    ] == [
        ["a"],
        ["b"],
    ]

    unchanged = LastModSrcCtx(_work(tmp_path, "run2"), ctx.attrs)
    assert _read_with(unchanged, system_azureblob, prefix, ordered) == [[], []]

    # A cursor is addressed by the path's position, not by the path itself, so
    # swapping two entries hands each slot the other one's cursor -- which knows
    # nothing about this slot's pattern -- and both objects are imported again.
    swapped = AzureBlobSrc(
        paths=["out/b*.parquet", "in/a*.parquet"], initial_last_modified=baseline
    )
    moved = LastModSrcCtx(_work(tmp_path, "run3"), ctx.attrs)
    assert [
        _names(slot) for slot in _read_with(moved, system_azureblob, prefix, swapped)
    ] == [
        ["b"],
        ["a"],
    ]
