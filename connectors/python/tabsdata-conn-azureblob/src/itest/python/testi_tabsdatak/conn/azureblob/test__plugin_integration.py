#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end Azure Blob integration tests: invoke ``td_transporter2`` against a
real Azure Blob container via the ``AzureBlob{Src,Dest}Plugin`` with a fabricated
context. Requires the ``td_transporter2`` binary in the active Python
environment's scripts folder (hydration is mandatory; a missing binary fails
loudly rather than skipping) and the tdx-provisioned ``azureblob`` system
(container + account key come from the ``azureblob_system`` fixture).

Backend-agnostic transporter behaviour (the CSV-option matrix, ragged lines,
encodings, ...) is exercised exhaustively by the LocalFile suite; here we cover
only the object-store-meaningful paths: object round-trips, wildcard object
matching, ``initial_last_modified`` filtering, and per-format round-trips.
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.azureblob import AzureblobSystem, azureblob_system
from tabsdatax.itest.fixture.functions.files import (
    ObjectStore,
    PluginDestCtx,
    PluginSrcCtx,
    download_or_skip,
)

# noinspection PyProtectedMember
import tabsdatak.conn.azureblob._plugin as plugin
from tabsdatak.api import Secret
from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.common._transporter.handler.handler2 import TRANSPORTER_BINARY_NAME
from tabsdatak.conn.azureblob import (
    AzureBlobDest,
    AzureBlobDestConn,
    AzureBlobSrc,
    AzureBlobSrcConn,
)
from tabsdatak.conn.common.file.typing_ import DestFileFormat, SrcFileFormat
from tabsdatak.conn.common.types import AzureAccountKey

pytestmark = pytest.mark.azureblob

TRANSPORTER_BINARY = PythonEnvironment().binary(TRANSPORTER_BINARY_NAME)


# ---------------------------------------------------------------------------
# Fixtures: a per-run object prefix + connections rooted at it
# ---------------------------------------------------------------------------


def _credentials(system: AzureblobSystem) -> AzureAccountKey:
    return AzureAccountKey(
        account_name=Secret(system.account_name),
        account_key=Secret(system.account_key),
    )


@pytest.fixture
def run_base(system_azureblob: AzureblobSystem) -> str:
    """A unique object prefix under the provisioned system's root, so each run
    is isolated and never collides with provisioned model data."""
    root = str(system_azureblob.sys_path).strip("/")
    suffix = f"td-integ/{uuid.uuid4().hex}"
    return f"{root}/{suffix}" if root else suffix


@pytest.fixture
def store(system_azureblob: AzureblobSystem, object_store) -> ObjectStore:
    return object_store(system_azureblob)


@pytest.fixture
def src_conn(system_azureblob: AzureblobSystem, run_base: str) -> AzureBlobSrcConn:
    return AzureBlobSrcConn(
        container=system_azureblob.container,
        credentials=_credentials(system_azureblob),
        base_path="/" + run_base.strip("/"),
    )


@pytest.fixture
def dest_conn(system_azureblob: AzureblobSystem, run_base: str) -> AzureBlobDestConn:
    return AzureBlobDestConn(
        container=system_azureblob.container,
        credentials=_credentials(system_azureblob),
        base_path="/" + run_base.strip("/"),
    )


def _upload_parquet(
    dest_conn: AzureBlobDestConn,
    name: str,
    rows: list[dict],
    tmp_path: Path,
) -> None:
    """Write ``rows`` as a parquet object at ``name`` (relative to the conn's
    base_path) via the dest plugin."""
    local = tmp_path / f"upload-{uuid.uuid4().hex}.parquet"
    pl.DataFrame(rows).write_parquet(local, use_pyarrow=True)
    work = tmp_path / f"work-dest-{uuid.uuid4().hex}"
    work.mkdir()
    dest = AzureBlobDest(paths=[name])
    plugin.AzureBlobDestPlugin().write_out(
        PluginDestCtx(work), dest_conn, dest, [Path(local)]
    )


def _read_back(src_conn: AzureBlobSrcConn, src: AzureBlobSrc, tmp_path: Path):
    work = tmp_path / f"work-src-{uuid.uuid4().hex}"
    work.mkdir()
    return plugin.AzureBlobSrcPlugin().read_in(PluginSrcCtx(work), src_conn, src)


# ---------------------------------------------------------------------------
# Round-trip: write a parquet via the dest plugin, read it back via the src
# ---------------------------------------------------------------------------


@pytest.mark.azureblob
@azureblob_system("azureblob")
def test_dest_then_src_round_trip(
    system_azureblob: AzureblobSystem,
    src_conn: AzureBlobSrcConn,
    dest_conn: AzureBlobDestConn,
    tmp_path: Path,
):
    _upload_parquet(
        dest_conn,
        "users.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
        tmp_path,
    )

    out = _read_back(src_conn, AzureBlobSrc(paths=["users.parquet"]), tmp_path)
    assert len(out) == 1
    assert len(out[0]) >= 1
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


# ---------------------------------------------------------------------------
# Wildcards over object keys
# ---------------------------------------------------------------------------


@pytest.mark.azureblob
@azureblob_system("azureblob")
def test_src_wildcard_in_basename_matches_multiple_objects(
    system_azureblob: AzureblobSystem,
    src_conn: AzureBlobSrcConn,
    dest_conn: AzureBlobDestConn,
    tmp_path: Path,
):
    for name in ("a.parquet", "b.parquet", "c.parquet"):
        _upload_parquet(dest_conn, name, [{"name": name}], tmp_path)
    # A non-matching neighbour must not show up.
    _upload_parquet(dest_conn, "skip.csv", [{"name": "skip"}], tmp_path)

    out = _read_back(src_conn, AzureBlobSrc(paths=["*.parquet"]), tmp_path)
    assert len(out) == 1
    assert len(out[0]) >= 3
    names = sorted(pl.read_parquet(f.file).select("name").item(0, 0) for f in out[0])
    assert names == ["a.parquet", "b.parquet", "c.parquet"]


@pytest.mark.azureblob
@azureblob_system("azureblob")
def test_src_wildcard_with_no_matches_returns_empty_slot(
    system_azureblob: AzureblobSystem,
    src_conn: AzureBlobSrcConn,
    dest_conn: AzureBlobDestConn,
    tmp_path: Path,
):
    _upload_parquet(dest_conn, "only.csv", [{"name": "x"}], tmp_path)

    out = _read_back(src_conn, AzureBlobSrc(paths=["*.parquet"]), tmp_path)
    assert len(out) == 1
    assert out[0] == []


# ---------------------------------------------------------------------------
# initial_last_modified filtering (object timestamps are server-side, so the
# two uploads are deliberately spaced and the cut-off taken between them)
# ---------------------------------------------------------------------------


@pytest.mark.azureblob
@azureblob_system("azureblob")
def test_src_initial_last_modified_filters_out_older_objects(
    system_azureblob: AzureblobSystem,
    src_conn: AzureBlobSrcConn,
    dest_conn: AzureBlobDestConn,
    tmp_path: Path,
):
    _upload_parquet(dest_conn, "old.parquet", [{"name": "old"}], tmp_path)
    time.sleep(2)
    cutoff = datetime.now(timezone.utc)
    time.sleep(2)
    _upload_parquet(dest_conn, "new.parquet", [{"name": "new"}], tmp_path)

    out = _read_back(
        src_conn,
        AzureBlobSrc(paths=["*.parquet"], initial_last_modified=cutoff),
        tmp_path,
    )
    names = sorted(pl.read_parquet(f.file).select("name").item(0, 0) for f in out[0])
    assert names == ["new"]


# ---------------------------------------------------------------------------
# Source-side formats: stage an object in a format, read with the matching
# SrcFileFormat. The transporter always emits parquet, so we assert on parquet.
# ---------------------------------------------------------------------------

_ROWS = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]


def _stage_object(
    store: ObjectStore, run_base: str, name: str, writer, tmp_path: Path
) -> None:
    """Write ``_ROWS`` locally in the target format and upload the raw bytes to
    ``<run_base>/<name>`` (a raw put, so the object genuinely carries that format
    -- the dest plugin would expect parquet input and transcode)."""
    local = tmp_path / f"stage-{uuid.uuid4().hex}-{name}"
    writer(pl.DataFrame(_ROWS), local)
    store.upload(local, f"{run_base}/{name}")


@pytest.mark.azureblob
@azureblob_system("azureblob")
@pytest.mark.parametrize(
    "name, writer, fmt",
    [
        ("users.csv", lambda df, p: df.write_csv(p), SrcFileFormat.CSV),
        ("users.json", lambda df, p: df.write_ndjson(p), SrcFileFormat.JSON),
        ("users.avro", lambda df, p: df.write_avro(p), SrcFileFormat.AVRO),
        (
            "users.parquet",
            lambda df, p: df.write_parquet(p, use_pyarrow=True),
            SrcFileFormat.PARQUET,
        ),
    ],
)
def test_src_reads_each_format(
    system_azureblob: AzureblobSystem,
    src_conn: AzureBlobSrcConn,
    store: ObjectStore,
    run_base: str,
    tmp_path: Path,
    name: str,
    writer,
    fmt: SrcFileFormat,
):
    _stage_object(store, run_base, name, writer, tmp_path)
    out = _read_back(src_conn, AzureBlobSrc(paths=[name], format=fmt), tmp_path)
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == _ROWS


# ---------------------------------------------------------------------------
# Destination-side formats: start from a parquet (the in-flight table format),
# write with the requested DestFileFormat, read the emitted object back with
# the matching reader.
# ---------------------------------------------------------------------------


@pytest.mark.azureblob
@azureblob_system("azureblob")
@pytest.mark.parametrize(
    "name, fmt, reader",
    [
        ("out.csv", DestFileFormat.CSV, pl.read_csv),
        ("out.json", DestFileFormat.JSON, pl.read_ndjson),
        ("out.avro", DestFileFormat.AVRO, pl.read_avro),
        ("out.parquet", DestFileFormat.PARQUET, pl.read_parquet),
    ],
)
def test_dest_writes_each_format(
    system_azureblob: AzureblobSystem,
    dest_conn: AzureBlobDestConn,
    store: ObjectStore,
    run_base: str,
    tmp_path: Path,
    name: str,
    fmt: DestFileFormat,
    reader,
):
    in_parquet = tmp_path / "in.parquet"
    pl.DataFrame(_ROWS).write_parquet(in_parquet, use_pyarrow=True)
    work = tmp_path / f"work-dest-{uuid.uuid4().hex}"
    work.mkdir()
    plugin.AzureBlobDestPlugin().write_out(
        PluginDestCtx(work),
        dest_conn,
        AzureBlobDest(paths=[name], format=fmt),
        [Path(in_parquet)],
    )

    # Download the raw object to verify the transporter wrote the requested
    # format (reading via the src plugin would re-transcode to parquet).
    fetched = download_or_skip(
        store, f"{run_base}/{name}", tmp_path / f"fetched-{name}"
    )
    df = reader(fetched).select("id", "name").sort("id")
    assert df.to_dicts() == _ROWS
