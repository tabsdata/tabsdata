#
# Copyright 2026 Tabs Data Inc.
#

"""End-to-end S3 integration tests: invoke ``td_transporter2`` against a real
S3 bucket via the ``S3{Src,Dest}Plugin`` with a fabricated context. Requires the
``td_transporter2`` binary in the active Python environment's scripts folder
(hydration is mandatory; a missing binary fails loudly rather than skipping) and
the tdx-provisioned ``s3`` system (bucket + credentials come from the
``s3_system`` fixture).

Backend-agnostic transporter behaviour (the CSV-option matrix, ragged lines,
encodings, ...) is exercised exhaustively by the LocalFile suite; here we cover
only the S3-meaningful paths: object round-trips, wildcard object matching,
``initial_last_modified`` filtering, and per-format round-trips.
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.s3 import S3System, s3_system

# noinspection PyProtectedMember
import tabsdatak.conn.s3._plugin as plugin
from tabsdatak.api import Secret
from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.common._transporter.handler.handler2 import TRANSPORTER_BINARY_NAME
from tabsdatak.conn.common.file.typing_ import DestFileFormat, SrcFileFormat
from tabsdatak.conn.common.types import AwsAccessSecretKey
from tabsdatak.conn.s3 import S3Dest, S3DestConn, S3Src, S3SrcConn
from tabsdatak.spi import DestContext, SrcPluginCtx

pytestmark = pytest.mark.s3

TRANSPORTER_BINARY = PythonEnvironment().binary(TRANSPORTER_BINARY_NAME)


# ---------------------------------------------------------------------------
# Test contexts
# ---------------------------------------------------------------------------


class _SrcCtx(SrcPluginCtx):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "s3-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "s3-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"

    @property
    def collection_id(self) -> str:
        return "test-collection-id"

    @property
    def collection_name(self) -> str:
        return "test-collection"

    @property
    def function_id(self) -> str:
        return "test-function-id"

    @property
    def function_name(self) -> str:
        return "test-function"

    def get_attr(self, attr):
        return None

    def set_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass


class _DestCtx(DestContext):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "s3-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "s3-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"

    @property
    def collection_id(self) -> str:
        return "test-collection-id"

    @property
    def collection_name(self) -> str:
        return "test-collection"

    @property
    def function_id(self) -> str:
        return "test-function-id"

    @property
    def function_name(self) -> str:
        return "test-function"

    def get_attr(self, attr):
        return None

    def set_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass


# ---------------------------------------------------------------------------
# Fixtures: a per-run object prefix + connections rooted at it
# ---------------------------------------------------------------------------


def _credentials(system: S3System) -> AwsAccessSecretKey:
    return AwsAccessSecretKey(
        access_key_id=Secret(system.access_key),
        secret_access_key=Secret(system.secret_key),
    )


@pytest.fixture
def run_base(system_s3: S3System) -> str:
    """A unique object prefix under the provisioned system's root, so each run
    is isolated and never collides with provisioned model data."""
    root = str(system_s3.sys_path).strip("/")
    suffix = f"td-integ/{uuid.uuid4().hex}"
    return f"{root}/{suffix}" if root else suffix


@pytest.fixture
def s3_client(system_s3: S3System):
    """A raw boto3 client, for downloading emitted objects to verify the bytes
    the transporter actually wrote (the src plugin would re-transcode to
    parquet, hiding the on-object format)."""
    import boto3

    return boto3.client(
        "s3",
        region_name=system_s3.region or None,
        aws_access_key_id=system_s3.access_key or None,
        aws_secret_access_key=system_s3.secret_key or None,
    )


@pytest.fixture
def src_conn(system_s3: S3System, run_base: str) -> S3SrcConn:
    return S3SrcConn(
        bucket=system_s3.bucket,
        region=system_s3.region,
        credentials=_credentials(system_s3),
        base_path=run_base,
    )


@pytest.fixture
def dest_conn(system_s3: S3System, run_base: str) -> S3DestConn:
    return S3DestConn(
        bucket=system_s3.bucket,
        region=system_s3.region,
        credentials=_credentials(system_s3),
        base_path=run_base,
    )


def _upload_parquet(
    dest_conn: S3DestConn,
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
    dest = S3Dest(paths=[name])
    plugin.S3DestPlugin().write_out(_DestCtx(work), dest_conn, dest, [Path(local)])


def _read_back(src_conn: S3SrcConn, src: S3Src, tmp_path: Path):
    work = tmp_path / f"work-src-{uuid.uuid4().hex}"
    work.mkdir()
    return plugin.S3SrcPlugin().read_in(_SrcCtx(work), src_conn, src)


# ---------------------------------------------------------------------------
# Round-trip: write a parquet via the dest plugin, read it back via the src
# ---------------------------------------------------------------------------


@pytest.mark.s3_specific
@s3_system("s3")
def test_dest_then_src_round_trip(
    src_conn: S3SrcConn, dest_conn: S3DestConn, tmp_path: Path
):
    _upload_parquet(
        dest_conn,
        "users.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
        tmp_path,
    )

    out = _read_back(src_conn, S3Src(paths=["users.parquet"]), tmp_path)
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


@pytest.mark.s3_specific
@s3_system("s3")
def test_src_wildcard_in_basename_matches_multiple_objects(
    src_conn: S3SrcConn, dest_conn: S3DestConn, tmp_path: Path
):
    for name in ("a.parquet", "b.parquet", "c.parquet"):
        _upload_parquet(dest_conn, name, [{"name": name}], tmp_path)
    # A non-matching neighbour must not show up.
    _upload_parquet(dest_conn, "skip.csv", [{"name": "skip"}], tmp_path)

    out = _read_back(src_conn, S3Src(paths=["*.parquet"]), tmp_path)
    assert len(out) == 1
    assert len(out[0]) >= 3
    names = sorted(pl.read_parquet(f.file).select("name").item(0, 0) for f in out[0])
    assert names == ["a.parquet", "b.parquet", "c.parquet"]


@pytest.mark.s3_specific
@s3_system("s3")
def test_src_wildcard_with_no_matches_returns_empty_slot(
    src_conn: S3SrcConn, dest_conn: S3DestConn, tmp_path: Path
):
    _upload_parquet(dest_conn, "only.csv", [{"name": "x"}], tmp_path)

    out = _read_back(src_conn, S3Src(paths=["*.parquet"]), tmp_path)
    assert len(out) == 1
    assert out[0] == []


# ---------------------------------------------------------------------------
# initial_last_modified filtering (object timestamps are server-side, so the
# two uploads are deliberately spaced and the cut-off taken between them)
# ---------------------------------------------------------------------------


@pytest.mark.s3_specific
@s3_system("s3")
def test_src_initial_last_modified_filters_out_older_objects(
    src_conn: S3SrcConn, dest_conn: S3DestConn, tmp_path: Path
):
    _upload_parquet(dest_conn, "old.parquet", [{"name": "old"}], tmp_path)
    time.sleep(2)
    cutoff = datetime.now(timezone.utc)
    time.sleep(2)
    _upload_parquet(dest_conn, "new.parquet", [{"name": "new"}], tmp_path)

    out = _read_back(
        src_conn,
        S3Src(paths=["*.parquet"], initial_last_modified=cutoff),
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
    s3_client, bucket: str, run_base: str, name: str, writer, tmp_path: Path
) -> None:
    """Write ``_ROWS`` locally in the target format and upload the raw bytes to
    ``<run_base>/<name>`` (a raw put, so the object genuinely carries that format
    -- the dest plugin would expect parquet input and transcode)."""
    local = tmp_path / f"stage-{uuid.uuid4().hex}-{name}"
    writer(pl.DataFrame(_ROWS), local)
    s3_client.upload_file(str(local), bucket, f"{run_base}/{name}")


@pytest.mark.s3_specific
@s3_system("s3")
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
    src_conn: S3SrcConn,
    s3_client,
    system_s3: S3System,
    run_base: str,
    tmp_path: Path,
    name: str,
    writer,
    fmt: SrcFileFormat,
):
    _stage_object(s3_client, system_s3.bucket, run_base, name, writer, tmp_path)
    out = _read_back(src_conn, S3Src(paths=[name], format=fmt), tmp_path)
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == _ROWS


# ---------------------------------------------------------------------------
# Destination-side formats: start from a parquet (the in-flight table format),
# write with the requested DestFileFormat, read the emitted object back with
# the matching reader.
# ---------------------------------------------------------------------------


@pytest.mark.s3_specific
@s3_system("s3")
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
    dest_conn: S3DestConn,
    system_s3: S3System,
    s3_client,
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
    plugin.S3DestPlugin().write_out(
        _DestCtx(work),
        dest_conn,
        S3Dest(paths=[name], format=fmt),
        [Path(in_parquet)],
    )

    # Download the raw object to verify the transporter wrote the requested
    # format (reading via the src plugin would re-transcode to parquet).
    fetched = tmp_path / f"fetched-{name}"
    s3_client.download_file(system_s3.bucket, f"{run_base}/{name}", str(fetched))
    df = reader(fetched).select("id", "name").sort("id")
    assert df.to_dicts() == _ROWS
