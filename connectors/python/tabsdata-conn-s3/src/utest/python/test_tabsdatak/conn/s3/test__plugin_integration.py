"""End-to-end S3 integration tests: invoke ``td_transporter`` against
a real S3 bucket. Skipped unless ALL of these are set:

  - ``TD_TRANSPORTER_BIN``    -- path to a built transporter binary
  - ``S30__S3_URI``           -- ``s3://bucket[/path/prefix]``
  - ``S30__S3_REGION``        -- bucket region (e.g. ``us-east-1``)
  - ``S30__S3_ACCESS_KEY``    -- access key
  - ``S30__S3_SECRET_KEY``    -- secret key

The URI's path component is used as ``S3Conn.base_path`` so each run
writes under ``<bucket>/<uri_path>/td-integ/<uuid>/``.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import AwsAccessSecretKey
from tabsdatak.conn.s3 import S3Dest, S3DestConn, S3Src, S3SrcConn
# noinspection PyProtectedMember
import tabsdatak.conn.s3._plugin as plugin



pytestmark = pytest.mark.s3


_TRANSPORTER_BIN = os.environ.get("TD_TRANSPORTER_BIN")
_URI = os.environ.get("S30__S3_URI")
_REGION = os.environ.get("S30__S3_REGION")
_ACCESS = os.environ.get("S30__S3_ACCESS_KEY")
_SECRET = os.environ.get("S30__S3_SECRET_KEY")


_skip_reason = (
    "S3 integration tests require TD_TRANSPORTER_BIN + S30__S3_URI + "
    "S30__S3_REGION + S30__S3_ACCESS_KEY + S30__S3_SECRET_KEY"
)


def _gates_ok() -> bool:
    return all([
        _TRANSPORTER_BIN and Path(_TRANSPORTER_BIN).is_file(),
        _URI, _REGION, _ACCESS, _SECRET,
    ])


pytestmark = [pytestmark, pytest.mark.skipif(not _gates_ok(), reason=_skip_reason)]


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


class _SrcCtx:
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

    def get_attr(self, attr):
        return None

    def set_attr(self, attr, val):
        pass

    def add_metadata(self, attr, val):
        pass


class _DestCtx(_SrcCtx):
    pass


def _parse_uri(uri: str) -> tuple[str, str]:
    """Split an ``s3://bucket/path`` URI into ``(bucket, path)``."""
    parsed = urlparse(uri)
    bucket = parsed.netloc
    path = parsed.path.lstrip("/")
    return bucket, path


@pytest.fixture
def run_prefix() -> str:
    return f"td-integ/{uuid.uuid4().hex}"


@pytest.fixture
def src_conn() -> S3SrcConn:
    bucket, base_path = _parse_uri(_URI)
    return S3SrcConn(
        bucket=bucket,
        region=_REGION,
        credentials=AwsAccessSecretKey(
            access_key_id=Secret(_ACCESS),
            secret_access_key=Secret(_SECRET),
        ),
        base_path=base_path,
    )


@pytest.fixture
def dest_conn() -> S3DestConn:
    bucket, base_path = _parse_uri(_URI)
    return S3DestConn(
        bucket=bucket,
        region=_REGION,
        credentials=AwsAccessSecretKey(
            access_key_id=Secret(_ACCESS),
            secret_access_key=Secret(_SECRET),
        ),
        base_path=base_path,
    )


# ---------------------------------------------------------------------------
# Round-trip: write a parquet via the dest plugin, read it back via the src
# ---------------------------------------------------------------------------


def _upload_parquet(
    dest_conn: S3DestConn, run_prefix: str, name: str, rows: list[dict],
    tmp_path: Path, work_dest: Path,
) -> None:
    local = tmp_path / name
    pl.DataFrame(rows).write_parquet(local, use_pyarrow=True)
    dest = S3Dest(paths=[f"{run_prefix}/{name}"])
    plugin.S3DestPlugin().write_out(
        _DestCtx(work_dest), dest_conn, dest, [Path(local)],
    )


def test_dest_then_src_round_trip(src_conn, dest_conn, run_prefix, tmp_path):
    work_dest = tmp_path / "work_dest"
    work_dest.mkdir()
    _upload_parquet(
        dest_conn, run_prefix, "users.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
        tmp_path, work_dest,
    )

    src = S3Src(paths=[f"{run_prefix}/users.parquet"])
    work_src = tmp_path / "work_src"
    work_src.mkdir()
    out = plugin.S3SrcPlugin().read_in(_SrcCtx(work_src), src_conn, src)

    assert len(out) == 1
    assert len(out[0]) >= 1
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


def test_src_wildcard_in_basename_matches_multiple_objects(
    src_conn, dest_conn, run_prefix, tmp_path,
):
    work_dest = tmp_path / "work_dest"
    work_dest.mkdir()
    for name in ("a.parquet", "b.parquet", "c.parquet"):
        _upload_parquet(
            dest_conn, run_prefix, name,
            [{"name": name}],
            tmp_path, work_dest,
        )

    src = S3Src(paths=[f"{run_prefix}/*.parquet"])
    work_src = tmp_path / "work_src"
    work_src.mkdir()
    out = plugin.S3SrcPlugin().read_in(_SrcCtx(work_src), src_conn, src)

    names = sorted(
        pl.read_parquet(f.file).select("name").item(0, 0) for f in out[0]
    )
    assert names == ["a.parquet", "b.parquet", "c.parquet"]


def test_src_initial_last_modified_filters_out_older_objects(
    src_conn, dest_conn, run_prefix, tmp_path,
):
    work_dest = tmp_path / "work_dest"
    work_dest.mkdir()

    _upload_parquet(
        dest_conn, run_prefix, "old.parquet", [{"name": "old"}],
        tmp_path, work_dest,
    )
    # Cut-off taken between the two uploads; remote object stamping is
    # done server-side so we need a brief sleep to make the second
    # upload measurably newer.
    import time as _time
    _time.sleep(2)
    cutoff = datetime.now(timezone.utc)
    _time.sleep(2)
    _upload_parquet(
        dest_conn, run_prefix, "new.parquet", [{"name": "new"}],
        tmp_path, work_dest,
    )

    src = S3Src(
        paths=[f"{run_prefix}/*.parquet"],
        initial_last_modified=cutoff,
    )
    work_src = tmp_path / "work_src"
    work_src.mkdir()
    out = plugin.S3SrcPlugin().read_in(_SrcCtx(work_src), src_conn, src)

    names = sorted(
        pl.read_parquet(f.file).select("name").item(0, 0) for f in out[0]
    )
    assert names == ["new"]
