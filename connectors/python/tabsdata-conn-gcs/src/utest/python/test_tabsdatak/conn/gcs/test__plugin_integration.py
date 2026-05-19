
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import GCPJson
from tabsdatak.conn.gcs import GCSConn, GCSDest, GCSSrc
# noinspection PyProtectedMember
import tabsdatak.conn.gcs._plugin as plugin
from tabsdatak.spi import ParquetFile


pytestmark = pytest.mark.gcs


_TRANSPORTER_BIN = os.environ.get("TD_TRANSPORTER_BIN")
_URI = os.environ.get("GCP0__GCP_STORAGE_URI")
_SA_JSON = os.environ.get("GCP0__GCP_SERVICE_ACCOUNT_KEY")


_skip_reason = (
    "GCS integration tests require TD_TRANSPORTER_BIN + "
    "GCP0__GCP_STORAGE_URI + GCP0__GCP_SERVICE_ACCOUNT_KEY"
)


def _gates_ok() -> bool:
    return all([
        _TRANSPORTER_BIN and Path(_TRANSPORTER_BIN).is_file(),
        _URI, _SA_JSON,
    ])


pytestmark = [pytestmark, pytest.mark.skipif(not _gates_ok(), reason=_skip_reason)]


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


class _SrcCtx:
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "gcs-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "gcs-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"


class _DestCtx(_SrcCtx):
    pass


def _parse_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    bucket = parsed.netloc
    path = parsed.path.lstrip("/")
    return bucket, path


@pytest.fixture
def run_prefix() -> str:
    return f"td-integ/{uuid.uuid4().hex}"


@pytest.fixture
def conn() -> GCSConn:
    bucket, base_path = _parse_uri(_URI)
    return GCSConn(
        bucket=bucket,
        credentials=GCPJson(json=Secret.lit(_SA_JSON)),
        base_path=base_path,
    )


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------


def _upload_parquet(
    conn: GCSConn, run_prefix: str, name: str, rows: list[dict],
    tmp_path: Path, work_dest: Path,
) -> None:
    local = tmp_path / name
    pl.DataFrame(rows).write_parquet(local, use_pyarrow=True)
    dest = GCSDest(conn=conn, paths=[f"{run_prefix}/{name}"])
    plugin.GCSDestPlugin().write_out(
        _DestCtx(work_dest), dest, [ParquetFile(local)],
    )


def test_dest_then_src_round_trip(conn, run_prefix, tmp_path):
    work_dest = tmp_path / "work_dest"
    work_dest.mkdir()
    _upload_parquet(
        conn, run_prefix, "users.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
        tmp_path, work_dest,
    )

    src = GCSSrc(conn=conn, paths=[f"{run_prefix}/users.parquet"])
    work_src = tmp_path / "work_src"
    work_src.mkdir()
    out = plugin.GCSSrcPlugin().read_in(_SrcCtx(work_src), src)

    assert len(out) == 1
    assert len(out[0]) >= 1
    df = pl.read_parquet(out[0][0]).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


def test_src_wildcard_in_basename_matches_multiple_objects(
    conn, run_prefix, tmp_path,
):
    work_dest = tmp_path / "work_dest"
    work_dest.mkdir()
    for name in ("a.parquet", "b.parquet", "c.parquet"):
        _upload_parquet(
            conn, run_prefix, name, [{"name": name}], tmp_path, work_dest,
        )

    src = GCSSrc(conn=conn, paths=[f"{run_prefix}/*.parquet"])
    work_src = tmp_path / "work_src"
    work_src.mkdir()
    out = plugin.GCSSrcPlugin().read_in(_SrcCtx(work_src), src)

    names = sorted(
        pl.read_parquet(f).select("name").item(0, 0) for f in out[0]
    )
    assert names == ["a.parquet", "b.parquet", "c.parquet"]


def test_src_initial_last_modified_filters_out_older_objects(
    conn, run_prefix, tmp_path,
):
    work_dest = tmp_path / "work_dest"
    work_dest.mkdir()

    _upload_parquet(
        conn, run_prefix, "old.parquet", [{"name": "old"}],
        tmp_path, work_dest,
    )
    import time as _time
    _time.sleep(2)
    cutoff = datetime.now(timezone.utc)
    _time.sleep(2)
    _upload_parquet(
        conn, run_prefix, "new.parquet", [{"name": "new"}],
        tmp_path, work_dest,
    )

    src = GCSSrc(
        conn=conn,
        paths=[f"{run_prefix}/*.parquet"],
        initial_last_modified=cutoff,
    )
    work_src = tmp_path / "work_src"
    work_src.mkdir()
    out = plugin.GCSSrcPlugin().read_in(_SrcCtx(work_src), src)

    names = sorted(
        pl.read_parquet(f).select("name").item(0, 0) for f in out[0]
    )
    assert names == ["new"]
