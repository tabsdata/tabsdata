#
# Copyright 2026 Tabs Data Inc.
#

"""Unit tests for the stage-backend abstraction (``_backend.py``).

These don't touch the ``td_transporter`` binary -- they exercise slot
resolution, location building, the credential env, and the object-store
flag directly, so they run regardless of ``TD_TRANSPORTER_BIN``.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.file import _transporter
from tabsdatak.conn.localfile import LocalFileSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.stream.stage._backend import (
    _CloudStageBackend,
    _LocalStageBackend,
    backend_for_conn,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def _conn(**slots):
    """Duck-typed ``ManifestStageConn`` with the four backend slots,
    defaulting to ``None`` (mirrors how the publisher builds it)."""
    base = {"local": None, "s3": None, "az": None, "gcs": None}
    base.update(slots)
    return SimpleNamespace(**base)


# --- backend_for_conn slot resolution -------------------------------------


def test_backend_for_conn_local_returns_local_backend():
    backend = backend_for_conn(_conn(local=LocalFileSrcConn(base_path="/stage")))
    assert isinstance(backend, _LocalStageBackend)
    assert backend.is_object_store is False
    assert backend.credential_env() is None


def test_backend_for_conn_cloud_slot_returns_object_store_backend():
    # A stand-in is enough: backend_for_conn only selects + wraps the
    # slot; build_location (which would call the real plugin) isn't hit.
    backend = backend_for_conn(_conn(s3=object()))
    assert isinstance(backend, _CloudStageBackend)
    assert backend.is_object_store is True


def test_backend_for_conn_returns_prebuilt_stage_backend():
    # The runner resolves a cloud stage's backend from the function_stage
    # Location and carries it on `stage_backend`; backend_for_conn returns
    # it as-is (no slot resolution).
    sentinel = _LocalStageBackend(LocalFileSrcConn(base_path="/x"))
    conn = _conn(stage_backend=sentinel)
    assert backend_for_conn(conn) is sentinel


def test_backend_for_conn_zero_slots_raises():
    with pytest.raises(ConnInitException):
        backend_for_conn(_conn())


def test_backend_for_conn_multiple_slots_raises():
    with pytest.raises(ConnInitException):
        backend_for_conn(_conn(local=LocalFileSrcConn(base_path="/s"), s3=object()))


# --- _LocalStageBackend ---------------------------------------------------


def test_local_backend_build_location_resolves_under_base_path():
    backend = _LocalStageBackend(LocalFileSrcConn(base_path="/stage"))
    loc = backend.build_location("files/x.parquet")
    assert isinstance(loc, _transporter.TransporterLocalFile)
    # The localfile connector applies base_path itself, so the resolved
    # url carries both the base path and the relative entry.
    assert "stage" in loc.url
    assert "x.parquet" in loc.url


def test_local_backend_upload_moves_file_under_base_path(tmp_path):
    stage = tmp_path / "stage"
    backend = _LocalStageBackend(LocalFileSrcConn(base_path=str(stage)))
    src = tmp_path / "src.parquet"
    src.write_text("data")
    backend.upload(src, "files/2026-06-26T00-00-00.000000/x.parquet")
    dst = stage / "files" / "2026-06-26T00-00-00.000000" / "x.parquet"
    assert dst.read_text() == "data"
    assert not src.exists()  # move (rename), not copy


def test_local_backend_read_text_absent_then_present(tmp_path):
    stage = tmp_path / "stage"
    backend = _LocalStageBackend(LocalFileSrcConn(base_path=str(stage)))
    assert backend.read_text("stream-state.json") is None
    stage.mkdir()
    (stage / "stream-state.json").write_text('{"k": "v"}')
    assert backend.read_text("stream-state.json") == '{"k": "v"}'


# --- _CloudStageBackend ---------------------------------------------------


class _FakeCloudConn:
    def full_path(self, rel: str) -> str:
        return f"bucket-prefix/{rel}"


class _FakePlugin:
    def __init__(self) -> None:
        self.seen_path: str | None = None

    def build_location(self, conn, path):
        self.seen_path = path
        return _transporter.TransporterLocalFile("file:///fake")

    def run_env(self, conn):
        return {"AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s"}


def test_cloud_backend_applies_full_path_before_building_location():
    plugin = _FakePlugin()
    backend = _CloudStageBackend(_FakeCloudConn(), plugin)
    backend.build_location("files/x.parquet")
    # The cloud connectors' build_location does NOT apply base_path, so
    # the backend must resolve through conn.full_path first.
    assert plugin.seen_path == "bucket-prefix/files/x.parquet"


def test_cloud_backend_credential_env_delegates_to_run_env():
    backend = _CloudStageBackend(_FakeCloudConn(), _FakePlugin())
    assert backend.is_object_store is True
    assert backend.credential_env() == {
        "AWS_ACCESS_KEY_ID": "k",
        "AWS_SECRET_ACCESS_KEY": "s",
    }
