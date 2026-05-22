from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _plugin as _file_plugin
# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _transporter
from tabsdatak.conn.localfile import LocalFileDest, LocalFileDestConn, LocalFileSrc, LocalFileSrcConn
# noinspection PyProtectedMember
import tabsdatak.conn.localfile._plugin as plugin
from tabsdatak.spi import ParquetFile


pytestmark = pytest.mark.localfile


# ---------------------------------------------------------------------------
# _build_location
# ---------------------------------------------------------------------------


def test_build_location_joins_base_path_with_entry():
    conn = LocalFileSrcConn(base_path="/data/")
    loc = plugin.build_location(conn, "users.csv")
    assert isinstance(loc, _transporter.TransporterLocalFile)
    assert loc.url.startswith("file:///")
    assert loc.url.endswith("/data/users.csv")


def test_build_location_treats_absolute_path_as_relative_to_base_path():
    # ``full_path`` strips the leading slash from the entry, so
    # ``"/sub/x"`` and ``"sub/x"`` produce the same location.
    conn = LocalFileSrcConn(base_path="/data/")
    abs_loc = plugin.build_location(conn, "/sub/users.csv")
    rel_loc = plugin.build_location(conn, "sub/users.csv")
    assert abs_loc.url == rel_loc.url


def test_build_location_empty_base_path_uses_root():
    conn = LocalFileSrcConn(base_path="")
    loc = plugin.build_location(conn, "users.csv")
    assert loc.url == "file:///users.csv"


# ---------------------------------------------------------------------------
# Plugin classes: delegate to _file_plugin orchestrators
# ---------------------------------------------------------------------------


def _src_ctx(work: Path):
    ctx = MagicMock()
    ctx.work_dir = work
    return ctx


def _dest_ctx(work: Path):
    ctx = MagicMock()
    ctx.work_dir = work
    return ctx


def test_src_plugin_passes_build_location_and_errors(monkeypatch, tmp_path):
    captured: dict = {}

    def fake(*, ctx, cfg, build_location, errors, run_env=None):
        captured.update(
            ctx=ctx, cfg=cfg, build_location=build_location,
            errors=errors, run_env=run_env,
        )
        return [[ParquetFile(tmp_path / "ok.parquet")]]

    monkeypatch.setattr(_file_plugin, "import_files", fake)

    src = LocalFileSrc(conn=LocalFileSrcConn(), paths=["a/b.csv"])
    out = plugin.LocalFileSrcPlugin().read_in(_src_ctx(tmp_path), src)

    assert out == [[ParquetFile(tmp_path / "ok.parquet")]]
    assert captured["build_location"] is plugin.build_location
    assert captured["errors"] is plugin.ERRORS
    # localfile has no creds -> no run_env passed
    assert captured["run_env"] is None
    assert captured["cfg"].paths == ["a/b.csv"]


def test_dest_plugin_passes_build_location_and_errors(monkeypatch, tmp_path):
    captured: dict = {}

    def fake(*, ctx, cfg, tables, build_location, errors, run_env=None):
        captured.update(
            ctx=ctx, cfg=cfg, tables=tables, build_location=build_location,
            errors=errors, run_env=run_env,
        )

    monkeypatch.setattr(_file_plugin, "copy_out", fake)

    dest = LocalFileDest(conn=LocalFileDestConn(), paths=["out/x.csv"])
    parquet = ParquetFile(tmp_path / "in.parquet")
    plugin.LocalFileDestPlugin().write_out(_dest_ctx(tmp_path), dest, [parquet])

    assert captured["tables"] == [parquet]
    assert captured["build_location"] is plugin.build_location
    assert captured["errors"] is plugin.ERRORS
    assert captured["run_env"] is None
    assert captured["cfg"].paths == ["out/x.csv"]


# ---------------------------------------------------------------------------
# Explorer stubs
# ---------------------------------------------------------------------------


def test_explorer_methods_are_stubs():
    exp = plugin.LocalFileSrcExplorer()
    conn = LocalFileSrcConn()
    assert exp.metadata(conn) is None
    assert exp.data(conn, "SELECT 1") is None


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_localfile_src_def_wires_plugin_and_explorer():
    assert plugin.LOCALFILE_SRC.conn is LocalFileSrcConn
    assert plugin.LOCALFILE_SRC.src is LocalFileSrc
    assert plugin.LOCALFILE_SRC.plugin is plugin.LocalFileSrcPlugin
    assert plugin.LOCALFILE_SRC.explorer is plugin.LocalFileSrcExplorer
    assert plugin.LOCALFILE_SRC.src_type == "localfile"


def test_localfile_dest_def_wires_plugin_and_explorer():
    assert plugin.LOCALFILE_DEST.conn is LocalFileDestConn
    assert plugin.LOCALFILE_DEST.dest is LocalFileDest
    assert plugin.LOCALFILE_DEST.plugin is plugin.LocalFileDestPlugin
    assert plugin.LOCALFILE_DEST.explorer is plugin.LocalFileDestExplorer
    assert plugin.LOCALFILE_DEST.dest_type == "localfile"


def test_localfile_cardinality_uses_paths_length():
    src = LocalFileSrc(conn=LocalFileSrcConn(), paths=["a", "b", "c"])
    assert plugin.LOCALFILE_SRC.cardinality(src) == 3
    dest = LocalFileDest(conn=LocalFileDestConn(), paths=["a", "b"])
    assert plugin.LOCALFILE_DEST.cardinality(dest) == 2
