#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _plugin as _file_plugin

# noinspection PyProtectedMember
from tabsdatak.common._transporter.handler.handler1 import TransporterHandler1
from tabsdatak.common._transporter.invoker import transporter1
from tabsdatak.conn.localfile import (
    LocalFileDest,
    LocalFileDestConn,
    LocalFileSrc,
    LocalFileSrcConn,
)
from tabsdatak.conn.localfile._explorer import LocalFileExplorer
from tabsdatak.spi import TableFileInput

# noinspection PyProtectedMember
import tabsdatak.conn.localfile._plugin as plugin


pytestmark = pytest.mark.localfile

HANDLER = TransporterHandler1()


# ---------------------------------------------------------------------------
# _build_location
# ---------------------------------------------------------------------------


def test_build_location_joins_base_path_with_entry():
    conn = LocalFileSrcConn(base_path="/data")
    loc = plugin.build_location(conn, "users.csv", HANDLER)
    assert isinstance(loc, transporter1.TransporterLocalFile)
    assert loc.url.startswith("file:///")
    assert loc.url.endswith("/data/users.csv")


def test_build_location_treats_absolute_path_as_relative_to_base_path():
    # ``full_path`` strips the leading slash from the entry, so
    # ``"/sub/x"`` and ``"sub/x"`` produce the same location.
    conn = LocalFileSrcConn(base_path="/data")
    abs_loc = plugin.build_location(conn, "/sub/users.csv", HANDLER)
    rel_loc = plugin.build_location(conn, "sub/users.csv", HANDLER)
    assert abs_loc.url == rel_loc.url


def test_build_location_root_base_path_uses_root():
    conn = LocalFileSrcConn(base_path="/")
    loc = plugin.build_location(conn, "users.csv", HANDLER)
    # `/` + `users.csv` is joined natively then rendered as a file URI; on
    # POSIX that anchors at the root (`file:///users.csv`), on Windows at
    # the cwd drive. Compare against the same native resolution the URI
    # builder performs.
    expected = Path(os.path.join("/", "users.csv")).resolve().as_uri()
    assert loc.url == expected


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
            ctx=ctx,
            cfg=cfg,
            build_location=build_location,
            errors=errors,
            run_env=run_env,
        )
        # ``import_files`` returns the metadata-carrying inputs itself;
        # the plugin hands them straight back.
        return [[TableFileInput(file=Path(tmp_path / "ok.parquet"))]]

    monkeypatch.setattr(_file_plugin, "import_files", fake)

    src = LocalFileSrc(paths=["a/b.csv"])
    out = plugin.LocalFileSrcPlugin().read_in(
        _src_ctx(tmp_path), LocalFileSrcConn(), src
    )

    assert out == [[TableFileInput(file=Path(tmp_path / "ok.parquet"))]]
    assert captured["build_location"] is plugin.build_location
    assert captured["errors"] is plugin.ERRORS
    # localfile has no creds -> no run_env passed
    assert captured["run_env"] is None
    assert captured["cfg"].paths == ["a/b.csv"]


def test_dest_plugin_passes_build_location_and_errors(monkeypatch, tmp_path):
    captured: dict = {}

    def fake(*, ctx, cfg, tables, build_location, errors, run_env=None):
        captured.update(
            ctx=ctx,
            cfg=cfg,
            tables=tables,
            build_location=build_location,
            errors=errors,
            run_env=run_env,
        )

    monkeypatch.setattr(_file_plugin, "copy_out", fake)

    dest = LocalFileDest(paths=["out/x.csv"])
    parquet = Path(tmp_path / "in.parquet")
    plugin.LocalFileDestPlugin().write_out(
        _dest_ctx(tmp_path), LocalFileDestConn(), dest, [parquet]
    )

    assert captured["tables"] == [parquet]
    assert captured["build_location"] is plugin.build_location
    assert captured["errors"] is plugin.ERRORS
    assert captured["run_env"] is None
    assert captured["cfg"].paths == ["out/x.csv"]


# ---------------------------------------------------------------------------
# SrcDef / DestDef wiring
# ---------------------------------------------------------------------------


def test_localfile_src_def_wires_plugin_and_explorer():
    assert plugin.LOCALFILE_SRC.conn is LocalFileSrcConn
    assert plugin.LOCALFILE_SRC.src is LocalFileSrc
    assert plugin.LOCALFILE_SRC.plugin is plugin.LocalFileSrcPlugin
    assert plugin.LOCALFILE_SRC.explorer is LocalFileExplorer
    assert plugin.LOCALFILE_SRC.type_ == "local-file-in"


def test_localfile_dest_def_wires_plugin_and_explorer():
    assert plugin.LOCALFILE_DEST.conn is LocalFileDestConn
    assert plugin.LOCALFILE_DEST.dest is LocalFileDest
    assert plugin.LOCALFILE_DEST.plugin is plugin.LocalFileDestPlugin
    assert plugin.LOCALFILE_DEST.explorer is LocalFileExplorer
    assert plugin.LOCALFILE_DEST.type_ == "local-file-out"


def test_localfile_cardinality_uses_paths_length():
    src = LocalFileSrc(paths=["a", "b", "c"])
    assert plugin.LOCALFILE_SRC.cardinality(src) == 3
    dest = LocalFileDest(paths=["a", "b"])
    assert plugin.LOCALFILE_DEST.cardinality(dest) == 2


def test_reader_conf_carries_the_drop_metadata_src_cfg_key():
    src = LocalFileSrc(
        paths=["a/b.csv"],
        src_cfg={"tabsdata.src_metadata.drop": True},
    )
    rc = _file_plugin.ReaderConf.from_src(object(), src)
    assert rc.drop_metadata is True
