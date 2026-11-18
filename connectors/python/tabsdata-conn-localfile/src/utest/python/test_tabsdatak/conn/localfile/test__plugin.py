#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

# noinspection PyProtectedMember
import tabsdatak.conn.localfile._plugin as plugin
from tabsdatak._handoff import DestinationContract

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _plugin as _file_plugin
from tabsdatak.conn.common.file.constants import FILE_META_PATH

# noinspection PyProtectedMember
from tabsdatak.conn.localfile import (
    LocalFileDest,
    LocalFileDestConn,
    LocalFileSrc,
    LocalFileSrcConn,
)
from tabsdatak.conn.localfile._explorer import LocalFileExplorer

pytestmark = pytest.mark.localfile


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


def test_src_plugin_reads_each_matched_file_lazily_naming_its_path(tmp_path):
    folder = tmp_path / "in dir"
    folder.mkdir()
    for name in ("a.parquet", "b.parquet"):
        pl.DataFrame({"name": [name]}).write_parquet(folder / name)
    src = LocalFileSrc(paths=["in dir/*.parquet"])

    (slot,) = plugin.LocalFileSrcPlugin().read_in(
        _src_ctx(tmp_path), LocalFileSrcConn(base_path=str(tmp_path)), src
    )

    assert not isinstance(slot, list)
    tables = list(slot)
    assert [table.frame.collect()["name"].item() for table in tables] == [
        "a.parquet",
        "b.parquet",
    ]
    assert [table.metadata.columns[FILE_META_PATH] for table in tables] == [
        str(folder / "a.parquet"),
        str(folder / "b.parquet"),
    ]


def test_dest_plugin_writes_each_table_under_the_base_path(tmp_path):
    ctx = _dest_ctx(tmp_path)
    ctx.triggered_on = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ctx.execution_id = "exec"
    ctx.transaction_id = "trx"
    conn = LocalFileDestConn(base_path=str(tmp_path))
    dest = LocalFileDest(paths=["out/x.csv"])
    plugin.LocalFileDestPlugin().write_out(
        ctx, conn, dest, [pl.LazyFrame({"a": [1, 2]})]
    )
    assert (tmp_path / "out" / "x.csv").read_text(encoding="utf-8") == "a\n1\n2\n"


def _written(tmp_path: Path, tables: list[pl.LazyFrame | None]) -> None:
    ctx = _dest_ctx(tmp_path)
    ctx.triggered_on = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ctx.execution_id = "exec"
    ctx.transaction_id = "trx"
    plugin.LocalFileDestPlugin().write_out(
        ctx,
        LocalFileDestConn(base_path=str(tmp_path)),
        LocalFileDest(paths=["out/x.csv", "out/y.csv"]),
        tables,
    )


def test_dest_plugin_takes_lazy_frames_or_none():
    assert DestinationContract(plugin.LocalFileDestPlugin).frames()


def test_dest_plugin_leaves_the_path_of_a_none_table_untouched(tmp_path):
    folder = tmp_path / "out"
    folder.mkdir()
    (folder / "x.csv").write_text("kept\n", encoding="utf-8")
    _written(tmp_path, [None, pl.LazyFrame({"a": [1]})])
    assert (folder / "x.csv").read_text(encoding="utf-8") == "kept\n"
    assert (folder / "y.csv").read_text(encoding="utf-8") == "a\n1\n"


def test_dest_plugin_writes_nothing_when_every_table_is_none(tmp_path):
    _written(tmp_path, [None, None])
    assert not (tmp_path / "out").exists()


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
