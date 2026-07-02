"""Integration tests that invoke the real ``td_transporter`` binary.

These tests are skipped unless ``TD_TRANSPORTER_BIN`` is set in the
environment and points at an existing executable. Set it once before
running pytest::

    export TD_TRANSPORTER_BIN=/path/to/td_transporter
    pytest packages/tabsdata-conn-common-file/src/utest/python -k integration

Everything else in the conn-common-file test suite mocks the
subprocess, so this file is the only end-to-end coverage of the
binary handshake (request YAML round-trip + report parsing).
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _input as _input_module
# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _output as _output_module
# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _plugin
from tabsdatak.common._transporter.facade import (
    TRANSPORTER_MODE_LEGACY,
    make_handler,
)
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    SrcFileFormat,
)
from tabsdatak.spi import DestContext, SrcPluginCtx


# ---------------------------------------------------------------------------
# Gate: skip the whole module if the binary isn't available
# ---------------------------------------------------------------------------


_TRANSPORTER_BIN = os.environ.get("TD_TRANSPORTER_BIN")

pytestmark = pytest.mark.skipif(
    not _TRANSPORTER_BIN or not Path(_TRANSPORTER_BIN).is_file(),
    reason=(
        "TD_TRANSPORTER_BIN env var is unset or doesn't point at an "
        "existing file; integration tests skipped"
    ),
)


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
        return "integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "integ-trx"

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
        return "integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "integ-trx"

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
# _input.import_one
# ---------------------------------------------------------------------------


def _write_input_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


def _input_dir(tmp_path: Path) -> Path:
    d = tmp_path / "src"
    d.mkdir()
    return d


def _output_dir(tmp_path: Path) -> Path:
    d = tmp_path / "out"
    d.mkdir()
    return d


def test_import_one_local_parquet_round_trip(tmp_path: Path):
    src_dir = _input_dir(tmp_path)
    out_dir = _output_dir(tmp_path)

    _write_input_parquet(
        src_dir / "users.parquet",
        [
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
        ],
    )

    handler = make_handler(mode=TRANSPORTER_MODE_LEGACY)
    source_path = src_dir / "users.parquet"
    report = _input_module.import_one(
        handler=handler,
        source=handler.local_path_location(source_path),
        target_dir=out_dir,
        file_format=SrcFileFormat.PARQUET,
        paths=[str(source_path)],
    )

    assert report.files, "transporter report must reference at least one file"
    target_path = report.files[0].get("to")
    assert target_path, f"no 'to' field in report.files entry: {report.files[0]!r}"

    # The binary stamps tabsdata-internal ``$td.opt.*`` columns into
    # the read output; project away to compare just the user schema.
    df = pl.read_parquet(target_path).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


def test_import_one_glob_pattern_in_basename(tmp_path: Path):
    src_dir = _input_dir(tmp_path)
    out_dir = _output_dir(tmp_path)

    _write_input_parquet(src_dir / "a.parquet", [{"id": 1}])
    _write_input_parquet(src_dir / "b.parquet", [{"id": 2}])

    handler = make_handler(mode=TRANSPORTER_MODE_LEGACY)
    glob = handler.local_uri(src_dir) + "*.parquet"
    report = _input_module.import_one(
        handler=handler,
        source=handler.local_location(glob),
        target_dir=out_dir,
        file_format=SrcFileFormat.PARQUET,
        paths=[glob],
    )

    targets = [f["to"] for f in report.files if isinstance(f, dict) and f.get("to")]
    assert len(targets) >= 2


# ---------------------------------------------------------------------------
# _input.wildcard_copy_one
# ---------------------------------------------------------------------------


def test_wildcard_copy_one_max_files_drains_one_at_a_time(tmp_path: Path):
    src_dir = _input_dir(tmp_path)
    out_dir = _output_dir(tmp_path)

    # Three matching files; mtimes are distinct enough to order them.
    import os as _os
    import time as _time

    for i, name in enumerate(("a.parquet", "b.parquet", "c.parquet")):
        _write_input_parquet(src_dir / name, [{"id": i}])
        _time.sleep(0.01)
        # Pin a strictly increasing mtime so ordering is deterministic.
        stamp = 1_700_000_000 + i
        _os.utime(src_dir / name, (stamp, stamp))

    handler = make_handler(mode=TRANSPORTER_MODE_LEGACY)
    source = handler.local_location(handler.local_uri(src_dir) + "*.parquet")
    # A baseline older than every file so all three are candidates.
    initial_lastmod = "2020-01-01T00:00:00.000000+00:00"

    # First call consumes only the oldest matched file.
    report = _input_module.wildcard_copy_one(
        handler=handler,
        source=source,
        target_dir=out_dir,
        initial_lastmod=initial_lastmod,
        max_files=1,
    )
    targets = [f["to"] for f in report.files if isinstance(f, dict) and f.get("to")]
    assert len(targets) == 1
    assert targets[0].endswith("a.parquet")
    assert report.lastmod_info is not None

    # Resuming from the cursor consumes the next oldest file.
    report = _input_module.wildcard_copy_one(
        handler=handler,
        source=source,
        target_dir=out_dir,
        initial_lastmod=initial_lastmod,
        lastmod_info=report.lastmod_info,
        max_files=1,
    )
    targets = [f["to"] for f in report.files if isinstance(f, dict) and f.get("to")]
    assert len(targets) == 1
    assert targets[0].endswith("b.parquet")


# ---------------------------------------------------------------------------
# _output.copy_one
# ---------------------------------------------------------------------------


def test_copy_one_local_to_local(tmp_path: Path):
    src_dir = _input_dir(tmp_path)
    out_dir = _output_dir(tmp_path)

    _write_input_parquet(
        src_dir / "in.parquet",
        [{"id": 100, "name": "carol"}],
    )

    handler = make_handler(mode=TRANSPORTER_MODE_LEGACY)
    src_loc = handler.local_path_location(src_dir / "in.parquet")
    target_path = out_dir / "out.parquet"
    tgt_loc = handler.local_path_location(target_path)

    report = _output_module.copy_one(
        handler=handler,
        source_target_pairs=[(src_loc, tgt_loc)],
        work_dir=tmp_path,
        file_format=DestFileFormat.PARQUET,
        paths=[str(target_path)],
    )

    assert report is not None
    assert (out_dir / "out.parquet").exists()
    df = pl.read_parquet(out_dir / "out.parquet")
    assert df.to_dicts() == [{"id": 100, "name": "carol"}]


# ---------------------------------------------------------------------------
# _plugin.run_read_in / run_write_out (orchestrator end-to-end)
# ---------------------------------------------------------------------------


_INTEG_ERRORS = _plugin.FilePluginErrors(
    binary_not_found=FileCommonErrorCode.FILE_COMMON_1,
    read_failed=FileCommonErrorCode.FILE_COMMON_3,
    write_failed=FileCommonErrorCode.FILE_COMMON_8,
    report_invalid=FileCommonErrorCode.FILE_COMMON_4,
)


def _local_location(_conn, entry: str, handler):
    return handler.local_location(handler.local_uri(entry))


def test_run_read_in_end_to_end_against_real_binary(tmp_path: Path):
    src_dir = _input_dir(tmp_path)
    work_dir = tmp_path / "work"
    work_dir.mkdir()

    _write_input_parquet(
        src_dir / "users.parquet",
        [{"id": 1, "name": "x"}, {"id": 2, "name": "y"}],
    )

    cfg = _plugin.ReaderConf(
        conn=object(),
        paths=[str(src_dir / "users.parquet")],
        file_format=SrcFileFormat.PARQUET,
    )
    out = _plugin.import_files(
        ctx=_SrcCtx(work_dir),
        cfg=cfg,
        build_location=_local_location,
        errors=_INTEG_ERRORS,
    )

    assert len(out) == 1
    assert len(out[0]) >= 1
    parquet = out[0][0]
    assert isinstance(parquet, Path)
    df = pl.read_parquet(parquet).select("id", "name").sort("id")
    assert df.to_dicts() == [{"id": 1, "name": "x"}, {"id": 2, "name": "y"}]


def test_run_write_out_end_to_end_against_real_binary(tmp_path: Path):
    src_dir = _input_dir(tmp_path)
    out_dir = _output_dir(tmp_path)
    work_dir = tmp_path / "work"
    work_dir.mkdir()

    _write_input_parquet(
        src_dir / "data.parquet",
        [{"id": 7, "name": "z"}],
    )

    cfg = _plugin.WriterConf(
        conn=object(),
        paths=[str(out_dir / "data.parquet")],
        file_format=DestFileFormat.PARQUET,
    )
    _plugin.copy_out(
        ctx=_DestCtx(work_dir),
        cfg=cfg,
        tables=[Path(src_dir / "data.parquet")],
        build_location=_local_location,
        errors=_INTEG_ERRORS,
    )

    assert (out_dir / "data.parquet").exists()
    df = pl.read_parquet(out_dir / "data.parquet")
    assert df.to_dicts() == [{"id": 7, "name": "z"}]
