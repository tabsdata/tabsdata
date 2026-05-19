
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest

from tabsdatak.conn.common.file.typing_ import DestFileFormat, SrcFileFormat
from tabsdatak.conn.localfile import LocalFileConn, LocalFileDest, LocalFileSrc
# noinspection PyProtectedMember
import tabsdatak.conn.localfile._plugin as plugin
from tabsdatak.spi import DestContext, ParquetFile, SrcContext


pytestmark = pytest.mark.localfile


_TRANSPORTER_BIN = os.environ.get("TD_TRANSPORTER_BIN")

pytestmark = [pytestmark, pytest.mark.skipif(
    not _TRANSPORTER_BIN or not Path(_TRANSPORTER_BIN).is_file(),
    reason=(
        "TD_TRANSPORTER_BIN env var is unset or doesn't point at an "
        "existing file; integration tests skipped"
    ),
)]


# ---------------------------------------------------------------------------
# Test contexts
# ---------------------------------------------------------------------------


class _SrcCtx(SrcContext):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "lf-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "lf-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"


class _DestCtx(DestContext):
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def exec_plan_id(self) -> str:
        return "lf-integ-exec-plan"

    @property
    def trx_id(self) -> str:
        return "lf-integ-trx"

    @property
    def exec_plan_ts(self) -> str:
        return "2026-01-01T00:00:00.000000+00:00"


# ---------------------------------------------------------------------------
# Round-trip via LocalFile{Src,Dest} plugins
# ---------------------------------------------------------------------------


def test_dest_then_src_round_trip(tmp_path: Path):
    # Use ``tmp_path`` itself as the base_path; entries are rooted
    # there. ``full_path`` strips leading/trailing slashes.
    base = tmp_path / "store"
    base.mkdir()
    work_dest = tmp_path / "work_dest"
    work_dest.mkdir()
    work_src = tmp_path / "work_src"
    work_src.mkdir()

    # Stage an input parquet outside the base path
    in_parquet = tmp_path / "in.parquet"
    pl.DataFrame(
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    ).write_parquet(in_parquet, use_pyarrow=True)

    conn = LocalFileConn(base_path=str(base))

    # 1. Write
    dest = LocalFileDest(conn=conn, paths=["users.parquet"])
    plugin.LocalFileDestPlugin().write_out(
        _DestCtx(work_dest),
        dest,
        [ParquetFile(in_parquet)],
    )
    assert (base / "users.parquet").exists()

    # 2. Read back
    src = LocalFileSrc(conn=conn, paths=["users.parquet"])
    out = plugin.LocalFileSrcPlugin().read_in(_SrcCtx(work_src), src)

    assert len(out) == 1
    assert len(out[0]) >= 1
    df = pl.read_parquet(out[0][0]).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


def test_src_treats_absolute_path_as_relative_to_base_path(tmp_path: Path):
    base = tmp_path / "store"
    base.mkdir()
    in_parquet = base / "data.parquet"
    pl.DataFrame([{"x": 1}]).write_parquet(in_parquet, use_pyarrow=True)

    conn = LocalFileConn(base_path=str(base))
    # Note the leading slash on the entry -- the plugin strips it.
    src = LocalFileSrc(conn=conn, paths=["/data.parquet"])

    work = tmp_path / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(_SrcCtx(work), src)
    assert len(out[0]) >= 1
    assert pl.read_parquet(out[0][0]).select("x").to_dicts() == [{"x": 1}]


# ---------------------------------------------------------------------------
# Wildcards
# ---------------------------------------------------------------------------


def test_src_wildcard_in_basename_matches_multiple_files(tmp_path: Path):
    base = tmp_path / "store"
    base.mkdir()
    for name in ("a.parquet", "b.parquet", "c.parquet"):
        pl.DataFrame([{"name": name}]).write_parquet(
            base / name, use_pyarrow=True,
        )
    # A non-matching neighbour: must NOT show up in the output
    pl.DataFrame([{"name": "skip.csv"}]).write_parquet(
        base / "skip.csv", use_pyarrow=True,
    )

    conn = LocalFileConn(base_path=str(base))
    src = LocalFileSrc(conn=conn, paths=["*.parquet"])

    work = tmp_path / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(_SrcCtx(work), src)

    assert len(out) == 1
    # Three parquet inputs -> at least three target files in the report
    assert len(out[0]) >= 3
    names = sorted(
        pl.read_parquet(f).select("name").item(0, 0) for f in out[0]
    )
    assert names == ["a.parquet", "b.parquet", "c.parquet"]


def test_src_wildcard_with_no_matches_returns_empty_slot(tmp_path: Path):
    base = tmp_path / "store"
    base.mkdir()
    pl.DataFrame([{"name": "x"}]).write_parquet(
        base / "x.csv", use_pyarrow=True,
    )

    conn = LocalFileConn(base_path=str(base))
    src = LocalFileSrc(conn=conn, paths=["*.parquet"])

    work = tmp_path / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(_SrcCtx(work), src)

    # One slot, zero files
    assert len(out) == 1
    assert out[0] == []


# ---------------------------------------------------------------------------
# initial_last_modified filtering
# ---------------------------------------------------------------------------


def _set_mtime(path: Path, when: datetime) -> None:
    ts = when.astimezone(timezone.utc).timestamp()
    os.utime(path, (ts, ts))


def test_src_initial_last_modified_filters_out_older_files(tmp_path: Path):
    base = tmp_path / "store"
    base.mkdir()

    # Two files, deliberately spaced in time
    old = base / "old.parquet"
    new = base / "new.parquet"
    pl.DataFrame([{"name": "old"}]).write_parquet(old, use_pyarrow=True)
    pl.DataFrame([{"name": "new"}]).write_parquet(new, use_pyarrow=True)

    t_old = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    t_new = datetime(2026, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
    _set_mtime(old, t_old)
    _set_mtime(new, t_new)

    # Cut-off lies between the two: only ``new.parquet`` should be picked up.
    cutoff = datetime(2026, 1, 1, 12, 30, 0, tzinfo=timezone.utc)
    conn = LocalFileConn(base_path=str(base))
    src = LocalFileSrc(
        conn=conn,
        paths=["*.parquet"],
        initial_last_modified=cutoff,
    )

    work = tmp_path / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(_SrcCtx(work), src)

    names = sorted(
        pl.read_parquet(f).select("name").item(0, 0) for f in out[0]
    )
    assert names == ["new"]


def test_src_initial_last_modified_at_or_before_oldest_returns_all(
    tmp_path: Path,
):
    base = tmp_path / "store"
    base.mkdir()
    pl.DataFrame([{"name": "a"}]).write_parquet(base / "a.parquet", use_pyarrow=True)
    pl.DataFrame([{"name": "b"}]).write_parquet(base / "b.parquet", use_pyarrow=True)

    floor = datetime(2020, 1, 1, tzinfo=timezone.utc)
    _set_mtime(base / "a.parquet", floor + timedelta(days=1))
    _set_mtime(base / "b.parquet", floor + timedelta(days=2))

    conn = LocalFileConn(base_path=str(base))
    src = LocalFileSrc(
        conn=conn,
        paths=["*.parquet"],
        initial_last_modified=floor,
    )

    work = tmp_path / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(_SrcCtx(work), src)

    names = sorted(
        pl.read_parquet(f).select("name").item(0, 0) for f in out[0]
    )
    assert names == ["a", "b"]


# ---------------------------------------------------------------------------
# Source-side formats
#
# Each test stages an input file in a specific format, then drives the
# read path with the matching ``SrcFileFormat``. The transporter always
# emits parquet, so the assertions read parquet and verify the rows
# match what the input file carried.
# ---------------------------------------------------------------------------


def test_src_format_csv_reads_with_default_options(tmp_path: Path):
    base = tmp_path / "store"
    base.mkdir()
    pl.DataFrame(
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    ).write_csv(base / "users.csv")

    conn = LocalFileConn(base_path=str(base))
    src = LocalFileSrc(
        conn=conn,
        paths=["users.csv"],
        format=SrcFileFormat.CSV,
    )
    work = tmp_path / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(_SrcCtx(work), src)
    df = pl.read_parquet(out[0][0]).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@pytest.mark.skip(
    reason=(
        "transporter binary ignores the ``separator`` field on CSV "
        "src reads -- it always reads with ``,``. The YAML carries "
        "the override correctly; the binary just doesn't use it. "
        "Re-enable once the binary honors separator on CSV reads."
    ),
)
def test_src_format_csv_honours_separator_option(tmp_path: Path):
    base = tmp_path / "store"
    base.mkdir()
    pl.DataFrame(
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    ).write_csv(base / "users.csv", separator=";")

    conn = LocalFileConn(base_path=str(base))
    src = LocalFileSrc(
        conn=conn,
        paths=["users.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: {"separator": ";"}},
    )
    work = tmp_path / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(_SrcCtx(work), src)
    df = pl.read_parquet(out[0][0]).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


def test_src_format_json_reads_records(tmp_path: Path):
    # The transporter binary reads newline-delimited JSON (one record
    # per line) for ``SrcFileFormat.JSON``.
    base = tmp_path / "store"
    base.mkdir()
    pl.DataFrame(
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    ).write_ndjson(base / "users.json")

    conn = LocalFileConn(base_path=str(base))
    src = LocalFileSrc(
        conn=conn,
        paths=["users.json"],
        format=SrcFileFormat.JSON,
    )
    work = tmp_path / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(_SrcCtx(work), src)
    df = pl.read_parquet(out[0][0]).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@pytest.mark.skip(
    reason=(
        "``TransporterAvroFormat`` serialises to the YAML scalar "
        "``Binary``, which signals the transporter binary to byte-"
        "copy the file -- it does NOT transcode avro -> parquet. The "
        "``.avro`` file is preserved as-is in the target dir, so any "
        "downstream parquet reader chokes on it. Re-enable once the "
        "binary actually transcodes avro on import (or rename "
        "``DestFileFormat.AVRO`` / ``SrcFileFormat.AVRO`` to make the "
        "byte-copy semantics explicit)."
    ),
)
def test_src_format_avro_reads_records(tmp_path: Path):
    base = tmp_path / "store"
    base.mkdir()
    pl.DataFrame(
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    ).write_avro(base / "users.avro")

    conn = LocalFileConn(base_path=str(base))
    src = LocalFileSrc(
        conn=conn,
        paths=["users.avro"],
        format=SrcFileFormat.AVRO,
    )
    work = tmp_path / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(_SrcCtx(work), src)
    df = pl.read_parquet(out[0][0]).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


def test_src_format_auto_infers_parquet_from_extension(tmp_path: Path):
    base = tmp_path / "store"
    base.mkdir()
    pl.DataFrame([{"id": 1, "name": "alice"}]).write_parquet(
        base / "users.parquet", use_pyarrow=True,
    )

    conn = LocalFileConn(base_path=str(base))
    src = LocalFileSrc(
        conn=conn,
        paths=["users.parquet"],
        # AUTO is the default; spell it out for clarity.
        format=SrcFileFormat.AUTO,
    )
    work = tmp_path / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(_SrcCtx(work), src)
    df = pl.read_parquet(out[0][0]).select("id", "name")
    assert df.to_dicts() == [{"id": 1, "name": "alice"}]


# ---------------------------------------------------------------------------
# Destination-side formats
#
# Each test starts from a small parquet file (the in-flight table
# format) and drives the write path with the requested
# ``DestFileFormat``. The assertion reads the emitted file with the
# matching reader and checks the row content.
# ---------------------------------------------------------------------------


def _stage_input_parquet(tmp_path: Path, rows: list[dict]) -> Path:
    p = tmp_path / "in.parquet"
    pl.DataFrame(rows).write_parquet(p, use_pyarrow=True)
    return p


# Non-parquet dest formats: ``_output.copy_one`` transcodes the
# source parquet locally via polars (``sink_csv`` / ``sink_ndjson``
# / ``write_avro``) before invoking the binary. The binary's copy
# step is a raw byte-copy, so the transcoded temp file IS what
# lands at the user-specified target.


def test_dest_format_csv_writes_csv_file(tmp_path: Path):
    base = tmp_path / "store"
    base.mkdir()
    src_parquet = _stage_input_parquet(
        tmp_path, [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )

    conn = LocalFileConn(base_path=str(base))
    dest = LocalFileDest(
        conn=conn,
        paths=["users.csv"],
        format=DestFileFormat.CSV,
    )
    work = tmp_path / "work"
    work.mkdir()
    plugin.LocalFileDestPlugin().write_out(
        _DestCtx(work), dest, [ParquetFile(src_parquet)],
    )

    out_file = base / "users.csv"
    assert out_file.exists()
    df = pl.read_csv(out_file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


def test_dest_format_csv_honours_separator_option(tmp_path: Path):
    base = tmp_path / "store"
    base.mkdir()
    src_parquet = _stage_input_parquet(
        tmp_path, [{"id": 1, "name": "alice"}],
    )

    conn = LocalFileConn(base_path=str(base))
    dest = LocalFileDest(
        conn=conn,
        paths=["users.csv"],
        format=DestFileFormat.CSV,
        format_cfg={DestFileFormat.CSV: {"separator": ";"}},
    )
    work = tmp_path / "work"
    work.mkdir()
    plugin.LocalFileDestPlugin().write_out(
        _DestCtx(work), dest, [ParquetFile(src_parquet)],
    )

    raw = (base / "users.csv").read_text()
    # ``;`` separates the columns; ``,`` would mean the option was ignored.
    assert ";" in raw
    assert "," not in raw


def test_dest_format_json_writes_json_file(tmp_path: Path):
    base = tmp_path / "store"
    base.mkdir()
    src_parquet = _stage_input_parquet(
        tmp_path, [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )

    conn = LocalFileConn(base_path=str(base))
    dest = LocalFileDest(
        conn=conn,
        paths=["users.json"],
        format=DestFileFormat.JSON,
    )
    work = tmp_path / "work"
    work.mkdir()
    plugin.LocalFileDestPlugin().write_out(
        _DestCtx(work), dest, [ParquetFile(src_parquet)],
    )

    out_file = base / "users.json"
    assert out_file.exists()
    # ``DestFileFormat.JSON`` emits NDJSON (one record per line) to
    # match what the src side consumes.
    df = pl.read_ndjson(out_file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


def test_dest_format_avro_writes_avro_file(tmp_path: Path):
    base = tmp_path / "store"
    base.mkdir()
    src_parquet = _stage_input_parquet(
        tmp_path, [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )

    conn = LocalFileConn(base_path=str(base))
    dest = LocalFileDest(
        conn=conn,
        paths=["users.avro"],
        format=DestFileFormat.AVRO,
    )
    work = tmp_path / "work"
    work.mkdir()
    plugin.LocalFileDestPlugin().write_out(
        _DestCtx(work), dest, [ParquetFile(src_parquet)],
    )

    out_file = base / "users.avro"
    assert out_file.exists()
    df = pl.read_avro(out_file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


def test_dest_format_auto_defaults_to_parquet(tmp_path: Path):
    base = tmp_path / "store"
    base.mkdir()
    src_parquet = _stage_input_parquet(
        tmp_path, [{"id": 1, "name": "alice"}],
    )

    conn = LocalFileConn(base_path=str(base))
    dest = LocalFileDest(
        conn=conn,
        paths=["users.parquet"],
        format=DestFileFormat.AUTO,
    )
    work = tmp_path / "work"
    work.mkdir()
    plugin.LocalFileDestPlugin().write_out(
        _DestCtx(work), dest, [ParquetFile(src_parquet)],
    )

    out_file = base / "users.parquet"
    assert out_file.exists()
    df = pl.read_parquet(out_file).select("id", "name")
    assert df.to_dicts() == [{"id": 1, "name": "alice"}]


# ---------------------------------------------------------------------------
# CSV src options -- per-option binary honor checks
#
# One test per ``CsvSrcKey`` option. Each test crafts a CSV that only
# parses correctly if the option is honored, and verifies the parquet
# output reflects the expected interpretation. Tests that confirm a
# known binary defect are marked ``xfail``; if the binary starts
# honoring the option, the test flips to ``XPASS`` and pytest will
# loudly surface the change.
# ---------------------------------------------------------------------------


def _read_csv_with_cfg(
    tmp_path: Path,
    csv_text: str,
    cfg: dict,
) -> pl.DataFrame:
    base = tmp_path / "store"
    base.mkdir()
    (base / "in.csv").write_text(csv_text)

    src = LocalFileSrc(
        conn=LocalFileConn(base_path=str(base)),
        paths=["in.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: cfg},
    )
    work = tmp_path / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(_SrcCtx(work), src)
    # Drop the binary's ``$td.opt.*`` provenance columns.
    df = pl.read_parquet(out[0][0])
    user_cols = [c for c in df.columns if not c.startswith("$td.")]
    return df.select(user_cols)


# --- separator -------------------------------------------------------


@pytest.mark.xfail(
    reason="binary ignores ``separator`` -- always reads with ','",
    strict=True,
)
def test_csv_src_separator(tmp_path: Path):
    df = _read_csv_with_cfg(
        tmp_path,
        "id;name\n1;alice\n2;bob\n",
        {"separator": ";"},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


# --- quote_char ------------------------------------------------------


@pytest.mark.xfail(
    reason="binary crashes (exit 201) when ``quote_char != '\"'``",
    strict=True,
)
def test_csv_src_quote_char(tmp_path: Path):
    # Apostrophe-quoted field with embedded comma -- only parses
    # correctly when ``quote_char`` is honored.
    df = _read_csv_with_cfg(
        tmp_path,
        "id,name\n1,'alice, the great'\n2,bob\n",
        {"quote_char": "'"},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice, the great"},
        {"id": 2, "name": "bob"},
    ]


# --- has_header ------------------------------------------------------


def test_csv_src_has_header_false(tmp_path: Path):
    df = _read_csv_with_cfg(
        tmp_path,
        "1,alice\n2,bob\n",
        {"has_header": False},
    )
    # Column names are positional; values are all data.
    assert len(df) == 2
    # Whatever the binary names the columns, they must NOT be 'id'/'name'.
    assert "id" not in df.columns
    assert "name" not in df.columns


# --- skip_rows -------------------------------------------------------


def test_csv_src_skip_rows(tmp_path: Path):
    df = _read_csv_with_cfg(
        tmp_path,
        "# noise 1\n# noise 2\nid,name\n1,alice\n2,bob\n",
        {"skip_rows": 2},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


# --- skip_rows_after_header ------------------------------------------


def test_csv_src_skip_rows_after_header(tmp_path: Path):
    df = _read_csv_with_cfg(
        tmp_path,
        "id,name\nskip_1,skip_1\nskip_2,skip_2\n1,alice\n2,bob\n",
        {"skip_rows_after_header": 2},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


# --- comment_prefix --------------------------------------------------


@pytest.mark.xfail(
    reason="binary crashes (exit 201) on input lines starting with ``comment_prefix``",
    strict=True,
)
def test_csv_src_comment_prefix(tmp_path: Path):
    df = _read_csv_with_cfg(
        tmp_path,
        "# header comment\nid,name\n# data comment\n1,alice\n2,bob\n",
        {"comment_prefix": "#"},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


# --- null_values -----------------------------------------------------


@pytest.mark.xfail(
    reason="binary ignores ``null_values`` -- literal strings stay as text",
    strict=True,
)
def test_csv_src_null_values(tmp_path: Path):
    df = _read_csv_with_cfg(
        tmp_path,
        "id,name\n1,NA\n2,bob\n",
        {"null_values": ["NA"]},
    )
    rows = df.sort("id").to_dicts()
    assert rows[0]["name"] is None
    assert rows[1]["name"] == "bob"


# --- missing_is_null -------------------------------------------------
#
# Note: probing showed the binary emits the same output for
# ``missing_is_null=True`` and ``=False``. The binary appears to
# always treat empty cells as nulls -- so this test is really only
# proving the default behavior, not that the option is honored.


def test_csv_src_missing_is_null_default_empty_cell_is_null(tmp_path: Path):
    df = _read_csv_with_cfg(
        tmp_path,
        "id,name\n1,\n2,bob\n",
        {"missing_is_null": True},
    )
    rows = df.sort("id").to_dicts()
    assert rows[0]["name"] is None
    assert rows[1]["name"] == "bob"


# --- try_parse_dates -------------------------------------------------


@pytest.mark.xfail(
    reason=(
        "binary ignores ``try_parse_dates``; ISO date strings stay as "
        "String dtype regardless of the option value."
    ),
    strict=True,
)
def test_csv_src_try_parse_dates(tmp_path: Path):
    df = _read_csv_with_cfg(
        tmp_path,
        "id,when\n1,2026-01-01\n2,2026-02-02\n",
        {"try_parse_dates": True},
    )
    # With try_parse_dates honored we'd expect a Date dtype.
    assert str(df.schema["when"]).lower() == "date"


# --- decimal_comma ---------------------------------------------------


@pytest.mark.xfail(
    reason=(
        "needs ``separator`` to be honored so we can use ``;`` as the "
        "field delimiter; binary ignores ``separator`` on reads."
    ),
    strict=False,
)
def test_csv_src_decimal_comma(tmp_path: Path):
    # Comma is the decimal point, so the field delimiter must be
    # something else (``;`` here).
    df = _read_csv_with_cfg(
        tmp_path,
        "id;value\n1;1,5\n2;2,75\n",
        {"separator": ";", "decimal_comma": True},
    )
    rows = df.sort("id").to_dicts()
    assert rows[0]["value"] == 1.5
    assert rows[1]["value"] == 2.75


# --- truncate_ragged_lines -------------------------------------------


@pytest.mark.xfail(
    reason="binary crashes (exit 201) on ragged input rows even with ``truncate_ragged_lines=True``",
    strict=True,
)
def test_csv_src_truncate_ragged_lines(tmp_path: Path):
    df = _read_csv_with_cfg(
        tmp_path,
        # Second row has an extra column.
        "id,name\n1,alice\n2,bob,extra\n",
        {"truncate_ragged_lines": True, "ignore_errors": True},
    )
    # Both rows should be retained (extra column truncated or ignored).
    assert len(df) == 2


# --- raise_if_empty --------------------------------------------------


def test_csv_src_raise_if_empty_false(tmp_path: Path):
    # Empty file -- with raise_if_empty=False the binary should NOT
    # error; output should be empty.
    df = _read_csv_with_cfg(
        tmp_path,
        "",
        {"raise_if_empty": False},
    )
    assert len(df) == 0


# --- ignore_errors ---------------------------------------------------


def test_csv_src_ignore_errors(tmp_path: Path):
    # 'not_a_number' would normally trip type inference on ``id``.
    # With ``ignore_errors=True`` the binary doesn't crash; it keeps
    # the column as String so every row passes through. Probing
    # showed all 3 rows survive.
    df = _read_csv_with_cfg(
        tmp_path,
        "id,name\n1,alice\nnot_a_number,bob\n3,carol\n",
        {"ignore_errors": True},
    )
    assert len(df) == 3
    # ``id`` ends up as String since type inference couldn't pick int.
    assert str(df.schema["id"]).lower() in {"string", "str", "utf8"}


# --- encoding --------------------------------------------------------


def test_csv_src_encoding_utf8(tmp_path: Path):
    # Default encoding is utf8 -- multibyte chars round-trip.
    base = tmp_path / "store"
    base.mkdir()
    (base / "in.csv").write_bytes(
        "id,name\n1,café\n2,piñata\n".encode("utf-8"),
    )
    src = LocalFileSrc(
        conn=LocalFileConn(base_path=str(base)),
        paths=["in.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: {"encoding": "utf8"}},
    )
    work = tmp_path / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(_SrcCtx(work), src)
    df = pl.read_parquet(out[0][0]).select("id", "name")
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "café"},
        {"id": 2, "name": "piñata"},
    ]


# --- eol_char --------------------------------------------------------


def test_csv_src_eol_char_default_lf(tmp_path: Path):
    # Default eol is LF; this validates the happy path.
    df = _read_csv_with_cfg(
        tmp_path,
        "id,name\n1,alice\n2,bob\n",
        {"eol_char": "\n"},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]
