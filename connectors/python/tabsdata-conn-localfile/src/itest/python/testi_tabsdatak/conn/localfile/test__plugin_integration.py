"""End-to-end LocalFile integration tests: invoke ``td_transporter2``
against real filesystem paths. Requires the ``td_transporter2`` binary in the
active Python environment's scripts folder (hydration is mandatory; a missing
binary fails loudly rather than skipping).

Unlike the cloud variants, no credentials are required.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx, PluginSrcCtx

# noinspection PyProtectedMember
import tabsdatak.conn.localfile._plugin as plugin
from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.common._transporter.handler.handler2 import TRANSPORTER_BINARY_NAME
from tabsdatak.conn.common.file.typing_ import DestFileFormat, SrcFileFormat
from tabsdatak.conn.localfile import (
    LocalFileDest,
    LocalFileDestConn,
    LocalFileSrc,
    LocalFileSrcConn,
)

pytestmark = pytest.mark.localfile

TRANSPORTER_BINARY = PythonEnvironment().binary(TRANSPORTER_BINARY_NAME)


def _test_root(domain_fs: FsDomain, request: pytest.FixtureRequest) -> Path:
    """A unique writable scratch dir for the current test under the
    session-shared ``fs`` domain base path (``request.node.name`` carries the
    param id, so parametrized cases stay isolated too)."""
    root = domain_fs.base_path / request.node.name
    root.mkdir(parents=True, exist_ok=True)
    return root


# ---------------------------------------------------------------------------
# Round-trip via LocalFile{Src,Dest} plugins
# ---------------------------------------------------------------------------


@fs_domain("fs")
def test_dest_then_src_round_trip(domain_fs: FsDomain, request: pytest.FixtureRequest):
    root = _test_root(domain_fs, request)
    # Use ``base`` as the base_path; entries are rooted there.
    # ``full_path`` strips leading/trailing slashes.
    base = root / "store"
    base.mkdir()
    work_dest = root / "work_dest"
    work_dest.mkdir()
    work_src = root / "work_src"
    work_src.mkdir()

    # Stage an input parquet outside the base path
    in_parquet = root / "in.parquet"
    pl.DataFrame(
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    ).write_parquet(in_parquet, use_pyarrow=True)

    conn = LocalFileDestConn(base_path=str(base))

    # 1. Write
    dest = LocalFileDest(paths=["users.parquet"])
    plugin.LocalFileDestPlugin().write_out(
        PluginDestCtx(work_dest),
        conn,
        dest,
        [Path(in_parquet)],
    )
    assert (base / "users.parquet").exists()

    # 2. Read back
    src = LocalFileSrc(paths=["users.parquet"])
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work_src), conn, src)

    assert len(out) == 1
    assert len(out[0]) >= 1
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_src_treats_absolute_path_as_relative_to_base_path(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root = _test_root(domain_fs, request)
    base = root / "store"
    base.mkdir()
    in_parquet = base / "data.parquet"
    pl.DataFrame([{"x": 1}]).write_parquet(in_parquet, use_pyarrow=True)

    conn = LocalFileSrcConn(base_path=str(base))
    # Note the leading slash on the entry -- the plugin strips it.
    src = LocalFileSrc(paths=["/data.parquet"])

    work = root / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), conn, src)
    assert len(out[0]) >= 1
    assert pl.read_parquet(out[0][0].file).select("x").to_dicts() == [{"x": 1}]


# ---------------------------------------------------------------------------
# Wildcards
# ---------------------------------------------------------------------------


@fs_domain("fs")
def test_src_wildcard_in_basename_matches_multiple_files(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root = _test_root(domain_fs, request)
    base = root / "store"
    base.mkdir()
    for name in ("a.parquet", "b.parquet", "c.parquet"):
        pl.DataFrame([{"name": name}]).write_parquet(
            base / name,
            use_pyarrow=True,
        )
    # A non-matching neighbour: must NOT show up in the output
    pl.DataFrame([{"name": "skip.csv"}]).write_parquet(
        base / "skip.csv",
        use_pyarrow=True,
    )

    conn = LocalFileSrcConn(base_path=str(base))
    src = LocalFileSrc(paths=["*.parquet"])

    work = root / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), conn, src)

    assert len(out) == 1
    # Three parquet inputs -> at least three target files in the report
    assert len(out[0]) >= 3
    names = sorted(pl.read_parquet(f.file).select("name").item(0, 0) for f in out[0])
    assert names == ["a.parquet", "b.parquet", "c.parquet"]


@fs_domain("fs")
def test_src_wildcard_with_no_matches_returns_empty_slot(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root = _test_root(domain_fs, request)
    base = root / "store"
    base.mkdir()
    pl.DataFrame([{"name": "x"}]).write_parquet(
        base / "x.csv",
        use_pyarrow=True,
    )

    conn = LocalFileSrcConn(base_path=str(base))
    src = LocalFileSrc(paths=["*.parquet"])

    work = root / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), conn, src)

    # One slot, zero files
    assert len(out) == 1
    assert out[0] == []


# ---------------------------------------------------------------------------
# initial_last_modified filtering
# ---------------------------------------------------------------------------


def _set_mtime(path: Path, when: datetime) -> None:
    """Set both atime and mtime to ``when`` (UTC) on ``path``."""
    ts = when.astimezone(timezone.utc).timestamp()
    os.utime(path, (ts, ts))


@fs_domain("fs")
def test_src_initial_last_modified_filters_out_older_files(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root = _test_root(domain_fs, request)
    base = root / "store"
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
    conn = LocalFileSrcConn(base_path=str(base))
    src = LocalFileSrc(
        paths=["*.parquet"],
        initial_last_modified=cutoff,
    )

    work = root / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), conn, src)

    names = sorted(pl.read_parquet(f.file).select("name").item(0, 0) for f in out[0])
    assert names == ["new"]


@fs_domain("fs")
def test_src_initial_last_modified_at_or_before_oldest_returns_all(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root = _test_root(domain_fs, request)
    base = root / "store"
    base.mkdir()
    pl.DataFrame([{"name": "a"}]).write_parquet(base / "a.parquet", use_pyarrow=True)
    pl.DataFrame([{"name": "b"}]).write_parquet(base / "b.parquet", use_pyarrow=True)

    floor = datetime(2020, 1, 1, tzinfo=timezone.utc)
    _set_mtime(base / "a.parquet", floor + timedelta(days=1))
    _set_mtime(base / "b.parquet", floor + timedelta(days=2))

    conn = LocalFileSrcConn(base_path=str(base))
    src = LocalFileSrc(
        paths=["*.parquet"],
        initial_last_modified=floor,
    )

    work = root / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), conn, src)

    names = sorted(pl.read_parquet(f.file).select("name").item(0, 0) for f in out[0])
    assert names == ["a", "b"]


# ---------------------------------------------------------------------------
# Source-side formats
#
# Each test stages an input file in a specific format, then drives the
# read path with the matching ``SrcFileFormat``. The transporter always
# emits parquet, so the assertions read parquet and verify the rows
# match what the input file carried.
# ---------------------------------------------------------------------------


@fs_domain("fs")
def test_src_format_csv_reads_with_default_options(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root = _test_root(domain_fs, request)
    base = root / "store"
    base.mkdir()
    pl.DataFrame(
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    ).write_csv(base / "users.csv")

    conn = LocalFileSrcConn(base_path=str(base))
    src = LocalFileSrc(
        paths=["users.csv"],
        format=SrcFileFormat.CSV,
    )
    work = root / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), conn, src)
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_src_format_csv_honours_separator_option(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root = _test_root(domain_fs, request)
    base = root / "store"
    base.mkdir()
    pl.DataFrame(
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    ).write_csv(base / "users.csv", separator=";")

    conn = LocalFileSrcConn(base_path=str(base))
    src = LocalFileSrc(
        paths=["users.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: {"separator": ";"}},
    )
    work = root / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), conn, src)
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_src_format_json_reads_records(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    # The transporter binary reads newline-delimited JSON (one record
    # per line) for ``SrcFileFormat.JSON``.
    root = _test_root(domain_fs, request)
    base = root / "store"
    base.mkdir()
    pl.DataFrame(
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    ).write_ndjson(base / "users.json")

    conn = LocalFileSrcConn(base_path=str(base))
    src = LocalFileSrc(
        paths=["users.json"],
        format=SrcFileFormat.JSON,
    )
    work = root / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), conn, src)
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_src_format_avro_reads_records(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root = _test_root(domain_fs, request)
    base = root / "store"
    base.mkdir()
    pl.DataFrame(
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    ).write_avro(base / "users.avro")

    conn = LocalFileSrcConn(base_path=str(base))
    src = LocalFileSrc(
        paths=["users.avro"],
        format=SrcFileFormat.AVRO,
    )
    work = root / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), conn, src)
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_src_format_auto_infers_parquet_from_extension(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root = _test_root(domain_fs, request)
    base = root / "store"
    base.mkdir()
    pl.DataFrame([{"id": 1, "name": "alice"}]).write_parquet(
        base / "users.parquet",
        use_pyarrow=True,
    )

    conn = LocalFileSrcConn(base_path=str(base))
    src = LocalFileSrc(
        paths=["users.parquet"],
        # AUTO is the default; spell it out for clarity.
        format=SrcFileFormat.AUTO,
    )
    work = root / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), conn, src)
    df = pl.read_parquet(out[0][0].file).select("id", "name")
    assert df.to_dicts() == [{"id": 1, "name": "alice"}]


# ---------------------------------------------------------------------------
# Destination-side formats
#
# Each test starts from a small parquet file (the in-flight table
# format) and drives the write path with the requested
# ``DestFileFormat``. The assertion reads the emitted file with the
# matching reader and checks the row content.
# ---------------------------------------------------------------------------


def _stage_input_parquet(root: Path, rows: list[dict]) -> Path:
    p = root / "in.parquet"
    pl.DataFrame(rows).write_parquet(p, use_pyarrow=True)
    return p


# Non-parquet dest formats: ``_output.copy_one`` transcodes the
# source parquet locally via polars (``sink_csv`` / ``sink_ndjson``
# / ``write_avro``) before invoking the binary. The binary's copy
# step is a raw byte-copy, so the transcoded temp file IS what
# lands at the user-specified target.


@fs_domain("fs")
def test_dest_format_csv_writes_csv_file(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root = _test_root(domain_fs, request)
    base = root / "store"
    base.mkdir()
    src_parquet = _stage_input_parquet(
        root,
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )

    conn = LocalFileSrcConn(base_path=str(base))
    dest = LocalFileDest(
        paths=["users.csv"],
        format=DestFileFormat.CSV,
    )
    work = root / "work"
    work.mkdir()
    plugin.LocalFileDestPlugin().write_out(
        PluginDestCtx(work),
        conn,
        dest,
        [Path(src_parquet)],
    )

    out_file = base / "users.csv"
    assert out_file.exists()
    df = pl.read_csv(out_file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_dest_format_csv_honours_separator_option(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root = _test_root(domain_fs, request)
    base = root / "store"
    base.mkdir()
    src_parquet = _stage_input_parquet(
        root,
        [{"id": 1, "name": "alice"}],
    )

    conn = LocalFileDestConn(base_path=str(base))
    dest = LocalFileDest(
        paths=["users.csv"],
        format=DestFileFormat.CSV,
        format_cfg={DestFileFormat.CSV: {"separator": ";"}},
    )
    work = root / "work"
    work.mkdir()
    plugin.LocalFileDestPlugin().write_out(
        PluginDestCtx(work),
        conn,
        dest,
        [Path(src_parquet)],
    )

    raw = (base / "users.csv").read_text()
    # ``;`` separates the columns; ``,`` would mean the option was ignored.
    assert ";" in raw
    assert "," not in raw


@fs_domain("fs")
def test_dest_format_json_writes_json_file(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root = _test_root(domain_fs, request)
    base = root / "store"
    base.mkdir()
    src_parquet = _stage_input_parquet(
        root,
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )

    conn = LocalFileDestConn(base_path=str(base))
    dest = LocalFileDest(
        paths=["users.json"],
        format=DestFileFormat.JSON,
    )
    work = root / "work"
    work.mkdir()
    plugin.LocalFileDestPlugin().write_out(
        PluginDestCtx(work),
        conn,
        dest,
        [Path(src_parquet)],
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


@fs_domain("fs")
def test_dest_format_avro_writes_avro_file(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root = _test_root(domain_fs, request)
    base = root / "store"
    base.mkdir()
    src_parquet = _stage_input_parquet(
        root,
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )

    conn = LocalFileDestConn(base_path=str(base))
    dest = LocalFileDest(
        paths=["users.avro"],
        format=DestFileFormat.AVRO,
    )
    work = root / "work"
    work.mkdir()
    plugin.LocalFileDestPlugin().write_out(
        PluginDestCtx(work),
        conn,
        dest,
        [Path(src_parquet)],
    )

    out_file = base / "users.avro"
    assert out_file.exists()
    df = pl.read_avro(out_file).select("id", "name").sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


@fs_domain("fs")
def test_dest_format_auto_defaults_to_parquet(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    root = _test_root(domain_fs, request)
    base = root / "store"
    base.mkdir()
    src_parquet = _stage_input_parquet(
        root,
        [{"id": 1, "name": "alice"}],
    )

    conn = LocalFileDestConn(base_path=str(base))
    dest = LocalFileDest(
        paths=["users.parquet"],
        format=DestFileFormat.AUTO,
    )
    work = root / "work"
    work.mkdir()
    plugin.LocalFileDestPlugin().write_out(
        PluginDestCtx(work),
        conn,
        dest,
        [Path(src_parquet)],
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
# output reflects the expected interpretation.
# ---------------------------------------------------------------------------


def _read_csv_with_cfg(
    root: Path,
    csv_text: str,
    cfg: dict,
) -> pl.DataFrame:
    base = root / "store"
    base.mkdir()
    (base / "in.csv").write_text(csv_text)

    conn = LocalFileSrcConn(base_path=str(base))
    src = LocalFileSrc(
        paths=["in.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: cfg},
    )
    work = root / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), conn, src)
    # Drop the binary's ``$td.opt.*`` provenance columns.
    df = pl.read_parquet(out[0][0].file)
    user_cols = [c for c in df.columns if not c.startswith("$td.")]
    return df.select(user_cols)


# --- separator -------------------------------------------------------


@fs_domain("fs")
def test_csv_src_separator(domain_fs: FsDomain, request: pytest.FixtureRequest):
    df = _read_csv_with_cfg(
        _test_root(domain_fs, request),
        "id;name\n1;alice\n2;bob\n",
        {"separator": ";"},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


# --- quote_char ------------------------------------------------------


@fs_domain("fs")
def test_csv_src_quote_char(domain_fs: FsDomain, request: pytest.FixtureRequest):
    # Apostrophe-quoted field with embedded comma -- only parses
    # correctly when ``quote_char`` is honored.
    df = _read_csv_with_cfg(
        _test_root(domain_fs, request),
        "id,name\n1,'alice, the great'\n2,bob\n",
        {"quote_char": "'"},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice, the great"},
        {"id": 2, "name": "bob"},
    ]


# --- has_header ------------------------------------------------------


@fs_domain("fs")
def test_csv_src_has_header_false(domain_fs: FsDomain, request: pytest.FixtureRequest):
    df = _read_csv_with_cfg(
        _test_root(domain_fs, request),
        "1,alice\n2,bob\n",
        {"has_header": False},
    )
    # Column names are positional; values are all data.
    assert len(df) == 2
    # Whatever the binary names the columns, they must NOT be 'id'/'name'.
    assert "id" not in df.columns
    assert "name" not in df.columns


# --- skip_rows -------------------------------------------------------


@fs_domain("fs")
def test_csv_src_skip_rows(domain_fs: FsDomain, request: pytest.FixtureRequest):
    df = _read_csv_with_cfg(
        _test_root(domain_fs, request),
        "# noise 1\n# noise 2\nid,name\n1,alice\n2,bob\n",
        {"skip_rows": 2},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


# --- skip_rows_after_header ------------------------------------------


@fs_domain("fs")
def test_csv_src_skip_rows_after_header(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    df = _read_csv_with_cfg(
        _test_root(domain_fs, request),
        "id,name\nskip_1,skip_1\nskip_2,skip_2\n1,alice\n2,bob\n",
        {"skip_rows_after_header": 2},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


# --- comment_prefix --------------------------------------------------


@fs_domain("fs")
def test_csv_src_comment_prefix(domain_fs: FsDomain, request: pytest.FixtureRequest):
    df = _read_csv_with_cfg(
        _test_root(domain_fs, request),
        "# header comment\nid,name\n# data comment\n1,alice\n2,bob\n",
        {"comment_prefix": "#"},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


# --- null_values -----------------------------------------------------


@fs_domain("fs")
def test_csv_src_null_values(domain_fs: FsDomain, request: pytest.FixtureRequest):
    df = _read_csv_with_cfg(
        _test_root(domain_fs, request),
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


@fs_domain("fs")
def test_csv_src_missing_is_null_default_empty_cell_is_null(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    df = _read_csv_with_cfg(
        _test_root(domain_fs, request),
        "id,name\n1,\n2,bob\n",
        {"missing_is_null": True},
    )
    rows = df.sort("id").to_dicts()
    assert rows[0]["name"] is None
    assert rows[1]["name"] == "bob"


# --- try_parse_dates -------------------------------------------------


@fs_domain("fs")
def test_csv_src_try_parse_dates(domain_fs: FsDomain, request: pytest.FixtureRequest):
    df = _read_csv_with_cfg(
        _test_root(domain_fs, request),
        "id,when\n1,2026-01-01\n2,2026-02-02\n",
        {"try_parse_dates": True},
    )
    # With try_parse_dates honored we'd expect a Date dtype.
    assert str(df.schema["when"]).lower() == "date"


# --- decimal_comma ---------------------------------------------------


@fs_domain("fs")
def test_csv_src_decimal_comma(domain_fs: FsDomain, request: pytest.FixtureRequest):
    # Comma is the decimal point, so the field delimiter must be
    # something else (``;`` here).
    df = _read_csv_with_cfg(
        _test_root(domain_fs, request),
        "id;value\n1;1,5\n2;2,75\n",
        {"separator": ";", "decimal_comma": True},
    )
    rows = df.sort("id").to_dicts()
    assert rows[0]["value"] == 1.5
    assert rows[1]["value"] == 2.75


# --- truncate_ragged_lines -------------------------------------------


@fs_domain("fs")
def test_csv_src_truncate_ragged_lines(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    df = _read_csv_with_cfg(
        _test_root(domain_fs, request),
        # Second row has an extra column.
        "id,name\n1,alice\n2,bob,extra\n",
        {"truncate_ragged_lines": True, "ignore_errors": True},
    )
    # Both rows should be retained (extra column truncated or ignored).
    assert len(df) == 2


# --- raise_if_empty --------------------------------------------------


@fs_domain("fs")
def test_csv_src_raise_if_empty_false(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    # Empty file -- with raise_if_empty=False the binary should NOT
    # error; output should be empty.
    df = _read_csv_with_cfg(
        _test_root(domain_fs, request),
        "",
        {"raise_if_empty": False},
    )
    assert len(df) == 0


# --- ignore_errors ---------------------------------------------------


@fs_domain("fs")
def test_csv_src_ignore_errors(domain_fs: FsDomain, request: pytest.FixtureRequest):
    # 'not_a_number' would normally trip type inference on ``id``.
    # With ``ignore_errors=True`` the binary doesn't crash; it keeps
    # the column as String so every row passes through. Probing
    # showed all 3 rows survive.
    df = _read_csv_with_cfg(
        _test_root(domain_fs, request),
        "id,name\n1,alice\nnot_a_number,bob\n3,carol\n",
        {"ignore_errors": True},
    )
    assert len(df) == 3
    # ``id`` ends up as String since type inference couldn't pick int.
    assert str(df.schema["id"]).lower() in {"string", "str", "utf8"}


# --- encoding --------------------------------------------------------


@fs_domain("fs")
def test_csv_src_encoding_utf8(domain_fs: FsDomain, request: pytest.FixtureRequest):
    # Default encoding is utf8 -- multibyte chars round-trip.
    root = _test_root(domain_fs, request)
    base = root / "store"
    base.mkdir()
    (base / "in.csv").write_bytes(
        "id,name\n1,café\n2,piñata\n".encode("utf-8"),
    )
    conn = LocalFileSrcConn(base_path=str(base))
    src = LocalFileSrc(
        paths=["in.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: {"encoding": "utf8"}},
    )
    work = root / "work"
    work.mkdir()
    out = plugin.LocalFileSrcPlugin().read_in(PluginSrcCtx(work), conn, src)
    df = pl.read_parquet(out[0][0].file).select("id", "name")
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "café"},
        {"id": 2, "name": "piñata"},
    ]


# --- eol_char --------------------------------------------------------


@fs_domain("fs")
def test_csv_src_eol_char_default_lf(
    domain_fs: FsDomain, request: pytest.FixtureRequest
):
    # Default eol is LF; this validates the happy path.
    df = _read_csv_with_cfg(
        _test_root(domain_fs, request),
        "id,name\n1,alice\n2,bob\n",
        {"eol_char": "\n"},
    )
    assert df.sort("id").to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]
