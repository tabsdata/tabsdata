#
# Copyright 2026 Tabsdata Inc.
#

"""Transporter integration tests that invoke the real ``td_transporter2``.

Skipped unless the binary is installed in the active Python environment's
scripts folder. Everything else in the conn-common-file suite mocks the
subprocess, so this file is the only end-to-end coverage of the binary
handshake (request YAML round-trip + report parsing).
"""

from __future__ import annotations

import os
import tempfile
import zipfile
from dataclasses import dataclass, field

# from decimal import Decimal
from datetime import datetime, timedelta, timezone
from pathlib import Path
from xml.sax.saxutils import escape

import polars as pl
import pytest

from tabsdatak.common._interpreter.python.error import BinaryNotFoundError
from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.common._transporter.facade import make_handler
from tabsdatak.common._transporter.handler.handler2 import TRANSPORTER_BINARY_NAME

# noinspection PyProtectedMember
# noinspection PyProtectedMember
# noinspection PyProtectedMember
from tabsdatak.conn.common.file import (
    _input as _input_module,
    _output as _output_module,
    _plugin,
)
from tabsdatak.conn.common.file.constants import FILE_META_PATH
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    SrcFileFormat,
)
from tabsdatak.spi import DestContext, SrcPluginCtx

# ---------------------------------------------------------------------------
# Gate: skip the whole module if the binary isn't available
# ---------------------------------------------------------------------------


try:
    TRANSPORTER_BINARY = PythonEnvironment().binary(TRANSPORTER_BINARY_NAME)
except BinaryNotFoundError:
    TRANSPORTER_BINARY = None

pytestmark = pytest.mark.skipif(
    TRANSPORTER_BINARY is None,
    reason=(
        f"{TRANSPORTER_BINARY_NAME} binary not found in the active Python "
        "environment; integration tests skipped"
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

    handler = make_handler()
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

    handler = make_handler()
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

    handler = make_handler()
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

    handler = make_handler()
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
    imported = out[0][0]
    assert isinstance(imported.file, Path)
    # The binary reports a local source as a plain path, and the temp dir
    # it sits under may itself be a symlink.
    assert (
        Path(imported.metadata[FILE_META_PATH]).resolve()
        == (src_dir / "users.parquet").resolve()
    )
    df = pl.read_parquet(imported.file).select("id", "name").sort("id")
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


# ---------------------------------------------------------------------------
# Excel (.xlsx) sources
# ---------------------------------------------------------------------------

_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PKG_REL = "http://schemas.openxmlformats.org/package/2006/relationships"
_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_TYPES = "http://schemas.openxmlformats.org/package/2006/content-types"
_OFFICE = "application/vnd.openxmlformats-officedocument.spreadsheetml"

# The 1900 date system counts days from here; the two-day offset from
# 1900-01-01 absorbs Excel's phantom 1900-02-29.
_EPOCH_1900 = datetime(1899, 12, 30)

# Cell-format indices declared by ``_STYLES``: plain, then a date format
# (``numFmtId`` 14), which is the only thing marking a serial as a date
# rather than a number.
_STYLE_DATE = 1
_STYLES = (
    f'<styleSheet xmlns="{_MAIN}"><cellXfs count="2">'
    '<xf numFmtId="0"/><xf numFmtId="14"/>'
    "</cellXfs></styleSheet>"
)


def _column_ref(index: int) -> str:
    """Spreadsheet column letters for a 0-based index (``0`` -> ``A``)."""
    ref = ""
    index += 1
    while index:
        index, remainder = divmod(index - 1, 26)
        ref = chr(ord("A") + remainder) + ref
    return ref


def _cell(ref: str, value) -> str:
    # An omitted cell is how a workbook spells "blank", so ``None`` emits
    # nothing at all rather than an empty cell.
    if value is None:
        return ""
    if isinstance(value, bool):
        return f'<c r="{ref}" t="b"><v>{int(value)}</v></c>'
    if isinstance(value, datetime):
        serial = (value - _EPOCH_1900).total_seconds() / 86400
        return f'<c r="{ref}" s="{_STYLE_DATE}"><v>{serial!r}</v></c>'
    if isinstance(value, (int, float)):
        return f'<c r="{ref}"><v>{value}</v></c>'
    return f'<c r="{ref}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>'


def _sheet_xml(rows: list[list]) -> str:
    body = "".join(
        '<row r="{n}">{cells}</row>'.format(
            n=n,
            cells="".join(
                _cell(f"{_column_ref(i)}{n}", value) for i, value in enumerate(row)
            ),
        )
        for n, row in enumerate(rows, 1)
    )
    return f'<worksheet xmlns="{_MAIN}"><sheetData>{body}</sheetData></worksheet>'


def _write_input_xlsx(path: Path, sheets: dict[str, list[list]]) -> Path:
    """Write a minimal ``.xlsx`` holding ``sheets``, in declaration order.

    Hand-rolled rather than produced with a spreadsheet library: nothing in
    the dependency set writes workbooks, and the parts a reader needs are
    few. Cells are inline strings and numbers, so there is no
    shared-strings table. The package is complete enough (manifest,
    namespaces, relationships) that any OOXML reader opens it, not just the
    one under test.
    """
    sheet_tags: list[str] = []
    rel_tags: list[str] = []
    overrides = [
        f'<Override PartName="/xl/workbook.xml"'
        f' ContentType="{_OFFICE}.sheet.main+xml"/>',
        f'<Override PartName="/xl/styles.xml" ContentType="{_OFFICE}.styles+xml"/>',
    ]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for number, (name, rows) in enumerate(sheets.items(), 1):
            part = f"xl/worksheets/sheet{number}.xml"
            sheet_tags.append(
                f'<sheet name="{escape(name)}" sheetId="{number}"'
                f' state="visible" r:id="rId{number}"/>'
            )
            rel_tags.append(
                f'<Relationship Id="rId{number}" Type="{_REL}/worksheet"'
                f' Target="worksheets/sheet{number}.xml"/>'
            )
            overrides.append(
                f'<Override PartName="/{part}" ContentType="{_OFFICE}.worksheet+xml"/>'
            )
            zf.writestr(part, _sheet_xml(rows))
        zf.writestr(
            "[Content_Types].xml",
            f'<Types xmlns="{_TYPES}">'
            '<Default Extension="rels" ContentType="application/vnd.'
            'openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            f"{''.join(overrides)}</Types>",
        )
        zf.writestr(
            "_rels/.rels",
            f'<Relationships xmlns="{_PKG_REL}">'
            f'<Relationship Id="rId1" Type="{_REL}/officeDocument"'
            ' Target="xl/workbook.xml"/></Relationships>',
        )
        zf.writestr(
            "xl/workbook.xml",
            f'<workbook xmlns="{_MAIN}" xmlns:r="{_REL}">'
            f"<sheets>{''.join(sheet_tags)}</sheets></workbook>",
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            f'<Relationships xmlns="{_PKG_REL}">{"".join(rel_tags)}</Relationships>',
        )
        zf.writestr("xl/styles.xml", _STYLES)
    return path


@dataclass
class _Src:
    """The attributes ``ReaderConf.from_src`` reads off a Src, and no more.

    Standing in for a real connector Src keeps these tests on the shared
    plugin path rather than on any one connector's surface -- s3, gcs,
    azureblob and localfile all reach Excel through exactly this.
    """

    paths: list[str]
    format: SrcFileFormat = SrcFileFormat.AUTO
    format_cfg: dict | None = None
    initial_last_modified: datetime | None = None
    src_cfg: dict = field(default_factory=dict)
    purge: bool = False


def _read_one(
    tmp_path: Path,
    source_path: Path,
    *,
    file_format: SrcFileFormat = SrcFileFormat.EXCEL,
    format_cfg: dict | None = None,
) -> pl.DataFrame:
    """Drive ``import_one`` over one workbook and return the parquet it
    produced, with the binary's internal ``$td.opt.*`` columns dropped."""
    out_dir = Path(tempfile.mkdtemp(prefix="out-", dir=tmp_path))
    handler = make_handler()
    report = _input_module.import_one(
        handler=handler,
        source=handler.local_path_location(source_path),
        target_dir=out_dir,
        file_format=file_format,
        paths=[str(source_path)],
        format_cfg=format_cfg,
    )
    assert report.files, "transporter report must reference at least one file"
    target = report.files[0].get("to")
    assert target, f"no 'to' field in report.files entry: {report.files[0]!r}"
    df = pl.read_parquet(target)
    return df.drop([c for c in df.columns if c.startswith("$td.")])


def test_import_one_local_xlsx_round_trip(tmp_path: Path):
    src_dir = _input_dir(tmp_path)
    source_path = _write_input_xlsx(
        src_dir / "users.xlsx",
        {"Data": [["id", "name"], [1, "alice"], [2, "bob"]]},
    )

    df = _read_one(tmp_path, source_path).sort("id")
    assert df.to_dicts() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


def test_import_one_xlsx_format_is_inferred_from_the_extension(tmp_path: Path):
    src_dir = _input_dir(tmp_path)
    source_path = _write_input_xlsx(
        src_dir / "users.xlsx",
        {"Data": [["id", "name"], [1, "alice"]]},
    )

    df = _read_one(tmp_path, source_path, file_format=SrcFileFormat.AUTO)
    assert df.to_dicts() == [{"id": 1, "name": "alice"}]


def test_import_one_xlsx_resolves_column_types(tmp_path: Path):
    src_dir = _input_dir(tmp_path)
    source_path = _write_input_xlsx(
        src_dir / "typed.xlsx",
        {
            "Data": [
                ["n", "ratio", "label", "when", "sparse"],
                [1, 1.5, "a", datetime(2026, 1, 15), 10],
                [2, 2.0, "b", datetime(2026, 2, 1), None],
            ]
        },
    )

    df = _read_one(tmp_path, source_path).sort("n")

    # Whole numbers narrow to Int64; one fractional cell widens the whole
    # column to Float64; a date format makes a serial a timestamp.
    assert dict(df.schema) == {
        "n": pl.Int64,
        "ratio": pl.Float64,
        "label": pl.String,
        "when": pl.Datetime("us"),
        "sparse": pl.Int64,
    }
    assert df.to_dicts() == [
        {
            "n": 1,
            "ratio": 1.5,
            "label": "a",
            "when": datetime(2026, 1, 15),
            "sparse": 10,
        },
        {
            "n": 2,
            "ratio": 2.0,
            "label": "b",
            "when": datetime(2026, 2, 1),
            # A blank cell is a null, not a zero and not an empty string.
            "sparse": None,
        },
    ]


def test_import_one_xlsx_has_header_flag_switches_the_header_row(tmp_path: Path):
    src_dir = _input_dir(tmp_path)
    source_path = _write_input_xlsx(
        src_dir / "headered.xlsx",
        {"Data": [["id", "name"], ["a", "alice"], ["b", "bob"]]},
    )

    # Default: row 1 names the columns and is not data.
    headered = _read_one(tmp_path, source_path)
    assert headered.columns == ["id", "name"]
    assert headered.to_dicts() == [
        {"id": "a", "name": "alice"},
        {"id": "b", "name": "bob"},
    ]

    # ``has_header: False``: positional names, and row 1 becomes a row.
    headerless = _read_one(
        tmp_path,
        source_path,
        format_cfg={SrcFileFormat.EXCEL: {"has_header": False}},
    )
    assert headerless.columns == ["column_1", "column_2"]
    assert headerless.to_dicts() == [
        {"column_1": "id", "column_2": "name"},
        {"column_1": "a", "column_2": "alice"},
        {"column_1": "b", "column_2": "bob"},
    ]


def test_import_one_xlsx_sheet_option_selects_the_worksheet(tmp_path: Path):
    src_dir = _input_dir(tmp_path)
    source_path = _write_input_xlsx(
        src_dir / "book.xlsx",
        {
            "First": [["id"], [1]],
            "Second": [["tag"], ["from-sheet-2"]],
        },
    )

    first = _read_one(tmp_path, source_path)
    assert first.to_dicts() == [{"id": 1}]

    second = _read_one(
        tmp_path,
        source_path,
        format_cfg={SrcFileFormat.EXCEL: {"sheet": 2}},
    )
    assert second.to_dicts() == [{"tag": "from-sheet-2"}]


def test_import_one_glob_pattern_matches_xlsx_files(tmp_path: Path):
    src_dir = _input_dir(tmp_path)
    out_dir = _output_dir(tmp_path)

    _write_input_xlsx(src_dir / "a.xlsx", {"Data": [["id"], [1]]})
    _write_input_xlsx(src_dir / "b.xlsx", {"Data": [["id"], [2]]})

    handler = make_handler()
    glob = handler.local_uri(src_dir) + "*.xlsx"
    report = _input_module.import_one(
        handler=handler,
        source=handler.local_location(glob),
        target_dir=out_dir,
        file_format=SrcFileFormat.EXCEL,
        paths=[glob],
    )

    targets = [f["to"] for f in report.files if isinstance(f, dict) and f.get("to")]
    assert len(targets) == 2
    assert sorted(
        row["id"] for target in targets for row in pl.read_parquet(target).to_dicts()
    ) == [1, 2]


# ---------------------------------------------------------------------------
# _plugin.import_files with ``#<n>`` worksheet fragments
# ---------------------------------------------------------------------------


def _import_files(tmp_path: Path, src: _Src) -> list[list]:
    work_dir = tmp_path / "work"
    work_dir.mkdir(exist_ok=True)
    return _plugin.import_files(
        ctx=_SrcCtx(work_dir),
        cfg=_plugin.ReaderConf.from_src(conn=object(), src=src),
        build_location=_local_location,
        errors=_INTEG_ERRORS,
    )


def test_run_read_in_xlsx_sheet_fragment_end_to_end(tmp_path: Path):
    src_dir = _input_dir(tmp_path)
    source_path = _write_input_xlsx(
        src_dir / "book.xlsx",
        {
            "First": [["tag"], ["from-sheet-1"]],
            "Second": [["tag"], ["from-sheet-2"]],
        },
    )

    out = _import_files(tmp_path, _Src(paths=[f"{source_path}#2"]))

    assert len(out) == 1 and len(out[0]) == 1
    imported = out[0][0]
    assert pl.read_parquet(imported.file).select("tag").to_dicts() == [
        {"tag": "from-sheet-2"}
    ]

    # Provenance names the worksheet, and does so in the form that can be
    # pasted straight back into ``paths``. The fragment never reached the
    # binary, so it is put back onto the uri the binary reported.
    origin = imported.metadata[FILE_META_PATH]
    head, _, fragment = origin.partition("#")
    assert fragment == "2"
    # The temp dir the source sits under may itself be a symlink.
    assert Path(head).resolve() == source_path.resolve()


def test_run_read_in_xlsx_without_a_fragment_has_no_sheet_in_provenance(
    tmp_path: Path,
):
    src_dir = _input_dir(tmp_path)
    source_path = _write_input_xlsx(
        src_dir / "book.xlsx",
        {
            "First": [["tag"], ["from-sheet-1"]],
            "Second": [["tag"], ["from-sheet-2"]],
        },
    )

    out = _import_files(tmp_path, _Src(paths=[str(source_path)]))

    imported = out[0][0]
    # No fragment reads the first worksheet, and stamps no ``#`` on the
    # provenance -- a path that named no sheet round-trips unchanged.
    assert pl.read_parquet(imported.file).select("tag").to_dicts() == [
        {"tag": "from-sheet-1"}
    ]
    assert "#" not in imported.metadata[FILE_META_PATH]


def test_run_read_in_xlsx_narrows_the_sheet_per_path(tmp_path: Path):
    src_dir = _input_dir(tmp_path)
    first = _write_input_xlsx(
        src_dir / "first.xlsx",
        {
            "A": [["tag"], ["first-sheet-1"]],
            "B": [["tag"], ["first-sheet-2"]],
        },
    )
    second = _write_input_xlsx(
        src_dir / "second.xlsx",
        {
            "A": [["tag"], ["second-sheet-1"]],
            "B": [["tag"], ["second-sheet-2"]],
        },
    )

    # One worksheet list covers both slots: slot 0 names sheet 2, slot 1
    # names none and must fall back to the first sheet rather than
    # inherit its neighbour's number.
    out = _import_files(tmp_path, _Src(paths=[f"{first}#2", str(second)]))

    assert len(out) == 2
    assert pl.read_parquet(out[0][0].file).select("tag").to_dicts() == [
        {"tag": "first-sheet-2"}
    ]
    assert pl.read_parquet(out[1][0].file).select("tag").to_dicts() == [
        {"tag": "second-sheet-1"}
    ]
    assert out[0][0].metadata[FILE_META_PATH].endswith("#2")
    assert "#" not in out[1][0].metadata[FILE_META_PATH]


def test_run_read_in_xlsx_fragment_and_has_header_coexist(tmp_path: Path):
    src_dir = _input_dir(tmp_path)
    source_path = _write_input_xlsx(
        src_dir / "book.xlsx",
        {
            "First": [["tag"], ["from-sheet-1"]],
            "Second": [["tag"], ["from-sheet-2"]],
        },
    )

    # The worksheet rides in the same EXCEL entry the user's own options
    # live in; neither may displace the other.
    out = _import_files(
        tmp_path,
        _Src(
            paths=[f"{source_path}#2"],
            format=SrcFileFormat.EXCEL,
            format_cfg={SrcFileFormat.EXCEL: {"has_header": False}},
        ),
    )

    df = pl.read_parquet(out[0][0].file)
    assert df.select("column_1").to_dicts() == [
        {"column_1": "tag"},
        {"column_1": "from-sheet-2"},
    ]


# EBCDIC fixed-length records. Every field encoding below is built by hand,
# because the point is to prove the reader agrees with the mainframe
# representation rather than with another of our own encoders.

#
# def _copybook(*lines: str) -> str:
#     """A copybook in fixed-format COBOL: columns 1-6 are sequence numbers and
#     the code area starts at column 8, which is what the parser expects."""
#     return "".join(f"       {line}\n" for line in lines)

#
# def _ebc(text: str, width: int | None = None) -> bytes:
#     """`text` as EBCDIC, space-padded to `width`."""
#     return (text if width is None else text.ljust(width)).encode("cp037")

#
# def _zoned(value: int, digits: int, signed: bool = True) -> bytes:
#     """`USAGE DISPLAY`: one byte per digit, the sign overpunched into the high
#     nibble of the last (`C` positive, `D` negative)."""
#     body = bytearray(f"{abs(value):0{digits}d}".encode("cp037"))
#     if signed:
#         body[-1] = (body[-1] & 0x0F) | (0xD0 if value < 0 else 0xC0)
#     return bytes(body)

#
# def _zoned_leading_sign(value: int, digits: int) -> bytes:
#     """`SIGN LEADING SEPARATE`: the sign is its own byte in front."""
#     return _ebc(("-" if value < 0 else "+") + f"{abs(value):0{digits}d}")

#
# def _packed(value: int, digits: int) -> bytes:
#     """`COMP-3`: two digits per byte, the final low nibble the sign."""
#     text = f"{abs(value):0{digits}d}"
#     if len(text) % 2 == 0:
#         text = "0" + text
#     nibbles = [int(c) for c in text] + [0x0D if value < 0 else 0x0C]
# return bytes((nibbles[i] << 4) | nibbles[i + 1] for i in range(0, len(nibbles), 2))

#
# def _binary(value: int, digits: int) -> bytes:
#     """`COMP`: big-endian two's complement, width from the declared digits."""
#     width = 2 if digits <= 4 else 4 if digits <= 9 else 8
#     return value.to_bytes(width, "big", signed=True)

#
# def _hfp(value: float, width: int) -> bytes:
#     """IBM hex float: sign, 7-bit excess-64 base-16 exponent, then a fraction
#     normalised into [1/16, 1)."""
#     fraction_bits = 24 if width == 4 else 56
#     if value == 0:
#         return bytes(width)
#     magnitude, exponent = abs(value), 0
#     while magnitude >= 1.0:
#         magnitude /= 16.0
#         exponent += 1
#     while magnitude < 1.0 / 16.0:
#         magnitude *= 16.0
#         exponent -= 1
#     raw = ((0x80 if value < 0 else 0) | (exponent + 64)) << fraction_bits
#     raw |= round(magnitude * (1 << fraction_bits))
#     return raw.to_bytes(width, "big")

#
# def _write_input_ebcdic(path: Path, records: list[bytes]) -> Path:
#     lengths = {len(r) for r in records}
#     assert len(lengths) == 1, f"records must be one fixed length, got {lengths}"
#     path.write_bytes(b"".join(records))
#     return path

#
# def _read_ebcdic(
#     tmp_path: Path, source_path: Path, copybook: str, **cfg
# ) -> pl.DataFrame:
#     out_dir = Path(tempfile.mkdtemp(prefix="out-", dir=tmp_path))
#     handler = make_handler()
#     report = _input_module.import_one(
#         handler=handler,
#         source=handler.local_path_location(source_path),
#         target_dir=out_dir,
#         file_format=SrcFileFormat.EBCDIC,
#         paths=[str(source_path)],
#         format_cfg={SrcFileFormat.EBCDIC: {"copybook": copybook, **cfg}},
#     )
#     assert report.files, "transporter report must reference at least one file"
#     df = pl.read_parquet(report.files[0]["to"])
#     return df.drop([c for c in df.columns if c.startswith("$td.")])

#
# def test_import_one_ebcdic_round_trip(tmp_path: Path):
#     src_dir = _input_dir(tmp_path)
#     copybook = _copybook(
#         "01  CUSTOMER.",
#         "    05  NAME  PIC X(6).",
#         "    05  COUNT PIC 9(4).",
#     )
#     source = _write_input_ebcdic(
#         src_dir / "cust.dat",
#         [_ebc("ALICE", 6) + _zoned(42, 4, signed=False)],
#     )
#
#     df = _read_ebcdic(tmp_path, source, copybook)
#     assert df.to_dicts() == [{"NAME": "ALICE", "COUNT": 42}]

#
# def test_import_one_ebcdic_decodes_every_supported_type(tmp_path: Path):
#     src_dir = _input_dir(tmp_path)
#     copybook = _copybook(
#         "01  EVERYTHING.",
#         "    05  NAME    PIC X(6).",
#         "    05  CODE    PIC A(3).",
#         "    05  COUNT   PIC 9(5).",
#         "    05  SIGNED  PIC S9(5).",
#         "    05  LEADSEP PIC S9(3) SIGN LEADING SEPARATE.",
#         "    05  SCALED  PIC S9(3)V99.",
#         "    05  BALANCE PIC S9(7)V99 COMP-3.",
#         "    05  QTY     PIC S9(4) COMP.",
#         "    05  BIGNUM  PIC S9(10) COMP.",
#         "    05  RATE    COMP-1.",
#         "    05  PRECISE COMP-2.",
#         "    05  EDITED  PIC ZZ,ZZ9.99.",
#     )
#     record = b"".join([
#         _ebc("ALICE", 6),
#         _ebc("GBP", 3),
#         _zoned(12345, 5, signed=False),
#         _zoned(-6789, 5),
#         _zoned_leading_sign(-42, 3),
#         _zoned(-1234, 5),  # S9(3)V99 -> unscaled -12.34
#         _packed(1234567, 9),  # S9(7)V99 -> unscaled 12345.67
#         _binary(-1234, 4),
#         _binary(9876543210, 10),
#         _hfp(1.0, 4),
#         _hfp(-0.5, 8),
#         _ebc("   123.45", 9),
#     ])
#     source = _write_input_ebcdic(src_dir / "all.dat", [record])
#
#     df = _read_ebcdic(tmp_path, source, copybook)
#
#     # Storage width never shows in the schema: a zoned, packed and binary
#     # field of the same precision all land on the same arrow type.
#     assert dict(df.schema) == {
#         "NAME": pl.String,
#         "CODE": pl.String,
#         "COUNT": pl.Int64,
#         "SIGNED": pl.Int64,
#         "LEADSEP": pl.Int64,
#         "SCALED": pl.Decimal(5, 2),
#         "BALANCE": pl.Decimal(9, 2),
#         "QTY": pl.Int64,
#         "BIGNUM": pl.Int64,
#         "RATE": pl.Float64,
#         "PRECISE": pl.Float64,
#         "EDITED": pl.Decimal(7, 2),
#     }
#     assert df.to_dicts() == [
#         {
#             "NAME": "ALICE",
#             "CODE": "GBP",
#             "COUNT": 12345,
#             "SIGNED": -6789,
#             "LEADSEP": -42,
#             "SCALED": Decimal("-12.34"),
#             "BALANCE": Decimal("12345.67"),
#             "QTY": -1234,
#             "BIGNUM": 9876543210,
#             "RATE": 1.0,
#             "PRECISE": -0.5,
#             "EDITED": Decimal("123.45"),
#         }
#     ]

#
# @pytest.mark.parametrize("value", [0, 1, -1, 98765, -98765])
# def test_import_one_ebcdic_signs_survive_every_numeric_encoding(
#     tmp_path: Path, value: int
# ):
#     # The sign lives in a different place in each encoding: an overpunched
#     # digit byte, a byte of its own, the final nibble, and the top bit.
#     src_dir = _input_dir(tmp_path)
#     copybook = _copybook(
#         "01  SIGNS.",
#         "    05  ZONE PIC S9(5).",
#         "    05  SEP  PIC S9(5) SIGN LEADING SEPARATE.",
#         "    05  PACK PIC S9(5) COMP-3.",
#         "    05  BIN  PIC S9(5) COMP.",
#     )
#     source = _write_input_ebcdic(
#         src_dir / f"signs{value}.dat",
#         [
#             _zoned(value, 5)
#             + _zoned_leading_sign(value, 5)
#             + _packed(value, 5)
#             + _binary(value, 5)
#         ],
#     )
#
#     df = _read_ebcdic(tmp_path, source, copybook)
# assert df.to_dicts() == [{"ZONE": value, "SEP": value, "PACK": value, "BIN": value}]

#
# def test_import_one_ebcdic_blank_fields_are_null(tmp_path: Path):
#     # A fixed record has no null marker, so an untouched field is spaces.
#     src_dir = _input_dir(tmp_path)
#     copybook = _copybook(
#         "01  SPARSE.",
#         "    05  NAME PIC X(4).",
#         "    05  NUM  PIC 9(4).",
#         "    05  PACK PIC S9(3) COMP-3.",
#     )
#     source = _write_input_ebcdic(
#         src_dir / "sparse.dat",
#         [
#             _ebc("BOB ", 4) + _zoned(7, 4, signed=False) + _packed(9, 3),
#             _ebc(" ", 4) + _ebc(" ", 4) + _ebc(" ", 2),
#         ],
#     )
#
#     df = _read_ebcdic(tmp_path, source, copybook)
#     assert df.to_dicts() == [
#         {"NAME": "BOB", "NUM": 7, "PACK": 9},
#         {"NAME": None, "NUM": None, "PACK": None},
#     ]

#
# def test_import_one_ebcdic_occurs_becomes_one_column_per_element(tmp_path: Path):
#     src_dir = _input_dir(tmp_path)
#     copybook = _copybook(
#         "01  TABLE-REC.",
#         "    05  ID     PIC 9(2).",
#         "    05  SCORES PIC 9(3) OCCURS 3.",
#     )
#     source = _write_input_ebcdic(
#         src_dir / "occurs.dat",
#         [
#             _zoned(7, 2, signed=False)
#             + b"".join(_zoned(n, 3, signed=False) for n in (10, 20, 30))
#         ],
#     )
#
#     df = _read_ebcdic(tmp_path, source, copybook)
#     assert df.columns == ["ID", "SCORES_1", "SCORES_2", "SCORES_3"]
# assert df.to_dicts() == [{"ID": 7, "SCORES_1": 10, "SCORES_2": 20, "SCORES_3": 30}]

#
# def test_import_one_ebcdic_filler_occupies_bytes_without_becoming_a_column(
#     tmp_path: Path,
# ):
#     src_dir = _input_dir(tmp_path)
#     copybook = _copybook(
#         "01  PADDED.",
#         "    05  FIRST  PIC X(3).",
#         "    05  FILLER PIC X(4).",
#         "    05  SECOND PIC X(3).",
#     )
# source = _write_input_ebcdic(src_dir / "filler.dat", [_ebc("ABC" + "????" + "XYZ")])
#
#     df = _read_ebcdic(tmp_path, source, copybook)
#     # The gap is skipped, and the field after it is still at the right offset.
#     assert df.columns == ["FIRST", "SECOND"]
#     assert df.to_dicts() == [{"FIRST": "ABC", "SECOND": "XYZ"}]

#
# def test_import_one_ebcdic_redefines_reads_the_same_bytes_twice(tmp_path: Path):
#     src_dir = _input_dir(tmp_path)
#     copybook = _copybook(
#         "01  OVERLAID.",
#         "    05  AS-TEXT   PIC X(5).",
#         "    05  AS-NUMBER REDEFINES AS-TEXT PIC 9(5).",
#     )
#     source = _write_input_ebcdic(
#         src_dir / "redef.dat", [_zoned(12345, 5, signed=False)]
#     )
#
#     df = _read_ebcdic(tmp_path, source, copybook)
#     assert df.to_dicts() == [{"AS-TEXT": "12345", "AS-NUMBER": 12345}]

#
# def test_import_one_ebcdic_codepage_changes_only_the_text_fields(tmp_path: Path):
#     # 0x4A is a cent sign under cp037 and A-umlaut under cp273; the digits
#     # decode from raw nibbles either way.
#     src_dir = _input_dir(tmp_path)
#     copybook = _copybook(
#         "01  REC.",
#         "    05  MARK PIC X(1).",
#         "    05  NUM  PIC 9(3).",
#     )
#     source = _write_input_ebcdic(
#         src_dir / "page.dat", [bytes([0x4A]) + _zoned(123, 3, signed=False)]
#     )
#
#     for codepage, expected in [("cp037", "¢"), ("cp273", "Ä")]:
#         df = _read_ebcdic(tmp_path, source, copybook, codepage=codepage)
#         assert df.to_dicts() == [{"MARK": expected, "NUM": 123}], codepage

#
# def test_import_one_ebcdic_several_records_decode_in_order(tmp_path: Path):
#     src_dir = _input_dir(tmp_path)
#     copybook = _copybook("01  ROW.", "    05  N PIC 9(3).")
#     source = _write_input_ebcdic(
#         src_dir / "many.dat",
#         [_zoned(n, 3, signed=False) for n in range(100, 110)],
#     )
#
#     df = _read_ebcdic(tmp_path, source, copybook)
#     assert df["N"].to_list() == list(range(100, 110))

#
# def test_import_one_ebcdic_a_file_that_is_not_whole_records_is_refused(
#     tmp_path: Path,
# ):
#     # The single most informative failure this reader has: it means the wrong
#     # copybook, or a transfer made in text mode rather than binary.
#     src_dir = _input_dir(tmp_path)
#     copybook = _copybook("01  ROW.", "    05  N PIC 9(4).")
#     source = src_dir / "ragged.dat"
#     source.write_bytes(_zoned(1234, 4, signed=False) + b"\x00\x00")
#
#     with pytest.raises(Exception) as excinfo:
#         _read_ebcdic(tmp_path, source, copybook)
#     assert "records" in str(excinfo.value)

#
# def test_run_read_in_ebcdic_reads_each_path_against_its_own_copybook(
#     tmp_path: Path,
# ):
#     src_dir = _input_dir(tmp_path)
#     customers = _copybook("01  CUSTOMER.", "    05  NAME PIC X(5).")
#     orders = _copybook("01  ORDER-REC.", "    05  QTY PIC 9(3).")
#     _write_input_ebcdic(src_dir / "cust.dat", [_ebc("ALICE", 5)])
#     _write_input_ebcdic(src_dir / "ord.dat", [_zoned(250, 3, signed=False)])
#
#     out = _import_files(
#         tmp_path,
#         _Src(
#             paths=[str(src_dir / "cust.dat"), str(src_dir / "ord.dat")],
#             format=SrcFileFormat.EBCDIC,
#             format_cfg={SrcFileFormat.EBCDIC: {"copybook": [customers, orders]}},
#         ),
#     )
#
#     # Two paths, two layouts: the per-path copybook list is narrowed per slot.
#     assert len(out) == 2
#     assert pl.read_parquet(out[0][0].file).select("NAME").to_dicts() == [
#         {"NAME": "ALICE"}
#     ]
#     assert pl.read_parquet(out[1][0].file).select("QTY").to_dicts() == [{"QTY": 250}]
# ---------------------------------------------------------------------------
# find-copy purge
#
# Purge deletes the source files the lastmod cursor turned down as already
# consumed. A fresh cursor turns nothing down -- everything at or after the
# baseline is read -- so purge only has anything to do on a resumed run.
# ---------------------------------------------------------------------------


def _write_csv(path: Path, rows: list[int], when: datetime) -> Path:
    path.write_text("id\n" + "".join(f"{r}\n" for r in rows))
    stamp = when.timestamp()
    os.utime(path, (stamp, stamp))
    return path


def _find_copy(
    tmp_path: Path,
    glob: str,
    *,
    initial_lastmod: str | None = None,
    lastmod_info: str | None = None,
    purge: bool = False,
):
    out_dir = Path(tempfile.mkdtemp(prefix="out-", dir=tmp_path))
    handler = make_handler()
    return _input_module.import_one(
        handler=handler,
        source=handler.local_location(handler.local_uri(glob)),
        target_dir=out_dir,
        file_format=SrcFileFormat.CSV,
        paths=[glob],
        initial_lastmod=initial_lastmod,
        lastmod_info=lastmod_info,
        purge=purge,
    )


_BASELINE = datetime(2026, 1, 1, tzinfo=timezone.utc)
_BASELINE_ISO = _BASELINE.isoformat(timespec="microseconds")


def _seed(tmp_path: Path) -> tuple[str, Path, Path]:
    """Two files after the baseline, and the glob that matches them."""
    src_dir = _input_dir(tmp_path)
    a = _write_csv(src_dir / "a.csv", [1], _BASELINE + timedelta(hours=1))
    b = _write_csv(src_dir / "b.csv", [2], _BASELINE + timedelta(hours=2))
    return str(src_dir / "*.csv"), a, b


def test_find_copy_purge_deletes_what_a_prior_run_consumed(tmp_path: Path):
    glob, a, b = _seed(tmp_path)

    first = _find_copy(tmp_path, glob, initial_lastmod=_BASELINE_ISO)
    assert len(first.files) == 2
    assert first.purged is None, "the first run has nothing behind the cursor"
    assert a.exists() and b.exists()

    second = _find_copy(
        tmp_path, glob, lastmod_info=first.lastmod_info, purge=True
    )
    assert second.files == [], "both files were already consumed"
    names = sorted(Path(u.removeprefix("file://")).name for u in second.purged.deleted)
    assert names == ["a.csv", "b.csv"]
    assert second.purged.failed == []
    assert not a.exists() and not b.exists()


def test_find_copy_purge_leaves_the_file_it_is_reading(tmp_path: Path):
    glob, a, b = _seed(tmp_path)
    first = _find_copy(tmp_path, glob, initial_lastmod=_BASELINE_ISO)

    src_dir = a.parent
    fresh = _write_csv(src_dir / "c.csv", [3], _BASELINE + timedelta(hours=3))

    second = _find_copy(
        tmp_path, glob, lastmod_info=first.lastmod_info, purge=True
    )
    assert len(second.files) == 1, "only the new file is read"
    assert len(second.purged.deleted) == 2
    # The file this run consumed survives -- it goes on the next run's list.
    assert fresh.exists()
    assert not a.exists() and not b.exists()


def test_find_copy_purge_never_deletes_below_the_baseline(tmp_path: Path):
    glob, a, b = _seed(tmp_path)
    # Older than the baseline, so it is never read -- and so never purged.
    stale = _write_csv(a.parent / "stale.csv", [0], _BASELINE - timedelta(days=1))

    first = _find_copy(tmp_path, glob, initial_lastmod=_BASELINE_ISO)
    assert len(first.files) == 2, "the stale file is below the baseline"

    purged = _find_copy(
        tmp_path, glob, lastmod_info=first.lastmod_info, purge=True
    ).purged
    assert len(purged.deleted) == 2
    assert stale.exists(), "a file the baseline excluded was never ours to delete"
    assert not a.exists() and not b.exists()


def test_find_copy_without_purge_deletes_nothing(tmp_path: Path):
    glob, a, b = _seed(tmp_path)
    first = _find_copy(tmp_path, glob, initial_lastmod=_BASELINE_ISO)

    second = _find_copy(tmp_path, glob, lastmod_info=first.lastmod_info)
    assert second.purged is None, "purge was not requested"
    assert a.exists() and b.exists()
