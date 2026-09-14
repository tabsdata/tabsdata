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

import tempfile
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
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
