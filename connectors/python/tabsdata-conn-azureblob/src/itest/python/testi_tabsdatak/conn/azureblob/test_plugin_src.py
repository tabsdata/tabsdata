#
# Copyright 2026 Tabsdata Inc.
#

"""AzureBlob source-plugin integration tests using fake context"""

from __future__ import annotations

import time
import uuid
import zipfile
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from xml.sax.saxutils import escape

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.azureblob import AzureblobSystem, azureblob_system
from tabsdatax.itest.fixture.functions.files import ObjectStore, PluginSrcCtx
from testi_tabsdatak.conn.azureblob.backend import run_prefix, src_conn, store

# from testi_tabsdatak.conn.common import ebcdic
from testi_tabsdatak.conn.common.plugin_ctx import LastModSrcCtx

# noinspection PyProtectedMember
from tabsdatak.conn.azureblob import AzureBlobSrc, _plugin as plugin
from tabsdatak.conn.common.file.typing_ import SrcFileFormat

# Backend-agnostic transporter behaviour (the CSV-option matrix, ragged lines,
# encodings, ...) is exercised exhaustively by the LocalFile suite; here only the
# object-store-meaningful paths are covered.
pytestmark = pytest.mark.azureblob

ROWS = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]

Writer = Callable[[pl.DataFrame, Path], None]


def _parquet(frame: pl.DataFrame, path: Path) -> None:
    frame.write_parquet(path, use_pyarrow=True)


# Hand-rolled rather than produced with a spreadsheet library: nothing in the
# dependency set writes workbooks, and only a handful of package parts are
# needed. Cells are inline strings and numbers, so there is no shared-strings
# table and no styles part.

_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PKG_REL = "http://schemas.openxmlformats.org/package/2006/relationships"
_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_TYPES = "http://schemas.openxmlformats.org/package/2006/content-types"
_OFFICE = "application/vnd.openxmlformats-officedocument.spreadsheetml"


def _xlsx_cell(ref: str, value) -> str:
    # An omitted cell is how a workbook spells "blank".
    if value is None:
        return ""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{ref}"><v>{value}</v></c>'
    return f'<c r="{ref}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>'


def _xlsx_sheet(rows: list[list]) -> str:
    body = "".join(
        '<row r="{n}">{cells}</row>'.format(
            n=n,
            cells="".join(
                _xlsx_cell(f"{chr(ord('A') + i)}{n}", value)
                for i, value in enumerate(row)
            ),
        )
        for n, row in enumerate(rows, 1)
    )
    return f'<worksheet xmlns="{_MAIN}"><sheetData>{body}</sheetData></worksheet>'


def _write_xlsx(path: Path, sheets: dict[str, list[list]]) -> None:
    """Write a workbook holding ``sheets``, in declaration order."""
    tags: list[str] = []
    rels: list[str] = []
    overrides = [
        f'<Override PartName="/xl/workbook.xml"'
        f' ContentType="{_OFFICE}.sheet.main+xml"/>'
    ]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for n, (name, rows) in enumerate(sheets.items(), 1):
            tags.append(f'<sheet name="{escape(name)}" sheetId="{n}" r:id="rId{n}"/>')
            rels.append(
                f'<Relationship Id="rId{n}" Type="{_REL}/worksheet"'
                f' Target="worksheets/sheet{n}.xml"/>'
            )
            overrides.append(
                f'<Override PartName="/xl/worksheets/sheet{n}.xml"'
                f' ContentType="{_OFFICE}.worksheet+xml"/>'
            )
            zf.writestr(f"xl/worksheets/sheet{n}.xml", _xlsx_sheet(rows))
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
            f"<sheets>{''.join(tags)}</sheets></workbook>",
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            f'<Relationships xmlns="{_PKG_REL}">{"".join(rels)}</Relationships>',
        )


def _xlsx(frame: pl.DataFrame, path: Path) -> None:
    """One-sheet workbook of ``frame`` -- the ``Writer`` shape ``_stage`` wants."""
    _write_xlsx(path, {"Data": [list(frame.columns), *(list(r) for r in frame.rows())]})


def _stage_workbook(
    objects: ObjectStore,
    prefix: str,
    name: str,
    tmp_path: Path,
    sheets: dict[str, list[list]],
) -> None:
    """The sibling of :func:`_stage` for the cases needing more than the one
    sheet a ``Writer`` can produce."""
    local = tmp_path / f"stage-{uuid.uuid4().hex}-{name.replace('/', '_')}"
    _write_xlsx(local, sheets)
    objects.upload(local, f"{prefix}/{name}")


def _stage(
    objects: ObjectStore,
    prefix: str,
    name: str,
    tmp_path: Path,
    rows: list[dict] = ROWS,
    writer: Writer = _parquet,
) -> None:
    """Write ``rows`` locally in the target format and put the raw bytes at
    ``<prefix>/<name>``, so the blob genuinely carries that format."""
    # `name` may be a nested object key; the local staging file stays flat.
    local = tmp_path / f"stage-{uuid.uuid4().hex}-{name.replace('/', '_')}"
    writer(pl.DataFrame(rows), local)
    objects.upload(local, f"{prefix}/{name}")


def _read_back(system: AzureblobSystem, prefix: str, src: AzureBlobSrc, tmp_path: Path):
    work = tmp_path / f"work-src-{uuid.uuid4().hex}"
    work.mkdir()
    return plugin.AzureBlobSrcPlugin().read_in(
        PluginSrcCtx(work), src_conn(system, prefix), src
    )


@azureblob_system("azureblob")
def test_src_wildcard_in_basename_matches_multiple_objects(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    objects = store(system_azureblob)
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    for name in ("a.parquet", "b.parquet", "c.parquet"):
        _stage(objects, prefix, name, tmp_path, rows=[{"name": name}])
    # A non-matching neighbour must not show up.
    _stage(objects, prefix, "skip.csv", tmp_path, rows=[{"name": "skip"}])

    out = _read_back(
        system_azureblob, prefix, AzureBlobSrc(paths=["*.parquet"]), tmp_path
    )
    assert len(out) == 1
    assert len(out[0]) >= 3
    names = sorted(pl.read_parquet(f.file).select("name").item(0, 0) for f in out[0])
    assert names == ["a.parquet", "b.parquet", "c.parquet"]


@azureblob_system("azureblob")
def test_src_wildcard_with_no_matches_returns_empty_slot(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage(store(system_azureblob), prefix, "only.csv", tmp_path, rows=[{"name": "x"}])

    out = _read_back(
        system_azureblob, prefix, AzureBlobSrc(paths=["*.parquet"]), tmp_path
    )
    assert len(out) == 1
    assert out[0] == []


@azureblob_system("azureblob")
def test_src_initial_last_modified_filters_out_older_objects(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    # Blob timestamps are server-side, so the two uploads are deliberately spaced
    # and the cut-off taken between them.
    objects = store(system_azureblob)
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage(objects, prefix, "old.parquet", tmp_path, rows=[{"name": "old"}])
    time.sleep(2)
    cutoff = datetime.now(timezone.utc)
    time.sleep(2)
    _stage(objects, prefix, "new.parquet", tmp_path, rows=[{"name": "new"}])

    out = _read_back(
        system_azureblob,
        prefix,
        AzureBlobSrc(paths=["*.parquet"], initial_last_modified=cutoff),
        tmp_path,
    )
    names = sorted(pl.read_parquet(f.file).select("name").item(0, 0) for f in out[0])
    assert names == ["new"]


@azureblob_system("azureblob")
@pytest.mark.parametrize(
    "name, writer, fmt",
    [
        ("users.csv", lambda df, p: df.write_csv(p), SrcFileFormat.CSV),
        ("users.json", lambda df, p: df.write_ndjson(p), SrcFileFormat.JSON),
        ("users.avro", lambda df, p: df.write_avro(p), SrcFileFormat.AVRO),
        ("users.parquet", _parquet, SrcFileFormat.PARQUET),
        ("users.xlsx", _xlsx, SrcFileFormat.EXCEL),
    ],
)
def test_src_reads_each_format(
    system_azureblob: AzureblobSystem,
    tmp_path: Path,
    name: str,
    writer: Writer,
    fmt: SrcFileFormat,
):
    # The transporter always emits parquet, so every format is asserted on the
    # parquet it produced.
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage(store(system_azureblob), prefix, name, tmp_path, writer=writer)
    out = _read_back(
        system_azureblob, prefix, AzureBlobSrc(paths=[name], format=fmt), tmp_path
    )
    df = pl.read_parquet(out[0][0].file).select("id", "name").sort("id")
    assert df.to_dicts() == ROWS


# A baseline safely older than anything this run uploads: object timestamps are
# server-assigned, so the cursor tests anchor on "an hour ago" rather than trying
# to place a cut-off between two uploads.
def _baseline() -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=1)


def _names(slot) -> list[str]:
    return sorted(pl.read_parquet(f.file).select("name").item(0, 0) for f in slot)


def _read_with(ctx, system: AzureblobSystem, prefix: str, src: AzureBlobSrc):
    return plugin.AzureBlobSrcPlugin().read_in(ctx, src_conn(system, prefix), src)


def _work(tmp_path: Path, name: str) -> Path:
    out = tmp_path / f"work-{name}"
    out.mkdir()
    return out


@azureblob_system("azureblob")
def test_src_one_slot_per_path_in_path_order(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    objects = store(system_azureblob)
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    for name in ("in/a1.parquet", "in/a2.parquet", "out/b1.parquet", "single.parquet"):
        _stage(objects, prefix, name, tmp_path, rows=[{"name": name}])

    src = AzureBlobSrc(paths=["out/*.parquet", "in/*.parquet", "single.parquet"])
    out = _read_back(system_azureblob, prefix, src, tmp_path)

    assert [_names(slot) for slot in out] == [
        ["out/b1.parquet"],
        ["in/a1.parquet", "in/a2.parquet"],
        ["single.parquet"],
    ]


@azureblob_system("azureblob")
def test_src_overlapping_globs_import_the_same_object_in_every_slot(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    objects = store(system_azureblob)
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage(objects, prefix, "shared.parquet", tmp_path, rows=[{"name": "shared"}])

    src = AzureBlobSrc(
        paths=["*.parquet", "sh*.parquet"], initial_last_modified=_baseline()
    )
    ctx = LastModSrcCtx(_work(tmp_path, "first"))

    out = _read_with(ctx, system_azureblob, prefix, src)

    # Each slot keeps its own cursor, so an object both patterns match is imported
    # once per slot rather than deduplicated across them.
    assert [_names(slot) for slot in out] == [["shared"], ["shared"]]
    assert sorted(ctx.attrs) == ["file-lastmod.slot0", "file-lastmod.slot1"]

    resumed = LastModSrcCtx(_work(tmp_path, "resumed"), ctx.attrs)
    assert _read_with(resumed, system_azureblob, prefix, src) == [[], []]


@azureblob_system("azureblob")
def test_src_second_run_resumes_from_the_stored_cursor(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    objects = store(system_azureblob)
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage(objects, prefix, "first.parquet", tmp_path, rows=[{"name": "first"}])

    src = AzureBlobSrc(paths=["*.parquet"], initial_last_modified=_baseline())
    ctx = LastModSrcCtx(_work(tmp_path, "run1"))

    assert _names(_read_with(ctx, system_azureblob, prefix, src)[0]) == ["first"]
    assert sorted(ctx.attrs) == ["file-lastmod.slot0"]

    time.sleep(2)
    _stage(objects, prefix, "second.parquet", tmp_path, rows=[{"name": "second"}])
    resumed = LastModSrcCtx(_work(tmp_path, "run2"), ctx.attrs)

    # The cursor, not the baseline, governs once one is stored: the object the
    # first run consumed stays consumed.
    assert _names(_read_with(resumed, system_azureblob, prefix, src)[0]) == ["second"]


@azureblob_system("azureblob")
def test_src_without_initial_last_modified_keeps_no_cursor(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    # A Src with no baseline is not incremental at all: nothing is stored on the
    # context, so every run re-imports whatever the glob matches.
    objects = store(system_azureblob)
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage(objects, prefix, "only.parquet", tmp_path, rows=[{"name": "only"}])

    src = AzureBlobSrc(paths=["*.parquet"])
    ctx = LastModSrcCtx(_work(tmp_path, "run1"))

    assert _names(_read_with(ctx, system_azureblob, prefix, src)[0]) == ["only"]
    assert ctx.attrs == {}

    again = LastModSrcCtx(_work(tmp_path, "run2"), ctx.attrs)
    assert _names(_read_with(again, system_azureblob, prefix, src)[0]) == ["only"]


@azureblob_system("azureblob")
def test_src_reordering_paths_reimports_because_slots_are_positional(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    objects = store(system_azureblob)
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage(objects, prefix, "in/a.parquet", tmp_path, rows=[{"name": "a"}])
    _stage(objects, prefix, "out/b.parquet", tmp_path, rows=[{"name": "b"}])

    # Distinct basename globs on purpose: a cursor entry is keyed by the basename
    # pattern alone (the transporter splits the path and keys on the part after the
    # last `/`), so two directory globs that share `*.parquet` would share the entry
    # and the swap below would turn on which object is newer.
    baseline = _baseline()
    ordered = AzureBlobSrc(
        paths=["in/a*.parquet", "out/b*.parquet"], initial_last_modified=baseline
    )
    ctx = LastModSrcCtx(_work(tmp_path, "run1"))

    assert [
        _names(slot) for slot in _read_with(ctx, system_azureblob, prefix, ordered)
    ] == [
        ["a"],
        ["b"],
    ]

    unchanged = LastModSrcCtx(_work(tmp_path, "run2"), ctx.attrs)
    assert _read_with(unchanged, system_azureblob, prefix, ordered) == [[], []]

    # A cursor is addressed by the path's position, not by the path itself, so
    # swapping two entries hands each slot the other one's cursor -- which knows
    # nothing about this slot's pattern -- and both objects are imported again.
    swapped = AzureBlobSrc(
        paths=["out/b*.parquet", "in/a*.parquet"], initial_last_modified=baseline
    )
    moved = LastModSrcCtx(_work(tmp_path, "run3"), ctx.attrs)
    assert [
        _names(slot) for slot in _read_with(moved, system_azureblob, prefix, swapped)
    ] == [
        ["b"],
        ["a"],
    ]


# Worksheet selection is a source-path concern, so it is covered here rather
# than left to the LocalFile suite: the `#<n>` fragment has to survive the
# object-store path handling (`build_location`, AUTO inference, the object key
# the transporter reports back) with nothing downstream ever seeing it.

_SHEETS = {
    "First": [["tag"], ["from-sheet-1"]],
    "Second": [["tag"], ["from-sheet-2"]],
}


@azureblob_system("azureblob")
def test_src_excel_sheet_fragment_selects_the_worksheet(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage_workbook(store(system_azureblob), prefix, "book.xlsx", tmp_path, _SHEETS)

    # No `format`: AUTO has to infer EXCEL from the extension once the
    # fragment has been split off the path.
    out = _read_back(
        system_azureblob, prefix, AzureBlobSrc(paths=["book.xlsx#2"]), tmp_path
    )

    assert len(out) == 1
    assert len(out[0]) == 1
    assert pl.read_parquet(out[0][0].file).select("tag").item(0, 0) == "from-sheet-2"


@azureblob_system("azureblob")
def test_src_excel_two_paths_name_two_sheets_of_one_workbook(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage_workbook(store(system_azureblob), prefix, "book.xlsx", tmp_path, _SHEETS)

    src = AzureBlobSrc(paths=["book.xlsx#2", "book.xlsx"], format=SrcFileFormat.EXCEL)
    out = _read_back(system_azureblob, prefix, src, tmp_path)

    # One worksheet list covers both slots: slot 0 named sheet 2, slot 1 named
    # none and reads the first sheet rather than inheriting its neighbour's.
    assert [pl.read_parquet(slot[0].file).select("tag").item(0, 0) for slot in out] == [
        "from-sheet-2",
        "from-sheet-1",
    ]


@azureblob_system("azureblob")
def test_src_excel_has_header_false_generates_column_names(
    system_azureblob: AzureblobSystem, tmp_path: Path
):
    prefix = run_prefix(system_azureblob.sys_path, "td-integ")
    _stage_workbook(
        store(system_azureblob),
        prefix,
        "flat.xlsx",
        tmp_path,
        {"Data": [["id", "name"], ["a", "alice"], ["b", "bob"]]},
    )

    src = AzureBlobSrc(
        paths=["flat.xlsx"],
        format=SrcFileFormat.EXCEL,
        format_cfg={SrcFileFormat.EXCEL: {"has_header": False}},
    )
    out = _read_back(system_azureblob, prefix, src, tmp_path)

    # With no header row the columns are named positionally and row 1 is data.
    df = pl.read_parquet(out[0][0].file).select("column_1", "column_2")
    assert df.to_dicts() == [
        {"column_1": "id", "column_2": "name"},
        {"column_1": "a", "column_2": "alice"},
        {"column_1": "b", "column_2": "bob"},
    ]


# Fixed-length EBCDIC records. The deep type matrix lives in the
# conn-common-file transporter suite; what matters here is that the bytes
# survive the object store untouched -- a record carries no delimiters, so a
# single byte gained or lost shifts every field after it.


#
# def _stage_records(
#     objects: ObjectStore, prefix: str, name: str, tmp_path: Path, records: list[bytes]
# ) -> None:
#     local = tmp_path / f"stage-{uuid.uuid4().hex}-{name.replace('/', '_')}"
#     ebcdic.write(local, records)
#     objects.upload(local, f"{prefix}/{name}")


#
# @azureblob_system("azureblob")
# def test_src_ebcdic_reads_a_fixed_length_record(
#     system_azureblob: AzureblobSystem, tmp_path: Path
# ):
#     prefix = run_prefix(system_azureblob.sys_path, "td-integ")
#     _stage_records(
#         store(system_azureblob),
#         prefix,
#         "cust.dat",
#         tmp_path,
#         [ebcdic.customer("ALICE", 42), ebcdic.customer("BOB", 7)],
#     )
#
#     src = AzureBlobSrc(
#         paths=["cust.dat"],
#         format=SrcFileFormat.EBCDIC,
#         format_cfg={SrcFileFormat.EBCDIC: {"copybook": [ebcdic.CUSTOMER]}},
#     )
#     out = _read_back(system_azureblob, prefix, src, tmp_path)
#
#     assert pl.read_parquet(out[0][0].file).select("NAME", "COUNT").to_dicts() == [
#         {"NAME": "ALICE", "COUNT": 42},
#         {"NAME": "BOB", "COUNT": 7},
#     ]


#
# @azureblob_system("azureblob")
# def test_src_ebcdic_bytes_are_not_mangled_in_transit(
#     system_azureblob: AzureblobSystem, tmp_path: Path
# ):
#     # Packed and binary fields are arbitrary bytes, including 0x00 and 0x1A.
#     # Anything that treated the object as text would corrupt them silently.
#     prefix = run_prefix(system_azureblob.sys_path, "td-integ")
#     layout = ebcdic.copybook(
#         "01  RAW.",
#         "    05  PACK PIC S9(5) COMP-3.",
#         "    05  BIN  PIC S9(9) COMP.",
#         "    05  RATE COMP-1.",
#     )
#     _stage_records(
#         store(system_azureblob),
#         prefix,
#         "raw.dat",
#         tmp_path,
#         [ebcdic.packed(-26, 5) + ebcdic.binary(0, 9) + ebcdic.hfp(1.0, 4)],
#     )
#
#     src = AzureBlobSrc(
#         paths=["raw.dat"],
#         format=SrcFileFormat.EBCDIC,
#         format_cfg={SrcFileFormat.EBCDIC: {"copybook": [layout]}},
#     )
#     out = _read_back(system_azureblob, prefix, src, tmp_path)
#
# assert pl.read_parquet(out[0][0].file).select("PACK", "BIN", "RATE").to_dicts() == [
#         {"PACK": -26, "BIN": 0, "RATE": 1.0}
#     ]


#
# @azureblob_system("azureblob")
# def test_src_ebcdic_reads_each_path_against_its_own_copybook(
#     system_azureblob: AzureblobSystem, tmp_path: Path
# ):
#     prefix = run_prefix(system_azureblob.sys_path, "td-integ")
#     objects = store(system_azureblob)
#     orders = ebcdic.copybook("01  ORDER-REC.", "    05  QTY PIC 9(3).")
#     _stage_records(objects, prefix, "cust.dat", tmp_path, [ebcdic.customer("ZOE", 1)])
#     _stage_records(
#         objects, prefix, "ord.dat", tmp_path, [ebcdic.zoned(250, 3, signed=False)]
#     )
#
#     src = AzureBlobSrc(
#         paths=["cust.dat", "ord.dat"],
#         format=SrcFileFormat.EBCDIC,
#         format_cfg={SrcFileFormat.EBCDIC: {"copybook": [ebcdic.CUSTOMER, orders]}},
#     )
#     out = _read_back(system_azureblob, prefix, src, tmp_path)
#
#     # Two objects, two layouts: the per-path copybook list is narrowed per slot.
#     assert len(out) == 2
#     assert pl.read_parquet(out[0][0].file).select("NAME").item(0, 0) == "ZOE"
#     assert pl.read_parquet(out[1][0].file).select("QTY").item(0, 0) == 250
