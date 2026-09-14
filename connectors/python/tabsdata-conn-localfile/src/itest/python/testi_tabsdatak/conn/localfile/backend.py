#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and local scratch trees"""

from __future__ import annotations

import os
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from xml.sax.saxutils import escape

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain
from tabsdatax.itest.fixture.monikers import moniker

from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.common._transporter.handler.handler2 import TRANSPORTER_BINARY_NAME
from tabsdatak.conn.localfile import LocalFileDestConn, LocalFileSrcConn

# The plugins shell out to this binary; resolving it at import time fails loudly
# on an unhydrated environment instead of mid-test.
TRANSPORTER_BINARY = PythonEnvironment().binary(TRANSPORTER_BINARY_NAME)


def src_conn(base_path: Path) -> LocalFileSrcConn:
    return LocalFileSrcConn(base_path=_absolute(base_path))


def dest_conn(base_path: Path) -> LocalFileDestConn:
    return LocalFileDestConn(base_path=_absolute(base_path))


def scratch(
    domain: FsDomain, request: pytest.FixtureRequest
) -> tuple[Path, Path, Path]:
    """A per-test ``(root, base, work)`` trio under the domain's own directory:
    ``base`` is what a connection is rooted at, ``work`` the plugin's work dir and
    ``root`` holds anything that must stay outside the connection."""
    root = domain.base_path / moniker(request)
    base = root / "store"
    work = root / "work"
    base.mkdir(parents=True)
    work.mkdir(parents=True)
    return root, base, work


def stage_parquet(base: Path, name: str, when: datetime | None = None) -> Path:
    """A one-row parquet at ``base/name`` whose single ``name`` column repeats that
    relative name, so a test can tell which file an imported frame came from.
    ``when``, if given, becomes the file's last-modified time -- what the
    incremental tests measure against ``initial_last_modified``."""
    path = base / name
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{"name": name}]).write_parquet(path, use_pyarrow=True)
    if when is not None:
        stamp = when.astimezone(timezone.utc).timestamp()
        os.utime(path, (stamp, stamp))
    return path


# Hand-rolled rather than produced with a spreadsheet library: nothing in the
# dependency set writes workbooks, and only a handful of package parts are
# needed. Cells are inline strings and numbers, so there is no shared-strings
# table and no styles part -- and therefore no date-formatted cells, which the
# conn-common-file transporter suite covers instead.

_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PKG_REL = "http://schemas.openxmlformats.org/package/2006/relationships"
_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_TYPES = "http://schemas.openxmlformats.org/package/2006/content-types"
_OFFICE = "application/vnd.openxmlformats-officedocument.spreadsheetml"


def stage_xlsx(base: Path, name: str, sheets: dict[str, list[list]]) -> Path:
    """A workbook at ``base/name`` holding ``sheets`` in tab order -- the Excel
    sibling of :func:`stage_parquet`.

    Each sheet is a list of rows of cell values: ``str`` becomes an inline
    string, ``int`` / ``float`` a number, and ``None`` a blank (the cell is
    omitted, which is how a workbook spells it)."""
    path = base / name
    path.parent.mkdir(parents=True, exist_ok=True)
    return _write_xlsx(path, sheets)


def _xlsx_cell(ref: str, value) -> str:
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


def _write_xlsx(path: Path, sheets: dict[str, list[list]]) -> Path:
    tags: list[str] = []
    rels: list[str] = []
    overrides = [
        f'<Override PartName="/xl/workbook.xml"'
        f' ContentType="{_OFFICE}.sheet.main+xml"/>'
    ]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for n, (sheet, rows) in enumerate(sheets.items(), 1):
            tags.append(f'<sheet name="{escape(sheet)}" sheetId="{n}" r:id="rId{n}"/>')
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
    return path


def _absolute(base_path: Path) -> str:
    text = Path(base_path).as_posix()
    return text if re.match(r"[A-Za-z]:/", text) else "/" + text.strip("/")
