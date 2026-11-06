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

# from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from tabsdatak.common._interpreter.python.error import BinaryNotFoundError
from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.common._transporter.handler.handler2 import TRANSPORTER_BINARY_NAME

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
# Helpers
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
