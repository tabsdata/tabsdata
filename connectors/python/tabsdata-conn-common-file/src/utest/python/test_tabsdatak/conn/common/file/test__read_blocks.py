#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io

import pytest

from tabsdata.expansions.foundation.readers import CsvCutter  # noqa: TID251

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._blocks import blocks, records

pytestmark = [pytest.mark.unit]


def cut(content: bytes, size: int, **options) -> list[bytes]:
    return list(blocks(io.BytesIO(content), size=size, **options))


@pytest.mark.parametrize("block_size", [1, 3, 7, 1 << 20])
def test_blocks_hold_whole_lines_and_lose_nothing(block_size):
    content = b"a\nbb\nccc\nlast"
    pieces = cut(content, block_size)
    assert b"".join(pieces) == content
    assert all(piece.endswith(b"\n") for piece in pieces[:-1])


def records_of(content: bytes, size: int, **dialect) -> list[tuple[bytes, int]]:
    cutter = CsvCutter(**{
        "separator": ",",
        "quote": '"',
        "terminator": "\n",
        **dialect,
    })
    return list(records(io.BytesIO(content), cutter, size=size))


@pytest.mark.parametrize("block_size", [1, 2, 5, 1 << 20])
def test_a_newline_inside_quotes_never_ends_a_record_block(block_size):
    content = b'a,"x\ny"\nb,"z""\n"\nc,w\n'
    pieces = records_of(content, block_size)
    assert b"".join(piece for piece, _ in pieces) == content
    assert all(piece.count(b'"') % 2 == 0 for piece, _ in pieces)


@pytest.mark.parametrize("block_size", [1, 3, 1 << 20])
def test_a_quote_inside_an_unquoted_field_does_not_hold_a_record_open(block_size):
    content = b'a,b\n5" pipe,10\nc,d\n'
    pieces = records_of(content, block_size)
    assert [piece for piece, _ in pieces][-1].endswith(b"c,d\n")
    if block_size == 1 << 20:
        assert pieces == [(content, 2)]


def test_a_comment_line_holding_a_quote_is_skipped():
    content = b"a,b\n# it's \"odd\nc,d\n"
    assert records_of(content, 1 << 20, comment="#") == [(content, 2)]


def test_each_record_block_reports_its_widest_record():
    pieces = records_of(b"a,b\nc,d,e\n", 1 << 20)
    assert pieces == [(b"a,b\nc,d,e\n", 3)]


def test_a_block_ends_on_the_end_of_line_given():
    assert cut(b"a|b|c", 1, eol=b"|") == [b"a|", b"b|", b"c"]


def test_an_empty_stream_has_no_blocks():
    assert cut(b"", 4) == []
