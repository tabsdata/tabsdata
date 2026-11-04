#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._blocks import blocks

pytestmark = [pytest.mark.unit]


def cut(content: bytes, size: int, **options) -> list[bytes]:
    return list(blocks(io.BytesIO(content), size=size, **options))


@pytest.mark.parametrize("block_size", [1, 3, 7, 1 << 20])
def test_blocks_hold_whole_lines_and_lose_nothing(block_size):
    content = b"a\nbb\nccc\nlast"
    pieces = cut(content, block_size)
    assert b"".join(pieces) == content
    assert all(piece.endswith(b"\n") for piece in pieces[:-1])


@pytest.mark.parametrize("block_size", [1, 2, 5, 1 << 20])
def test_a_newline_inside_quotes_never_ends_a_block(block_size):
    content = b'a,"x\ny"\nb,"z""\n"\nc,w\n'
    pieces = cut(content, block_size, quote=b'"')
    assert b"".join(pieces) == content
    assert all(piece.count(b'"') % 2 == 0 for piece in pieces)


def test_a_block_ends_on_the_end_of_line_given():
    assert cut(b"a|b|c", 1, eol=b"|") == [b"a|", b"b|", b"c"]


def test_an_empty_stream_has_no_blocks():
    assert cut(b"", 4) == []
