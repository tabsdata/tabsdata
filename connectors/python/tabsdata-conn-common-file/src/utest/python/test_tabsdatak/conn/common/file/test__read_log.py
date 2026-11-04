#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._log import Log
from test_tabsdatak.conn.common.file.landed import Source, land

pytestmark = [pytest.mark.unit]


@pytest.mark.parametrize("block_size", [1, 4, 1 << 20], ids=["byte", "small", "one"])
def test_every_line_is_a_row_named_after_its_file(tmp_path, block_size):
    content = b"one\r\n\ntwo\nthree"
    landed = land(Log("app.log", block_size=block_size).read(Source(content)), tmp_path)
    assert landed.schema is None
    assert landed.served.to_dict(as_series=False) == {
        "file": ["app.log"] * 4,
        "message": ["one", "", "two", "three"],
    }


def test_an_empty_file_lands_no_rows(tmp_path):
    landed = land(Log("app.log").read(Source(b"")), tmp_path)
    assert landed.data.columns == ["file", "message"]
    assert landed.data.height == 0
