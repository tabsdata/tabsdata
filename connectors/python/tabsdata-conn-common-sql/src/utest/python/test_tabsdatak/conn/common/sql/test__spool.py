#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._spool import spooled_parquet

pytestmark = pytest.mark.sql_common


def test_the_frame_is_written_under_the_given_name():
    frame = pl.LazyFrame({"id": [1, 2]})
    with spooled_parquet(frame, "batch.parquet") as path:
        assert path.name == "batch.parquet"
        assert pl.read_parquet(path).equals(frame.collect())


def test_the_file_is_removed_on_the_way_out():
    with spooled_parquet(pl.LazyFrame({"id": [1]}), "batch.parquet") as path:
        assert path.is_file()
    assert not path.exists()


def test_the_file_is_removed_when_the_body_fails():
    with pytest.raises(RuntimeError):
        with spooled_parquet(pl.LazyFrame({"id": [1]}), "batch.parquet") as path:
            raise RuntimeError("upload failed")
    assert not path.exists()
