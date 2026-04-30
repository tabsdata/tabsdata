#
# Copyright 2026 Tabs Data Inc.
#

import pytest

from tabsdata._tableinfo import TableInfo, TableKind, TypedTable
from tabsdata.expansions.foundation.symbols import TableType


class TestTableKind:
    def test_regular_kind(self):
        assert TableKind.REGULAR.type == TableType.Regular
        assert TableKind.REGULAR.suffix is None
        assert TableKind.REGULAR.description == "Regular data table"

    def test_dq_enrich_kind(self):
        assert TableKind.DQ_ENRICH.type == TableType.DQData
        assert TableKind.DQ_ENRICH.suffix == "_dq_enrich"

    def test_dq_filter_kind(self):
        assert TableKind.DQ_FILTER.type == TableType.DQData
        assert TableKind.DQ_FILTER.suffix == "_dq_filter"

    def test_dq_select_kind(self):
        assert TableKind.DQ_SELECT.type == TableType.DQData
        assert TableKind.DQ_SELECT.suffix == "_dq_select"

    def test_dq_summary_kind(self):
        assert TableKind.DQ_SUMMARY.type == TableType.DQSummary
        assert TableKind.DQ_SUMMARY.suffix == "_dq_summary"

    def test_all_kinds_have_description(self):
        for kind in TableKind:
            assert isinstance(kind.description, str)
            assert len(kind.description) > 0

    def test_code_property(self):
        for kind in TableKind:
            assert kind.code is not None


class TestTableInfo:
    def test_basic_construction(self):
        info = TableInfo(
            from_table="src_table",
            from_collection="src_coll",
            kind=TableKind.REGULAR,
            table="dst_table",
            collection="dst_coll",
            position=0,
        )
        assert info.from_table == "src_table"
        assert info.from_collection == "src_coll"
        assert info.kind == TableKind.REGULAR
        assert info.table == "dst_table"
        assert info.collection == "dst_coll"
        assert info.position == 0

    def test_empty_from_table_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            TableInfo("", "coll", TableKind.REGULAR, "t", "c", 0)

    def test_whitespace_from_table_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            TableInfo("   ", "coll", TableKind.REGULAR, "t", "c", 0)

    def test_non_string_from_table_raises(self):
        with pytest.raises(ValueError, match="must be an string"):
            TableInfo(123, "coll", TableKind.REGULAR, "t", "c", 0)

    def test_empty_collection_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            TableInfo("t", "coll", TableKind.REGULAR, "t", "", 0)

    def test_none_collection_raises(self):
        with pytest.raises(ValueError, match="cannot be None"):
            TableInfo("t", "coll", TableKind.REGULAR, "t", None, 0)

    def test_none_kind_raises(self):
        with pytest.raises(ValueError, match="cannot be None"):
            TableInfo("t", "coll", None, "t", "c", 0)

    def test_invalid_kind_raises(self):
        with pytest.raises(ValueError, match="must be an TableKind"):
            TableInfo("t", "coll", "not_a_kind", "t", "c", 0)

    def test_negative_position_raises(self):
        with pytest.raises(ValueError, match="cannot be negative"):
            TableInfo("t", "coll", TableKind.REGULAR, "t", "c", -1)

    def test_non_int_position_raises(self):
        with pytest.raises(ValueError, match="must be an integer"):
            TableInfo("t", "coll", TableKind.REGULAR, "t", "c", "zero")

    def test_repr(self):
        info = TableInfo("src", "src_c", TableKind.REGULAR, "dst", "dst_c", 0)
        r = repr(info)
        assert "TableInfo" in r
        assert "src" in r
        assert "dst" in r

    def test_strips_whitespace(self):
        info = TableInfo("  src  ", "  coll  ", TableKind.REGULAR, "  t  ", "  c  ", 0)
        assert info.from_table == "src"
        assert info.from_collection == "coll"
        assert info.table == "t"
        assert info.collection == "c"


class TestTypedTable:
    def test_basic_construction(self):
        tt = TypedTable("my_table", TableType.Regular)
        assert tt.table_name == "my_table"
        assert tt.table_type == TableType.Regular

    def test_repr(self):
        tt = TypedTable("my_table", TableType.DQSummary)
        r = repr(tt)
        assert "my_table" in r
