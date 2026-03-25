#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import pytest
import sqlalchemy as sa

from tabsdata.connector.cdc.db2._matching import (
    CD_SYSTEM_COLUMN_NAMES,
    match_cd_columns,
)
from tabsdata.exceptions import Db2CdcError, ErrorCode


def _cols(*names_and_types: tuple[str, sa.types.TypeEngine]) -> list[tuple[str, sa.types.TypeEngine]]:
    return list(names_and_types)


def _int_cols(*names: str) -> list[tuple[str, sa.types.TypeEngine]]:
    return [(name, sa.Integer()) for name in names]


class TestNoPrefixError:

    def test_none_prefix(self):
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(_int_cols("a", "_a"), None)
        assert exc_info.value.code == ErrorCode.CDCDB26.code()

    def test_empty_string_prefix(self):
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(_int_cols("a", "_a"), "")
        assert exc_info.value.code == ErrorCode.CDCDB26.code()


class TestSimpleMatching:

    def test_single_pair(self):
        cd = _int_cols("a", "_a")
        result = match_cd_columns(cd, "_")
        assert [name for name, _ in result.after_columns_list] == ["a"]
        assert result.before_columns_map == {"a": "_a"}

    def test_multiple_pairs(self):
        cd = _int_cols("x", "_x", "y", "_y", "z", "_z")
        result = match_cd_columns(cd, "_")
        after_names = [name for name, _ in result.after_columns_list]
        assert set(after_names) == {"x", "y", "z"}
        assert result.before_columns_map == {"x": "_x", "y": "_y", "z": "_z"}

    def test_preserves_original_column_order(self):
        cd = _int_cols("c", "_c", "a", "_a", "b", "_b")
        result = match_cd_columns(cd, "_")
        after_names = [name for name, _ in result.after_columns_list]
        assert after_names == ["c", "a", "b"]

    def test_preserves_sa_types(self):
        cd = _cols(
            ("id", sa.Integer()),
            ("_id", sa.Integer()),
            ("name", sa.String(50)),
            ("_name", sa.String(50)),
        )
        result = match_cd_columns(cd, "_")
        types_by_name = {name: t for name, t in result.after_columns_list}
        assert isinstance(types_by_name["id"], sa.Integer)
        assert isinstance(types_by_name["name"], sa.String)


class TestSystemColumnFiltering:

    def test_system_columns_ignored(self):
        cd = _int_cols(
            "IBMSNAP_OPERATION", "IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ",
            "a", "_a",
        )
        result = match_cd_columns(cd, "_")
        after_names = [name for name, _ in result.after_columns_list]
        assert after_names == ["a"]
        for sys_col in CD_SYSTEM_COLUMN_NAMES:
            assert sys_col not in after_names

    def test_system_columns_case_insensitive(self):
        cd = _int_cols(
            "ibmsnap_operation", "Ibmsnap_Commitseq", "IBMSNAP_INTENTSEQ",
            "a", "_a",
        )
        result = match_cd_columns(cd, "_")
        after_names = [name for name, _ in result.after_columns_list]
        assert after_names == ["a"]

    def test_prefixed_system_column_name_is_unmatched(self):
        cd = _int_cols(
            "IBMSNAP_OPERATION",
            "_IBMSNAP_OPERATION",
            "val", "_val",
        )
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(cd, "_")
        assert exc_info.value.code == ErrorCode.CDCDB25.code()
        assert "_IBMSNAP_OPERATION" in str(exc_info.value)


class TestMultiCharPrefix:

    def test_prefix_old_(self):
        cd = _int_cols("col1", "old_col1", "col2", "old_col2")
        result = match_cd_columns(cd, "old_")
        assert set(name for name, _ in result.after_columns_list) == {"col1", "col2"}
        assert result.before_columns_map == {"col1": "old_col1", "col2": "old_col2"}

    def test_prefix_bf_(self):
        cd = _int_cols("AMOUNT", "BF_AMOUNT", "STATUS", "BF_STATUS")
        result = match_cd_columns(cd, "BF_")
        assert set(name for name, _ in result.after_columns_list) == {"AMOUNT", "STATUS"}
        assert result.before_columns_map == {
            "AMOUNT": "BF_AMOUNT",
            "STATUS": "BF_STATUS",
        }


class TestPrefixAmbiguity:

    def test_prefix_x_with_column_named_x(self):
        cd = _int_cols("x", "xx")
        result = match_cd_columns(cd, "x")
        assert [name for name, _ in result.after_columns_list] == ["x"]
        assert result.before_columns_map == {"x": "xx"}

    def test_prefix_ab_with_columns_ab_and_abab(self):
        cd = _int_cols("ab", "abab")
        result = match_cd_columns(cd, "ab")
        assert [name for name, _ in result.after_columns_list] == ["ab"]
        assert result.before_columns_map == {"ab": "abab"}

    def test_triple_prefix_nesting(self):
        cd = _int_cols("a", "_a", "__a")
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(cd, "_")
        assert exc_info.value.code == ErrorCode.CDCDB25.code()

    def test_double_prefix_both_paired(self):
        cd = _int_cols("a", "_a", "__a", "___a")
        result = match_cd_columns(cd, "_")
        after_names = set(name for name, _ in result.after_columns_list)
        assert after_names == {"a", "__a"}
        assert result.before_columns_map == {"a": "_a", "__a": "___a"}


class TestUnmatchedBefore:

    def test_prefixed_column_without_after_counterpart(self):
        cd = _int_cols("a", "_a", "_orphan")
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(cd, "_")
        assert exc_info.value.code == ErrorCode.CDCDB25.code()
        assert "_orphan" in str(exc_info.value)

    def test_multiple_unmatched_before(self):
        cd = _int_cols("a", "_a", "_b", "_c")
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(cd, "_")
        assert exc_info.value.code == ErrorCode.CDCDB25.code()
        assert "_b" in str(exc_info.value)
        assert "_c" in str(exc_info.value)

    def test_all_prefixed_no_after_columns(self):
        cd = _int_cols("_x", "_y")
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(cd, "_")
        assert exc_info.value.code == ErrorCode.CDCDB25.code()


class TestUnmatchedAfter:

    def test_non_prefixed_column_without_before_counterpart(self):
        cd = _int_cols("a", "_a", "orphan")
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(cd, "_")
        assert exc_info.value.code == ErrorCode.CDCDB27.code()
        assert "orphan" in str(exc_info.value)

    def test_multiple_unmatched_after(self):
        cd = _int_cols("a", "_a", "b", "c")
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(cd, "_")
        assert exc_info.value.code == ErrorCode.CDCDB27.code()
        assert "b" in str(exc_info.value)
        assert "c" in str(exc_info.value)

    def test_all_non_prefixed_no_before_columns(self):
        cd = _int_cols("x", "y")
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(cd, "_")
        assert exc_info.value.code == ErrorCode.CDCDB27.code()

    def test_unmatched_before_takes_precedence_over_unmatched_after(self):
        cd = _int_cols("orphan_after", "_orphan_before")
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(cd, "_")
        assert exc_info.value.code == ErrorCode.CDCDB25.code()


class TestColumnOrdering:

    def test_before_image_columns_listed_first(self):
        cd = _int_cols("_a", "a", "_b", "b")
        result = match_cd_columns(cd, "_")
        after_names = [name for name, _ in result.after_columns_list]
        assert after_names == ["a", "b"]

    def test_interleaved_order(self):
        cd = _int_cols("a", "b", "_a", "_b")
        result = match_cd_columns(cd, "_")
        after_names = [name for name, _ in result.after_columns_list]
        assert after_names == ["a", "b"]

    def test_reverse_order(self):
        cd = _int_cols("_b", "_a", "b", "a")
        result = match_cd_columns(cd, "_")
        after_names = [name for name, _ in result.after_columns_list]
        assert after_names == ["b", "a"]

    def test_system_columns_interspersed(self):
        cd = _int_cols(
            "IBMSNAP_OPERATION", "a", "IBMSNAP_COMMITSEQ",
            "_a", "b", "IBMSNAP_INTENTSEQ", "_b",
        )
        result = match_cd_columns(cd, "_")
        after_names = [name for name, _ in result.after_columns_list]
        assert after_names == ["a", "b"]


class TestPrefixSubstringEdgeCases:

    def test_prefix_equals_column_name_no_match(self):
        cd = _int_cols("_")
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(cd, "_")
        assert exc_info.value.code == ErrorCode.CDCDB27.code()

    def test_column_name_is_exactly_prefix(self):
        cd = _int_cols("old_")
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(cd, "old_")
        assert exc_info.value.code == ErrorCode.CDCDB27.code()

    def test_prefix_longer_than_some_columns(self):
        cd = _int_cols("a", "old_a")
        result = match_cd_columns(cd, "old_")
        assert [name for name, _ in result.after_columns_list] == ["a"]
        assert result.before_columns_map == {"a": "old_a"}


class TestCaseSensitivity:

    def test_prefix_matching_is_case_sensitive(self):
        cd = _int_cols("A", "_a")
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(cd, "_")
        assert exc_info.value.code == ErrorCode.CDCDB25.code()

    def test_matching_same_case(self):
        cd = _int_cols("A", "_A")
        result = match_cd_columns(cd, "_")
        assert [name for name, _ in result.after_columns_list] == ["A"]
        assert result.before_columns_map == {"A": "_A"}


class TestManyColumns:

    def test_fifty_pairs(self):
        names = []
        for i in range(50):
            names.extend([f"col{i:03d}", f"_col{i:03d}"])
        cd = _int_cols(*names)
        result = match_cd_columns(cd, "_")
        after_names = {name for name, _ in result.after_columns_list}
        assert len(after_names) == 50
        for i in range(50):
            col = f"col{i:03d}"
            assert col in after_names
            assert result.before_columns_map[col] == f"_col{i:03d}"


class TestEmptyInputs:

    def test_only_system_columns(self):
        cd = _int_cols("IBMSNAP_OPERATION", "IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ")
        result = match_cd_columns(cd, "_")
        assert result.after_columns_list == []
        assert result.before_columns_map == {}

    def test_empty_column_list(self):
        result = match_cd_columns([], "_")
        assert result.after_columns_list == []
        assert result.before_columns_map == {}


class TestTrickyPrefixOverlaps:

    def test_prefix_underscore_column_starts_with_double_underscore(self):
        cd = _int_cols("_a", "__a")
        result = match_cd_columns(cd, "_")
        assert [name for name, _ in result.after_columns_list] == ["_a"]
        assert result.before_columns_map == {"_a": "__a"}

    def test_chain_of_prefixes_three_levels(self):
        cd = _int_cols("a", "_a", "__a")
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(cd, "_")
        assert exc_info.value.code == ErrorCode.CDCDB25.code()
        assert "__a" in str(exc_info.value)

    def test_prefix_ab_column_abc_and_c(self):
        cd = _int_cols("c", "abc")
        result = match_cd_columns(cd, "ab")
        assert [name for name, _ in result.after_columns_list] == ["c"]
        assert result.before_columns_map == {"c": "abc"}

    def test_prefix_ab_column_a_and_aba(self):
        cd = _int_cols("a", "aba")
        result = match_cd_columns(cd, "ab")
        assert [name for name, _ in result.after_columns_list] == ["a"]
        assert result.before_columns_map == {"a": "aba"}

    def test_prefix_collision_same_candidate(self):
        cd = _int_cols("a", "_a", "b_a")
        with pytest.raises(Db2CdcError) as exc_info:
            match_cd_columns(cd, "_")
        assert exc_info.value.code == ErrorCode.CDCDB27.code()
        assert "b_a" in str(exc_info.value)


class TestRealisticLayouts:

    def test_typical_cd_table(self):
        cd = _cols(
            ("IBMSNAP_OPERATION", sa.CHAR(1)),
            ("IBMSNAP_COMMITSEQ", sa.VARBINARY(16)),
            ("IBMSNAP_INTENTSEQ", sa.VARBINARY(16)),
            ("ID", sa.Integer()),
            ("X_ID", sa.Integer()),
            ("NAME", sa.String(100)),
            ("X_NAME", sa.String(100)),
            ("BALANCE", sa.Numeric(15, 2)),
            ("X_BALANCE", sa.Numeric(15, 2)),
        )
        result = match_cd_columns(cd, "X_")
        after_names = [name for name, _ in result.after_columns_list]
        assert after_names == ["ID", "NAME", "BALANCE"]
        assert result.before_columns_map == {
            "ID": "X_ID",
            "NAME": "X_NAME",
            "BALANCE": "X_BALANCE",
        }

    def test_cd_table_mixed_types(self):
        cd = _cols(
            ("IBMSNAP_OPERATION", sa.CHAR(1)),
            ("IBMSNAP_COMMITSEQ", sa.VARBINARY(16)),
            ("IBMSNAP_INTENTSEQ", sa.VARBINARY(16)),
            ("TS", sa.DateTime()),
            ("B_TS", sa.DateTime()),
            ("FLAG", sa.Boolean()),
            ("B_FLAG", sa.Boolean()),
        )
        result = match_cd_columns(cd, "B_")
        types_by_name = {name: t for name, t in result.after_columns_list}
        assert isinstance(types_by_name["TS"], sa.DateTime)
        assert isinstance(types_by_name["FLAG"], sa.Boolean)
        assert result.before_columns_map == {"TS": "B_TS", "FLAG": "B_FLAG"}
