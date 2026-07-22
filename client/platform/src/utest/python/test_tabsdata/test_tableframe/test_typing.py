#
# Copyright 2025 Tabsdata Inc.
#

import pytest

import tabsdata.tableframe.dtypes as td_dtypes
from tabsdata.tableframe.typing import TF_DATA_TYPES


class TestDataTypes:

    def test_data_types_is_frozenset(self):
        assert isinstance(TF_DATA_TYPES, frozenset)

    def test_data_types_contains_all_basic_types(self):
        expected_types = [
            td_dtypes.Boolean,
            td_dtypes.Categorical,
            td_dtypes.Date,
            td_dtypes.Datetime,
            td_dtypes.Decimal,
            td_dtypes.Duration,
            td_dtypes.Enum,
            td_dtypes.Float16,
            td_dtypes.Float32,
            td_dtypes.Float64,
            td_dtypes.Int8,
            td_dtypes.Int16,
            td_dtypes.Int32,
            td_dtypes.Int64,
            td_dtypes.Int128,
            td_dtypes.Null,
            td_dtypes.String,
            td_dtypes.Time,
            td_dtypes.UInt8,
            td_dtypes.UInt16,
            td_dtypes.UInt32,
            td_dtypes.UInt64,
            td_dtypes.Utf8,
        ]

        for dtype in expected_types:
            assert dtype in TF_DATA_TYPES, f"{dtype} should be in TF_DATA_TYPES"

    def test_membership_checking(self):
        assert td_dtypes.Int32 in TF_DATA_TYPES
        assert td_dtypes.String in TF_DATA_TYPES
        assert td_dtypes.Float64 in TF_DATA_TYPES

    def test_data_types_is_immutable(self):
        with pytest.raises(AttributeError):
            # noinspection PyUnresolvedReferences
            TF_DATA_TYPES.add(td_dtypes.Boolean)

        with pytest.raises(AttributeError):
            # noinspection PyUnresolvedReferences
            TF_DATA_TYPES.remove(td_dtypes.Boolean)

    def test_data_types_excludes_invalid_types(self):
        invalid_types = [
            int,
            str,
            float,
            bool,
            list,
            dict,
            set,
            tuple,
            object,
            type,
        ]

        for invalid_type in invalid_types:
            assert (
                invalid_type not in TF_DATA_TYPES
            ), f"{invalid_type} should not be in TF_DATA_TYPES"
