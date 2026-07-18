#
# Copyright 2026 Tabsdata Inc.
#

import math
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import polars as pl
import pytest

from tabsdatak.common._actions.dataquality.assistant import TypeHandler
from tabsdatak.dataquality._error import DataQualityProcessingError

pytestmark = pytest.mark.dq


class TestTypeHandler:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (True, True),
            (False, False),
            (1, True),
            (0, False),
            (1.0, True),
            (0.0, False),
            ("true", True),
            ("false", False),
            ("TRUE", True),
            ("FALSE", False),
            (" T ", True),
            (" F ", False),
            ("yes", True),
            ("no", False),
            ("y", True),
            ("n", False),
        ],
    )
    def test_to_boolean_valid(self, value, expected):
        assert TypeHandler.to_boolean(value) == expected

    @pytest.mark.parametrize(
        "value",
        [
            "invalid",
            1.1,
            None,
        ],
    )
    def test_to_boolean_invalid(self, value):
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_boolean(value)

    def test_to_boolean_null(self):
        assert TypeHandler.to_boolean(None, allow_null=True) is None
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_boolean(None, allow_null=False)

    @pytest.mark.parametrize(
        "value, expected",
        [
            (date(2023, 1, 1), date(2023, 1, 1)),
            (datetime(2023, 1, 1, 12, 0), date(2023, 1, 1)),
            (19358, date(2023, 1, 1)),  # polars date representation
            ("2023-01-01", date(2023, 1, 1)),
        ],
    )
    def test_to_date_valid(self, value, expected):
        assert TypeHandler.to_date(value) == expected

    @pytest.mark.parametrize(
        "value",
        ["invalid-date", 1.1, None],
    )
    def test_to_date_invalid(self, value):
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_date(value)

    def test_to_date_null(self):
        assert TypeHandler.to_date(None, allow_null=True) is None
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_date(None, allow_null=False)

    @pytest.mark.parametrize(
        "value, expected",
        [
            (datetime(2023, 1, 1, 12, 0), datetime(2023, 1, 1, 12, 0)),
            (date(2023, 1, 1), datetime(2023, 1, 1, 0, 0)),
            (1672574400000, datetime(2023, 1, 1, 12, 0)),
            ("2023-01-01T12:00:00", datetime(2023, 1, 1, 12, 0)),
        ],
    )
    def test_to_datetime_valid(self, value, expected):
        dtype = pl.Datetime("ms", None)  # milliseconds, timezone-naive
        result = TypeHandler.to_datetime(value, dtype)
        if isinstance(result, datetime):
            result = result.replace(tzinfo=None)
        assert result == expected

    @pytest.mark.parametrize(
        "value",
        ["invalid-datetime", 1.1, None],
    )
    def test_to_datetime_invalid(self, value):
        dtype = pl.Datetime("ms", None)
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_datetime(value, dtype)

    def test_to_datetime_null(self):
        dtype = pl.Datetime("ms", None)
        assert TypeHandler.to_datetime(None, dtype, allow_null=True) is None
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_datetime(None, dtype, allow_null=False)

    @pytest.mark.parametrize(
        "value, expected",
        [
            (time(12, 0), time(12, 0)),
            (datetime(2023, 1, 1, 12, 0), time(12, 0)),
            (date(2023, 1, 1), time(0, 0)),
            (43200000, time(12, 0)),
            ("12:00:00", time(12, 0)),
        ],
    )
    def test_to_time_valid(self, value, expected):
        assert TypeHandler.to_time(value) == expected

    @pytest.mark.parametrize(
        "value",
        ["invalid-time", 1.1, None],
    )
    def test_to_time_invalid(self, value):
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_time(value)

    def test_to_time_null(self):
        assert TypeHandler.to_time(None, allow_null=True) is None
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_time(None, allow_null=False)

    @pytest.mark.parametrize(
        "value, expected",
        [
            (timedelta(days=1), timedelta(days=1)),
            (86400000, timedelta(days=1)),
            ("P1D", timedelta(days=1)),
        ],
    )
    def test_to_duration_valid(self, value, expected):
        dtype = pl.Duration("ms")  # milliseconds
        assert TypeHandler.to_duration(value, dtype) == expected

    @pytest.mark.parametrize(
        "value",
        ["invalid-duration", 1.1, None],
    )
    def test_to_duration_invalid(self, value):
        dtype = pl.Duration("ms")
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_duration(value, dtype)

    def test_to_duration_null(self):
        dtype = pl.Duration("ms")
        assert TypeHandler.to_duration(None, dtype, allow_null=True) is None
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_duration(None, dtype, allow_null=False)

    @pytest.mark.parametrize(
        "method, min_val, max_val",
        [
            ("to_int8", -128, 127),
            ("to_int16", -32768, 32767),
            ("to_int32", -2147483648, 2147483647),
            ("to_int64", -9223372036854775808, 9223372036854775807),
            ("to_uint8", 0, 255),
            ("to_uint16", 0, 65535),
            ("to_uint32", 0, 4294967295),
            ("to_uint64", 0, 18446744073709551615),
        ],
    )
    def test_integer_types_valid(self, method, min_val, max_val):
        to_int_method = getattr(TypeHandler, method)
        assert to_int_method(min_val) == min_val
        assert to_int_method(max_val) == max_val
        assert to_int_method(str(min_val)) == min_val
        if method not in ("to_int64", "to_uint64"):
            assert to_int_method(float(max_val)) == max_val

    @pytest.mark.parametrize(
        "method, min_val, max_val",
        [
            ("to_int8", -128, 127),
            ("to_int16", -32768, 32767),
            ("to_int32", -2147483648, 2147483647),
            ("to_int64", -9223372036854775808, 9223372036854775807),
            ("to_uint8", 0, 255),
            ("to_uint16", 0, 65535),
            ("to_uint32", 0, 4294967295),
            ("to_uint64", 0, 18446744073709551615),
        ],
    )
    def test_integer_types_invalid(self, method, min_val, max_val):
        to_int_method = getattr(TypeHandler, method)
        with pytest.raises(DataQualityProcessingError):
            to_int_method(min_val - 1)
        with pytest.raises(DataQualityProcessingError):
            to_int_method(max_val + 1)
        with pytest.raises(DataQualityProcessingError):
            to_int_method(1.1)
        with pytest.raises(DataQualityProcessingError):
            to_int_method(None)

    @pytest.mark.parametrize(
        "method",
        [
            "to_int8",
            "to_int16",
            "to_int32",
            "to_int64",
            "to_uint8",
            "to_uint16",
            "to_uint32",
            "to_uint64",
        ],
    )
    def test_integer_types_null(self, method):
        to_int_method = getattr(TypeHandler, method)
        assert to_int_method(None, allow_null=True) is None

    @pytest.mark.parametrize(
        "value",
        [
            123,
            "123",
            123.0,
            float("inf"),
            float("-inf"),
        ],
    )
    def test_to_float16_valid(self, value):
        assert TypeHandler.to_float16(value) == float(value)

    def test_to_float16_nan(self):
        assert TypeHandler.to_float16(float("nan"), allow_nan=True) is not None
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_float16(float("nan"))

    @pytest.mark.parametrize(
        "value",
        [
            "invalid-float",
            None,
        ],
    )
    def test_to_float16_invalid(self, value):
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_float16(value)

    def test_to_float16_null(self):
        assert TypeHandler.to_float16(None, allow_null=True) is None

    @pytest.mark.parametrize(
        "value",
        [
            123,
            "123",
            123.0,
            float("inf"),
            float("-inf"),
        ],
    )
    def test_to_float32_valid(self, value):
        assert TypeHandler.to_float32(value) == float(value)

    def test_to_float32_nan(self):
        assert TypeHandler.to_float32(float("nan"), allow_nan=True) is not None
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_float32(float("nan"))

    @pytest.mark.parametrize(
        "value",
        [
            "invalid-float",
            None,
        ],
    )
    def test_to_float32_invalid(self, value):
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_float32(value)

    def test_to_float32_null(self):
        assert TypeHandler.to_float32(None, allow_null=True) is None

    @pytest.mark.parametrize(
        "value",
        [
            123,
            "123",
            123.0,
            float("inf"),
            float("-inf"),
        ],
    )
    def test_to_float64_valid(self, value):
        assert TypeHandler.to_float64(value) == float(value)

    def test_to_float64_nan(self):
        assert TypeHandler.to_float64(float("nan"), allow_nan=True) is not None
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_float64(float("nan"))

    @pytest.mark.parametrize(
        "value",
        [
            "invalid-float",
            None,
        ],
    )
    def test_to_float64_invalid(self, value):
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_float64(value)

    def test_to_float64_null(self):
        assert TypeHandler.to_float64(None, allow_null=True) is None

    @pytest.mark.parametrize(
        "value, expected",
        [
            (123, Decimal("123")),
            (123.45, Decimal("123.45")),
            ("123.45", Decimal("123.45")),
            (Decimal("123.45"), Decimal("123.45")),
        ],
    )
    def test_to_decimal_valid(self, value, expected):
        dtype = pl.Decimal(38, 9)  # precision=38, scale=9
        assert TypeHandler.to_decimal(value, dtype) == expected

    @pytest.mark.parametrize(
        "value",
        [
            "invalid-decimal",
            float("nan"),
            None,
        ],
    )
    def test_to_decimal_invalid(self, value):
        dtype = pl.Decimal(38, 9)
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_decimal(value, dtype)

    def test_to_decimal_null(self):
        dtype = pl.Decimal(38, 9)
        assert TypeHandler.to_decimal(None, dtype, allow_null=True) is None

    @pytest.mark.parametrize(
        "value, expected",
        [
            (123, "123"),
            (123.45, "123.45"),
            (True, "True"),
            ([1, 2], "[1, 2]"),
        ],
    )
    def test_to_string_valid(self, value, expected):
        assert TypeHandler.to_string(value) == expected

    def test_to_string_null(self):
        assert TypeHandler.to_string(None, allow_null=True) is None
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_string(None, allow_null=False)

    @pytest.mark.parametrize(
        "value, expected",
        [
            (123, "123"),
            (123.45, "123.45"),
            (True, "True"),
            ([1, 2], "[1, 2]"),
        ],
    )
    def test_to_utf8_valid(self, value, expected):
        assert TypeHandler.to_utf8(value) == expected

    def test_to_utf8_null(self):
        assert TypeHandler.to_utf8(None, allow_null=True) is None
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.to_utf8(None, allow_null=False)

    @pytest.mark.parametrize(
        "value, dtype, expected",
        [
            ("true", pl.Boolean(), True),
            ("2023-01-01", pl.Date(), date(2023, 1, 1)),
            (123, pl.Int32(), 123),
            (123.45, pl.Float64(), 123.45),
            ("hello", pl.String(), "hello"),
        ],
    )
    def test_convert_value(self, value, dtype, expected):
        assert TypeHandler.convert_value(value, dtype) == expected

    def test_convert_value_unsupported(self):
        with pytest.raises(DataQualityProcessingError):
            TypeHandler.convert_value("any", pl.Object())

    @pytest.mark.parametrize(
        "values, dtype, expected",
        [
            ({1, 0, "true"}, pl.Boolean(), {True, False}),
            (
                {"2023-01-01", "2023-01-02"},
                pl.Date(),
                {date(2023, 1, 1), date(2023, 1, 2)},
            ),
            ({1, 2, "3", 4.0}, pl.Int16(), {1, 2, 3, 4}),
            ({"2.2", 1.1, 3}, pl.Float32(), {1.1, 2.2, 3.0}),
        ],
    )
    def test_convert_set(self, values, dtype, expected):
        converted = TypeHandler.convert_set(values, dtype)
        if isinstance(dtype, (pl.Float16, pl.Float32, pl.Float64)):
            assert len(converted) == len(expected)
            for exp in expected:
                if isinstance(exp, float) and math.isnan(exp):
                    assert any(
                        isinstance(value, float) and math.isnan(value)
                        for value in converted
                    )
                else:
                    assert any(pytest.approx(value) == exp for value in converted)
        else:
            assert converted == expected

    def test_convert_set_with_nulls(self):
        values = {1, None, 0}
        dtype = pl.Boolean()
        expected = {True, None, False}
        assert TypeHandler.convert_set(values, dtype) == expected

    def test_convert_set_nan_unsupported(self):
        values = {1, float("nan"), 0}
        dtype = pl.Int32()
        expected = {1, 0}
        assert TypeHandler.convert_set(values, dtype) == expected

    def test_convert_set_nan_supported(self):
        values = {1.0, float("nan"), 0.0}
        dtype = pl.Float64()
        result = TypeHandler.convert_set(values, dtype)
        assert len(result) == 3
        assert any(pytest.approx(value) == 1.0 for value in result)
        assert any(pytest.approx(value) == 0.0 for value in result)
        assert any(isinstance(value, float) and math.isnan(value) for value in result)
