#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from typing import AbstractSet, Any, cast

import isodate
import polars as pl

from tabsdatak.dataquality._error import DataQualityErrorCode
from tabsdatak.tableframe import datatypes as td_dtypes


class IntBounds(Enum):
    INT8 = (pl.select(pl.Int8.min()).item(), pl.select(pl.Int8.max()).item())
    INT16 = (pl.select(pl.Int16.min()).item(), pl.select(pl.Int16.max()).item())
    INT32 = (pl.select(pl.Int32.min()).item(), pl.select(pl.Int32.max()).item())
    INT64 = (pl.select(pl.Int64.min()).item(), pl.select(pl.Int64.max()).item())
    INT128 = (pl.select(pl.Int128.min()).item(), pl.select(pl.Int128.max()).item())
    UINT8 = (pl.select(pl.UInt8.min()).item(), pl.select(pl.UInt8.max()).item())
    UINT16 = (pl.select(pl.UInt16.min()).item(), pl.select(pl.UInt16.max()).item())
    UINT32 = (pl.select(pl.UInt32.min()).item(), pl.select(pl.UInt32.max()).item())
    UINT64 = (pl.select(pl.UInt64.min()).item(), pl.select(pl.UInt64.max()).item())


class PolarsTimeUnits(Enum):
    us = "us"
    ms = "ms"
    ns = "ns"


class TimeDataTimeUnits(Enum):
    microseconds = "microseconds"
    milliseconds = "milliseconds"
    nanoseconds = "microseconds"


TIME_UNIT_MAP = {
    PolarsTimeUnits.us.value: TimeDataTimeUnits.microseconds.value,
    PolarsTimeUnits.ms.value: TimeDataTimeUnits.milliseconds.value,
    PolarsTimeUnits.ns.value: TimeDataTimeUnits.nanoseconds.value,
}


# noinspection DuplicatedCode
class TypeHandler:
    @staticmethod
    def to_boolean(value: Any, allow_null: bool = False) -> bool | None:  # noqa: C901
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to Boolean")
            if isinstance(value, bool):
                return value
            if isinstance(value, int):
                return bool(value)
            if isinstance(value, float):
                if math.isnan(value):
                    raise ValueError("Cannot convert NaN to Boolean")
                if not value.is_integer():
                    raise ValueError(
                        f"Float value {value} has decimals; cannot convert to Boolean"
                    )
                return bool(int(value))
            if isinstance(value, str):
                canonical = value.lower().strip()
                if canonical in ("true", "1", "yes", "y", "t"):
                    return True
                if canonical in ("false", "0", "no", "n", "f"):
                    return False
                raise ValueError(f"Cannot convert string '{value}' to Boolean")
            raise TypeError(f"Cannot convert {type(value).__name__} to Boolean")
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to Boolean: {str(error)}"
            )

    @staticmethod
    def to_date(value: Any, allow_null: bool = False) -> date | None:  # noqa: C901
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to Date")
            if isinstance(value, datetime):
                return value.date()
            if isinstance(value, date):
                return value
            if isinstance(value, int):
                return pl.Series([value]).cast(pl.Date).item()
            if isinstance(value, float):
                if not value.is_integer():
                    raise ValueError(
                        f"Float value {value} has decimals; cannot convert to Date"
                    )
                canonical = int(value)
                return pl.Series([canonical]).cast(pl.Date).item()
            if isinstance(value, str):
                return pl.Series([value]).str.to_date().item()
            raise TypeError(f"Cannot convert {type(value).__name__} to Date")
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to Date: {str(error)}"
            )

    @staticmethod
    def to_datetime(  # noqa: C901
        value: Any, dtype: pl.Datetime, allow_null: bool = False
    ) -> datetime | None:
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to Datetime")

            time_zone = dtype.time_zone
            time_unit = dtype.time_unit

            if isinstance(value, datetime):
                if value.tzinfo is None and time_zone is not None:
                    return (
                        pl
                        .Series([value])
                        .cast(pl.Datetime(time_unit))
                        .dt.replace_time_zone(time_zone)
                        .item()
                    )
                return value
            if isinstance(value, date):
                dt = datetime.combine(value, time.min)
                if time_zone is not None:
                    return (
                        pl
                        .Series([dt])
                        .cast(pl.Datetime(time_unit))
                        .dt.replace_time_zone(time_zone)
                        .item()
                    )
                return dt
            if isinstance(value, int):
                series = pl.Series([value]).cast(pl.Datetime(time_unit))
                if time_zone is not None:
                    series = series.dt.replace_time_zone(time_zone)
                return series.item()
            if isinstance(value, float):
                if not value.is_integer():
                    raise ValueError(
                        f"Float value {value} has decimals; cannot convert to Datetime"
                    )
                canonical = int(value)
                series = pl.Series([canonical]).cast(pl.Datetime(time_unit))
                if time_zone is not None:
                    series = series.dt.replace_time_zone(time_zone)
                return series.item()
            if isinstance(value, str):
                if time_zone is not None:
                    return (
                        pl.Series([value]).str.to_datetime(time_zone=time_zone).item()
                    )
                return pl.Series([value]).str.to_datetime().item()
            raise TypeError(f"Cannot convert {type(value).__name__} to Datetime")
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to Datetime: {str(error)}"
            )

    @staticmethod
    def to_time(value: Any, allow_null: bool = False) -> time | None:  # noqa: C901
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to Time")
            if isinstance(value, datetime):
                return value.timetz()
            if isinstance(value, date):
                return time.min
            if isinstance(value, time):
                return value
            if isinstance(value, int):
                return (datetime.min + timedelta(milliseconds=value)).time()
            if isinstance(value, float):
                if not value.is_integer():
                    raise ValueError(
                        f"Float value {value} has decimals; cannot convert to Time"
                    )
                canonical = int(value)
                return (datetime.min + timedelta(milliseconds=canonical)).time()
            if isinstance(value, str):
                return pl.Series([value]).str.to_time().item()
            raise TypeError(f"Cannot convert {type(value).__name__} to Time")
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to Time: {str(error)}"
            )

    @staticmethod
    def to_duration(  # noqa: C901
        value: Any, dtype: pl.Duration, allow_null: bool = False
    ) -> timedelta | None:
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to Duration")

            time_unit = dtype.time_unit
            timedelta_unit = TIME_UNIT_MAP.get(time_unit, "microseconds")

            if isinstance(value, timedelta):
                return value
            if isinstance(value, int):
                if time_unit == PolarsTimeUnits.ns.value:
                    value = value // 1000
                return timedelta(**{timedelta_unit: value})
            if isinstance(value, float):
                if math.isnan(value):
                    raise ValueError("Cannot convert NaN to Duration")
                if not value.is_integer():
                    raise ValueError(
                        f"Float value {value} has decimals; cannot convert to Duration"
                    )
                canonical = int(value)
                if time_unit == PolarsTimeUnits.ns.value:
                    canonical = canonical // 1000
                return timedelta(**{timedelta_unit: canonical})
            if isinstance(value, str):
                return isodate.parse_duration(value)
            raise TypeError(f"Cannot convert {type(value).__name__} to Duration")
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to Duration: {str(error)}"
            )

    @staticmethod
    def to_int8(value: Any, allow_null: bool = False) -> int | None:  # noqa: C901
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to Int8")
            if not isinstance(value, (int, float, str)):
                raise TypeError(f"Cannot convert {type(value).__name__} to Int8")
            if isinstance(value, float):
                if math.isnan(value):
                    raise ValueError("Cannot convert NaN to Int8")
                if not value.is_integer():
                    raise ValueError(
                        f"Float value {value} has decimals; cannot convert to Int8"
                    )
            canonical = int(value)
            min_val, max_val = IntBounds.INT8.value
            if canonical < min_val or canonical > max_val:
                raise ValueError(
                    f"Value {canonical} out of range for Int8 ({min_val} to {max_val})"
                )
            return canonical
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to Int8: {str(error)}"
            )

    @staticmethod
    def to_int16(value: Any, allow_null: bool = False) -> int | None:  # noqa: C901
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to Int16")
            if not isinstance(value, (int, float, str)):
                raise TypeError(f"Cannot convert {type(value).__name__} to Int16")
            if isinstance(value, float):
                if math.isnan(value):
                    raise ValueError("Cannot convert NaN to Int16")
                if not value.is_integer():
                    raise ValueError(
                        f"Float value {value} has decimals; cannot convert to Int16"
                    )
            canonical = int(value)
            min_val, max_val = IntBounds.INT16.value
            if canonical < min_val or canonical > max_val:
                raise ValueError(
                    f"Value {canonical} out of range for Int16 ({min_val} to {max_val})"
                )
            return canonical
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to Int16: {str(error)}"
            )

    @staticmethod
    def to_int32(value: Any, allow_null: bool = False) -> int | None:  # noqa: C901
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to Int32")
            if not isinstance(value, (int, float, str)):
                raise TypeError(f"Cannot convert {type(value).__name__} to Int32")
            if isinstance(value, float):
                if math.isnan(value):
                    raise ValueError("Cannot convert NaN to Int32")
                if not value.is_integer():
                    raise ValueError(
                        f"Float value {value} has decimals; cannot convert to Int32"
                    )
            canonical = int(value)
            min_val, max_val = IntBounds.INT32.value
            if canonical < min_val or canonical > max_val:
                raise ValueError(
                    f"Value {canonical} out of range for Int32 ({min_val} to {max_val})"
                )
            return canonical
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to Int32: {str(error)}"
            )

    @staticmethod
    def to_int64(value: Any, allow_null: bool = False) -> int | None:  # noqa: C901
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to Int64")
            if not isinstance(value, (int, float, str)):
                raise TypeError(f"Cannot convert {type(value).__name__} to Int64")
            if isinstance(value, float):
                if math.isnan(value):
                    raise ValueError("Cannot convert NaN to Int64")
                if not value.is_integer():
                    raise ValueError(
                        f"Float value {value} has decimals; cannot convert to Int64"
                    )
            canonical = int(value)
            min_val, max_val = IntBounds.INT64.value
            if canonical < min_val or canonical > max_val:
                raise ValueError(
                    f"Value {canonical} out of range for Int64 ({min_val} to {max_val})"
                )
            return canonical
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to Int64: {str(error)}"
            )

    @staticmethod
    def to_int128(value: Any, allow_null: bool = False) -> int | None:  # noqa: C901
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to Int128")
            if not isinstance(value, (int, float, str)):
                raise TypeError(f"Cannot convert {type(value).__name__} to Int128")
            if isinstance(value, float):
                if math.isnan(value):
                    raise ValueError("Cannot convert NaN to Int128")
                if not value.is_integer():
                    raise ValueError(
                        f"Float value {value} has decimals; cannot convert to Int128"
                    )
            canonical = int(value)
            min_val, max_val = IntBounds.INT128.value
            if canonical < min_val or canonical > max_val:
                raise ValueError(f"Value {canonical} out of range for Int128")
            return canonical
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to Int128: {str(error)}"
            )

    @staticmethod
    def to_uint8(value: Any, allow_null: bool = False) -> int | None:  # noqa: C901
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to UInt8")
            if not isinstance(value, (int, float, str)):
                raise TypeError(f"Cannot convert {type(value).__name__} to UInt8")
            if isinstance(value, float):
                if math.isnan(value):
                    raise ValueError("Cannot convert NaN to UInt8")
                if not value.is_integer():
                    raise ValueError(
                        f"Float value {value} has decimals; cannot convert to UInt8"
                    )
            canonical = int(value)
            min_val, max_val = IntBounds.UINT8.value
            if canonical < min_val or canonical > max_val:
                raise ValueError(
                    f"Value {canonical} out of range for UInt8 ({min_val} to {max_val})"
                )
            return canonical
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to UInt8: {str(error)}"
            )

    @staticmethod
    def to_uint16(value: Any, allow_null: bool = False) -> int | None:  # noqa: C901
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to UInt16")
            if not isinstance(value, (int, float, str)):
                raise TypeError(f"Cannot convert {type(value).__name__} to UInt16")
            if isinstance(value, float):
                if math.isnan(value):
                    raise ValueError("Cannot convert NaN to UInt16")
                if not value.is_integer():
                    raise ValueError(
                        f"Float value {value} has decimals; cannot convert to UInt16"
                    )
            canonical = int(value)
            min_val, max_val = IntBounds.UINT16.value
            if canonical < min_val or canonical > max_val:
                raise ValueError(
                    f"Value {canonical} out of range for UInt16 ({min_val} to"
                    f" {max_val})"
                )
            return canonical
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to UInt16: {str(error)}"
            )

    @staticmethod
    def to_uint32(value: Any, allow_null: bool = False) -> int | None:  # noqa: C901
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to UInt32")
            if not isinstance(value, (int, float, str)):
                raise TypeError(f"Cannot convert {type(value).__name__} to UInt32")
            if isinstance(value, float):
                if math.isnan(value):
                    raise ValueError("Cannot convert NaN to UInt32")
                if not value.is_integer():
                    raise ValueError(
                        f"Float value {value} has decimals; cannot convert to UInt32"
                    )
            canonical = int(value)
            min_val, max_val = IntBounds.UINT32.value
            if canonical < min_val or canonical > max_val:
                raise ValueError(
                    f"Value {canonical} out of range for UInt32 ({min_val} to"
                    f" {max_val})"
                )
            return canonical
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to UInt32: {str(error)}"
            )

    @staticmethod
    def to_uint64(value: Any, allow_null: bool = False) -> int | None:  # noqa: C901
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to UInt64")
            if not isinstance(value, (int, float, str)):
                raise TypeError(f"Cannot convert {type(value).__name__} to UInt64")
            if isinstance(value, float):
                if math.isnan(value):
                    raise ValueError("Cannot convert NaN to UInt64")
                if not value.is_integer():
                    raise ValueError(
                        f"Float value {value} has decimals; cannot convert to UInt64"
                    )
            canonical = int(value)
            min_val, max_val = IntBounds.UINT64.value
            if canonical < min_val or canonical > max_val:
                raise ValueError(
                    f"Value {canonical} out of range for UInt64 ({min_val} to"
                    f" {max_val})"
                )
            return canonical
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to UInt64: {str(error)}"
            )

    @staticmethod
    def to_float16(  # noqa: C901
        value: Any, allow_null: bool = False, allow_nan: bool = False
    ) -> float | None:
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to Float16")
            if not isinstance(value, (int, float, str)):
                raise TypeError(f"Cannot convert {type(value).__name__} to Float16")
            if isinstance(value, float):
                if math.isnan(value):
                    if allow_nan:
                        return value
                    raise ValueError("Cannot convert NaN to Float16")
                canonical = value
            else:
                canonical = float(value)
                if math.isnan(canonical):
                    if allow_nan:
                        return canonical
                    raise ValueError("Cannot convert NaN to Float16")
            try:
                converted = pl.Series([canonical], dtype=pl.Float16).item()
            except Exception as error:
                raise ValueError(
                    f"Value {canonical} out of range for Float16"
                ) from error
            if not math.isinf(canonical) and math.isinf(converted):
                raise ValueError(f"Value {canonical} out of range for Float16")
            return converted
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to Float16: {str(error)}"
            )

    @staticmethod
    def to_float32(  # noqa: C901
        value: Any, allow_null: bool = False, allow_nan: bool = False
    ) -> float | None:
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to Float32")
            if not isinstance(value, (int, float, str)):
                raise TypeError(f"Cannot convert {type(value).__name__} to Float32")
            if isinstance(value, float):
                if math.isnan(value):
                    if allow_nan:
                        return value
                    raise ValueError("Cannot convert NaN to Float32")
                canonical = value
            else:
                canonical = float(value)
                if math.isnan(canonical):
                    if allow_nan:
                        return canonical
                    raise ValueError("Cannot convert NaN to Float32")
            try:
                converted = pl.Series([canonical], dtype=pl.Float32).item()
            except Exception as error:
                raise ValueError(
                    f"Value {canonical} out of range for Float32"
                ) from error
            if not math.isinf(canonical) and math.isinf(converted):
                raise ValueError(f"Value {canonical} out of range for Float32")
            return converted
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to Float32: {str(error)}"
            )

    @staticmethod
    def to_float64(  # noqa: C901
        value: Any, allow_null: bool = False, allow_nan: bool = False
    ) -> float | None:
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to Float64")
            if not isinstance(value, (int, float, str)):
                raise TypeError(f"Cannot convert {type(value).__name__} to Float64")
            if isinstance(value, float):
                if math.isnan(value):
                    if allow_nan:
                        return value
                    raise ValueError("Cannot convert NaN to Float64")
                canonical = value
            else:
                canonical = float(value)
                if math.isnan(canonical):
                    if allow_nan:
                        return canonical
                    raise ValueError("Cannot convert NaN to Float64")
            try:
                converted = pl.Series([canonical], dtype=pl.Float64).item()
            except Exception as error:
                raise ValueError(
                    f"Value {canonical} out of range for Float64"
                ) from error
            if not math.isinf(canonical) and math.isinf(converted):
                raise ValueError(f"Value {canonical} out of range for Float64")
            return converted
        except Exception as error:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to Float64: {str(error)}"
            )

    @staticmethod
    def to_decimal(  # noqa: C901
        value: Any, dtype: pl.Decimal, allow_null: bool = False
    ) -> Decimal | None:
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to Decimal")

            precision = dtype.precision
            scale = dtype.scale

            if isinstance(value, Decimal):
                return value
            if isinstance(value, float) and math.isnan(value):
                raise ValueError("Cannot convert NaN to Decimal")
            if isinstance(value, (int, float, str)):
                return pl.Series([value]).cast(pl.Decimal(precision, scale)).item()
            raise TypeError(f"Cannot convert {type(value).__name__} to Decimal")
        except Exception as e:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to Decimal: {str(e)}"
            )

    @staticmethod
    def to_string(value: Any, allow_null: bool = False) -> str | None:  # noqa: C901
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to String")
            return str(value)
        except Exception as e:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to String: {str(e)}"
            )

    @staticmethod
    def to_utf8(value: Any, allow_null: bool = False) -> str | None:  # noqa: C901
        try:
            if value is None:
                if allow_null:
                    return None
                raise ValueError("Cannot convert None to Utf8")
            return str(value)
        except Exception as e:
            raise DataQualityErrorCode.DQPE_13.exception(
                detail=f"Failed to convert value '{value}' to Utf8: {str(e)}"
            )

    @classmethod
    def convert_value(
        cls,
        value: Any,
        dtype: pl.DataType,
        allow_null: bool = False,
        allow_nan: bool = False,
    ) -> Any:
        match type(dtype):
            case td_dtypes.Boolean:
                return cls.to_boolean(value, allow_null)
            case td_dtypes.Date:
                return cls.to_date(value, allow_null)
            case td_dtypes.Datetime:
                return cls.to_datetime(value, cast(pl.Datetime, dtype), allow_null)
            case td_dtypes.Time:
                return cls.to_time(value, allow_null)
            case td_dtypes.Duration:
                return cls.to_duration(value, cast(pl.Duration, dtype), allow_null)
            case td_dtypes.Int8:
                return cls.to_int8(value, allow_null)
            case td_dtypes.Int16:
                return cls.to_int16(value, allow_null)
            case td_dtypes.Int32:
                return cls.to_int32(value, allow_null)
            case td_dtypes.Int64:
                return cls.to_int64(value, allow_null)
            case td_dtypes.Int128:
                return cls.to_int128(value, allow_null)
            case td_dtypes.UInt8:
                return cls.to_uint8(value, allow_null)
            case td_dtypes.UInt16:
                return cls.to_uint16(value, allow_null)
            case td_dtypes.UInt32:
                return cls.to_uint32(value, allow_null)
            case td_dtypes.UInt64:
                return cls.to_uint64(value, allow_null)
            case td_dtypes.Float16:
                return cls.to_float16(value, allow_null, allow_nan)
            case td_dtypes.Float32:
                return cls.to_float32(value, allow_null, allow_nan)
            case td_dtypes.Float64:
                return cls.to_float64(value, allow_null, allow_nan)
            case td_dtypes.Decimal:
                return cls.to_decimal(value, cast(pl.Decimal, dtype), allow_null)
            case td_dtypes.String:
                return cls.to_string(value, allow_null)
            case td_dtypes.Utf8:
                return cls.to_utf8(value, allow_null)
            case _:
                raise DataQualityErrorCode.DQPE_13.exception(
                    detail=f"Unsupported data type for conversion: {dtype}"
                )

    @classmethod
    def convert_set(
        cls, values: AbstractSet[Any], target_dtype: pl.DataType
    ) -> AbstractSet[type[pl.DataType] | None]:
        supports_nan = type(target_dtype) in {
            td_dtypes.Float16,
            td_dtypes.Float32,
            td_dtypes.Float64,
        }
        canonicals = []
        for value in values:
            if isinstance(value, float) and math.isnan(value) and not supports_nan:
                continue
            canonical = cls.convert_value(
                value,
                target_dtype,
                allow_null=True,
                allow_nan=supports_nan,
            )
            canonicals.append(canonical)
        return set(canonicals)
