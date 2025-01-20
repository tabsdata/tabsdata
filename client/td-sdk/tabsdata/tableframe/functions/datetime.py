from __future__ import annotations

import datetime as dt
from typing import Iterable

# noinspection PyProtectedMember
from polars._typing import Ambiguous, EpochTimeUnit, NonExistent, Roll, TimeUnit
from polars.expr.datetime import ExprDateTimeNameSpace

import tabsdata.tableframe.expr.expr as td_expr

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._translator as td_translator


class TdExprDateTimeNameSpace:
    def __init__(self, expr: ExprDateTimeNameSpace) -> None:
        self._expr = expr

    def add_business_days(
        self,
        n: int | td_expr.IntoTdExpr,
        week_mask: Iterable[bool] = (True, True, True, True, True, False, False),
        holidays: Iterable[dt.date] = (),
        roll: Roll = "raise",
    ) -> td_expr.TdExpr:
        # noinspection PyProtectedMember
        return td_expr.TdExpr(
            self._expr.add_business_days(
                n=td_translator._unwrap_into_tdexpr(n),
                week_mask=week_mask,
                holidays=holidays,
                roll=roll,
            )
        )

    def truncate(self, every: str | dt.timedelta | td_expr.TdExpr) -> td_expr.TdExpr:
        # noinspection PyProtectedMember
        return td_expr.TdExpr(
            self._expr.truncate(every=td_translator._unwrap_tdexpr(every))
        )

    def replace(
        self,
        *,
        year: int | td_expr.IntoTdExpr | None = None,
        month: int | td_expr.IntoTdExpr | None = None,
        day: int | td_expr.IntoTdExpr | None = None,
        hour: int | td_expr.IntoTdExpr | None = None,
        minute: int | td_expr.IntoTdExpr | None = None,
        second: int | td_expr.IntoTdExpr | None = None,
        microsecond: int | td_expr.IntoTdExpr | None = None,
        ambiguous: Ambiguous | td_expr.TdExpr = "raise",
    ) -> td_expr.TdExpr:
        # noinspection PyProtectedMember
        return td_expr.TdExpr(
            self._expr.replace(
                year=td_translator._unwrap_into_tdexpr(year),
                month=td_translator._unwrap_into_tdexpr(month),
                day=td_translator._unwrap_into_tdexpr(day),
                hour=td_translator._unwrap_into_tdexpr(hour),
                minute=td_translator._unwrap_into_tdexpr(minute),
                second=td_translator._unwrap_into_tdexpr(second),
                microsecond=td_translator._unwrap_into_tdexpr(microsecond),
                ambiguous=td_translator._unwrap_into_tdexpr(ambiguous),
            )
        )

    def combine(
        self, time: dt.time | td_expr.TdExpr, time_unit: TimeUnit = "us"
    ) -> td_expr.TdExpr:
        # noinspection PyProtectedMember
        return td_expr.TdExpr(
            self._expr.combine(
                time=td_translator._unwrap_tdexpr(time), time_unit=time_unit
            )
        )

    def to_string(self, fmt: str | None = None) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.to_string(format=fmt))

    def strftime(self, fmt: str) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.strftime(format=fmt))

    def millennium(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.millennium())

    def century(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.century())

    def year(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.year())

    def is_leap_year(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.is_leap_year())

    def iso_year(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.iso_year())

    def quarter(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.quarter())

    def month(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.month())

    def week(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.week())

    def weekday(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.weekday())

    def day(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.day())

    def ordinal_day(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.ordinal_day())

    def time(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.time())

    def date(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.date())

    def datetime(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.datetime())

    def hour(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.hour())

    def minute(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.minute())

    def second(self, *, fractional: bool = False) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.second(fractional=fractional))

    def millisecond(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.millisecond())

    def microsecond(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.microsecond())

    def nanosecond(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.nanosecond())

    def epoch(self, time_unit: EpochTimeUnit = "us") -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.epoch(time_unit=time_unit))

    def timestamp(self, time_unit: TimeUnit = "us") -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.timestamp(time_unit=time_unit))

    def with_time_unit(self, time_unit: TimeUnit) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.with_time_unit(time_unit))

    def cast_time_unit(self, time_unit: TimeUnit) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.cast_time_unit(time_unit))

    def convert_time_zone(self, time_zone: str) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.convert_time_zone(time_zone))

    def replace_time_zone(
        self,
        time_zone: str | None,
        *,
        ambiguous: Ambiguous | td_expr.TdExpr = "raise",
        non_existent: NonExistent = "raise",
    ) -> td_expr.TdExpr:
        # noinspection PyProtectedMember
        return td_expr.TdExpr(
            self._expr.replace_time_zone(
                time_zone=time_zone,
                ambiguous=td_translator._unwrap_tdexpr(ambiguous),
                non_existent=non_existent,
            )
        )

    def total_days(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.total_days())

    def total_hours(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.total_hours())

    def total_minutes(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.total_minutes())

    def total_seconds(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.total_seconds())

    def total_milliseconds(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.total_milliseconds())

    def total_microseconds(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.total_microseconds())

    def total_nanoseconds(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.total_nanoseconds())

    def offset_by(self, by: str | td_expr.TdExpr) -> td_expr.TdExpr:
        # noinspection PyProtectedMember
        return td_expr.TdExpr(
            self._expr.offset_by(by=td_translator._unwrap_into_tdexpr(by))
        )

    def month_start(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.month_start())

    def month_end(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.month_end())

    def base_utc_offset(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.base_utc_offset())

    def dst_offset(self) -> td_expr.TdExpr:
        return td_expr.TdExpr(self._expr.dst_offset())
