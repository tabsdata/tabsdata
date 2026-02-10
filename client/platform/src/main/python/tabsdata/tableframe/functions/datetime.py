from __future__ import annotations

import datetime as dt
from typing import Iterable

import polars.expr.datetime as pl_datetime

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._translator as td_translator
import tabsdata.tableframe.expr.expr as td_expr

# noinspection PyProtectedMember
import tabsdata.tableframe.typing as td_typing
from tabsdata._utils.annotations import pydoc


class ExprDateTimeNameSpace:
    def __init__(self, expr: pl_datetime.ExprDateTimeNameSpace) -> None:
        self._expr = expr

    @pydoc(categories="date")
    def add_business_days(
        self,
        n: int | td_typing.IntoExpr,
        week_mask: Iterable[bool] = (True, True, True, True, True, False, False),
        holidays: Iterable[dt.date] = (),
        roll: td_typing.Roll = "raise",
    ) -> td_expr.Expr:
        # noinspection PyProtectedMember
        return td_expr.Expr(
            self._expr.add_business_days(
                n=td_translator._unwrap_into_tdexpr(n),
                week_mask=week_mask,
                holidays=holidays,
                roll=roll,
            )
        )

    @pydoc(categories="date")
    def truncate(self, every: str | dt.timedelta | td_expr.Expr) -> td_expr.Expr:
        # noinspection PyProtectedMember
        return td_expr.Expr(
            self._expr.truncate(every=td_translator._unwrap_tdexpr(every))
        )

    def replace(
        self,
        *,
        year: int | td_typing.IntoExpr | None = None,
        month: int | td_typing.IntoExpr | None = None,
        day: int | td_typing.IntoExpr | None = None,
        hour: int | td_typing.IntoExpr | None = None,
        minute: int | td_typing.IntoExpr | None = None,
        second: int | td_typing.IntoExpr | None = None,
        microsecond: int | td_typing.IntoExpr | None = None,
        ambiguous: td_typing.Ambiguous | td_expr.Expr = "raise",
    ) -> td_expr.Expr:
        # noinspection PyProtectedMember
        return td_expr.Expr(
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

    @pydoc(categories="date")
    def combine(
        self, time: dt.time | td_expr.Expr, time_unit: td_typing.TimeUnit = "us"
    ) -> td_expr.Expr:
        # noinspection PyProtectedMember
        return td_expr.Expr(
            self._expr.combine(
                time=td_translator._unwrap_tdexpr(time), time_unit=time_unit
            )
        )

    @pydoc(categories="type_casting")
    def to_string(self, fmt: str | None = None) -> td_expr.Expr:
        return td_expr.Expr(self._expr.to_string(format=fmt))

    @pydoc(categories="date")
    def strftime(self, fmt: str) -> td_expr.Expr:
        return td_expr.Expr(self._expr.strftime(format=fmt))

    @pydoc(categories="date")
    def millennium(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.millennium())

    @pydoc(categories="date")
    def century(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.century())

    @pydoc(categories="date")
    def year(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.year())

    @pydoc(categories="date")
    def is_leap_year(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.is_leap_year())

    @pydoc(categories="date")
    def iso_year(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.iso_year())

    @pydoc(categories="date")
    def quarter(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.quarter())

    @pydoc(categories="date")
    def month(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.month())

    @pydoc(categories="date")
    def week(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.week())

    @pydoc(categories="date")
    def weekday(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.weekday())

    @pydoc(categories="date")
    def day(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.day())

    @pydoc(categories="date")
    def ordinal_day(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.ordinal_day())

    @pydoc(categories="date")
    def time(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.time())

    @pydoc(categories="date")
    def date(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.date())

    @pydoc(categories="date")
    def datetime(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.datetime())

    @pydoc(categories="date")
    def hour(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.hour())

    @pydoc(categories="date")
    def minute(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.minute())

    @pydoc(categories="date")
    def second(self, *, fractional: bool = False) -> td_expr.Expr:
        return td_expr.Expr(self._expr.second(fractional=fractional))

    @pydoc(categories="date")
    def millisecond(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.millisecond())

    @pydoc(categories="date")
    def microsecond(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.microsecond())

    @pydoc(categories="date")
    def nanosecond(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.nanosecond())

    @pydoc(categories="date")
    def epoch(self, time_unit: td_typing.EpochTimeUnit = "us") -> td_expr.Expr:
        return td_expr.Expr(self._expr.epoch(time_unit=time_unit))

    @pydoc(categories="date")
    def timestamp(self, time_unit: td_typing.TimeUnit = "us") -> td_expr.Expr:
        return td_expr.Expr(self._expr.timestamp(time_unit=time_unit))

    @pydoc(categories="date")
    def with_time_unit(self, time_unit: td_typing.TimeUnit) -> td_expr.Expr:
        return td_expr.Expr(self._expr.with_time_unit(time_unit))

    @pydoc(categories="date")
    def cast_time_unit(self, time_unit: td_typing.TimeUnit) -> td_expr.Expr:
        return td_expr.Expr(self._expr.cast_time_unit(time_unit))

    @pydoc(categories="date")
    def convert_time_zone(self, time_zone: str) -> td_expr.Expr:
        return td_expr.Expr(self._expr.convert_time_zone(time_zone))

    @pydoc(categories="date")
    def replace_time_zone(
        self,
        time_zone: str | None,
        *,
        ambiguous: td_typing.Ambiguous | td_expr.Expr = "raise",
        non_existent: td_typing.NonExistent = "raise",
    ) -> td_expr.Expr:
        # noinspection PyProtectedMember
        return td_expr.Expr(
            self._expr.replace_time_zone(
                time_zone=time_zone,
                ambiguous=td_translator._unwrap_tdexpr(ambiguous),
                non_existent=non_existent,
            )
        )

    @pydoc(categories="date")
    def total_days(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.total_days())

    @pydoc(categories="date")
    def total_hours(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.total_hours())

    @pydoc(categories="date")
    def total_minutes(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.total_minutes())

    @pydoc(categories="date")
    def total_seconds(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.total_seconds())

    @pydoc(categories="date")
    def total_milliseconds(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.total_milliseconds())

    @pydoc(categories="date")
    def total_microseconds(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.total_microseconds())

    @pydoc(categories="date")
    def total_nanoseconds(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.total_nanoseconds())

    @pydoc(categories="date")
    def offset_by(self, by: str | td_expr.Expr) -> td_expr.Expr:
        # noinspection PyProtectedMember
        return td_expr.Expr(
            self._expr.offset_by(by=td_translator._unwrap_into_tdexpr(by))
        )

    @pydoc(categories="date")
    def month_start(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.month_start())

    @pydoc(categories="date")
    def month_end(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.month_end())

    @pydoc(categories="date")
    def base_utc_offset(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.base_utc_offset())

    @pydoc(categories="date")
    def dst_offset(self) -> td_expr.Expr:
        return td_expr.Expr(self._expr.dst_offset())
