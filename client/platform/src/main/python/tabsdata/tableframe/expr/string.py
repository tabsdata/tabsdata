#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from collections.abc import Mapping

import polars as pl
import polars.expr.string as pl_string

# noinspection PyProtectedMember
from polars._utils.various import NoDefault, no_default

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._translator as td_translator
import tabsdata.tableframe.expr.expr as td_expr
import tabsdata.tableframe.functions.col as td_col

# noinspection PyProtectedMember
import tabsdata.tableframe.typing as td_typing

# noinspection PyProtectedMember
from tabsdata._utils.annotations import pydoc

# noinspection PyProtectedMember
from tabsdata.expansions.tableframe.features.grok.api._handler import GrokParser


# ToDo: This requires some refactoring to unify transformer methods
def _to_tdexpr(expr: pl.Expr) -> td_expr.Expr:
    return td_expr.Expr(expr)


class ExprStringNameSpace:
    def __init__(self, expr: pl_string.ExprStringNameSpace) -> None:
        # noinspection PyProtectedMember
        self._expr = expr

    @pydoc(categories="type_casting")
    def to_date(
        self,
        fmt: str | None = None,
        *,
        strict: bool = True,
    ) -> td_expr.Expr:
        """
        Convert the string to a date.

        Args:
            fmt: The date format string (default %Y-%m-%d)
                 [formats]
                (https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html).
            strict: Whether to parse the date strictly.
        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.to_date().alias("to_date"))
        >>>
        ┌────────────┬────────────┐
        │ a          ┆ to_date    │
        │ ---        ┆ ---        │
        │ str        ┆ date       │
        ╞════════════╪════════════╡
        │ 2024-12-13 ┆ 2024-12-13 │
        │ 2024-12-15 ┆ 2024-12-15 │
        │ null       ┆ null       │
        └────────────┴────────────┘
        """
        return _to_tdexpr(
            self._expr.to_date(format=fmt, strict=strict, exact=True, cache=True)
        )

    # noinspection PyShadowingBuiltins
    @pydoc(categories="type_casting")
    def to_datetime(
        self,
        format: str | None = None,
        *,
        time_unit: td_typing.TimeUnit | None = None,
        time_zone: str | None = None,
        strict: bool = True,
        ambiguous: td_typing.Ambiguous | td_expr.Expr = "raise",
    ) -> td_expr.Expr:
        """
        Convert the string to a datetime.

        Args:
            format: The datetime format string (default %Y-%m-%d %H:%M:%S)
                 [formats]
                (https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html).
            time_unit: {None, ‘us’, ‘ns’, ‘ms’}
                If None (default), it inferred from the format string
            time_zone: Time zone for the resulting value.
            strict: If the conversion fails an error will be raised.
            ambiguous: Policy to apply on ambiguos Datetimes:
                'raise': saises an error
                'earliest': use the earliest datetime
                'latest': use the latest datetime
                'null': set to null

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.to_datetime().alias("to_datetime"))
        >>>
        ┌─────────────────────┬─────────────────────┐
        │ a                   ┆ to_datetime         │
        │ ---                 ┆ ---                 │
        │ str                 ┆ datetime[μs]        │
        ╞═════════════════════╪═════════════════════╡
        │ 2024-12-13 08:45:34 ┆ 2024-12-13 08:45:34 │
        │ 2024-12-15 18:33:00 ┆ 2024-12-15 18:33:00 │
        │ null                ┆ null                │
        └─────────────────────┴─────────────────────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.to_datetime(
                format=format,
                time_unit=time_unit,
                time_zone=time_zone,
                strict=strict,
                exact=True,
                cache=False,
                ambiguous=td_expr.td_translator._unwrap_tdexpr(ambiguous),
            )
        )

    @pydoc(categories="type_casting")
    def to_time(
        self, fmt: str | None = None, *, strict: bool = True, cache: bool = True
    ) -> td_expr.Expr:
        """
        Convert the string to a time.

        Args:
            fmt: The time format string (default %H:%M:%S)
                 [formats]
                (https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html).
            strict: Whether to parse the date strictly.
            cache: Whether to cache the date.
        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.to_time().alias("to_time"))
        >>>
        ┌─────────────────────┬─────────────────────┐
        │ a                   ┆ to_datetime         │
        │ ---                 ┆ ---                 │
        │ str                 ┆ datetime[μs]        │
        ╞═════════════════════╪═════════════════════╡
        │ 2024-12-13 08:45:34 ┆ 2024-12-13 08:45:34 │
        │ 2024-12-15 18:33:00 ┆ 2024-12-15 18:33:00 │
        │ null                ┆ null                │
        └─────────────────────┴─────────────────────┘
        """
        return _to_tdexpr(self._expr.to_time(format=fmt, strict=strict, cache=cache))

    @pydoc(categories="string")
    def len_bytes(self) -> td_expr.Expr:
        """
        Return number of bytes (not chars) of a string.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.len_bytes().alias("len_bytes"))
        >>>
        ┌──────┬────────────┐
        │ a    ┆ to_decimal │
        │ ---  ┆ ---        │
        │ str  ┆ u32        │
        ╞══════╪════════════╡
        │ ab   ┆ 2          │
        │ 再   ┆ 3          │
        │ null ┆ null       │
        └──────┴────────────┘
        """
        return _to_tdexpr(self._expr.len_bytes())

    @pydoc(categories="string")
    def len_chars(self) -> td_expr.Expr:
        """
        Return number of chars (not bytes) of a string.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.len_chars().alias("len_chars"))
        >>>
        ┌──────┬────────────┐
        │ a    ┆ to_decimal │
        │ ---  ┆ ---        │
        │ str  ┆ u32        │
        ╞══════╪════════════╡
        │ ab   ┆ 2          │
        │ 再   ┆ 3          │
        │ null ┆ null       │
        └──────┴────────────┘
        """
        return _to_tdexpr(self._expr.len_chars())

    @pydoc(categories="string")
    def to_uppercase(self) -> td_expr.Expr:
        """
        Return the uppercase of a string.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.to_uppercase().alias("to_uppercase"))
        >>>
        ┌──────┬──────────────┐
        │ a    ┆ to_uppercase │
        │ ---  ┆ ---          │
        │ str  ┆ u32          │
        ╞══════╪══════════════╡
        │ aB   ┆ AB           │
        │ null ┆ null         │
        └──────┴──────────────┘
        """
        return _to_tdexpr(self._expr.to_uppercase())

    @pydoc(categories="string")
    def to_lowercase(self) -> td_expr.Expr:
        """
        Return the lowercase of a string.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.to_lowercase().alias("to_lowercase"))
        >>>
        ┌──────┬───────────────┐
        │ a    ┆ to_lowerrcase │
        │ ---  ┆ ---           │
        │ str  ┆ u32           │
        ╞══════╪═══════════════╡
        │ aB   ┆ ab            │
        │ null ┆ null          │
        └──────┴───────────────┘
        """
        return _to_tdexpr(self._expr.to_lowercase())

    @pydoc(categories="string")
    def to_titlecase(self) -> td_expr.Expr:
        """
        Uppercase the first character and lowercase all the others ones of a string.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.to_titlecase().alias("titlecase"))
        >>>
        ┌──────┬───────────┐
        │ a    ┆ titlecase │
        │ ---  ┆ ---       │
        │ str  ┆ str       │
        ╞══════╪═══════════╡
        │ ab   ┆ Ab        │
        │ Ab   ┆ Ab        │
        │ AB   ┆ Ab        │
        │ aB   ┆ Ab        │
        │ null ┆ null      │
        └──────┴───────────┘
        """
        return _to_tdexpr(self._expr.to_titlecase())

    @pydoc(categories="string")
    def strip_chars(self, characters: td_typing.IntoExpr = None) -> td_expr.Expr:
        """
        Trim string values.

        Args:
            characters: Characters to trim from start and end of the string.
                        All characteres in the given string are removed,
                        regardless the order. Default is whitespace.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a")
              .str.strip_chars("a ").alias("strip_chars"))
        >>>
        ┌─────────────────────────────────┬─────────────┐
        │ a                               ┆ strip_chars │
        │ ---                             ┆ ---         │
        │ str                             ┆ str         │
        ╞═════════════════════════════════╪═════════════╡
        │ acba cda                      … ┆ cba cd      │
        │    xy z                         ┆ xy z        │
        │ null                            ┆ null        │
        └─────────────────────────────────┴─────────────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.strip_chars(
                characters=td_translator._unwrap_into_tdexpr(characters)
            )
        )

    @pydoc(categories="string")
    def strip_chars_start(self, characters: td_typing.IntoExpr = None) -> td_expr.Expr:
        """
        Trim string values from the start of the string.

        Args:
            characters: Characters to trim from start of the string.
                        All starting characteres in the given string are removed,
                        regardless the order. Default is whitespace.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a")
              .str.strip_chars_start("abc").alias("strip_chars_start"))
        >>>
        ┌───────────────────────────────┬────────────────────────────┐
        │ a                             ┆ strip_chars_start          │
        │ ---                           ┆ ---                        │
        │ str                           ┆ str                        │
        ╞═══════════════════════════════╪════════════════════════════╡
        │ cba cd                        ┆  cd                        │
        │    xy z                       ┆    xy z                    │
        │ null                          ┆ null                       │
        └───────────────────────────────┴────────────────────────────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.strip_chars_start(
                characters=td_translator._unwrap_into_tdexpr(characters)
            )
        )

    @pydoc(categories="string")
    def strip_chars_end(self, characters: td_typing.IntoExpr = None) -> td_expr.Expr:
        """
        Trim string values from the end of the string.

        Args:
            characters: Characters to trim from start of the string.
                        All ending characteres in the given string are removed,
                        regardless the order. Default is whitespace.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a")
              .str.strip_chars_end("dc ").alias("strip_chars_end"))
        >>>
        ┌───────────────────────────────┬─────────────────┐
        │ a                             ┆ strip_chars_end │
        │ ---                           ┆ ---             │
        │ str                           ┆ str             │
        ╞═══════════════════════════════╪═════════════════╡
        │ cba cd                        ┆ cba             │
        │    xy z                       ┆    xy z         │
        │ null                          ┆ null            │
        └───────────────────────────────┴─────────────────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.strip_chars_end(
                characters=td_translator._unwrap_into_tdexpr(characters)
            )
        )

    @pydoc(categories="string")
    def strip_prefix(self, prefix: td_typing.IntoExpr) -> td_expr.Expr:
        """
        Trim string values removing the given prefix

        Args:
            prefix: Prefix to remove from the string.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a")
              .str.strip_prefix("cb").alias("strip_prefix"))
        >>>
        ┌───────────────────────────────┬─────────────────┐
        │ a                             ┆ strip_prefix    │
        │ ---                           ┆ ---             │
        │ str                           ┆ str             │
        ╞═══════════════════════════════╪═════════════════╡
        │ cba cd                        ┆ a cd            │
        │ bx                            ┆ bx              │
        │ null                          ┆ null            │
        └───────────────────────────────┴─────────────────┘
        """
        return _to_tdexpr(self._expr.strip_prefix(prefix=prefix))

    @pydoc(categories="string")
    def strip_suffix(self, suffix: td_typing.IntoExpr) -> td_expr.Expr:
        """
        Trim string values removing the given suffix

        Args:
            suffix: Suffix to remove from the string.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a")
              .str.strip_suffix("cd").alias("strip_suffix"))
        >>>
        ┌───────────────────────────────┬─────────────────┐
        │ a                             ┆ strip_suffix    │
        │ ---                           ┆ ---             │
        │ str                           ┆ str             │
        ╞═══════════════════════════════╪═════════════════╡
        │ cba cd                        ┆ cba             │
        │ bx                            ┆ bx              │
        │ null                          ┆ null            │
        └───────────────────────────────┴─────────────────┘
        """
        return _to_tdexpr(self._expr.strip_suffix(suffix=suffix))

    @pydoc(categories="string")
    def pad_start(self, length: int, fill_char: str = " ") -> td_expr.Expr:
        """
        Pad string values at the front to the given length using the given
        fill character.

        Args:
            length: The length to front pad the string to.
            fill_char: The character to use for padding.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.pad_start(6, "-").alias("pad_start"))
        >>>
        ┌────────┬───────────┐
        │ a      ┆ pad_start │
        │ ---    ┆ ---       │
        │ str    ┆ str       │
        ╞════════╪═══════════╡
        │ abc    ┆ ---abc    │
        │    def ┆    def    │
        │ null   ┆ null      │
        └────────┴───────────┘
        """
        return _to_tdexpr(self._expr.pad_start(length=length, fill_char=fill_char))

    @pydoc(categories="string")
    def pad_end(self, length: int, fill_char: str = " ") -> td_expr.Expr:
        """
        Pad string values at the end to the given length using the given
        fill character.

        Args:
            length: The length to end pad the string to.
            fill_char: The character to use for padding.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.pad_end(6, "-").alias("pad_end"))
        >>>
        ┌────────┬─────────┐
        │ a      ┆ pad_end │
        │ ---    ┆ ---     │
        │ str    ┆ str     │
        ╞════════╪═════════╡
        │ abc    ┆ abc---  │
        │    def ┆    def  │
        │ null   ┆ null    │
        └────────┴─────────┘
        """
        return _to_tdexpr(self._expr.pad_end(length=length, fill_char=fill_char))

    @pydoc(categories="string")
    def zfill(self, length: int | td_typing.IntoExprColumn) -> td_expr.Expr:
        """
        Pad numeric string values at the start to the given length using zeros.

        Args:
            length: The length to end pad the string to.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.zfill(2).alias("zfill"))
        >>>
        ┌──────┬───────┐
        │ a    ┆ zfill │
        │ ---  ┆ ---   │
        │ str  ┆ str   │
        ╞══════╪═══════╡
        │ 0    ┆ 00    │
        │ 1    ┆ 01    │
        │ 1000 ┆ 1000  │
        │ null ┆ null  │
        └──────┴───────┘
        """
        return _to_tdexpr(self._expr.zfill(length=length))

    @pydoc(categories="string")
    def contains(
        self,
        pattern: str | td_expr.Expr,
        *,
        literal: bool = False,
        strict: bool = True,
    ) -> td_expr.Expr:
        """
        Evaluate if the string contains a pattern.

        Args:
            pattern: The pattern to search for.
            literal: Take the pattern as a literal string (not a regex).
            strict: if the given pattern is not valid regex, raise an error.
        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.contains("ab").alias("contains"))
        >>>
        ┌──────┬──────────┐
        │ a    ┆ contains │
        │ ---  ┆ ---      │
        │ str  ┆ bool     │
        ╞══════╪══════════╡
        │ a    ┆ false    │
        │ ab   ┆ true     │
        │ b    ┆ false    │
        │ xaby ┆ true     │
        │ null ┆ null     │
        └──────┴──────────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.contains(
                pattern=td_expr.td_translator._unwrap_tdexpr(pattern),
                literal=literal,
                strict=strict,
            )
        )

    @pydoc(categories="string")
    def find(
        self,
        pattern: str | td_expr.Expr,
        *,
        literal: bool = False,
        strict: bool = True,
    ) -> td_expr.Expr:
        """
        Find the position of the first occurrence of the given pattern.

        Args:
            pattern: The pattern to search for.
            literal: Take the pattern as a literal string (not a regex).
            strict: if the given pattern is not valid regex, raise an error.
        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.find("b").alias("find"))
        >>>
        ┌──────┬──────┐
        │ a    ┆ find │
        │ ---  ┆ ---  │
        │ str  ┆ u32  │
        ╞══════╪══════╡
        │ a    ┆ null │
        │ ab   ┆ 1    │
        │ b    ┆ 0    │
        │ xaby ┆ 2    │
        │ null ┆ null │
        └──────┴──────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.find(
                pattern=td_expr.td_translator._unwrap_tdexpr(pattern),
                literal=literal,
                strict=strict,
            )
        )

    @pydoc(categories="string")
    def ends_with(self, suffix: str | td_expr.Expr) -> td_expr.Expr:
        """
        Evaluate if the string ends with.

        Args:
            suffix: The suffix to search for.
        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.ends_with("b").alias("ends_with"))
        >>>
        ┌──────┬───────────┐
        │ a    ┆ ends_with │
        │ ---  ┆ ---       │
        │ str  ┆ bool      │
        ╞══════╪═══════════╡
        │ a    ┆ false     │
        │ ab   ┆ true      │
        │ b    ┆ true      │
        │ xaby ┆ false     │
        │ null ┆ null      │
        └──────┴───────────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.ends_with(suffix=td_expr.td_translator._unwrap_tdexpr(suffix))
        )

    @pydoc(categories="string")
    def starts_with(self, prefix: str | td_expr.Expr) -> td_expr.Expr:
        """
        Evaluate if the string start with.

        Args:
            prefix: The suffix to search for.
        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a")
              .str.starts_with("a").alias("starts_with"))
        >>>
        ┌──────┬────────────┐
        │ a    ┆ start_with │
        │ ---  ┆ ---        │
        │ str  ┆ bool       │
        ╞══════╪════════════╡
        │ a    ┆ true       │
        │ ab   ┆ true       │
        │ b    ┆ false      │
        │ xaby ┆ false      │
        │ null ┆ null       │
        └──────┴────────────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.starts_with(prefix=td_expr.td_translator._unwrap_tdexpr(prefix))
        )

    @pydoc(categories="string")
    def extract(
        self, pattern: td_typing.IntoExprColumn, group_index: int = 1
    ) -> td_expr.Expr:
        """
        Extract a pattern from the string.

        Args:
            pattern: The pattern to extract.
            group_index: The group index to extract.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.extract("(b.b)", 1).alias("extract"))
        >>>
        ┌───────────┬─────────┐
        │ a         ┆ extract │
        │ ---       ┆ ---     │
        │ str       ┆ str     │
        ╞═══════════╪═════════╡
        │ a bAb c d ┆ bAb     │
        │ bCbb c d  ┆ bCb     │
        │ bb        ┆ null    │
        │ null      ┆ null    │
        └───────────┴─────────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.extract(
                pattern=td_translator._unwrap_into_tdexpr_column(pattern),
                group_index=group_index,
            )
        )

    @pydoc(categories="string")
    def count_matches(
        self, pattern: str | td_expr.Expr, *, literal: bool = False
    ) -> td_expr.Expr:
        """
        Counts the ocurrrences of the given pattern in the string.

        Args:
            pattern: The pattern to extract.
            literal: Take the pattern as a literal string (not a regex).

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a")
              .str.count_matches("b.").alias("count_matches"))
        >>>
        ┌───────────┬───────────────┐
        │ a         ┆ count_matches │
        │ ---       ┆ ---           │
        │ str       ┆ u32           │
        ╞═══════════╪═══════════════╡
        │ a bAb c d ┆ 2             │
        │ bCbb c d  ┆ 2             │
        │ bb        ┆ 1             │
        │ b         ┆ 0             │
        │ a         ┆ 0             │
        │ null      ┆ null          │
        └───────────┴───────────────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.count_matches(
                pattern=td_expr.td_translator._unwrap_tdexpr(pattern), literal=literal
            )
        )

    @pydoc(categories="string")
    def replace(
        self,
        pattern: str | td_expr.Expr,
        value: str | td_expr.Expr,
        *,
        literal: bool = False,
        n: int = 1,
    ) -> td_expr.Expr:
        """
        Replace the first occurence of a pattern with the given string.

        Args:
            pattern: The pattern to replace.
            value: The value to replace the pattern with.
            literal: Take the pattern as a literal string (not a regex).
            n: Number of matches to replace.
        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.replace("b", "X").alias("replace"))
        >>>
        ┌───────────┬───────────┐
        │ a         ┆ replace   │
        │ ---       ┆ ---       │
        │ str       ┆ str       │
        ╞═══════════╪═══════════╡
        │ a bAb c d ┆ a XAb c d │
        │ bCbb c d  ┆ XCbb c d  │
        │ bb        ┆ Xb        │
        │ b         ┆ X         │
        │ a         ┆ a         │
        │ null      ┆ null      │
        └───────────┴───────────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.replace(
                pattern=td_expr.td_translator._unwrap_tdexpr(pattern),
                value=td_expr.td_translator._unwrap_tdexpr(value),
                literal=literal,
                n=n,
            )
        )

    @pydoc(categories="string")
    def replace_all(
        self,
        pattern: str | td_expr.Expr,
        value: str | td_expr.Expr,
        *,
        literal: bool = False,
    ) -> td_expr.Expr:
        """
        Replace the all occurences of a pattern with the given string.

        Args:
            pattern: The pattern to replace.
            value: The value to replace the pattern with.
            literal: Take the pattern as a literal string (not a regex).

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.replace("b", "X").alias("replace"))
        >>>
        ┌───────────┬─────────────┐
        │ a         ┆ replace_all │
        │ ---       ┆ ---         │
        │ str       ┆ str         │
        ╞═══════════╪═════════════╡
        │ a bAb c d ┆ a XAX c d   │
        │ bCbb c d  ┆ XCXX c d    │
        │ bb        ┆ XX          │
        │ b         ┆ X           │
        │ a         ┆ a           │
        │ null      ┆ null        │
        └───────────┴─────────────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.replace_all(
                pattern=td_expr.td_translator._unwrap_tdexpr(pattern),
                value=td_expr.td_translator._unwrap_tdexpr(value),
                literal=literal,
            )
        )

    @pydoc(categories="string")
    def reverse(self) -> td_expr.Expr:
        """
        Reverse the string.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.reverse().alias("reverse"))
        >>>
        ┌──────┬─────────┐
        │ a    ┆ reverse │
        │ ---  ┆ ---     │
        │ str  ┆ str     │
        ╞══════╪═════════╡
        │ abc  ┆ cba     │
        │ a    ┆ a       │
        │ null ┆ null    │
        └──────┴─────────┘
        """
        return _to_tdexpr(self._expr.reverse())

    @pydoc(categories="string")
    def slice(
        self,
        offset: int | td_typing.IntoExprColumn,
        length: int | td_typing.IntoExprColumn | None = None,
    ) -> td_expr.Expr:
        """
        Extract the substring at the given offset for the given length.

        Args:
            offset: The offset to start the slice.
            length: The length of the slice. If None, slice until the end of the string.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.slice(1,1).alias("slice"))
        >>>
        ┌──────┬───────┐
        │ a    ┆ slice │
        │ ---  ┆ ---   │
        │ str  ┆ str   │
        ╞══════╪═══════╡
        │ abc  ┆ b     │
        │ a    ┆       │
        │ null ┆ null  │
        └──────┴───────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.slice(
                offset=td_translator._unwrap_into_tdexpr(offset),
                length=td_translator._unwrap_into_tdexpr(length),
            )
        )

    @pydoc(categories="string")
    def head(self, n: int | td_typing.IntoExprColumn) -> td_expr.Expr:
        """
        Extract the start of the string up to the given length.

        Args:
            n: The length of the head.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.head(2).alias("head"))
        >>>
        ┌──────┬──────┐
        │ a    ┆ head │
        │ ---  ┆ ---  │
        │ str  ┆ str  │
        ╞══════╪══════╡
        │ abc  ┆ ab   │
        │ a    ┆ a    │
        │ null ┆ null │
        └──────┴──────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.head(n=td_translator._unwrap_into_tdexpr_column(n))
        )

    @pydoc(categories="string")
    def tail(self, n: int | td_typing.IntoExprColumn) -> td_expr.Expr:
        """
        Extract the end of the string up to the given length.

        Args:
            n: The length of the tail.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a").str.tail(2).alias("tail"))
        >>>
        ┌──────┬──────┐
        │ a    ┆ tail │
        │ ---  ┆ ---  │
        │ str  ┆ str  │
        ╞══════╪══════╡
        │ abc  ┆ bc   │
        │ a    ┆ a    │
        │ null ┆ null │
        └──────┴──────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.tail(n=td_translator._unwrap_into_tdexpr_column(n))
        )

    @pydoc(categories="type_casting")
    def to_integer(
        self, *, base: int | td_typing.IntoExprColumn = 10, strict: bool = True
    ) -> td_expr.Expr:
        """
        Covert a string to integer.

        Args:
            base: The base of the integer.
            strict: If true, raise an error if the string is not a valid integer.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a")
              .str.to_integer(strict=False).alias("to_integer"))
        >>>
        ┌──────┬────────────┐
        │ a    ┆ to_integer │
        │ ---  ┆ ---        │
        │ str  ┆ i64        │
        ╞══════╪════════════╡
        │ 1    ┆ 1          │
        │ 2.2  ┆ null       │
        │ a    ┆ null       │
        │ null ┆ null       │
        └──────┴────────────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.to_integer(
                base=td_translator._unwrap_into_tdexpr_column(base), strict=strict
            )
        )

    @pydoc(categories="string")
    def contains_any(
        self, patterns: td_typing.IntoExpr, *, ascii_case_insensitive: bool = False
    ) -> td_expr.Expr:
        """
        Evaluate if the string contains any of the given patterns.

        Args:
            patterns: The patterns to search for.
            ascii_case_insensitive: If true, the search is case-insensitive.
        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a")
              .str.contains_any(["a", "b"]).alias("contains_any"))
        >>>
        ┌──────┬──────────────┐
        │ a    ┆ contains_any │
        │ ---  ┆ ---          │
        │ str  ┆ bool         │
        ╞══════╪══════════════╡
        │ abc  ┆ true         │
        │ axy  ┆ true         │
        │ xyb  ┆ true         │
        │ xyz  ┆ false        │
        │ null ┆ null         │
        └──────┴──────────────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.contains_any(
                patterns=td_translator._unwrap_into_tdexpr(patterns),
                ascii_case_insensitive=ascii_case_insensitive,
            )
        )

    @pydoc(categories="string")
    def replace_many(
        self,
        patterns: td_typing.IntoExpr | Mapping[str, str],
        replace_with: td_typing.IntoExpr | NoDefault = no_default,
        *,
        ascii_case_insensitive: bool = False,
    ) -> td_expr.Expr:
        """
        Replace the all occurences of any the given patterns with the given string.

        Args:
            patterns: The patterns to replace.
            replace_with: The value to replace the pattern with.
            ascii_case_insensitive: If true, the search is case-insensitive.

        Example:

        >>> import tabsdata as td
        >>>
        >>> tf: td.TableFrame ...
        >>>
        >>> tf.select(td.col("a"), td.col("a")
              .str.replace_many(["a", "b"], "X").alias("replace_many"))
        >>>
        ┌──────┬──────────────┐
        │ a    ┆ replace_many │
        │ ---  ┆ ---          │
        │ str  ┆ str          │
        ╞══════╪══════════════╡
        │ abc  ┆ XXc          │
        │ axy  ┆ Xxy          │
        │ xyb  ┆ xyX          │
        │ xyz  ┆ xyz          │
        │ null ┆ null         │
        └──────┴──────────────┘
        """
        # noinspection PyProtectedMember
        return _to_tdexpr(
            self._expr.replace_many(
                patterns=td_translator._unwrap_into_tdexpr(patterns),
                replace_with=td_translator._unwrap_into_tdexpr(replace_with),
                ascii_case_insensitive=ascii_case_insensitive,
            )
        )

    @pydoc(categories="string")
    def grok(self, pattern: str, schema: dict[str, td_col.Column]) -> td_expr.Expr:
        """
        Parse log text into structured fields using a Grok pattern.

        Applies the given Grok pattern to the values in the current string expression.
        Each **named capture group** in the pattern becomes a new output column.
        Rows that do not match the pattern will return `null` for the extracted fields.

        Args:
            pattern (str): Grok pattern with named captures (e.g., `%{WORD:user}`).
            schema (dict[str, td_col.Column]): A mapping where each capture name
                is associated with its corresponding column definition, specifying
                both the column name and its data type.
        Example:

        >>> import tabsdata as td
        >>> tf = td.TableFrame({"logs": [
        ...     "alice-login-2023",
        ...     "bob-logout-2024",
        ... ]})
        >>>
        >>> log_pattern = r"%{WORD:user}-%{WORD:action}-%{INT:year}"
        >>> log_schema = {
        >>>     "word": td_col.Column("user", td.String),
        >>>     "action": td_col.Column("action", td.String),
        >>>     "year": td_col.Column("year", td.Int8),
        >>> }
        >>> out = tf.grok("logs", log_pattern, log_schema)
        >>> tf.select(
        ...     td.col("logs"),
        ...     td.col("logs").str.grok(log_pattern, log_schema)
        ... )
        >>>
        ┌──────────────────┬───────┬────────┬──────┐
        │ logs             ┆ user  ┆ action ┆ year │
        │ ---              ┆ ---   ┆ ---    ┆ ---  │
        │ str              ┆ str   ┆ str    ┆ i64  │
        ╞══════════════════╪═══════╪════════╪══════╡
        │ alice-login-2023 ┆ alice ┆ login  ┆ 2023 │
        │ bob-logout-2024  ┆ bob   ┆ logout ┆ 2024 │
        └──────────────────┴───────┴────────┴──────┘

        Notes:
            - The function automatically expands the Grok captures into separate
              columns.
            - Non-matching rows will show `null` for the extracted columns.
            - If a pattern defines duplicate capture names, numeric suffixes like
              `field`, `field[1]` will be used to disambiguate them.
        """

        # noinspection PyProtectedMember
        return _to_tdexpr(
            GrokParser(pattern, schema).rust(pl.Expr._from_pyexpr(self._expr._pyexpr))
        )
