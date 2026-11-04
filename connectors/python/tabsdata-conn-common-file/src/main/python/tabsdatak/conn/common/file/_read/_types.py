#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Mapping

import polars as pl


class TypeConflictError(ValueError):
    pass


# The type that serves the values of two parts of a file:
# - the same type: itself;
# - two integer types: the wider;
# - integers and floats: Float64;
# - objects: their fields merged, lists: their items merged;
# - a nested type with any other: no type serves both;
# - any other pair: String.
def merge(name: str, first: pl.DataType, second: pl.DataType) -> pl.DataType:
    if first == second:
        return first
    if first == pl.Null:
        return second
    if second == pl.Null:
        return first
    if first.is_integer() and second.is_integer():
        return widest(first, second)
    if first.is_numeric() and second.is_numeric():
        return pl.Float64()
    if isinstance(first, pl.Struct) and isinstance(second, pl.Struct):
        return pl.Struct(merged(name, fields(first), fields(second)))
    if isinstance(first, pl.List) and isinstance(second, pl.List):
        return pl.List(merge(name, first.inner, second.inner))
    if first.is_nested() or second.is_nested():
        raise TypeConflictError(
            f"The field '{name}' holds both {first} and {second} values",
        )
    return pl.String()


def widest(first: pl.DataType, second: pl.DataType) -> pl.DataType:
    probe = pl.concat(
        [pl.DataFrame(schema={"v": first}), pl.DataFrame(schema={"v": second})],
        how="vertical_relaxed",
    )
    return probe.schema["v"]


def fields(dtype: pl.Struct) -> dict[str, pl.DataType]:
    return {field.name: field.dtype for field in dtype.fields}


def merged(
    name: str,
    first: Mapping[str, pl.DataType],
    second: Mapping[str, pl.DataType],
) -> dict[str, pl.DataType]:
    result = dict(first)
    for key, dtype in second.items():
        known = result.get(key)
        result[key] = dtype if known is None else merge(f"{name}.{key}", known, dtype)
    return result


def resolved(dtype: pl.DataType) -> pl.DataType:
    if dtype == pl.Null:
        return pl.String()
    if isinstance(dtype, pl.Struct):
        return pl.Struct({key: resolved(inner) for key, inner in fields(dtype).items()})
    if isinstance(dtype, pl.List):
        return pl.List(resolved(dtype.inner))
    return dtype


class Fold:
    def __init__(self) -> None:
        self.types: dict[str, pl.DataType] = {}

    def observe(self, types: Mapping[str, pl.DataType]) -> None:
        for name, dtype in types.items():
            known = self.types.get(name)
            self.types[name] = dtype if known is None else merge(name, known, dtype)

    def schema(self, columns: list[str] | None = None) -> pl.Schema:
        names = list(self.types) if columns is None else columns
        return pl.Schema({
            name: resolved(self.types.get(name, pl.Null())) for name in names
        })
