#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl


def registry():
    from tabsdatak.extensions.tableframe._engine.column.registry import (
        registry as edition_registry,
    )

    return edition_registry()


def is_system_column(name: str) -> bool:
    return registry().is_system_column(name)


def is_user_column(name: str) -> bool:
    return not registry().is_system_column(name)


def is_required_column(name: str) -> bool:
    return name in registry().required()


def system_columns(names: list[str]) -> list[str]:
    return [name for name in names if is_system_column(name)]


def user_columns(names: list[str]) -> list[str]:
    return [name for name in names if is_user_column(name)]


def required_columns() -> list[str]:
    return registry().required()


def drop_system_columns(frame: pl.LazyFrame | pl.DataFrame):
    return frame.drop(system_columns(frame.collect_schema().names()))


def assert_required_columns_present(names: list[str]) -> None:
    missing = [name for name in required_columns() if name not in names]
    assert not missing, f"Missing required system columns: {missing}."
