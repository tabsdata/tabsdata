#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import threading
from collections.abc import Iterator
from typing import Any, TypeVar

import polars as pl

T = TypeVar("T")

_lock = threading.Lock()
_failure: Exception | None = None


def record(error: Exception) -> None:
    global _failure
    with _lock:
        _failure = error


def forget() -> None:
    global _failure
    with _lock:
        _failure = None


def taken() -> Exception | None:
    global _failure
    with _lock:
        error, _failure = _failure, None
    return error


def recorded(items: Iterator[T]) -> Iterator[T]:
    try:
        yield from items
    except Exception as error:
        record(error)
        raise


class RecordedStream:
    def __init__(self, source: Any) -> None:
        self.source = source

    def read(self, size: int = -1) -> bytes:
        try:
            return self.source.read(size)
        except Exception as error:
            record(error)
            raise

    def __getattr__(self, name: str) -> Any:
        return getattr(self.source, name)


def collected(plan: pl.LazyFrame, **options: Any) -> pl.DataFrame:
    forget()
    try:
        return plan.collect(**options)
    except pl.exceptions.PolarsError as error:
        original = taken()
        if original is None:
            raise
        raise original from error
