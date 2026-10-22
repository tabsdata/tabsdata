#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

DEFAULT_MAX_WIDTH = 48
PLACEHOLDER = "-"
TOPIC_DELIMITER = " | "
DETAIL_DELIMITER = " - "

Context = list[str | tuple[str, str]]


class Kind(Enum):
    TEXT = "text"
    ID = "id"
    TIME = "time"
    STATUS = "status"
    NUMBER = "number"
    FLAG = "flag"


@dataclass(frozen=True)
class Column:
    name: str
    kind: Kind = Kind.TEXT
    detail: str | None = None
    width: int | None = None

    @property
    def wraps(self) -> bool:
        return self.kind is Kind.TEXT


@dataclass
class TableView:
    title: str
    columns: list[Column]
    rows: list[list[object]] = field(default_factory=list)
    context: Context = field(default_factory=list)
    count: str | None = None
    notes: list[str] = field(default_factory=list)
    empty: str | None = None
    rules: bool = False
    placeholder: str = PLACEHOLDER
    max_width: int = DEFAULT_MAX_WIDTH

    def footer(self) -> list[str]:
        lines = []
        if self.count is not None:
            lines.append(f"Number of {self.count}: {len(self.rows)}")
        return [*lines, *self.notes]

    def empty_message(self) -> str:
        if self.empty is not None:
            return self.empty
        return f"No {self.count} found." if self.count else "No rows found."


@dataclass
class RecordView:
    title: str
    fields: list[tuple[str, object]]
    context: Context = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    kinds: dict[str, Kind] = field(default_factory=dict)
    widths: dict[str, int] = field(default_factory=dict)
    placeholder: str = PLACEHOLDER
    max_width: int = DEFAULT_MAX_WIDTH

    def footer(self) -> list[str]:
        return list(self.notes)


@dataclass(frozen=True)
class Display:
    browse: bool = False
    wrap: bool = False
    max_width: int | None = None

    def limit(self, view: TableView | RecordView, column: Column | None = None) -> int:
        if self.max_width is not None:
            return self.max_width
        if column is not None and column.width is not None:
            return column.width
        return view.max_width
