#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING

import polars as pl

from tabsdatak.server._tdrunner.function._handlers.tables import TableFile

if TYPE_CHECKING:
    from tabsdatak.tableframe import TableFrame
    from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode


# The form content takes at each point where a run reads or writes it. Tables are
# the content functions store as their tables; data is any other content. Plugins
# take part in INGRESS and EGRESS, where data enters or leaves the system;
# functions take part in HOST and LAND, where their output becomes tables or data.
# Each zone has a ColumnRule stating how its files are read back into a TableFrame
# and which Shape a frame takes when written to it.
class Zone(Enum):
    # Input data, as read from outside the system through a source plugin.
    INGRESS = "ingress"
    # A function's output, as written to tables.
    HOST = "host"
    # A function's output, as written to data.
    LAND = "land"
    # Output data, as written outside the system through a destination plugin.
    EGRESS = "egress"

    @property
    def rule(self) -> ColumnRule:
        from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

        match self:
            case Zone.INGRESS:
                return ColumnRule(
                    entry=None,
                    exit=None,
                )
            case Zone.HOST:
                return ColumnRule(
                    entry=None,
                    exit=Shape.INTERNAL,
                )
            case Zone.LAND:
                return ColumnRule(
                    entry=CreationMode.TAB,
                    exit=Shape.INTERNAL,
                )
            case Zone.EGRESS:
                return ColumnRule(
                    entry=None,
                    exit=Shape.PUBLIC,
                )


# The set of columns a frame carries, and their state, when it is written.
class Shape(Enum):
    # Frames as functions produce them: user columns plus the system and connector
    # columns the system retains, with the per-column inception policy applied.
    INTERNAL = auto()
    # Frames as functions receive them: data as read, with the column processing
    # that prepares it for the function.
    EXTERNAL = auto()
    # Frames as the system sees them: every column it handles, including virtual
    # ones.
    PRIVATE = auto()
    # Frames as external systems see them: generally user columns only, without
    # system or connector columns.
    PUBLIC = auto()


@dataclass(frozen=True)
class ColumnRule:
    entry: CreationMode | None
    exit: Shape | None


class ColumnPolicyError(ValueError):
    pass


@dataclass(frozen=True)
class ColumnPolicy:
    zone: Zone

    @property
    def rule(self) -> ColumnRule:
        return self.zone.rule

    def scan(self, source: TableFile) -> TableFrame:
        from tabsdatak.tableframe import TableFrame

        if self.rule.entry is None:
            raise ColumnPolicyError(f"Zone {self.zone.name} has no rule to read a file")
        return TableFrame._build(
            source.scan(),
            self.rule.entry,
        )

    def sink(self, frame: TableFrame, target: TableFile) -> pl.LazyFrame:
        return target.sink(
            frame._observed(
                self.extract(frame),
                target.uri,
            ),
        )

    def extract(self, frame: TableFrame) -> pl.LazyFrame:
        match self.rule.exit:
            case Shape.INTERNAL:
                return frame._persisted()
            case Shape.PUBLIC:
                return frame._strip(
                    drop_system_columns=True,
                    drop_internal_columns=True,
                )
            case Shape.EXTERNAL | Shape.PRIVATE:
                raise ColumnPolicyError(
                    f"Writing a frame with the {self.rule.exit.name} shape is not "
                    "supported yet"
                )
            case None:
                raise ColumnPolicyError(
                    f"Zone {self.zone.name} has no rule to write a frame"
                )
