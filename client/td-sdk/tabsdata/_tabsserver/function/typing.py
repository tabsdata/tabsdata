#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from enum import Enum, auto
from typing import TypeAlias

from tabsdata.tableframe.lazyframe.frame import TableFrame


class DerivedTableFrameStatus(Enum):
    INITIALIZED = auto()
    COMPUTED = auto()
    HARVESTED = auto()


InputTableSpec: TypeAlias = list[TableFrame | None | list[TableFrame | None]]
OutputTableSpec: TypeAlias = list[TableFrame | None]

SimpleTableSpec: TypeAlias = TableFrame | None
OriginalTableSpec: TypeAlias = SimpleTableSpec | list[SimpleTableSpec]
DerivedTableSpec: TypeAlias = SimpleTableSpec


class DerivedTableFrame:

    def __init__(
        self,
        status: DerivedTableFrameStatus,
        original_tf: OriginalTableSpec,
        derived_tf: DerivedTableSpec,
    ):
        self.status = status
        self.original_tf = original_tf
        self.derived_tf = derived_tf

    @property
    def status(self) -> DerivedTableFrameStatus:
        return self._status

    @status.setter
    def status(self, status: DerivedTableFrameStatus):
        self._status = status

    @property
    def original_tf(self) -> list[SimpleTableSpec]:
        return self._original_tf

    @original_tf.setter
    def original_tf(self, original_tf: InputTableSpec):
        match original_tf:
            case None:
                self._original_tf = []
            case TableFrame():
                self._original_tf = [original_tf]
            case list():
                self._original_tf = original_tf
            case _:
                raise ValueError(f"Invalid type for original_tf: {type(original_tf)}")

    @property
    def derived_tf(self) -> SimpleTableSpec:
        return self._derived_tf

    @derived_tf.setter
    def derived_tf(self, derived_tf: DerivedTableSpec):
        match derived_tf:
            case None:
                self._derived_tf = None
            case TableFrame():
                self._derived_tf = derived_tf
            case _:
                raise ValueError(f"Invalid type for derived_tf: {type(derived_tf)}")


DerivedTableFrameSpec: TypeAlias = list[DerivedTableFrame | None]
