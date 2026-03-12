#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias, Union

from tabsdata.connector.cdc.common.typing import (
    OriginPositionSpec,
    TimestampPositionSpec,
)


@dataclass(frozen=True)
class GtidPositionSpec:
    gtid: str


@dataclass(frozen=True)
class BinlogPositionSpec:
    file: str
    pos: int


MySQLCdcStartFromSpec: TypeAlias = Union[
    OriginPositionSpec,
    GtidPositionSpec,
    BinlogPositionSpec,
    TimestampPositionSpec,
]
