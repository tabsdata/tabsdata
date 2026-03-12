#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias, Union

from tabsdata.connector.cdc.common.typing import OriginPositionSpec


@dataclass(frozen=True)
class LsnPositionSpec:
    lsn: int


PostgresCdcStartFromSpec: TypeAlias = Union[
    OriginPositionSpec,
    LsnPositionSpec,
]


SlotBehaviorSpec = Literal[
    "create",
    "reuse",
]
