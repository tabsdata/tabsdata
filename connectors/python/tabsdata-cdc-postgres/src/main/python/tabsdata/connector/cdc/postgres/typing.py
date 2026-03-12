#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias, Union

from tabsdata.connector.cdc.common.typing import RelativePositionSpec


@dataclass(frozen=True)
class LsnPositionSpec:
    lsn: int


PostgresCdcStartFromSpec: TypeAlias = Union[
    RelativePositionSpec,
    LsnPositionSpec,
]


ReplicationSlotBehaviorSpec = Literal[
    "create",
    "reuse",
]
