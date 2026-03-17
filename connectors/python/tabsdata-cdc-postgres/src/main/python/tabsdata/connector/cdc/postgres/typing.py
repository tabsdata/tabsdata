#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias, Union

# noinspection PyProtectedMember
from tabsdata._utils.validation import _td_validate_call
from tabsdata.connector.cdc.common.typing import RelativePositionSpec
from tabsdata.exceptions import ErrorCode


@dataclass(frozen=True, init=False)
class LsnPosition:
    """
    Specifies a PostgreSQL CDC start position based on a Log Sequence Number (LSN).

    Attributes:
        lsn: The LSN from which to start reading changes.
    """

    lsn: int

    @_td_validate_call(ErrorCode.CDCPOSTGRES7)
    def __init__(self, lsn: int):
        object.__setattr__(self, "lsn", lsn)


PostgresCdcStartFromSpec: TypeAlias = Union[
    RelativePositionSpec,
    LsnPosition,
]


ReplicationSlotBehaviorSpec = Literal[
    "create",
    "reuse",
]
