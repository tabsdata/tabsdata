#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias, Union

# noinspection PyProtectedMember
from tabsdata._utils.validation import _td_validate_call
from tabsdata.connector.cdc.common.typing import (
    RelativePositionSpec,
    TimestampPosition,
)
from tabsdata.exceptions import ErrorCode


@dataclass(frozen=True, init=False)
class GtidPosition:
    """
    Specifies a MySQL CDC start position based on a Global Transaction ID (GTID).

    Attributes:
        gtid: The GTID string from which to start reading changes.
    """

    gtid: str

    @_td_validate_call(ErrorCode.CDCMYSQL4)
    def __init__(self, gtid: str):
        object.__setattr__(self, "gtid", gtid)


@dataclass(frozen=True, init=False)
class BinlogPosition:
    """
    Specifies a MySQL CDC start position based on a binlog file and position.

    Attributes:
        file: The binlog file name.
        pos: The byte position within the binlog file.
    """

    file: str
    pos: int

    @_td_validate_call(ErrorCode.CDCMYSQL5)
    def __init__(self, file: str, pos: int):
        object.__setattr__(self, "file", file)
        object.__setattr__(self, "pos", pos)


MySQLCdcStartFromSpec: TypeAlias = Union[
    RelativePositionSpec,
    GtidPosition,
    BinlogPosition,
    TimestampPosition,
]
