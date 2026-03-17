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
class CommitSeqPosition:
    """
    Specifies a Db2 CDC start position based on a commit sequence number.

    Attributes:
        seq: The commit sequence string from which to start reading changes.
    """

    seq: str

    @_td_validate_call(ErrorCode.CDCDB23)
    def __init__(self, seq: str):
        object.__setattr__(self, "seq", seq)


@dataclass(frozen=True, init=False)
class TableCommitSeqPosition:
    """
    Specifies a Db2 CDC start position with per-table commit sequence numbers.

    Attributes:
        seqs: A mapping of qualified table names to their commit sequence strings.
    """

    seqs: dict[str, str]

    @_td_validate_call(ErrorCode.CDCDB24)
    def __init__(self, seqs: dict[str, str]):
        object.__setattr__(self, "seqs", seqs)


Db2CdcStartFromSpec: TypeAlias = Union[
    RelativePositionSpec,
    CommitSeqPosition,
    TableCommitSeqPosition,
    TimestampPosition,
]
