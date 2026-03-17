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
    TimestampPositionSpec,
)
from tabsdata.exceptions import ErrorCode


@dataclass(frozen=True, init=False)
class CommitSeqPositionSpec:
    seq: str

    @_td_validate_call(ErrorCode.CDCDB23)
    def __init__(self, seq: str):
        object.__setattr__(self, "seq", seq)


@dataclass(frozen=True, init=False)
class TableCommitSeqPositionSpec:
    seqs: dict[str, str]

    @_td_validate_call(ErrorCode.CDCDB24)
    def __init__(self, seqs: dict[str, str]):
        object.__setattr__(self, "seqs", seqs)


Db2CdcStartFromSpec: TypeAlias = Union[
    RelativePositionSpec,
    CommitSeqPositionSpec,
    TableCommitSeqPositionSpec,
    TimestampPositionSpec,
]
