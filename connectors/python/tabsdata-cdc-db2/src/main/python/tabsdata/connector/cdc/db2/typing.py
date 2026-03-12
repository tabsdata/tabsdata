#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias, Union

from tabsdata.connector.cdc.common.typing import (
    RelativePositionSpec,
    TimestampPositionSpec,
)


@dataclass(frozen=True)
class CommitSeqPositionSpec:
    seq: str


@dataclass(frozen=True)
class TableCommitSeqPositionSpec:
    seqs: dict[str, str]


Db2CdcStartFromSpec: TypeAlias = Union[
    RelativePositionSpec,
    CommitSeqPositionSpec,
    TableCommitSeqPositionSpec,
    TimestampPositionSpec,
]
