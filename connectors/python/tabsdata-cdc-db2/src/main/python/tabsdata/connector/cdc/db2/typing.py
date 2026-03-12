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
class CommitSeqPositionSpec:
    seq: str


@dataclass(frozen=True)
class TableCommitSeqPositionSpec:
    seqs: dict[str, str]


Db2CdcStartFromSpec: TypeAlias = Union[
    OriginPositionSpec,
    CommitSeqPositionSpec,
    TableCommitSeqPositionSpec,
    TimestampPositionSpec,
]
