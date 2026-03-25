#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from tabsdata.exceptions import Db2CdcError, ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

CD_SYSTEM_COLUMN_NAMES = frozenset({
    "IBMSNAP_OPERATION",
    "IBMSNAP_COMMITSEQ",
    "IBMSNAP_INTENTSEQ",
})


@dataclass
class MatchedColumns:
    after_columns_list: list[tuple[str, Any]]
    before_columns_map: dict[str, str]


def match_cd_columns(
    cd_columns: list[tuple[str, Any]],
    before_column_prefix: str | None,
) -> MatchedColumns:
    user_columns = [
        (name, sa_type)
        for name, sa_type in cd_columns
        if name.upper() not in CD_SYSTEM_COLUMN_NAMES
    ]

    if not before_column_prefix:
        raise Db2CdcError(ErrorCode.CDCDB26)

    prefix = before_column_prefix
    prefix_len = len(prefix)

    user_columns_map: dict[str, Any] = {name: sa_type for name, sa_type in user_columns}
    user_columns_set = set(user_columns_map.keys())

    sorted_user_columns = sorted(user_columns_set, key=lambda n: (len(n), n))

    matched_before: set[str] = set()
    matched_after: set[str] = set()

    before_columns_map: dict[str, str] = {}

    for name in sorted_user_columns:
        if name in matched_before or name in matched_after:
            continue
        if name.startswith(prefix) and len(name) > prefix_len:
            candidate_after = name[prefix_len:]
            if (
                candidate_after in user_columns_set
                and candidate_after not in matched_after
                and candidate_after not in matched_before
            ):
                matched_before.add(name)
                matched_after.add(candidate_after)
                before_columns_map[candidate_after] = name

    unmatched_before: list[str] = []
    unmatched_after: list[str] = []

    for name in sorted_user_columns:
        if name in matched_before or name in matched_after:
            continue
        if name.startswith(prefix) and len(name) > prefix_len:
            unmatched_before.append(name)
        else:
            unmatched_after.append(name)

    if unmatched_before:
        raise Db2CdcError(
            ErrorCode.CDCDB25,
            ", ".join(sorted_user_columns),
            ", ".join(unmatched_before),
            ", ".join(unmatched_after),
            prefix,
        )
    if unmatched_after:
        raise Db2CdcError(
            ErrorCode.CDCDB27,
            ", ".join(sorted_user_columns),
            ", ".join(unmatched_before),
            ", ".join(unmatched_after),
            prefix,
        )

    after_names = matched_after
    new_columns = [
        (name, sa_type) for name, sa_type in user_columns if name in after_names
    ]

    return MatchedColumns(
        after_columns_list=new_columns,
        before_columns_map=before_columns_map,
    )
