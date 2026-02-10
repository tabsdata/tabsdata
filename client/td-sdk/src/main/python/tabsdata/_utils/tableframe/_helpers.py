#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import tabsdata._utils.tableframe._constants as td_constants

# noinspection PyProtectedMember
import tabsdata.extensions._tableframe.extension as te_tableframe


def standard_system_columns() -> list[str]:
    return [member.value for member in td_constants.StandardSystemColumns]


def extended_system_columns() -> list[str]:
    return [member.value for member in te_tableframe.ExtendedSystemColumns]


def system_columns() -> list[str]:
    # noinspection PyTypeChecker
    return [member.value for member in te_tableframe.SystemColumns]


def system_columns_metadata() -> dict[str, td_constants.SystemColumn]:
    return te_tableframe.SYSTEM_COLUMNS_METADATA


def required_columns() -> list[str]:
    # noinspection PyTypeChecker
    return [member.value for member in te_tableframe.RequiredColumns]


def required_columns_metadata() -> dict[str, td_constants.SystemColumn]:
    return te_tableframe.REQUIRED_COLUMNS_METADATA


def non_optional_columns() -> list[str]:
    return [
        col
        for col in required_columns()
        if not any(
            col.startswith(prefix)
            for prefix in td_constants.TD_NAMESPACED_VIRTUAL_COLUMN_PREFIXES
        )
    ]


STANDARD_SYSTEM_COLUMNS = standard_system_columns()

EXTENDED_SYSTEM_COLUMNS = extended_system_columns()

SYSTEM_COLUMNS = system_columns()

SYSTEM_COLUMNS_METADATA = system_columns_metadata()

REQUIRED_COLUMNS = required_columns()

REQUIRED_COLUMNS_METADATA = required_columns_metadata()

NON_OPTIONAL_COLUMNS = non_optional_columns()
