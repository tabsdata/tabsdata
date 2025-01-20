#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import Any

import tabsdata.utils.tableframe._constants as td_constants
import td_interceptor.interceptor as td_interceptor


def standard_system_columns() -> list[str]:
    return [member.value for member in td_constants.StandardSystemColumns]


def extended_system_columns() -> list[str]:
    return [member.value for member in td_interceptor.ExtendedSystemColumns]


def system_columns() -> list[str]:
    return [member.value for member in td_interceptor.SystemColumns]


def system_columns_metadata() -> dict[str, Any]:
    return td_interceptor.SYSTEM_COLUMNS_METADATA


def required_columns() -> list[str]:
    return [member.value for member in td_interceptor.RequiredColumns]


def required_columns_metadata() -> dict[str, Any]:
    return td_interceptor.REQUIRED_COLUMNS_METADATA


STANDARD_SYSTEM_COLUMNS = standard_system_columns()

EXTENDED_SYSTEM_COLUMNS = extended_system_columns()

SYSTEM_COLUMNS = system_columns()

SYSTEM_COLUMNS_METADATA = system_columns_metadata()

REQUIRED_COLUMNS = required_columns()

REQUIRED_COLUMNS_METADATA = required_columns_metadata()
