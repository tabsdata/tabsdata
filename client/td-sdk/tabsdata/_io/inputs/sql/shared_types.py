#
# Copyright 2025 Tabs Data Inc.
#

from typing import Union

from tabsdata.tableframe.typing import DataType

QueriesSpec = Union[str, list[str]]
SchemaOverrideSpec = Union[dict[str, DataType], list[dict[str, DataType]], None]
