#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import TypeAlias, Union

from tabsdata.dataquality._data_quality import DataQuality

OnTablesSpec: TypeAlias = Union[DataQuality | list[DataQuality] | None]
