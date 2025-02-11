#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import Mapping, Sequence, TypeAlias, Union

from polars import DataType
from polars.datatypes import DataTypeClass

TableDictionary = Mapping[str, Union[Sequence[object], Mapping[str, Sequence[object]]]]

TdDataType: TypeAlias = Union["DataTypeClass", "DataType"]
