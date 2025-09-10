#
# Copyright 2025 Tabs Data Inc.
#


from __future__ import annotations

import collections.abc as collections
from typing import Iterable, Mapping, Union

import polars as pl

import tabsdata.tableframe.functions.col as td_col
import tabsdata.tableframe.typing as td_typing


class Schema(pl.Schema):

    def __init__(
        self,
        schema: Union[
            Iterable[td_col.Column],
            Mapping[str, td_typing.SchemaInitDataType]
            | Iterable[
                tuple[str, td_typing.SchemaInitDataType]
                | td_typing.ArrowSchemaExportable
            ]
            | td_typing.ArrowSchemaExportable
            | None,
        ] = None,
        *,
        check_dtypes: bool = True,
    ) -> None:
        if schema is None:
            super().__init__(schema, check_dtypes=check_dtypes)
            return
        if isinstance(schema, collections.Iterable) and not isinstance(
            schema, (str, bytes, collections.Mapping)
        ):
            columns = list(schema)
            if not columns:
                super().__init__({}, check_dtypes=check_dtypes)
                return
            if all(isinstance(column, td_col.Column) for column in columns):
                dictionary: dict[str, pl.DataType] = {
                    column.name: column.dtype for column in columns
                }
                super().__init__(dictionary, check_dtypes=check_dtypes)
                return
            schema = columns
        super().__init__(schema, check_dtypes=check_dtypes)
