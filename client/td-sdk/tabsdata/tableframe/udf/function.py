#
#  Copyright 2025 Tabs Data Inc.
#

from abc import ABC
from typing import Any, Union, cast

import polars as pl

import tabsdata.tableframe.functions.col as td_col
import tabsdata.tableframe.schema as td_schema
import tabsdata.tableframe.typing as td_typing


class UDF(ABC):
    def __init__(self):
        if self.__class__ is UDF:
            raise TypeError(
                "Cannot instantiate UDF directly. Create a subclass instead."
            )

        self._on_batch_is_overridden = self.__class__.on_batch is not UDF.on_batch
        self._on_element_is_overridden = self.__class__.on_element is not UDF.on_element

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        call_is_overridden = cls.__call__ is not UDF.__call__
        if call_is_overridden:
            raise TypeError(f"{cls.__name__} must not override '__call__' method")

        on_batch_is_overridden = cls.on_batch is not UDF.on_batch
        on_element_is_overridden = cls.on_element is not UDF.on_element
        schema_is_overridden = cls.schema is not UDF.schema

        if not (on_batch_is_overridden or on_element_is_overridden):
            raise TypeError(
                f"{cls.__name__} must override at least one of"
                f" '{UDF.on_element.__name__}' or '{UDF.on_batch.__name__}' methods"
            )

        if on_batch_is_overridden and on_element_is_overridden:
            raise TypeError(
                f"{cls.__name__} must override only one of"
                f" '{UDF.on_element.__name__}' and '{UDF.on_batch.__name__}' methods"
            )

        if on_element_is_overridden and not schema_is_overridden:
            raise TypeError(
                f"{cls.__name__} uses 'on_element' and must override"
                f" '{UDF.schema.__name__}' method to provide output schema"
            )

    def on_batch(self, series: list[td_typing.Series]) -> list[td_typing.Series]:
        pass

    def on_element(self, values: list[Any]) -> list[Any]:
        pass

    def schema(
        self,
    ) -> Union[td_schema.Schema, list[str], list[td_typing.DataType], None]:
        return None

    # noinspection DuplicatedCode
    def _names(  # noqa: C901
        self,
        width: int,
    ) -> list[str] | None:
        schema_out = self.schema()
        if schema_out is None:
            return None
        elif isinstance(schema_out, (td_schema.Schema, pl.Schema)):
            names = list(schema_out.names())
            if len(names) != width:
                raise ValueError(
                    f"Output schema specification has {len(names)} column names but "
                    f"UDF produced {width} output columns."
                )
            for i, name in enumerate(names):
                if name is None:
                    raise ValueError(
                        f"Output schema specification contains None at index {i}. All "
                        "column names must be provided."
                    )
            return names
        elif isinstance(schema_out, list):
            if not schema_out:
                if width == 0:
                    return []
                raise ValueError(
                    "Output schema specification is an empty list but UDF produced "
                    f"{width} output columns. Either provide column names in output "
                    "schema specification or produce columns with names."
                )
            elif all(isinstance(item, td_col.Column) for item in schema_out):
                names = [
                    td_column.name
                    for td_column in cast(list[td_col.Column], cast(object, schema_out))
                ]
                if len(names) != width:
                    raise ValueError(
                        f"Output schema specification has {len(names)} columns but "
                        f"UDF produced {width} output columns."
                    )
                for i, name in enumerate(names):
                    if name is None:
                        raise ValueError(
                            f"Output schema specification Column at index {i} has "
                            "column name None. All column names must be provided."
                        )
                return names
            elif all(isinstance(item, str) for item in schema_out):
                if len(schema_out) != width:
                    raise ValueError(
                        f"Output schema specification has {len(schema_out)} column "
                        f"names but UDF produced {width} output columns."
                    )
                return schema_out
            elif all(isinstance(item, pl.DataType) for item in schema_out):
                return None
            else:
                raise TypeError(
                    "Output schema specification is a list with mixed or invalid "
                    "types. Must be all strings (column names), or all DataTypes "
                    "(column data types), or all Column objects."
                )
        else:
            raise TypeError(
                "Output schema specification must be of type Schema, list[str], "
                "list[DataType], list[Column], or None. Got "
                f"{type(schema_out).__name__}"
            )

    # noinspection DuplicatedCode
    def _dtypes(  # noqa: C901
        self,
        width: int,
    ) -> list[td_typing.DataType] | None:
        schema_out = self.schema()
        if schema_out is None:
            return None
        elif isinstance(schema_out, (td_schema.Schema, pl.Schema)):
            dtypes = list(schema_out.dtypes())
            if len(dtypes) != width:
                raise ValueError(
                    f"Output schema specification has {len(dtypes)} column names but "
                    f"UDF produced {width} output columns."
                )
            for i, dtype in enumerate(dtypes):
                if dtype is None:
                    raise ValueError(
                        f"Output schema specification contains None at index {i}. All "
                        "column data types must be provided."
                    )
            return dtypes
        elif isinstance(schema_out, list):
            if not schema_out:
                if width == 0:
                    return []
                raise ValueError(
                    "Output schema specification is an empty list but UDF produced "
                    f"{width} output columns. Provide column data types in output "
                    "schema specification."
                )
            elif all(isinstance(item, td_col.Column) for item in schema_out):
                dtypes = [
                    td_column.dtype
                    for td_column in cast(list[td_col.Column], cast(object, schema_out))
                ]
                if len(dtypes) != width:
                    raise ValueError(
                        f"Output schema specification has {len(dtypes)} columns but "
                        f"UDF produced {width} output columns."
                    )
                for i, dtype in enumerate(dtypes):
                    if dtype is None:
                        raise ValueError(
                            f"Output schema specification Column at index {i} has "
                            "column data type None. All column data types must be "
                            "provided."
                        )
                return dtypes
            elif all(isinstance(item, str) for item in schema_out):
                return None
            elif all(isinstance(item, pl.DataType) for item in schema_out):
                if len(schema_out) != width:
                    raise ValueError(
                        f"Output schema specification has {len(schema_out)} column "
                        f"data types but UDF produced {width} output columns."
                    )
                return schema_out
            else:
                raise TypeError(
                    "Output schema specification is a list with mixed or invalid "
                    "types. Must be all strings (column names), or all DataTypes "
                    "(column data types), or all Column objects."
                )
        else:
            raise TypeError(
                "Output schema specification must be of type Schema, list[str], "
                "list[DataType], list[Column], or None. Got "
                f"{type(schema_out).__name__}"
            )

    def __call__(  # noqa: C901
        self,
        series: list[td_typing.Series],
    ) -> list[td_typing.Series]:
        if self._on_batch_is_overridden:
            series_out = self.on_batch(series)
            series_out_width = len(series_out)
            f_names = self._names(series_out_width)
            f_dtypes = self._dtypes(series_out_width)

            if f_names is not None:
                names_out = f_names
            else:
                names_out = [s.name for s in series_out]
            for i, name_out in enumerate(names_out):
                if not name_out:
                    raise ValueError(
                        f"Method on_batch() produced series at index {i} without a "
                        "column name. Either use .alias() on the series or "
                        "provide column names in schema()"
                    )

            series_out_with_spec = []
            for i, (column, name) in enumerate(zip(series_out, names_out)):
                series_with_spec = column.alias(name)
                if f_dtypes is not None:
                    series_with_spec = series_with_spec.cast(f_dtypes[i])
                series_out_with_spec.append(series_with_spec)
            return series_out_with_spec
        elif self._on_element_is_overridden:
            h_series = []
            for values in zip(*series):
                h_values = self.on_element(list(values))
                h_series.append(h_values)
            v_series = list(zip(*h_series))
            v_series_width = len(v_series)

            f_names = self._names(v_series_width)
            f_dtypes = self._dtypes(v_series_width)
            if f_names is None:
                raise ValueError(
                    "Method on_element() requires column names to be provided via "
                    "schema()."
                )

            series_out_with_spec = []
            for i, (column, name) in enumerate(zip(v_series, f_names)):
                series_with_spec = td_typing.Series(name=name, values=list(column))
                if f_dtypes is not None:
                    series_with_spec = series_with_spec.cast(f_dtypes[i])
                series_out_with_spec.append(series_with_spec)
            return series_out_with_spec
        else:
            raise RuntimeError(
                f"{self.__class__.__name__} has neither on_batch nor on_element"
                " overridden"
            )
