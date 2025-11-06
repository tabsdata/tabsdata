#
#  Copyright 2025 Tabs Data Inc.
#
import copy
from abc import ABC, ABCMeta
from dataclasses import dataclass
from typing import Any, Literal, Union, get_args, overload

import polars as pl
from polars._typing import PolarsDataType

import tabsdata.tableframe.dtypes as td_dtypes
import tabsdata.tableframe.functions.col as td_col
import tabsdata.tableframe.schema as td_schema
import tabsdata.tableframe.typing as td_typing

SIGNATURE = Literal[
    "list",
    "unpacked",
]
(
    SIGNATURE_LIST,
    SIGNATURE_UNPACKED,
) = get_args(SIGNATURE)


@dataclass
class _Column:
    name: str | None = None
    dtype: td_typing.DataType | None = None


@dataclass
class _Schema:
    columns: list[_Column]

    def __init__(self, columns: list[_Column] | None = None):
        self.columns = columns if columns is not None else []

    def items(self) -> list[tuple[str | None, td_typing.DataType | None]]:
        return [(column.name, column.dtype) for column in self.columns]

    def __iter__(self):
        return iter(self.columns)

    def __len__(self):
        return len(self.columns)

    @classmethod
    def from_td_schema(
        cls, schema: Union[td_schema.Schema, td_typing.Schema]
    ) -> "_Schema":
        columns = []
        for name, dtype in schema.items():
            columns.append(_Column(name=name, dtype=dtype))
        return cls(columns)

    def to_td_schema(self) -> td_schema.Schema:
        columns = []
        for column in self.columns:
            name = column.name
            dtype = column.dtype if column.dtype is not None else td_dtypes.String
            columns.append(td_col.Column(name=name, dtype=dtype))
        return td_schema.Schema(columns)


class UDF(ABC):
    def __init__(  # noqa: C901
        self,
        output_columns: Union[
            list[tuple[str, td_typing.DataType]], tuple[str, td_typing.DataType]
        ],
    ):
        if self.__class__ is UDF:
            raise TypeError(
                "Cannot instantiate UDF directly. Create a subclass instead."
            )

        self._initialized = True

        columns_in = output_columns
        if isinstance(columns_in, tuple):
            columns_in = [columns_in]

        if not isinstance(columns_in, list):
            raise TypeError("UDF input must be a list of (name, dtype) tuples.")

        if not columns_in:
            raise ValueError("The columns list provided cannot be empty.")

        schema_columns = []
        for i, column in enumerate(columns_in):
            if not isinstance(column, tuple) or len(column) != 2:
                raise TypeError(
                    f"Column at index {i} is not a (name, data type) tuple."
                )
            name, dtype = column
            if name is None:
                raise ValueError(f"Column name at index {i} cannot be None.")
            if not isinstance(name, str):
                raise TypeError(
                    f"Column name at index {i} must be a string; "
                    f"got {type(name).__name__} instead."
                )
            if dtype is None:
                raise ValueError(f"Column data type at index {i} cannot be None.")
            if not isinstance(dtype, td_typing.DataType):
                raise TypeError(
                    f"Column data type at index {i} must be a DataType; "
                    f"got {type(dtype).__name__} instead."
                )
            schema_columns.append(_Column(name=name, dtype=dtype))

        self.__schema = _Schema(schema_columns)

        self._on_batch = self.__class__.on_batch is not UDF.on_batch
        self._on_element = self.__class__.on_element is not UDF.on_element

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        if ABC in cls.__bases__ or (
            isinstance(cls, ABCMeta) and getattr(cls, "__abstractmethods__", None)
        ):
            return

        output_columns_implemented = cls.with_columns is not UDF.with_columns
        if output_columns_implemented:
            raise TypeError(
                f"{cls.__name__} must not implement 'output_columns' method."
            )

        call_implemented = cls.__call__ is not UDF.__call__
        if call_implemented:
            raise TypeError(f"{cls.__name__} must not implement '__call__' method.")

        is_on_batch = cls.on_batch is not UDF.on_batch
        is_on_element = cls.on_element is not UDF.on_element

        if not (is_on_batch or is_on_element):
            raise TypeError(
                f"{cls.__name__} must implement exactly one of"
                f" '{UDF.on_element.__name__}' or '{UDF.on_batch.__name__}' methods."
            )

        if is_on_batch and is_on_element:
            raise TypeError(
                f"{cls.__name__} must implement exactly one of"
                f" '{UDF.on_element.__name__}' and '{UDF.on_batch.__name__}' methods."
            )

    @property
    def signature(self) -> SIGNATURE:
        """
        Defines how parameters are passed to on_batch and on_element methods.

        Returns:
            "list": Parameters are passed as a single list (default).
            "unpacked": Each parameter is passed as a separate argument.

        Override this property in your UDF subclass to change the parameter style.
        """
        return SIGNATURE_LIST

    # fmt: off
    @overload
    def on_batch(self, series: list[td_typing.Series]) -> list[td_typing.Series]:
        ...

    # fmt: on

    # fmt: off
    @overload
    def on_batch(self, *series: td_typing.Series) -> list[td_typing.Series]:
        ...

    # fmt: on

    def on_batch(self, *args) -> list[td_typing.Series]:
        """
        Creating UDFs:
            1. Subclass :class:`tabsdata.tableframe.udf.function.UDF`.
            2. Implement ``__init__`` to call ``super().__init__(output_columns)`` where
               ``output_columns`` is a tuple or list of tuples ``(name, data type)``
               specifying the UDF default output schema (column names and data types).
               Each tuple must contain a column name (string) and a data type
               (DataType).
            3. Override exactly one of `on_batch` or `on_element`, to implement the UDF
               function logic.
            4. Return a list of TabsData Series (for `on_batch`) or TabsData supported
               scalars (for `on_element`) with the same length as specified in the
               output schema.
            4. If overriding the `on_batch` method, the return type must be a list of
               TabsData Series. If overriding the `on_element` method, the return type
               must be a list of supported TabsData scalar values. For both cases, the
               number of elements in the returned lists must match the number of
               elements in the output_columns list provided to the UDF constructor.

        Using UDFs:
            1. Instantiate a function created as above.
            2. Pass it to TableFrame method udf().
            3. Optionally use :meth:`UDF.output_columns` to override output column names
               or data types after instantiation.
            4. By default, `on_batch` receives a list of series. Override the
               `signature` property to return "unpacked" to receive each series as a
               separate argument instead.
        """

    pass

    # fmt: off
    @overload
    def on_element(self, values: list[Any]) -> list[Any]:
        ...
    # fmt: on

    # fmt: off
    @overload
    def on_element(self, *values: Any) -> list[Any]:
        ...

    # fmt: on

    def on_element(self, *args) -> list[Any]:
        """
        Creating UDFs:
            1. Subclass :class:`tabsdata.tableframe.udf.function.UDF`.
            2. Implement ``__init__`` to call ``super().__init__(output_columns)`` where
               ``output_columns`` is a tuple or list of tuples ``(name, data type)``
               specifying the UDF default output schema (column names and data types).
               Each tuple must contain a column name (string) and a data type
               (DataType).
            3. Override exactly one of `on_batch` or `on_element`, to implement the UDF
               function logic.
            4. Return a list of TabsData Series (for `on_batch`) or TabsData supported
               scalars (for `on_element`) with the same length as specified in the
               output schema.
            4. If overriding the `on_batch` method, the return type must be a list of
               TabsData Series. If overriding the `on_element` method, the return type
               must be a list of supported TabsData scalar values. For both cases, the
               number of elements in the returned lists must match the number of
               elements in the output_columns list provided to the UDF constructor.

        Using UDFs:
            1. Instantiate a function created as above.
            2. Pass it to TableFrame method udf().
            3. Optionally use :meth:`UDF.output_columns` to override output column names
               or data types after instantiation.
            4. By default, `on_element` receives a list of values. Override the
               `signature` property to return "unpacked" to receive each value as a
               separate argument instead.
        """

    pass

    def columns(self) -> list[tuple[str, td_typing.DataType]]:
        columns = self.__schema.columns
        if len(columns) == 0:
            raise ValueError("Output schema must be specified.")

        names = []
        columns = []
        for i, column in enumerate(columns):
            if column.name is None:
                raise ValueError(
                    f"Column at index {i} is missing a name. "
                    "Use output_columns() method to provide missing column names."
                )
            if column.dtype is None:
                raise ValueError(
                    f"Column at index {i} is missing a data type. "
                    "Use output_columns method to provide missing column data types."
                )
            names.append(column.name)
            columns.append((column.name, column.dtype))
        if len(set(names)) != len(names):
            raise ValueError("Output schema cannot have duplicate column names.")
        return columns

    def with_columns(
        self,
        output_columns: Union[
            tuple[str | None, td_typing.DataType | None],
            list[tuple[str | None, td_typing.DataType | None]],
            dict[int, tuple[str | None, td_typing.DataType | None]],
        ],
    ) -> "UDF":
        if isinstance(output_columns, tuple) or isinstance(output_columns, list):
            return self._columns_list(output_columns)
        elif isinstance(output_columns, dict):
            return self._columns_dict(output_columns)
        raise TypeError(
            "Wrong specification type for output_columns."
            f"Use a tuple, a list or a dict instead of {type(output_columns).__name__}."
        )

    def _columns_list(
        self,
        columns: Union[
            list[tuple[str | None, td_typing.DataType | None]],
            tuple[str | None, td_typing.DataType | None],
        ],
    ) -> "UDF":
        columns_in = columns
        if isinstance(columns_in, tuple):
            columns_in = [columns_in]

        schema_length = len(self.__schema.columns)
        if len(columns_in) > schema_length:
            raise ValueError(
                f"Method output_columns expects at most {schema_length} columns, but"
                f" {len(columns_in)} were provided."
            )

        schema_columns = []
        for i, (c_alias, c_cast) in enumerate(columns_in):
            if c_alias is not None and not isinstance(c_alias, str):
                raise TypeError(
                    f"Column name at index {i} must be a string or None; "
                    f"got {type(c_alias).__name__} instead."
                )
            if c_cast is not None and not isinstance(c_cast, td_typing.DataType):
                raise TypeError(
                    f"Column data type at index {i} must be a DataType or None; "
                    f"got {type(c_cast).__name__} instead."
                )

            column = self.__schema.columns[i]
            name = c_alias if c_alias is not None else column.name
            dtype = c_cast if c_cast is not None else column.dtype
            schema_columns.append(_Column(name=name, dtype=dtype))
        if len(columns_in) < schema_length:
            schema_columns.extend(self.__schema.columns[len(columns_in) :])

        self.__schema.columns = schema_columns
        return copy.deepcopy(self)

    def _columns_dict(
        self, columns: dict[int, tuple[str | None, td_typing.DataType | None]]
    ) -> "UDF":
        schema_length = len(self.__schema.columns)
        if any(i >= schema_length or i < 0 for i in columns.keys()):
            raise IndexError(
                "Invalid index provided in output_columns. "
                "All indexes must be non-negative and less than the number of columns."
            )

        schema_columns = list(self.__schema.columns)
        for i, (c_alias, c_cast) in columns.items():
            if c_alias is not None and not isinstance(c_alias, str):
                raise TypeError(
                    f"Column name at index {i} must be a string or None; "
                    f"got {type(c_alias).__name__} instead."
                )

            if c_cast is not None and not isinstance(c_cast, td_typing.DataType):
                raise TypeError(
                    f"Column data type at index {i} must be a DataType or None; "
                    f"got {type(c_cast).__name__} instead."
                )

            column = schema_columns[i]
            name = c_alias if c_alias is not None else column.name
            dtype = c_cast if c_cast is not None else column.dtype
            schema_columns[i] = _Column(name=name, dtype=dtype)

        self.__schema.columns = schema_columns
        return copy.deepcopy(self)

    @property
    def _schema(self) -> _Schema:
        if not hasattr(self, "_initialized"):
            raise RuntimeError(
                f"{self.__class__.__name__}.__init__() did not call super().__init__()."
                " Subclasses must call super().__init__(columns)."
            )
        return self.__schema

    def _names(self, width: int) -> list[str]:
        columns = self.__schema.columns
        if len(columns) == 0:
            raise ValueError("Output schema must be specified.")
        if len(columns) != width:
            raise ValueError(
                f"Output schema specification has {len(columns)} columns "
                f"but UDF produced {width} output columns."
            )
        names = []
        for i, column in enumerate(columns):
            if column.name is None:
                raise ValueError(
                    f"Column at index {i} is missing a name. "
                    "Use output_columns() method to provide missing column names."
                )
            names.append(column.name)

        if len(set(names)) != len(names):
            raise ValueError("Output schema cannot have duplicate column names.")

        return names

    def _dtypes(self, width: int) -> list[td_typing.DataType]:
        columns = self.__schema.columns
        if len(columns) == 0:
            raise ValueError("Output schema must be specified.")
        if len(columns) != width:
            raise ValueError(
                f"Output schema specification has {len(columns)} columns "
                f"but UDF produced {width} output columns."
            )
        dtypes = []
        for i, column in enumerate(columns):
            if column.dtype is None:
                raise ValueError(
                    f"Column at index {i} is missing a data type. "
                    "Use output_columns method to provide missing column data types."
                )
            dtypes.append(column.dtype)
        return dtypes

    def _columns(self) -> PolarsDataType:
        width = len(self._schema.columns)
        names = self._names(width)
        dtypes = self._dtypes(width)
        s_dtype = pl.Struct(
            [pl.Field(c_name, c_dtype) for c_name, c_dtype in zip(names, dtypes)]
        )
        return s_dtype

    def __call__(  # noqa: C901
        self,
        series: list[td_typing.Series],
    ) -> list[td_typing.Series]:
        if not hasattr(self, "_initialized"):
            raise RuntimeError(
                f"{self.__class__.__name__}.__init__() did not call super().__init__()."
                " Subclasses must call super().__init__(columns)."
            )
        signature = self.signature
        if signature not in get_args(SIGNATURE):
            raise ValueError(
                f"Invalid signature: {signature}. Must be one of {get_args(SIGNATURE)}."
            )
        if self._on_batch:
            series_out = None
            if signature == SIGNATURE_LIST:
                series_out = self.on_batch(series)
            elif signature == SIGNATURE_UNPACKED:
                series_out = self.on_batch(*series)
            series_out_width = len(series_out)
            names = self._names(series_out_width)
            dtypes = self._dtypes(series_out_width)
            series_out_with_spec = []
            for column_out, name, dtype in zip(series_out, names, dtypes):
                series_with_spec = column_out.alias(name).cast(dtype)
                series_out_with_spec.append(series_with_spec)
            return series_out_with_spec
        elif self._on_element:
            rows_out = []
            row_out = None
            for values in zip(*series):
                if signature == SIGNATURE_LIST:
                    row_out = self.on_element(list(values))
                elif signature == SIGNATURE_UNPACKED:
                    row_out = self.on_element(*values)
                rows_out.append(row_out)
            columns_out = list(zip(*rows_out))
            columns_out_width = len(columns_out)
            names = self._names(columns_out_width)
            dtypes = self._dtypes(columns_out_width)
            series_out_with_spec = []
            for column_data, name, dtype in zip(columns_out, names, dtypes):
                series_with_spec = td_typing.Series(
                    name=name, values=list(column_data), dtype=dtype
                )
                series_out_with_spec.append(series_with_spec)
            return series_out_with_spec
        else:
            raise RuntimeError(
                f"{self.__class__.__name__} "
                "has neither on_batch nor on_element implemented."
            )


class UDFList(UDF, ABC):
    """
    Abstract base class for UDFs that use list-style parameter passing.

    When subclassing UDFList, implement on_batch or on_element
    with list signature:
        - on_batch(self, series: list[Series]) -> list[Series]
        - on_element(self, values: list[Any]) -> list[Any]
    """

    def __init__(self, output_columns):
        if self.__class__ is UDFList:
            raise TypeError(
                "Cannot instantiate UDFList directly. Create a subclass instead."
            )
        super().__init__(output_columns)

    @property
    def signature(self) -> SIGNATURE:
        return SIGNATURE_LIST


class UDFUnpacked(UDF, ABC):
    """
    Abstract base class for UDFs that use unpacked-style parameter passing.

    When subclassing UDFUnpacked, implement on_batch or on_element
    with unpacked signature:
        - on_batch(self, col1: Series, col2: Series, ...) -> list[Series]
        - on_element(self, val1: Any, val2: Any, ...) -> list[Any]
    """

    def __init__(self, output_columns):
        if self.__class__ is UDFUnpacked:
            raise TypeError(
                "Cannot instantiate UDFUnpacked directly. Create a subclass instead."
            )
        super().__init__(output_columns)

    @property
    def signature(self) -> SIGNATURE:
        return SIGNATURE_UNPACKED
