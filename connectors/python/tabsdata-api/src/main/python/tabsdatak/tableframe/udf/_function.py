#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import copy
from abc import ABC
from collections import Counter
from typing import TYPE_CHECKING, Any, Callable, Literal, TypeAlias, get_args

from tabsdatak.tableframe.datatypes import Array, ColumnDataType, List, _valid_type
from tabsdatak.tableframe.udf.error import UDFErrorCode, UDFException

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

Signature: TypeAlias = Literal["packed", "unpacked"]
SIGNATURE_PACKED, SIGNATURE_UNPACKED = get_args(Signature)

OutputColumn: TypeAlias = tuple[str, ColumnDataType]
OutputColumns: TypeAlias = OutputColumn | list[OutputColumn]
OutputOverride: TypeAlias = tuple[str | None, ColumnDataType | None]
OutputOverrides: TypeAlias = (
    OutputOverride | list[OutputOverride] | dict[int, OutputOverride]
)

_SEALED = ("__call__", "columns", "with_columns")


class UDF(ABC):
    """A user defined function, applied to a `TableFrame` with
    `TableFrame.udf`.

    :categories: udf

    Subclass it, call `super().__init__(output_columns)` with the columns the
    function adds, and implement exactly one of `on_batch` or `on_element`.
    The function reads the columns `TableFrame.udf` selects, and its output
    columns are appended to the frame; it never sees any other column.

    Data is processed as a stream of batches: `on_batch` is called once per
    batch with pyarrow arrays, and `on_element` once per row with Python
    values. A batch holds an arbitrary slice of rows, so the output for a row
    must depend only on that row.

    To write the function as a plain function or a lambda instead of a
    class, use the `on_batch` and `on_element` decorators.

    Args:
        output_columns: The `(name, data type)` of the column the function
            adds, or a list of them for several.

    Examples:
        .. code-block:: python

            import pyarrow.compute as pc

            class Total(UDF):
                def __init__(self):
                    super().__init__(("total", Int64))

                def on_batch(self, arrays):
                    return [pc.add(arrays[0], arrays[1])]

            tf.udf(["a", "b"], Total())
    """

    def __init__(self, output_columns: OutputColumns):
        if type(self) in _BASES:
            raise UDFErrorCode.UDF_1.exception(udf=type(self).__name__)
        self._outputs = _outputs(output_columns)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for member in _SEALED:
            if getattr(cls, member) is not getattr(UDF, member):
                raise UDFErrorCode.UDF_3.exception(udf=cls.__name__, member=member)
        if ABC in cls.__bases__:
            return
        if (cls.on_batch is UDF.on_batch) == (cls.on_element is UDF.on_element):
            raise UDFErrorCode.UDF_2.exception(udf=cls.__name__)

    @property
    def signature(self) -> Signature:
        """How the inputs are passed to `on_batch` and `on_element`.

        `"packed"` (the default) passes them together as one list;
        `"unpacked"` passes each as its own argument. `UDFPacked` and
        `UDFUnpacked` fix it.

        Examples:
            .. code-block:: python

                class Ratio(UDF):
                    def __init__(self):
                        super().__init__(("ratio", Float64))

                    @property
                    def signature(self):
                        return "unpacked"

                    def on_element(self, numerator, denominator):
                        return [numerator / denominator]

                tf.udf(["a", "n"], Ratio())
        """
        return SIGNATURE_PACKED

    def on_batch(self, *arrays: pa.Array) -> list[pa.Array | pa.ChunkedArray]:
        """Compute the output columns for a batch of rows.

        Receives one pyarrow array per input column, and returns a list with
        one pyarrow array per output column, each holding exactly one value
        per input row. With a single output column, the array alone may be
        returned instead of a list.

        Examples:
            .. code-block:: python

                import pyarrow.compute as pc

                class Total(UDF):
                    def __init__(self):
                        super().__init__(("total", Int64))

                    def on_batch(self, arrays):
                        return [pc.add(arrays[0], arrays[1])]

                tf.udf(["a", "b"], Total())
        """
        raise NotImplementedError

    def on_element(self, *values: Any) -> list[Any]:
        """Compute the output values for one row.

        Receives one Python value per input column, and returns a list with
        one value per output column. With a single output column, the value
        alone may be returned instead of a list, unless that column is a
        `List` or an `Array`, whose value is a list itself.

        Examples:
            .. code-block:: python

                class Initial(UDF):
                    def __init__(self):
                        super().__init__(("initial", String))

                    def on_element(self, values):
                        name = values[0]
                        return [name.strip()[:1].upper() if name else None]

                tf.udf("name", Initial())
        """
        raise NotImplementedError

    def columns(self) -> list[OutputColumn]:
        """The `(name, data type)` of each output column, in order.

        Examples:
            .. code-block:: python

                class Bounds(UDF):
                    def __init__(self):
                        super().__init__([("low", Int64), ("high", Int64)])

                    def on_element(self, values):
                        return [min(values), max(values)]

                Bounds().columns()
        """
        return list(self._declared())

    def with_columns(self, output_columns: OutputOverrides) -> UDF:
        """A copy of this function with its output columns renamed or retyped.

        A `(name, data type)` tuple or a list of them overrides the output
        columns from the first one on; a dictionary overrides them by index.
        A `None` name or data type keeps the current one. The function itself
        is left unchanged.

        Examples:
            .. code-block:: python

                class Bounds(UDF):
                    def __init__(self):
                        super().__init__([("low", Int64), ("high", Int64)])

                    def on_element(self, values):
                        return [min(values), max(values)]

                tf.udf(["a", "b"], Bounds().with_columns(("smallest", None)))
                tf.udf(["a", "b"], Bounds().with_columns({1: ("largest", Float64)}))
        """
        current = self._declared()
        merged = list(current)
        for index, (name, dtype) in _overrides(output_columns, len(current)).items():
            merged[index] = (
                current[index][0] if name is None else name,
                current[index][1] if dtype is None else dtype,
            )
        function = copy.copy(self)
        function._outputs = _outputs(merged)
        return function

    def __call__(self, batch: pl.Series) -> pl.Series:
        import polars as pl

        inputs = [batch.struct.field(name) for name in batch.struct.fields]
        try:
            if type(self).on_batch is not UDF.on_batch:
                outputs = self._batch(inputs, len(batch))
            else:
                outputs = self._elements(inputs)
        except UDFException:
            raise
        except Exception as error:
            # Polars re-raises an error from inside a UDF as type(error)(text),
            # which fails for exceptions whose constructor needs other
            # arguments; a UDFException always survives that.
            raise UDFErrorCode.UDF_0.exception(
                cause=error,
                udf=self._label(),
                error=type(error).__name__,
            )
        return pl.DataFrame(outputs).to_struct()

    def _declared(self) -> tuple[OutputColumn, ...]:
        outputs = getattr(self, "_outputs", None)
        if outputs is None:
            raise UDFErrorCode.UDF_4.exception(udf=self._label())
        return outputs

    def _dtype(self) -> pl.Struct:
        import polars as pl

        return pl.Struct(dict(self._declared()))

    def _arguments(self, values: list) -> tuple:
        signature = self.signature
        if signature == SIGNATURE_PACKED:
            return (values,)
        if signature == SIGNATURE_UNPACKED:
            return tuple(values)
        raise UDFErrorCode.UDF_8.exception(
            udf=self._label(),
            signature=signature,
            signatures=list(get_args(Signature)),
        )

    def _batch(self, inputs: list[pl.Series], rows: int) -> list[pl.Series]:
        import polars as pl
        import pyarrow as pa

        arrays = [series.to_arrow() for series in inputs]
        result = self.on_batch(*self._arguments(arrays))
        declared = self._declared()
        if len(declared) == 1 and isinstance(result, (pa.Array, pa.ChunkedArray)):
            result = [result]
        if (
            not isinstance(result, (list, tuple))
            or len(result) != len(declared)
            or not all(
                isinstance(array, (pa.Array, pa.ChunkedArray)) for array in result
            )
        ):
            raise UDFErrorCode.UDF_10.exception(
                udf=self._label(),
                method="on_batch",
                width=len(declared),
                kind="pyarrow arrays",
                got=_described(result),
            )
        outputs = []
        for array, (name, dtype) in zip(result, declared):
            if len(array) != rows:
                raise UDFErrorCode.UDF_11.exception(
                    udf=self._label(), length=len(array), column=name, rows=rows
                )
            outputs.append(
                self._typed(name, dtype, lambda: pl.Series(name, array).cast(dtype))
            )
        return outputs

    def _elements(self, inputs: list[pl.Series]) -> list[pl.Series]:
        import polars as pl

        declared = self._declared()
        bare = len(declared) == 1 and not _holds_sequences(declared[0][1])
        columns: list[list] = [[] for _ in declared]
        for values in zip(*inputs):
            result = self.on_element(*self._arguments(list(values)))
            if bare and not isinstance(result, (list, tuple)):
                result = [result]
            if not isinstance(result, (list, tuple)) or len(result) != len(declared):
                raise UDFErrorCode.UDF_10.exception(
                    udf=self._label(),
                    method="on_element",
                    width=len(declared),
                    kind="values",
                    got=_described(result),
                )
            for column, value in zip(columns, result):
                column.append(value)
        return [
            self._typed(name, dtype, lambda: pl.Series(name, values, dtype=dtype))
            for values, (name, dtype) in zip(columns, declared)
        ]

    def _typed(self, name: str, dtype: ColumnDataType, build) -> pl.Series:
        try:
            return build()
        except UDFException:
            raise
        except Exception as error:
            raise UDFErrorCode.UDF_12.exception(
                cause=error, udf=self._label(), column=name, dtype=dtype
            )

    def _label(self) -> str:
        return type(self).__name__


class UDFPacked(UDF, ABC):
    """A `UDF` that receives its inputs together, as one list.

    :categories: udf

    Implement `on_batch(self, arrays: list[pa.Array]) -> list[pa.Array]` or
    `on_element(self, values: list[Any]) -> list[Any]`.

    Examples:
        .. code-block:: python

            class Spread(UDFPacked):
                def __init__(self):
                    super().__init__(("spread", Int64))

                def on_element(self, values):
                    return [max(values) - min(values)]

            tf.udf(["a", "b", "n"], Spread())
    """

    @property
    def signature(self) -> Signature:
        return SIGNATURE_PACKED


class UDFUnpacked(UDF, ABC):
    """A `UDF` that receives each input as its own argument.

    :categories: udf

    Implement `on_batch(self, first: pa.Array, second: pa.Array, ...) ->
    list[pa.Array]` or `on_element(self, first: Any, second: Any, ...) ->
    list[Any]`.

    Examples:
        .. code-block:: python

            class Ratio(UDFUnpacked):
                def __init__(self):
                    super().__init__(("ratio", Float64))

                def on_element(self, numerator, denominator):
                    return [numerator / denominator]

            tf.udf(["a", "n"], Ratio())
    """

    @property
    def signature(self) -> Signature:
        return SIGNATURE_UNPACKED


class _FunctionUDF(UDF, ABC):
    def __init__(self, output_columns, function, signature):
        self._function = function
        self._signature = signature
        if not callable(function):
            raise UDFErrorCode.UDF_14.exception(got=type(function).__name__)
        if signature not in get_args(Signature):
            raise UDFErrorCode.UDF_8.exception(
                udf=self._label(),
                signature=signature,
                signatures=list(get_args(Signature)),
            )
        super().__init__(output_columns)

    @property
    def signature(self) -> Signature:
        return self._signature

    def _label(self) -> str:
        return getattr(self._function, "__qualname__", type(self._function).__name__)


class _BatchFunction(_FunctionUDF):
    def on_batch(self, *arrays):
        return self._function(*arrays)


class _ElementFunction(_FunctionUDF):
    def on_element(self, *values):
        return self._function(*values)


_BASES = (UDF, UDFPacked, UDFUnpacked, _FunctionUDF)


def on_batch(
    output_columns: OutputColumns, *, signature: Signature = SIGNATURE_UNPACKED
) -> Callable[[Callable[..., Any]], UDF]:
    """Turn a function over batches of pyarrow arrays into a `UDF`.

    :categories: udf

    The function behaves like `UDF.on_batch`: it receives the input columns
    of a batch as pyarrow arrays, and returns one pyarrow array per output
    column, each holding exactly one value per input row. With a single
    output column it may return the array alone instead of a list. Any
    callable works: a function, a lambda or an object with `__call__`.

    Args:
        output_columns: The `(name, data type)` of the column the function
            adds, or a list of them for several.
        signature: `"unpacked"` (the default) passes each input array as its
            own argument; `"packed"` passes them together as one list.

    Returns:
        A decorator turning the function into a `UDF`, ready for
        `TableFrame.udf`.

    Examples:
        .. code-block:: python

            import pyarrow.compute as pc

            @on_batch(("total", Int64))
            def total(a, b):
                return pc.add(a, b)

            tf.udf(["a", "b"], total)

        Several output columns, from a list of arrays:

        .. code-block:: python

            import pyarrow.compute as pc

            @on_batch([("low", Int64), ("high", Int64)])
            def bounds(a, b):
                return [pc.min_element_wise(a, b), pc.max_element_wise(a, b)]

            tf.udf(["a", "b"], bounds)

        Inline, with a lambda receiving its inputs packed in one list:

        .. code-block:: python

            import pyarrow.compute as pc

            tf.udf(
                ["a", "b", "n"],
                on_batch(("largest", Int64), signature="packed")(
                    lambda arrays: pc.max_element_wise(*arrays)
                ),
            )
    """

    def decorate(function: Callable[..., Any]) -> UDF:
        return _BatchFunction(output_columns, function, signature)

    return decorate


def on_element(
    output_columns: OutputColumns, *, signature: Signature = SIGNATURE_UNPACKED
) -> Callable[[Callable[..., Any]], UDF]:
    """Turn a function over the values of one row into a `UDF`.

    :categories: udf

    The function behaves like `UDF.on_element`: it receives the input values
    of a row as Python values, and returns one value per output column.
    With a single output column it may return the value alone instead of a
    list, unless that column is a `List` or an `Array`, whose value is a
    list itself. Any callable works: a function, a lambda or an object with
    `__call__`.

    Args:
        output_columns: The `(name, data type)` of the column the function
            adds, or a list of them for several.
        signature: `"unpacked"` (the default) passes each input value as its
            own argument; `"packed"` passes them together as one list.

    Returns:
        A decorator turning the function into a `UDF`, ready for
        `TableFrame.udf`.

    Examples:
        .. code-block:: python

            @on_element(("ratio", Float64))
            def ratio(numerator, denominator):
                return numerator / denominator

            tf.udf(["a", "n"], ratio)

        Several output columns, from a list of values:

        .. code-block:: python

            @on_element([("ratio", Float64), ("label", String)])
            def ratio(numerator, denominator):
                return [numerator / denominator, f"{numerator}/{denominator}"]

            tf.udf(["a", "n"], ratio)

        Inline, with a lambda, plainly or receiving its inputs packed in one
        list:

        .. code-block:: python

            tf.udf("name", on_element(("upper", String))(lambda name: name.upper()))
            tf.udf(
                ["a", "b", "n"],
                on_element(("spread", Int64), signature="packed")(
                    lambda values: max(values) - min(values)
                ),
            )
    """

    def decorate(function: Callable[..., Any]) -> UDF:
        return _ElementFunction(output_columns, function, signature)

    return decorate


def _outputs(output_columns) -> tuple[OutputColumn, ...]:
    columns = [output_columns] if isinstance(output_columns, tuple) else output_columns
    if not isinstance(columns, list) or not columns:
        raise UDFErrorCode.UDF_5.exception(got=_described(output_columns))
    outputs = []
    for index, column in enumerate(columns):
        if not isinstance(column, tuple) or len(column) != 2:
            raise UDFErrorCode.UDF_6.exception(
                index=index, reason="it is not a (name, data type) tuple"
            )
        name, dtype = column
        if not isinstance(name, str) or not name:
            raise UDFErrorCode.UDF_6.exception(
                index=index, reason="its name must be a non-empty string"
            )
        try:
            _valid_type(dtype)
        except TypeError:
            raise UDFErrorCode.UDF_6.exception(
                index=index, reason=f"{dtype!r} is not a supported data type"
            ) from None
        outputs.append((name, dtype))
    counts = Counter(name for name, _ in outputs)
    repeated = sorted(name for name, count in counts.items() if count > 1)
    if repeated:
        raise UDFErrorCode.UDF_7.exception(names=repeated)
    return tuple(outputs)


def _overrides(output_columns, width: int) -> dict[int, OutputOverride]:
    if isinstance(output_columns, tuple):
        output_columns = [output_columns]
    if isinstance(output_columns, list):
        if len(output_columns) > width:
            raise UDFErrorCode.UDF_5.exception(
                got=f"{len(output_columns)} columns for a function with {width}"
            )
        output_columns = dict(enumerate(output_columns))
    if not isinstance(output_columns, dict):
        raise UDFErrorCode.UDF_5.exception(got=_described(output_columns))
    for index, column in output_columns.items():
        if not isinstance(index, int) or not 0 <= index < width:
            raise UDFErrorCode.UDF_6.exception(
                index=index, reason="there is no output column at that index"
            )
        if not isinstance(column, tuple) or len(column) != 2:
            raise UDFErrorCode.UDF_6.exception(
                index=index, reason="it is not a (name, data type) tuple"
            )
    return output_columns


def _holds_sequences(dtype) -> bool:
    base = dtype if isinstance(dtype, type) else type(dtype)
    return base in (List, Array)


def _described(value) -> str:
    if isinstance(value, (list, tuple)):
        kinds = ", ".join(type(item).__name__ for item in value)
        return f"{type(value).__name__} of {len(value)} ({kinds})"
    return type(value).__name__
