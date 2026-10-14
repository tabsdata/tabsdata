#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import copy
import inspect
from abc import ABC, ABCMeta
from typing import TYPE_CHECKING, Any, Callable, get_args

from tabsdatak import _api
from tabsdatak.tableframe.datatypes import Array, ColumnDataType, List
from tabsdatak.tableframe.udf._typing import (
    SIGNATURE_PACKED,
    SIGNATURE_UNPACKED,
    OutputAffixSpec,
    OutputColumnSpec,
    OutputColumnsSpec,
    OutputNamesSpec,
    SignatureSpec,
)
from tabsdatak.tableframe.udf.error import UDFErrorCode, UDFException

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

_SEALED = ("__call__", "columns", "with_names")
_RENAMING = ("output_names", "output_prefix", "output_suffix")


class _UDFMeta(ABCMeta):
    def __call__(
        cls,
        *args: Any,
        output_names: OutputNamesSpec | None = None,
        output_prefix: OutputAffixSpec | None = None,
        output_suffix: OutputAffixSpec | None = None,
        **kwargs,
    ):
        function = super().__call__(*args, **kwargs)
        if (output_names, output_prefix, output_suffix) != (None, None, None):
            function._outputs = function._renamed(
                output_names, output_prefix, output_suffix
            )
        return function


class UDF(ABC, metaclass=_UDFMeta):
    """A user defined function, applied to a `TableFrame` with
    `TableFrame.udf`.

    :categories: udf

    Subclass it, call `super().__init__(output_columns)` with the columns the
    function adds, and implement `on_batch`, `on_element` or both. The
    function reads the columns `TableFrame.udf` selects, and its output
    columns are appended to the frame; it never sees any other column.

    Data is processed as a stream of batches: `on_batch` is called once per
    batch with pyarrow arrays, and `on_element` once per row with Python
    values. A batch holds an arbitrary slice of rows, so the output for a row
    must depend only on that row. A function implementing only `on_element`
    gets an `on_batch` that calls it for each row of the batch; one
    implementing both is run through its own `on_batch`.

    Every function also takes the `output_names`, `output_prefix` and
    `output_suffix` keywords when it is created, renaming all its output
    columns, so the same function can be applied more than once without
    their names colliding. `output_names` gives one new name per output
    column, in order; `output_prefix` and `output_suffix` are then added to
    every name. Any of them may be used alone or together, and the data
    types stay as declared.

    To write the function as a plain function or a lambda instead of a
    class, use the `on_batch` and `on_element` decorators.

    Args:
        output_columns: The `(name, data type)` of the column the function
            adds, or a list of them for several.

    Examples:
        .. code-block:: python

            import pyarrow as pa
            import pyarrow.compute as pc

            class Total(UDF):
                def __init__(self):
                    super().__init__(("total", Int64))

                def on_batch(self, arrays: list[pa.Int64Array]) -> list[pa.Int64Array]:
                    return [pc.add(arrays[0], arrays[1])]

            tf.udf(["a", "b"], Total())

        Renaming the output column when the function is created:

        .. code-block:: python

            import pyarrow as pa
            import pyarrow.compute as pc

            class Total(UDF):
                def __init__(self):
                    super().__init__(("total", Int64))

                def on_batch(self, arrays: list[pa.Int64Array]) -> list[pa.Int64Array]:
                    return [pc.add(arrays[0], arrays[1])]

            tf.udf(["a", "b"], Total(output_names="a_plus_b"))
            tf.udf(["c", "n"], Total(output_names=["c_plus_n"]))

        Several output columns, each renamed:

        .. code-block:: python

            class Bounds(UDF):
                def __init__(self):
                    super().__init__([("low", Int64), ("high", Int64)])

                def on_element(self, values: list[int]) -> list[int]:
                    return [min(values), max(values)]

            tf.udf(["a", "b"], Bounds())
            tf.udf(["a", "b"], Bounds(output_names=["smallest", "largest"]))

        The same function applied twice, told apart by a prefix or a suffix:

        .. code-block:: python

            class Bounds(UDF):
                def __init__(self):
                    super().__init__([("low", Int64), ("high", Int64)])

                def on_element(self, values: list[int]) -> list[int]:
                    return [min(values), max(values)]

            tf.udf(["a", "b"], Bounds(output_prefix="ab_")).udf(
                ["c", "n"], Bounds(output_prefix="cn_")
            )
            tf.udf(["a", "b"], Bounds(output_suffix="_ab")).udf(
                ["c", "n"], Bounds(output_names=["min", "max"], output_suffix="_cn")
            )
    """

    @_api.validate_call(UDFErrorCode.UDF_5)
    def __init__(self, output_columns: OutputColumnsSpec):
        if type(self) in _BASES:
            raise UDFErrorCode.UDF_1.exception(udf=type(self).__name__)
        self._outputs = tuple(output_columns)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for member in _SEALED:
            if getattr(cls, member) is not getattr(UDF, member):
                raise UDFErrorCode.UDF_3.exception(udf=cls.__name__, member=member)
        if "__init__" in cls.__dict__:
            parameters = inspect.signature(cls.__init__).parameters
            reserved = [name for name in _RENAMING if name in parameters]
            if reserved:
                raise UDFErrorCode.UDF_18.exception(
                    udf=cls.__name__, parameters=reserved
                )
        if ABC in cls.__bases__:
            return
        if cls.on_batch is UDF.on_batch and cls.on_element is UDF.on_element:
            raise UDFErrorCode.UDF_2.exception(udf=cls.__name__)

    @property
    def signature(self) -> SignatureSpec:
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
                    def signature(self) -> SignatureSpec:
                        return "unpacked"

                    def on_element(
                        self, numerator: int, denominator: int
                    ) -> list[float]:
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

        Each input array is of the pyarrow array class for its column's data
        type: an `Int64` column arrives as a `pa.Int64Array`, a `Float64` one
        as a `pa.DoubleArray`, a `String` one as a `pa.LargeStringArray`, and
        so on.

        Unless overridden, it calls `on_element` for each row of the batch.

        Examples:
            .. code-block:: python

                import pyarrow as pa
                import pyarrow.compute as pc

                class Total(UDF):
                    def __init__(self):
                        super().__init__(("total", Int64))

                    def on_batch(
                        self, arrays: list[pa.Int64Array]
                    ) -> list[pa.Int64Array]:
                        return [pc.add(arrays[0], arrays[1])]

                tf.udf(["a", "b"], Total())

            Calling the `on_batch` of a function implementing only
            `on_element`:

            .. code-block:: python

                import pyarrow as pa

                class Initial(UDF):
                    def __init__(self):
                        super().__init__(("initial", String))

                    def on_element(self, values: list[str | None]) -> list[str | None]:
                        name = values[0]
                        return [name.strip()[:1].upper() if name else None]

                Initial().on_batch([pa.array(["ada", None])])
        """
        import polars as pl

        columns = arrays[0] if self.signature == SIGNATURE_PACKED else arrays
        outputs = self._elements([pl.Series(array) for array in columns])
        return [series.to_arrow() for series in outputs]

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

                    def on_element(self, values: list[str | None]) -> list[str | None]:
                        name = values[0]
                        return [name.strip()[:1].upper() if name else None]

                tf.udf("name", Initial())
                tf.udf("name", Initial(output_names="first_letter"))
        """
        raise NotImplementedError

    def columns(self) -> list[OutputColumnSpec]:
        """The `(name, data type)` of each output column, in order.

        Examples:
            .. code-block:: python

                class Bounds(UDF):
                    def __init__(self):
                        super().__init__([("low", Int64), ("high", Int64)])

                    def on_element(self, values: list[int]) -> list[int]:
                        return [min(values), max(values)]

                Bounds().columns()
                Bounds(output_names=["smallest", "largest"]).columns()
                Bounds(output_prefix="ab_").columns()
        """
        return list(self._declared())

    def with_names(
        self,
        output_names: OutputNamesSpec | None = None,
        *,
        output_prefix: OutputAffixSpec | None = None,
        output_suffix: OutputAffixSpec | None = None,
    ) -> UDF:
        """A copy of this function with all its output columns renamed.

        Takes the same keywords as creating the function: `output_names`
        gives one new name per output column, in order (a name alone for a
        function with a single output column, or a list of them), and
        `output_prefix` and `output_suffix` are then added to every name.
        The data types stay as declared, and the function itself is left
        unchanged.

        Examples:
            .. code-block:: python

                class Bounds(UDF):
                    def __init__(self):
                        super().__init__([("low", Int64), ("high", Int64)])

                    def on_element(self, values: list[int]) -> list[int]:
                        return [min(values), max(values)]

                bounds = Bounds()
                tf.udf(["a", "b"], bounds.with_names(["smallest", "largest"]))

            A decorated function renamed the same way:

            .. code-block:: python

                @on_element(("ratio", Float64))
                def ratio(numerator: int, denominator: int) -> float:
                    return numerator / denominator

                tf.udf(["a", "n"], ratio.with_names("a_per_n"))
                tf.udf(["b", "n"], ratio.with_names("b_per_n"))

            Adding a prefix, a suffix or both to every output column:

            .. code-block:: python

                @on_element([("ratio", Float64), ("label", String)])
                def ratio(numerator: int, denominator: int) -> list[float | str]:
                    return [numerator / denominator, f"{numerator}/{denominator}"]

                tf.udf(["a", "n"], ratio.with_names(output_prefix="a_"))
                tf.udf(["b", "n"], ratio.with_names(output_suffix="_b"))
                tf.udf(
                    ["c", "n"],
                    ratio.with_names(
                        ["share", "fraction"], output_prefix="c_", output_suffix="_n"
                    ),
                )
        """
        function = copy.copy(self)
        function._outputs = self._renamed(output_names, output_prefix, output_suffix)
        return function

    def __call__(self, batch: pl.Series) -> pl.Series:
        import polars as pl

        inputs = [batch.struct.field(name) for name in batch.struct.fields]
        try:
            outputs = self._batch(inputs, len(batch))
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

    def _declared(self) -> tuple[OutputColumnSpec, ...]:
        outputs = getattr(self, "_outputs", None)
        if outputs is None:
            raise UDFErrorCode.UDF_4.exception(udf=self._label())
        return outputs

    def _renamed(
        self,
        output_names: OutputNamesSpec | None,
        output_prefix: OutputAffixSpec | None,
        output_suffix: OutputAffixSpec | None,
    ) -> tuple[OutputColumnSpec, ...]:
        names, prefix, suffix = _renaming(output_names, output_prefix, output_suffix)
        declared = self._declared()
        if names is None:
            names = [name for name, _ in declared]
        elif len(names) != len(declared):
            raise UDFErrorCode.UDF_17.exception(
                udf=self._label(), width=len(declared), count=len(names)
            )
        return tuple(
            (f"{prefix}{name}{suffix}", dtype)
            for name, (_, dtype) in zip(names, declared)
        )

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
            signatures=list(get_args(SignatureSpec)),
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

    Implement `on_batch(self, arrays: list[pa.Array]) -> list[pa.Array]`,
    `on_element(self, values: list[Any]) -> list[Any]` or both.

    Examples:
        .. code-block:: python

            class Spread(UDFPacked):
                def __init__(self):
                    super().__init__(("spread", Int64))

                def on_element(self, values: list[int]) -> list[int]:
                    return [max(values) - min(values)]

            tf.udf(["a", "b", "n"], Spread())
            tf.udf(["a", "b", "n"], Spread(output_names="range"))
    """

    @property
    def signature(self) -> SignatureSpec:
        return SIGNATURE_PACKED


class UDFUnpacked(UDF, ABC):
    """A `UDF` that receives each input as its own argument.

    :categories: udf

    Implement `on_batch(self, first: pa.Array, second: pa.Array, ...) ->
    list[pa.Array]`, `on_element(self, first: Any, second: Any, ...) ->
    list[Any]` or both.

    Examples:
        .. code-block:: python

            class Ratio(UDFUnpacked):
                def __init__(self):
                    super().__init__(("ratio", Float64))

                def on_element(self, numerator: int, denominator: int) -> list[float]:
                    return [numerator / denominator]

            tf.udf(["a", "n"], Ratio())
            tf.udf(["a", "n"], Ratio(output_names="share"))

        Both methods, the batch one doing the work and the row one at hand
        for checking a single row:

        .. code-block:: python

            import pyarrow as pa
            import pyarrow.compute as pc

            class Ratio(UDFUnpacked):
                def __init__(self):
                    super().__init__(("ratio", Float64))

                def on_batch(
                    self, numerator: pa.Int64Array, denominator: pa.Int64Array
                ) -> pa.DoubleArray:
                    return pc.divide(pc.cast(numerator, pa.float64()), denominator)

                def on_element(self, numerator: int, denominator: int) -> list[float]:
                    return [numerator / denominator]

            tf.udf(["a", "n"], Ratio())
            Ratio().on_element(3, 4)
    """

    @property
    def signature(self) -> SignatureSpec:
        return SIGNATURE_UNPACKED


class _FunctionUDF(UDF, ABC):
    def __init__(self, output_columns, function, signature):
        self._function = function
        self._signature = signature
        super().__init__(output_columns)

    @property
    def signature(self) -> SignatureSpec:
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


@_api.validate_call(UDFErrorCode.UDF_6)
def on_batch(
    output_columns: OutputColumnsSpec,
    *,
    signature: SignatureSpec = SIGNATURE_UNPACKED,
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

            import pyarrow as pa
            import pyarrow.compute as pc

            @on_batch(("total", Int64))
            def total(a: pa.Int64Array, b: pa.Int64Array) -> pa.Int64Array:
                return pc.add(a, b)

            tf.udf(["a", "b"], total)
            tf.udf(["c", "n"], total.with_names("c_plus_n"))

        Several output columns, from a list of arrays:

        .. code-block:: python

            import pyarrow as pa
            import pyarrow.compute as pc

            @on_batch([("low", Int64), ("high", Int64)])
            def bounds(
                a: pa.Int64Array, b: pa.Int64Array
            ) -> list[pa.Int64Array]:
                return [pc.min_element_wise(a, b), pc.max_element_wise(a, b)]

            tf.udf(["a", "b"], bounds)
            tf.udf(["a", "b"], bounds.with_names(["smallest", "largest"]))

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

    @_api.validate_call(UDFErrorCode.UDF_14)
    def decorate(function: Callable[..., Any]) -> UDF:
        return _BatchFunction(output_columns, function, signature)

    return decorate


@_api.validate_call(UDFErrorCode.UDF_6)
def on_element(
    output_columns: OutputColumnsSpec,
    *,
    signature: SignatureSpec = SIGNATURE_UNPACKED,
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
            def ratio(numerator: int, denominator: int) -> float:
                return numerator / denominator

            tf.udf(["a", "n"], ratio)
            tf.udf(["b", "n"], ratio.with_names("b_per_n"))

        Several output columns, from a list of values:

        .. code-block:: python

            @on_element([("ratio", Float64), ("label", String)])
            def ratio(numerator: int, denominator: int) -> list[float | str]:
                return [numerator / denominator, f"{numerator}/{denominator}"]

            tf.udf(["a", "n"], ratio)
            tf.udf(["a", "n"], ratio.with_names(["share", "fraction"]))

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

    @_api.validate_call(UDFErrorCode.UDF_14)
    def decorate(function: Callable[..., Any]) -> UDF:
        return _ElementFunction(output_columns, function, signature)

    return decorate


@_api.validate_call(UDFErrorCode.UDF_19)
def _renaming(
    output_names: OutputNamesSpec | None,
    output_prefix: OutputAffixSpec | None,
    output_suffix: OutputAffixSpec | None,
) -> tuple[list[str] | None, str, str]:
    return output_names, output_prefix or "", output_suffix or ""


def _holds_sequences(dtype) -> bool:
    base = dtype if isinstance(dtype, type) else type(dtype)
    return base in (List, Array)


def _described(value) -> str:
    if isinstance(value, (list, tuple)):
        kinds = ", ".join(type(item).__name__ for item in value)
        return f"{type(value).__name__} of {len(value)} ({kinds})"
    return type(value).__name__
