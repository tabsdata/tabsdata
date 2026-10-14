#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections import Counter
from typing import Annotated, Any, Literal, TypeAlias, Union, get_args

from pydantic import AfterValidator, Discriminator, Field, PlainValidator, Strict, Tag

from tabsdatak.error import ConstraintError
from tabsdatak.tableframe.datatypes import ColumnDataType, DataType, _valid_type
from tabsdatak.tableframe.udf.error import UDFErrorCode


def _shape(value: Any) -> str:
    return "many" if isinstance(value, list) else "one"


def _data_type(dtype: Any) -> ColumnDataType:
    if not isinstance(dtype, DataType) and not (
        isinstance(dtype, type) and issubclass(dtype, DataType)
    ):
        raise ValueError(
            f"{dtype!r} is not a data type from tabsdatak.tableframe.datatypes"
        )
    try:
        return _valid_type(dtype)
    except TypeError as error:
        raise ValueError(str(error)) from None


def _repeated(names: list[str]) -> list[str]:
    return sorted(name for name, count in Counter(names).items() if count > 1)


def _columns(columns: Any) -> list[tuple[str, ColumnDataType]]:
    columns = columns if isinstance(columns, list) else [columns]
    repeated = _repeated([name for name, _ in columns])
    if repeated:
        raise ConstraintError(UDFErrorCode.UDF_7, names=repeated)
    return columns


def _names(names: Any) -> list[str]:
    names = names if isinstance(names, list) else [names]
    repeated = _repeated(names)
    if repeated:
        raise ConstraintError(UDFErrorCode.UDF_7, names=repeated)
    return names


SignatureSpec: TypeAlias = Literal["packed", "unpacked"]
"""How the inputs are passed to a user defined function: `"packed"`
together as one list, `"unpacked"` each as its own argument."""

SIGNATURE_PACKED, SIGNATURE_UNPACKED = get_args(SignatureSpec)

OutputNameSpec: TypeAlias = Annotated[str, Field(min_length=1)]
"""The name of an output column: a non-empty string."""

OutputDataTypeSpec: TypeAlias = Annotated[ColumnDataType, PlainValidator(_data_type)]
"""The data type of an output column, from `tabsdatak.tableframe.datatypes`."""

OutputColumnSpec: TypeAlias = Annotated[
    tuple[OutputNameSpec, OutputDataTypeSpec], Strict()
]
"""An output column, as a `(name, data type)` tuple."""

OutputColumnsSpec: TypeAlias = Annotated[
    Union[
        Annotated[OutputColumnSpec, Tag("one")],
        Annotated[list[OutputColumnSpec], Field(min_length=1), Tag("many")],
    ],
    Discriminator(_shape),
    AfterValidator(_columns),
]
"""The output columns of a user defined function: one `(name, data type)`
tuple, or a non-empty list of them with unique names."""

OutputNamesSpec: TypeAlias = Annotated[
    Union[
        Annotated[OutputNameSpec, Tag("one")],
        Annotated[list[OutputNameSpec], Field(min_length=1), Tag("many")],
    ],
    Discriminator(_shape),
    AfterValidator(_names),
]
"""New names for the output columns of a user defined function: a name
alone, or a non-empty list of unique names."""

OutputAffixSpec: TypeAlias = str
"""A prefix or a suffix added to the name of every output column."""
