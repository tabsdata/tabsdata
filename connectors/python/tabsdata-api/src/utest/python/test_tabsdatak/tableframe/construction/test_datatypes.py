#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest

from tabsdatak.tableframe import datatypes

pytestmark = pytest.mark.tableframe

ALLOWED = {
    "Array",
    "Binary",
    "Boolean",
    "Categorical",
    "DataType",
    "Date",
    "Datetime",
    "Decimal",
    "Duration",
    "Enum",
    "Float16",
    "Float32",
    "Float64",
    "Int128",
    "Int16",
    "Int32",
    "Int64",
    "Int8",
    "List",
    "Map",
    "Null",
    "String",
    "Struct",
    "Time",
    "UInt128",
    "UInt16",
    "UInt32",
    "UInt64",
    "UInt8",
    "Utf8",
}
REFUSED = {"BaseExtension", "Extension", "Object", "Unknown"}


def test_every_polars_datatype_is_allowed_or_refused():
    polars = {
        name
        for name in dir(pl)
        if isinstance(getattr(pl, name), type)
        and issubclass(getattr(pl, name), pl.DataType)
    }
    unknown = polars - ALLOWED - REFUSED
    assert not unknown, f"Classify these new Polars datatypes: {sorted(unknown)}"
    assert polars == ALLOWED | REFUSED


def test_the_allowed_datatypes_are_the_storable_ones():
    # noinspection PyProtectedMember
    assert set(datatypes._ALLOWED_TYPES) == {getattr(pl, name) for name in ALLOWED}


@pytest.mark.parametrize("name", sorted(REFUSED))
def test_a_refused_datatype_is_not_allowed(name):
    with pytest.raises(TypeError, match="is not allowed"):
        # noinspection PyProtectedMember
        datatypes._valid_type(getattr(pl, name))
