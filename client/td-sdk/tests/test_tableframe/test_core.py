#
# Copyright 2025 Tabs Data Inc.
#

import tabsdata as td

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_import_data_types():
    _ = td.Boolean()
    _ = td.Date()
    _ = td.Datetime()
    _ = td.Duration()
    _ = td.Float32()
    _ = td.Float64()
    _ = td.Int8()
    _ = td.Int16()
    _ = td.Int32()
    _ = td.Int64()
    _ = td.Int128()
    _ = td.Null()
    _ = td.String()
    _ = td.Time()
    _ = td.UInt8()
    _ = td.UInt16()
    _ = td.UInt32()
    _ = td.UInt64()
