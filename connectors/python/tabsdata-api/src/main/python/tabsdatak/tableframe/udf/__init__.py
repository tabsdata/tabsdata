#
# Copyright 2026 Tabsdata Inc.
#

"""User defined functions for `TableFrame`: Python code computing new columns.

Subclass `UDF` (or `UDFPacked` / `UDFUnpacked`, which fix how inputs are
passed), declare the columns it adds, and implement `on_batch` (vectorized,
over pyarrow arrays) or `on_element` (row by row, over Python values). Or
turn a plain function or lambda into one with the `on_batch` / `on_element`
decorators. Apply it with `TableFrame.udf`.
"""

from __future__ import annotations

from tabsdatak.tableframe.udf._function import (
    UDF as UDF,
    OutputColumn as OutputColumn,
    OutputColumns as OutputColumns,
    OutputOverride as OutputOverride,
    OutputOverrides as OutputOverrides,
    Signature as Signature,
    UDFPacked as UDFPacked,
    UDFUnpacked as UDFUnpacked,
    on_batch as on_batch,
    on_element as on_element,
)
