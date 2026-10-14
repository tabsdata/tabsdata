#
# Copyright 2026 Tabsdata Inc.
#

"""User defined functions for `TableFrame`: Python code computing new columns.

Subclass `UDF` (or `UDFPacked` / `UDFUnpacked`, which fix how inputs are
passed), declare the columns it adds, and implement `on_batch` (vectorized,
over pyarrow arrays), `on_element` (row by row, over Python values) or both.
Or turn a plain function or lambda into one with the `on_batch` /
`on_element` decorators. Apply it with `TableFrame.udf`, renaming its output
columns with `output_names`, `output_prefix` and `output_suffix`, or
`UDF.with_names`, when needed.
"""

from __future__ import annotations

from tabsdatak.tableframe.udf._function import (
    UDF as UDF,
    UDFPacked as UDFPacked,
    UDFUnpacked as UDFUnpacked,
    on_batch as on_batch,
    on_element as on_element,
)
from tabsdatak.tableframe.udf._typing import (
    OutputAffixSpec as OutputAffixSpec,
    OutputColumnSpec as OutputColumnSpec,
    OutputColumnsSpec as OutputColumnsSpec,
    OutputDataTypeSpec as OutputDataTypeSpec,
    OutputNameSpec as OutputNameSpec,
    OutputNamesSpec as OutputNamesSpec,
    SignatureSpec as SignatureSpec,
)
