#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatak.api import TableFrameSpec, transformer
from tabsdatak.tableframe.functions import col, lit


@transformer(
    input_tables=["input/persons"],
    output_tables=["people"],
)
def qs_tfr(persons: TableFrameSpec) -> TableFrameSpec:
    if persons is None:
        return None
    return persons.with_columns(
        (col("first_name") + lit(" ") + col("last_name")).alias("full_name")
    ).drop("age")
