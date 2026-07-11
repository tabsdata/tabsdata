#
# Copyright 2026 Tabs Data Inc.
#

# Quickstart transformer: add a `full_name` column and drop `age`.

from tabsdatak.api import TableFrameSpec, transformer
from tabsdatak.tableframe.functions import col, lit


@transformer(
    input_tables=["input/persons"],
    output_tables=["people"],
)
def qs_tfr(persons: TableFrameSpec) -> TableFrameSpec:
    return persons.with_columns(
        (col("first_name") + lit(" ") + col("last_name")).alias("full_name")
    ).drop("age")
