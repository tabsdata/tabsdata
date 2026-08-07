#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatak.api import TableFrameSpec, transformer
from tabsdatak.tableframe.functions import col


@transformer(
    input_tables=["persons"],
    output_tables=["spanish", "french", "german"],
)
def persons_transformer(
    persons: TableFrameSpec,
) -> tuple[TableFrameSpec, TableFrameSpec, TableFrameSpec]:
    assert persons is not None
    persons = persons.select([
        "identifier",
        "name",
        "surname",
        "nationality",
        "language",
    ])
    result = {}
    for nationality in ["Spanish", "French", "German"]:
        result[nationality] = persons.filter(col("nationality").eq(nationality)).drop(
            "nationality"
        )
    return result["Spanish"], result["French"], result["German"]
