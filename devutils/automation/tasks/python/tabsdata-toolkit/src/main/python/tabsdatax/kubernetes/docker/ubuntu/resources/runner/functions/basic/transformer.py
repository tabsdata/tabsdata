#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


@td.transformer(
    input_tables="persons",
    output_tables=["spanish", "french", "german"],
)
def persons_transformer(
    persons: td.TableFrame,
) -> (td.TableFrame, td.TableFrame, td.TableFrame):
    persons = persons.select([
        "identifier",
        "name",
        "surname",
        "nationality",
        "language",
    ])
    result = {}
    for nationality in ["Spanish", "French", "German"]:
        result[nationality] = persons.filter(
            td.col("nationality").eq(nationality)
        ).drop("nationality")
    return result["Spanish"], result["French"], result["German"]
