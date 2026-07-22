#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


@td.publisher(
    source=td.LocalFileSource("/root/.td/functions/basic/input/persons.csv"),
    tables="persons",
)
def persons_publisher(persons: td.TableFrame) -> td.TableFrame:
    return persons
