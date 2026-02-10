import os

import tabsdata as td


@td.publisher(
    source=td.LocalFileSource(os.path.join(os.getcwd(), "input", "persons.csv")),
    tables="persons",
)
def pub(persons: td.TableFrame) -> td.TableFrame:
    return persons
