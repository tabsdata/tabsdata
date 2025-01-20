import os

import tabsdata as td


@td.publisher(
    source=td.LocalFileSource(os.path.join(os.getenv("TDX"), "input", "persons.csv")),
    tables=["persons"],
)
def pub(persons: td.TableFrame):
    return persons
