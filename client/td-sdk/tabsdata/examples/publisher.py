import os

from verify_environment import verify_publisher_environment

import tabsdata as td

verify_publisher_environment()


@td.publisher(
    source=td.LocalFileSource(os.path.join(os.getenv("TDX"), "input", "persons.csv")),
    tables=["persons"],
)
def pub(persons: td.TableFrame):
    return persons
