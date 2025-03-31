#
# Copyright 2024 Tabs Data Inc.
#

import os

from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.conftest import LOCAL_PACKAGES_LIST

import tabsdata as td
from tabsdata.utils.bundle_utils import create_bundle_archive

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))
ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        )
    )
)
DEFAULT_SAVE_LOCATION = TDLOCAL_FOLDER

# In this example, we are obtaining the data from the file invoice-headers.csv and
# returning it as is in expected-result1.json. The files invoice-items-*.csv are being
# concatenated and returned in expected-result2.json.

path = [
    os.path.join(ABSOLUTE_LOCATION, "invoice-headers.csv"),
    os.path.join(ABSOLUTE_LOCATION, "invoice-items-*.csv"),
]
format = td.CSVFormat(separator=",", input_has_header=True)


@td.publisher(
    name="multiple_inputs_multiple_outputs",
    source=td.LocalFileSource(
        path,
        format=format,
        initial_last_modified="2024-09-09T00:00:00",
    ),
    tables=["output1", "output2"],  # required,
)
def multiple_inputs_multiple_outputs(
    headers: td.TableFrame, items: [td.TableFrame]
) -> (td.TableFrame, td.TableFrame):
    # transformations can be done here
    return headers, td.concat(items)


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        multiple_inputs_multiple_outputs,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
