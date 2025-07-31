#
# Copyright 2024 Tabs Data Inc.
#

import os

import pandas as pd
from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.conftest import LOCAL_PACKAGES_LIST

import tabsdata as td
from tabsdata._utils.bundle_utils import create_bundle_archive

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))
# Currently points to the root of the tabsdata project
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
format = td.CSVFormat(separator=",", input_has_header=True)
path = [
    os.path.join(ABSOLUTE_LOCATION, "invoice-headers.csv"),
    os.path.join(ABSOLUTE_LOCATION, "invoice-items-*.csv"),
]


@td.publisher(
    td.LocalFileSource(
        path,
        format=format,
    ),
    ["output1", "output2"],  # required,
)
def custom_requirements(
    headers: td.TableFrame, items: [td.TableFrame]
) -> (td.TableFrame, td.TableFrame):
    # This is only here to test that pandas was installed properly
    data = {"calories": [420, 380, 390], "duration": [50, 40, 45]}
    pd.DataFrame(data)
    # transformations can be done here
    return headers, td.concat(items)


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    # To run this, you need to create a custom_requirements.yaml file in the same folder
    create_bundle_archive(
        custom_requirements,
        requirements=os.path.join(ABSOLUTE_LOCATION, "custom_requirements.yaml"),
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
