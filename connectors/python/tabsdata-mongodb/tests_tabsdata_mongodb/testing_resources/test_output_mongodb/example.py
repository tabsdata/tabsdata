#
# Copyright 2025 Tabs Data Inc.
#

import os

import tabsdata as td
from tabsdata._utils.bundle_utils import create_bundle_archive
from tests_tabsdata.bootest import TDLOCAL_FOLDER

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))
ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        )
    )
)
DEFAULT_SAVE_LOCATION = TDLOCAL_FOLDER


@td.subscriber(
    name="output_mongodb",
    tables="collection/table",
    destination=td.MongoDBDestination(
        uri="mongodb://fake_uri",
        collections_with_ids=("database.collection", "id_column"),
    ),
)
def output_mongodb(df: td.TableFrame) -> td.TableFrame:
    new_df = df.drop_nulls()
    return new_df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        output_mongodb,
        save_location=DEFAULT_SAVE_LOCATION,
    )
