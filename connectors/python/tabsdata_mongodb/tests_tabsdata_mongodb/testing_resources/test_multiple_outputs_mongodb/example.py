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
    name="multiple_outputs_mongodb",
    tables="collection/table",
    destination=td.MongoDBDestination(
        uri="mongodb://fake_uri",
        collections_with_ids=[
            ("database1.collection1", "id_column1"),
            ("database2.collection2", "id_column2"),
        ],
    ),
)
def multiple_outputs_mongodb(df: td.TableFrame):
    new_df = df.drop_nulls()
    return new_df, new_df


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        multiple_outputs_mongodb,
        save_location=DEFAULT_SAVE_LOCATION,
    )
