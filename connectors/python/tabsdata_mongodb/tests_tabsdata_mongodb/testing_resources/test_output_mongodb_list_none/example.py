#
# Copyright 2025 Tabs Data Inc.
#

import os

from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata_mongodb.conftest import (
    DB_PASSWORD,
    DB_USER,
    MONGODB_URI_WITHOUT_CREDENTIALS,
)

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

database_name = "test_list_none_database"
collection_name = "test_list_none_collection"


@td.subscriber(
    name="multiple_outputs_mongodb",
    tables="collection/table",
    destination=td.MongoDBDestination(
        MONGODB_URI_WITHOUT_CREDENTIALS,
        credentials=td.UserPasswordCredentials(DB_USER, DB_PASSWORD),
        collections_with_ids=[
            (f"{database_name}.{collection_name}", "id"),
            (f"{database_name}.{collection_name}", "id"),
        ],
        if_collection_exists="replace",
        maintain_order=True,
        update_existing=True,
    ),
)
def output_mongodb_list_none(
    df: td.TableFrame,
) -> tuple[td.TableFrame | None, td.TableFrame | None]:
    df.drop_nulls()
    return None, None


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        output_mongodb_list_none,
        save_location=DEFAULT_SAVE_LOCATION,
    )
