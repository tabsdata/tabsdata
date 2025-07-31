#
# Copyright 2025 Tabs Data Inc.
#

import os

import polars as pl
from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.conftest import LOCAL_PACKAGES_LIST

import tabsdata as td
from tabsdata._utils.bundle_utils import create_bundle_archive

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))
ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        )
    )
)
DEFAULT_SAVE_LOCATION = TDLOCAL_FOLDER


# In this example, we are obtaining the data from the file data.json
# We then check that numeric columns are inferred its correct data type.
@td.publisher(
    td.LocalFileSource(os.path.join(ABSOLUTE_LOCATION, "data.ndjson"), format="ndjson"),
    "output",
)
def input_file_ndjson_dtype_inference(tf: td.TableFrame):
    schema = tf.schema
    assert (
        schema.get("col_i_f") == pl.Float64
    ), f"Column 'col_i_f' was expected to be Float64, but got {schema.get("col_i_f")}"
    assert (
        schema.get("col_b_s") == pl.Utf8
    ), f"Column 'col_b_s' was expected to be Utf8, but got {schema.get("col_b_s")}"
    return tf


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_file_ndjson_dtype_inference,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
