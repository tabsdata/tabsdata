#
# Copyright 2024 Tabs Data Inc.
#

import os

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
DEFAULT_SAVE_LOCATION = os.path.join(ROOT_PROJECT_DIR, "local_dev")


# In this example, we are obtaining the data from the file invoice_headers.parquet
# as the first parameter, and the data in both invoice_items.parquet as the second
# parameter. Then we return invoice_headers as is, and concatenate the dataframes in
# the invoice_items list. The output is saved in output1.json and output2.json, and
# expected_result1.json and expected_result2.json contain the expected output of
# applying the function to the input data.
# The URI provided is just a Mock, what will happen is we will inject the URI of
# parquet files into the input.yaml sent to the tabsserver.
@td.transformer(
    name="input_table_multiple_tables",
    input_tables=["collection/invoice_headers", "collection/invoice_items@HEAD^..HEAD"],
    output_tables=["output1", "output2"],
)
def input_table_multiple_tables(df: td.TableFrame, df2: [td.TableFrame]):
    return df, td.concat(df2)


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        input_table_multiple_tables,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
