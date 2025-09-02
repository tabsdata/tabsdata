#
# Copyright 2025 Tabs Data Inc.
#

import os

import tabsdata as td

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))


# In this example, we are obtaining the data from the file data.csv in a s3
# bucket, and then dropping the null values. The output is saved in output.json, and
# expected_result.json contains the expected output of applying the function to the
# input data.
@td.publisher(
    name="test_input_salesforce_report_initial_values",
    source=td.SalesforceReportSource(
        username=td.EnvironmentSecret("SALESFORCE_USERNAME"),
        password=td.EnvironmentSecret("SALESFORCE_PASSWORD"),
        security_token=td.EnvironmentSecret("SALESFORCE_SECURITY_TOKEN"),
        column_by="label",
        report="FAKE REPORT ID",
        last_modified_column="FAKE COLUMN",
        initial_last_modified="2024-03-10T11:03:08.000+0000",
    ),
    tables="output",
)
def input_salesforce_report_initial_values(tf: td.TableFrame):
    new_df = tf
    return new_df
