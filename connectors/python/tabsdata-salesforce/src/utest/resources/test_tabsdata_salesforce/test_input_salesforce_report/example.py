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
    name="test_input_salesforce_report",
    source=td.SalesforceReportSource(
        td.SalesforceTokenCredentials(
            username=td.EnvironmentSecret("SF0__USERNAME"),
            password=td.EnvironmentSecret("SF0__PASSWORD"),
            security_token=td.EnvironmentSecret("SF0__SECURITY_TOKEN"),
        ),
        report="FAKE_REPORT",
        column_name_strategy="columnName",
    ),
    tables="output",
)
def input_salesforce_report(df: td.TableFrame):
    return df
