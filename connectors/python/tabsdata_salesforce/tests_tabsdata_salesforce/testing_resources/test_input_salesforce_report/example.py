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
        username=td.EnvironmentSecret("SALESFORCE_USERNAME"),
        password=td.EnvironmentSecret("SALESFORCE_PASSWORD"),
        security_token=td.EnvironmentSecret("SALESFORCE_SECURITY_TOKEN"),
        report="FAKE_REPORT",
        column_by="columnName",
    ),
    tables="output",
)
def input_salesforce_report(df: td.TableFrame):
    return df
