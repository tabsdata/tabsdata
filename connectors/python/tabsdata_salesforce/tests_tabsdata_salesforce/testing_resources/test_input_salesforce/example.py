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
    name="test_input_salesforce",
    source=td.SalesforceSource(
        username=td.EnvironmentSecret("SALESFORCE_USERNAME"),
        password=td.EnvironmentSecret("SALESFORCE_PASSWORD"),
        security_token=td.EnvironmentSecret("SALESFORCE_SECURITY_TOKEN"),
        query="SELECT Name FROM Contact",
    ),
    tables="output",
)
def input_salesforce(df: td.TableFrame):
    new_df = df.drop_nulls()
    return new_df
