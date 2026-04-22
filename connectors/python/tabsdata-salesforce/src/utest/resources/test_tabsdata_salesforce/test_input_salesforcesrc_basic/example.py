#
# Copyright 2026 Tabs Data Inc.
#

import os

import tabsdata as td

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))


@td.publisher(
    name="test_input_salesforcesrc",
    source=td.SalesforceSrc(
        conn=td.SalesforceConn(
            credentials=td.SalesforceTokenCredentials(
                username=td.EnvironmentSecret("SF0__USERNAME"),
                password=td.EnvironmentSecret("SF0__PASSWORD"),
                security_token=td.EnvironmentSecret("SF0__SECURITY_TOKEN"),
            ),
        ),
        query="SELECT Name FROM Contact",
    ),
    tables="output",
)
def input_salesforcesrc(df: td.TableFrame):
    new_df = df.drop_nulls()
    return new_df
