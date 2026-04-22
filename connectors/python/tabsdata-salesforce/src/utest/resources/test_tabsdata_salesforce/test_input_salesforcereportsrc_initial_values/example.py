#
# Copyright 2026 Tabs Data Inc.
#

import os

import tabsdata as td

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))


@td.publisher(
    name="test_input_salesforcereportsrc_initial_values",
    source=td.SalesforceReportSrc(
        conn=td.SalesforceConn(
            credentials=td.SalesforceTokenCredentials(
                username=td.EnvironmentSecret("SF0__USERNAME"),
                password=td.EnvironmentSecret("SF0__PASSWORD"),
                security_token=td.EnvironmentSecret("SF0__SECURITY_TOKEN"),
            ),
        ),
        report="FAKE REPORT ID",
        column_name_strategy="label",
        last_modified_column="FAKE COLUMN",
        initial_last_modified="2024-03-10T11:03:08.000+0000",
    ),
    tables="output",
)
def input_salesforcereportsrc_initial_values(tf: td.TableFrame):
    new_df = tf
    return new_df
