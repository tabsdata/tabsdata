#
# Copyright 2026 Tabs Data Inc.
#

import os

import tabsdata as td

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))


@td.publisher(
    name="test_input_salesforcereportsrc",
    source=td.SalesforceReportSrc(
        conn=td.SalesforceConn(
            credentials=td.SalesforceTokenCredentials(
                username=td.EnvironmentSecret("SF0__USERNAME"),
                password=td.EnvironmentSecret("SF0__PASSWORD"),
                security_token=td.EnvironmentSecret("SF0__SECURITY_TOKEN"),
            ),
        ),
        report="FAKE_REPORT",
        column_name_strategy="columnName",
    ),
    tables="output",
)
def input_salesforcereportsrc(df: td.TableFrame):
    return df
