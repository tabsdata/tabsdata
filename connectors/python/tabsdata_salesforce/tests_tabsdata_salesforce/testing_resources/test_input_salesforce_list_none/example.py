#
# Copyright 2025 Tabs Data Inc.
#

import os

import tabsdata as td

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))


@td.publisher(
    name="test_input_salesforce",
    source=td.SalesforceSource(
        td.SalesforceTokenCredentials(
            username=td.EnvironmentSecret("SALESFORCE_USERNAME"),
            password=td.EnvironmentSecret("SALESFORCE_PASSWORD"),
            security_token=td.EnvironmentSecret("SALESFORCE_SECURITY_TOKEN"),
        ),
        query="SELECT Name FROM Contact",
    ),
    tables=["output_", "1output_2"],
)
def input_salesforce_list_none(
    df: td.TableFrame,
) -> tuple[td.TableFrame | None, td.TableFrame | None]:
    df.drop_nulls()
    return None, None
