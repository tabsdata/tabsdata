#
# Copyright 2026 Tabs Data Inc.
#

import os

import tabsdata as td

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))


@td.publisher(
    name="test_input_salesforcesrc_initial_values",
    source=td.SalesforceSrc(
        conn=td.SalesforceConn(
            credentials=td.SalesforceTokenCredentials(
                username=td.EnvironmentSecret("SF0__USERNAME"),
                password=td.EnvironmentSecret("SF0__PASSWORD"),
                security_token=td.EnvironmentSecret("SF0__SECURITY_TOKEN"),
            ),
        ),
        query=[
            (
                "SELECT Name,SystemModstamp FROM Contact "
                "WHERE SystemModstamp > $lastModified"
            ),
            (
                "SELECT Name,SystemModstamp FROM Contact "
                "WHERE SystemModstamp > $lastModified"
            ),
        ],
        initial_last_modified="2024-03-10T11:03:08.000+0000",
    ),
    tables=["output", "second_output"],
)
def input_salesforcesrc_initial_values(tf: td.TableFrame, second_tf: td.TableFrame):
    new_df = tf.drop_nulls() if tf is not None else tf
    second_new_tf = second_tf.drop_nulls() if second_tf is not None else second_tf
    return new_df, second_new_tf
