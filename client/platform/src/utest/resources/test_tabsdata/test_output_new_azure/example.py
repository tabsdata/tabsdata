#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    container_uri: str,
    path: str,
    credentials=None,
    format=None,
):
    @td.subscriber(
        name="output_new_azure",
        tables="collection/table",
        destination=td.AzureDest(
            conn=td.AzureConn(
                container_uri=container_uri,
                credentials=credentials,
            ),
            path=path,
            format=format,
        ),
    )
    def output_new_azure(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return output_new_azure
