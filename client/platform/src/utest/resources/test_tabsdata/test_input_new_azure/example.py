#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    bucket_uri: str,
    azure_credentials=None,
    format=None,
    path=None,
    initial_last_modified=None,
    file_info=None,
    infer_schema_length=None,
):
    @td.publisher(
        name="test_input_new_azure",
        source=td.AzureSrc(
            conn=td.AzureConn(
                container_uri=bucket_uri,
                credentials=azure_credentials,
            ),
            path=path,
            format=format,
            initial_last_modified=initial_last_modified,
            file_info=file_info,
            infer_schema_length=infer_schema_length,
        ),
        tables="output",
    )
    def input_azure_csv(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df

    return input_azure_csv
