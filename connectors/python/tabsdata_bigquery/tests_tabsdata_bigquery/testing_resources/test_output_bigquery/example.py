#
# Copyright 2025 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    gcs_folder: str,
    credentials: td._credentials.GCPCredentials,
    project: str,
    dataset: str,
    tables: str | list[str] | None,
    if_table_exists: str = "append",
    schema_strategy: str = "update",
    input_table: str = "collection/table",
):
    @td.subscriber(
        name="output_bigquery",
        tables=input_table,
        destination=td.BigQueryDest(
            td.BigQueryConn(
                gcs_folder,
                credentials,
                project=project,
                dataset=dataset,
            ),
            tables,
            if_table_exists=if_table_exists,
            schema_strategy=schema_strategy,
        ),
    )
    def output_bigquery(df: td.TableFrame) -> td.TableFrame:
        new_df = df.drop_nulls()
        return new_df

    return output_bigquery
