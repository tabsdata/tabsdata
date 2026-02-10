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
):
    @td.subscriber(
        name="output_bigquery_none",
        tables="collection/table",
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
    def output_bigquery_none(_: td.TableFrame) -> None:
        return None

    return output_bigquery_none
