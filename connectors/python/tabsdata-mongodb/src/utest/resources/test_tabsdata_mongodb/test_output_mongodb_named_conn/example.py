#
# Copyright 2026 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    conn_name: str, database_name: str, collection_name: str
):
    @td.subscriber(
        name="output_mongodb_named_conn",
        tables="collection/table",
        destination=td.MongoDBDest(
            conn=td.NamedConn(name=conn_name),
            collections_with_ids=(f"{database_name}.{collection_name}", None),
            if_collection_exists="append",
            update_existing=False,
        ),
    )
    def output_mongodb_named_conn(df: td.TableFrame) -> td.TableFrame:
        return df.drop_nulls()

    return output_mongodb_named_conn
