#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td


def testing_tabsdata_function(
    conn_name: str,
    report_id: str,
    column_name_strategy: str = "columnName",
):
    @td.publisher(
        name="input_salesforcereportsrc_named_conn",
        source=td.SalesforceReportSrc(
            conn=td.NamedConn(name=conn_name),
            report=report_id,
            column_name_strategy=column_name_strategy,
        ),
        tables="output",
    )
    def input_salesforcereportsrc_named_conn(df: td.TableFrame) -> td.TableFrame:
        return df

    return input_salesforcereportsrc_named_conn
