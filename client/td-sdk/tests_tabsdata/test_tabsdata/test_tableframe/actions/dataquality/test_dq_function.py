#
# Copyright 2025 Tabs Data Inc.
#

import pytest
from fntest import tdfnrun
from test_tabsdata.test_tableframe.actions.dataquality.dq_helper import dataset

import tabsdata as td
import tabsdata.dataquality as dq


@pytest.mark.tdfnrun
def test_dq_transformer():
    @td.transformer(
        name="transformer",
        input_tables=["table_i"],
        output_tables=["table_o"],
        on_output_tables=[
            dq.DataQuality(
                table="table_o",
                classifiers=[
                    dq.IsNull("number01"),
                ],
                operations=[
                    dq.Summary(),
                ],
            )
        ],
    )
    def dq_transformer(tf: td.TableFrame) -> td.TableFrame:
        return tf

    fnout = tdfnrun(
        function=dq_transformer,
        input_tables=dataset(),
    )
