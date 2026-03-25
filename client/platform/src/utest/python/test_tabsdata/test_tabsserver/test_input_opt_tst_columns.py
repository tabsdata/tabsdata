#
# Copyright 2026 Tabs Data Inc.
#

import logging

import pytest

import tabsdata as td
from test_tabsdata.fntest import tdfnrun
from test_tabsdata.test_tabsserver.test_input_file_info import write_test_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


@pytest.mark.localfile
@pytest.mark.tdfnrun
def test_opt_tst_columns_present(tmp_path):
    data_path = write_test_data(str(tmp_path), "csv")
    source = td.LocalFileSource(path=data_path, format="csv")

    @td.publisher(
        name="test_opt_tst_columns",
        source=source,
        tables="output",
    )
    def publisher_fn(df: td.TableFrame) -> td.TableFrame:
        return df

    fnrun = tdfnrun(function=publisher_fn)
    assert fnrun.status == 0

    result_df = fnrun.output[0].as_pl_df(with_sys_columns=True)

    assert "$td.opt.tst.srv" in result_df.columns
    assert "$td.opt.tst.usr" in result_df.columns
    assert "$td.opt.tst.ts" in result_df.columns
    assert "$td.opt.tst.fn" in result_df.columns

    srv_values = result_df["$td.opt.tst.srv"].unique().to_list()
    assert len(srv_values) == 1
    assert srv_values[0] is not None

    usr_values = result_df["$td.opt.tst.usr"].unique().to_list()
    assert len(usr_values) == 1
    assert usr_values[0] is not None

    ts_values = result_df["$td.opt.tst.ts"].unique().to_list()
    assert len(ts_values) == 1
    assert ts_values[0] is not None

    fn_values = result_df["$td.opt.tst.fn"].unique().to_list()
    assert len(fn_values) == 1
    assert fn_values[0] is not None
