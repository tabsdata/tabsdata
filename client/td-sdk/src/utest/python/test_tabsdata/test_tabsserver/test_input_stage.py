#
# Copyright 2026 Tabs Data Inc.
#

import base64
import json
import logging
from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

import tabsdata as td
from tabsdata._tabsserver.function.offset_utils import (
    OFFSET_LAST_MODIFIED_VARIABLE_NAME,
)
from test_tabsdata.bootest import TDLOCAL_FOLDER
from test_tabsdata.fntest import tdfnrun

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER

PAST_REGISTRATION_TIME = (datetime.now() - timedelta(30)).timestamp() * 1000.0
FUTURE_REGISTRATION_TIME = (datetime.now() + timedelta(30)).timestamp() * 1000.0


def generate_mocked_lastmod_info(date: str) -> str:
    # We use a fake skeleton of a lastmod_info json to mock the input we would get
    # from the supervisor
    lastmod_info = {
        "V1": {
            "initial_last_modified": "2024-09-05T01:01:00.010Z",
            "entries": {"/testing_nested_import/*.csv": [date, ["new_data.csv"]]},
        }
    }
    return base64.b64encode(json.dumps(lastmod_info).encode("utf-8")).decode("utf-8")


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.parametrize("use_existing_data", [True, False])
@pytest.mark.skip
def test_stage(use_existing_data):
    """In this test case, the staging directory will have 2 files, which are put
    there after the mocked function_defined_on time. Therefore, the function should
    consume both of them."""

    @td.publisher(
        source=td.Stage(use_existing_data=use_existing_data),
        tables="output",
    )
    def tfr(tfs: list[td.TableFrame]) -> td.TableFrame:
        new_tfs = td.concat(tfs)
        return new_tfs

    file_1 = pl.DataFrame({"first_column": [1, 2, 3], "second_column": [4, 5, 6]})
    file_2 = pl.DataFrame({"first_column": [4, 5, 6], "second_column": [7, 8, 9]})
    function_defined_on = PAST_REGISTRATION_TIME
    fnrun = tdfnrun(
        function=tfr,
        staged_files=[file_1, file_2],
        function_defined_on=function_defined_on,
    )
    output = fnrun.output.as_pl_df(False)
    expected_output = pl.concat([file_1, file_2])
    assert len(output) == 1
    assert output[0].equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.parametrize("use_existing_data", [True, False])
@pytest.mark.skip
def test_stage_function_defined_on_late(use_existing_data):
    """In this test case, the staging directory will have 2 files, which are put
    there before the mocked function_defined_on time. Therefore, the function should
    not consume them."""

    @td.publisher(
        source=td.Stage(use_existing_data=use_existing_data),
        tables="output",
    )
    def tfr(tfs: list[td.TableFrame]) -> None:
        assert isinstance(tfs, list)
        assert len(tfs) == 0
        return None

    file_1 = pl.DataFrame({"first_column": [1, 2, 3], "second_column": [4, 5, 6]})
    file_2 = pl.DataFrame({"first_column": [4, 5, 6], "second_column": [7, 8, 9]})
    function_defined_on = FUTURE_REGISTRATION_TIME
    tdfnrun(
        function=tfr,
        staged_files=[file_1, file_2],
        function_defined_on=function_defined_on,
        require_output=False,
    )


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.skip
def test_stage_function_re_registered_use_existing():
    """In this test case, the staging directory will have 2 files, which are put
    there before the mocked function_defined_on time. Therefore, the function should
    not consume them."""

    @td.publisher(
        source=td.Stage(use_existing_data=True),
        tables="output",
    )
    def tfr(tfs: list[td.TableFrame]) -> td.TableFrame:
        new_tfs = td.concat(tfs)
        return new_tfs

    file_1 = pl.DataFrame({"first_column": [1, 2, 3], "second_column": [4, 5, 6]})
    file_2 = pl.DataFrame({"first_column": [4, 5, 6], "second_column": [7, 8, 9]})
    function_defined_on = FUTURE_REGISTRATION_TIME
    input_initial_values = pl.DataFrame(
        {
            OFFSET_LAST_MODIFIED_VARIABLE_NAME: generate_mocked_lastmod_info(
                "2024-09-06T14:33:45Z"
            )
        }
    )
    fnrun = tdfnrun(
        function=tfr,
        staged_files=[file_1, file_2],
        function_defined_on=function_defined_on,
        input_initial_values=input_initial_values,
    )
    output = fnrun.output.as_pl_df(False)
    expected_output = pl.concat([file_1, file_2])
    assert len(output) == 1
    assert output[0].equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.skip
def test_stage_function_re_registered_no_use_existing():
    """In this test case, the staging directory will have 2 files, which are put
    there before the mocked function_defined_on time. Therefore, the function should
    not consume them."""

    @td.publisher(
        source=td.Stage(use_existing_data=False),
        tables="output",
    )
    def tfr(tfs: list[td.TableFrame]) -> None:
        assert isinstance(tfs, list)
        assert len(tfs) == 0
        return None

    file_1 = pl.DataFrame({"first_column": [1, 2, 3], "second_column": [4, 5, 6]})
    file_2 = pl.DataFrame({"first_column": [4, 5, 6], "second_column": [7, 8, 9]})
    function_defined_on = FUTURE_REGISTRATION_TIME
    input_initial_values = pl.DataFrame(
        {
            OFFSET_LAST_MODIFIED_VARIABLE_NAME: generate_mocked_lastmod_info(
                "2024-09-06T14:33:45Z"
            )
        }
    )
    tdfnrun(
        function=tfr,
        staged_files=[file_1, file_2],
        function_defined_on=function_defined_on,
        input_initial_values=input_initial_values,
        require_output=False,
    )


@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.parametrize("use_existing_data", [True, False])
@pytest.mark.skip(
    "Pending a decision about whether to mock the test or turn it "
    "into an integration test."
)
def test_stage_function_re_registered_second_run(use_existing_data):
    """In this test case, the staging directory will have 2 files, which are put
    there before the mocked function_defined_on time. Therefore, the function should
    not consume them."""

    @td.publisher(
        source=td.Stage(use_existing_data=use_existing_data),
        tables="output",
    )
    def tfr(tfs: list[td.TableFrame]) -> None:
        assert isinstance(tfs, list)
        assert len(tfs) == 0
        return None

    file_1 = pl.DataFrame({"first_column": [1, 2, 3], "second_column": [4, 5, 6]})
    file_2 = pl.DataFrame({"first_column": [4, 5, 6], "second_column": [7, 8, 9]})
    function_defined_on = PAST_REGISTRATION_TIME
    input_initial_values = pl.DataFrame(
        {
            OFFSET_LAST_MODIFIED_VARIABLE_NAME: generate_mocked_lastmod_info(
                (datetime.now() + timedelta(30)).strftime("%Y-%m-%dT%H:%M:%SZ")
            )
        }
    )
    tdfnrun(
        function=tfr,
        staged_files=[file_1, file_2],
        function_defined_on=function_defined_on,
        input_initial_values=input_initial_values,
        require_output=False,
    )
