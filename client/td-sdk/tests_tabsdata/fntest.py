#
# Copyright 2025 Tabs Data Inc.
#

import inspect
import logging
import os
import tempfile
from enum import Enum
from io import StringIO
from pathlib import Path
from typing import Any, Callable, TypeAlias, Union, cast
from unittest import mock

import pandas as pd
import polars as pl
import pytest
from test_tabsdata.test_tableframe.common import pretty_pandas, pretty_polars

import tabsdata as td
from tabsdata import (
    TableInput,
    TableOutput,
    TabsdataFunction,
    publisher,
    subscriber,
    transformer,
)
from tabsdata._tabsserver import invoker
from tabsdata._tabsserver.function.global_utils import (
    DEFAULT_DEVELOPMENT_LOCKS_LOCATION,
)
from tabsdata._tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata._tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata._utils.bundle_utils import create_bundle_archive
from tabsdata.tableframe._actions.dataquality.processor import DataQualityProcessor
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder
from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.conftest import (
    LOCAL_PACKAGES_LIST,
    MOCK_COLLECTION,
    write_v2_yaml_file,
)


def pytest_configure(config):
    """Register the tdfnrun marker."""
    config.addinivalue_line(
        "markers",
        "tdfnrun: automatically add behaviours to 'tdfnrun' tests (mocks sys.stdin).",
    )


@pytest.fixture(autouse=True)
def tdfnrun_fixture(request):
    if request.node.get_closest_marker("tdfnrun"):
        with mock.patch("sys.stdin", StringIO("mock_key: mock_value\n")):
            yield
    else:
        yield


class FunctionConstants(Enum):
    ENVIRONMENT_PREFIX = "tdtest"
    INSTANCE = "tdfnrun"
    WORK = "0"
    COLLECTION = MOCK_COLLECTION
    BUNDLE_FOLDER_NAME = "bundle"
    REQUEST_FOLDER_NAME = "request"
    REQUEST_FILE_NAME = REQUEST_FILE_NAME
    RESPONSE_FOLDER_NAME = "response"
    RESPONSE_FILE_NAME = RESPONSE_FILE_NAME
    INPUT_FOLDER_NAME = "input"
    OUTPUT_FOLDER_NAME = "output"
    OFFSET_FOLDER_NAME = "offset"
    INPUT_OFFSET_FILE_NAME = "offset.i.parquet"
    OUTPUT_OFFSET_FILE_NAME = "offset.i.parquet"
    DATA_FOLDER_NAME = "data"
    BIN_FOLDER_NAME = "bin"
    LOG_FOLDER_NAME = "log"
    LOCK_FOLDER = DEFAULT_DEVELOPMENT_LOCKS_LOCATION
    TABSDATA_PACKAGES = LOCAL_PACKAGES_LIST
    TEMP_ROOT = ".tdtest"
    TEMP_PREFIX = "td_"
    TEMP_SUFFIX = ".run"


# ToDo: accept parquet files files too
MockTableFrame: TypeAlias = Union[
    td.TableFrame,
    pl.LazyFrame,
    pl.DataFrame,
    pd.DataFrame,
    dict[str, list[Any]],
]

MockTableFrameSpec: TypeAlias = Union[
    None,
    MockTableFrame,
    list[Union[MockTableFrame, list]],
    tuple[Union[MockTableFrame, list], ...],
]


# ToDo: New helper functions like output(n) -> frame
class FnOut:
    def __init__(
        self,
        folder: Path,
        request: Path,
        response: Path,
        output: list[Path],
        offset: Path,
        environment: str,
        status: int,
        output_initial_values: bool = False,
    ):
        self._folder = folder
        self._request = request
        self._response = response
        self._output = output
        self._offset = offset
        self._environment = environment
        self._status = status
        self._output_initial_values = output_initial_values
        self.check()

    def check(self):
        if self._status != 0:
            raise ValueError(f"Status is not 0: '{self._status}'")

        if not self._folder.exists():
            raise FileNotFoundError(f"Function folder does not exist: '{self._folder}'")

        if not self._request.exists():
            raise FileNotFoundError(f"Request file does not exist: '{self._request}'")

        if not self._response.exists():
            raise FileNotFoundError(f"Response file does not exist: '{self._response}'")

        for output_file in self._output:
            if not output_file.exists():
                raise FileNotFoundError(f"Output file does not exist: '{output_file}'")

        if self._output_initial_values and not self._offset.exists():
            raise FileNotFoundError(f"Offset file does not exist: '{self._offset}'")

        if self._environment is None:
            raise ValueError("Environment is None")

    @property
    def folder(self) -> Path:
        return self._folder

    @property
    def request(self) -> Path:
        return self._request

    @property
    def response(self) -> Path:
        return self._response

    @property
    def output(self) -> list[Path]:
        return self._output

    @property
    def offset(self) -> Path:
        return self._offset

    @property
    def environment(self) -> str:
        return self._environment

    @property
    def status(self) -> int:
        return self._status


def paths_to_strings(item):
    if isinstance(item, Path):
        return str(item)
    elif isinstance(item, (list, tuple)):
        return [paths_to_strings(item) for item in item]
    else:
        return item


def convert_table(data: MockTableFrame) -> td.TableFrame:
    match data:
        case td.TableFrame():
            return data
        case pl.DataFrame() | pl.LazyFrame():
            return td.TableFrame.__build__(
                df=data,
                mode="raw",
                idx=None,
                properties=TableFramePropertiesBuilder.empty(),
            )
        case pd.DataFrame() | dict():
            return td.TableFrame.__build__(
                df=pl.DataFrame(data),
                mode="raw",
                idx=None,
                properties=TableFramePropertiesBuilder.empty(),
            )
        case _:
            raise ValueError(f"Unsupported table data type: {type(data)}")


def save_tables(
    tables: MockTableFrameSpec,
    input_folder: Path,
    position: str = "",
) -> list:
    if tables is None:
        tables = []
    elif not isinstance(tables, (list, tuple)):
        tables = [tables]

    files = []
    for index, table in enumerate(tables):
        table_stem = f"{position}.{index}" if position else str(index)
        if isinstance(table, (list, tuple)):
            files.append(save_tables(table, input_folder, table_stem))
        else:
            tf = convert_table(table)
            filename = f"{table_stem}.parquet"
            filepath = input_folder / filename
            tf._lf.collect().write_parquet(filepath)
            files.append(filepath)

    return files


def save_offset(table: MockTableFrame, filepath: Path):
    tf = convert_table(table)
    tf._lf.collect().write_parquet(filepath)


def name_files(
    function: TabsdataFunction | Callable[..., Any], output_folder: Path
) -> list[Path]:
    tdfunction = cast(TabsdataFunction, function)
    fntype = tdfunction.type
    if fntype == subscriber.__name__:
        paths = []
    elif fntype in (transformer.__name__, publisher.__name__):
        if isinstance(tdfunction.output, TableOutput):
            tdoutput = cast(TableOutput, tdfunction.output)
            tables = tdoutput.table
            output_tables = tables if isinstance(tables, list) else [tables]
            paths = [
                output_folder / f"{i}.{table_name}.parquet"
                for i, table_name in enumerate(output_tables)
            ]
        else:
            paths = []
    else:
        paths = []

    collection = FunctionConstants.COLLECTION.value
    input_tables = (
        cast(TableInput, tdfunction.input)._table_uris_map
        if isinstance(tdfunction.input, TableInput)
        else {}
    )
    output_tables = (
        cast(TableOutput, tdfunction.output)._table_list
        if isinstance(tdfunction.output, TableOutput)
        else []
    )
    # ToDo: Modify when support to input actions is added.
    input_actions = []
    output_actions = tdfunction.output_actions
    dq_processor = DataQualityProcessor(
        collection=collection,
        input_tables=input_tables,
        output_tables=output_tables,
        input_actions=input_actions,
        output_actions=output_actions,
    )
    input_dq_tables, output_dq_tables = dq_processor.table_names()
    dq_tables = [*input_dq_tables, *output_dq_tables]
    total_dq_table_names = [table.table for table in dq_tables]
    index = len(output_tables)
    dq_paths = [
        output_folder / f"{index + i}.{table_name}.parquet"
        for i, table_name in enumerate(total_dq_table_names)
    ]
    paths.extend(dq_paths)
    return paths


# ToDo: implement a mechanism that allows avoiding the creation a the running venv.
def tdfnrun(
    function: TabsdataFunction | Callable[..., Any],
    input_tables: MockTableFrameSpec = None,
    input_initial_values: MockTableFrame = None,
    output_initial_values: bool = False,
) -> FnOut:
    tdtest = Path(tempfile.gettempdir()) / FunctionConstants.TEMP_ROOT.value
    os.makedirs(tdtest, exist_ok=True)
    with tempfile.TemporaryDirectory(
        suffix=FunctionConstants.TEMP_SUFFIX.value,
        prefix=FunctionConstants.TEMP_PREFIX.value,
        dir=str(tdtest),
        ignore_cleanup_errors=True,
        delete=False,
    ) as root:
        os.chdir(root)

        logger = logging.getLogger(__name__)

        pretty_polars()
        pretty_pandas()
        pl.Config.set_verbose(False)

        logger.debug(f"Running test: {inspect.stack()[1].function} @ folder {root}")

        # noinspection PyBroadException
        try:
            file = function._func_original_file
        except Exception:
            file = None
        file = Path(file).stem if file else "unknown.p"
        # noinspection PyBroadException
        try:
            test = inspect.stack()[1].function
        except Exception:
            test = None
        test = test if test else "unknown.t"
        # noinspection PyBroadException
        try:
            fn = function._func.__name__
        except Exception:
            fn = None
        fn = fn if fn else "unknown.f"
        td_root = Path(TDLOCAL_FOLDER) / file / test / fn
        fn_root = Path(root)

        bundle_folder = fn_root / FunctionConstants.BUNDLE_FOLDER_NAME.value
        os.makedirs(bundle_folder, exist_ok=True)

        bundle_file = create_bundle_archive(
            function=function,
            local_packages=FunctionConstants.TABSDATA_PACKAGES.value,
            save_location=bundle_folder,
        )

        request_folder = fn_root / FunctionConstants.REQUEST_FOLDER_NAME.value
        os.makedirs(request_folder, exist_ok=True)
        response_folder = fn_root / FunctionConstants.RESPONSE_FOLDER_NAME.value
        os.makedirs(response_folder, exist_ok=True)

        request_file = request_folder / FunctionConstants.REQUEST_FILE_NAME.value
        response_file = response_folder / FunctionConstants.RESPONSE_FILE_NAME.value

        input_folder = fn_root / FunctionConstants.INPUT_FOLDER_NAME.value
        os.makedirs(input_folder, exist_ok=True)
        output_folder = fn_root / FunctionConstants.OUTPUT_FOLDER_NAME.value
        os.makedirs(output_folder, exist_ok=True)

        offset_folder = fn_root / FunctionConstants.OFFSET_FOLDER_NAME.value
        os.makedirs(offset_folder, exist_ok=True)

        input_offset_file = (
            offset_folder / FunctionConstants.INPUT_OFFSET_FILE_NAME.value
        )
        output_offset_file = (
            offset_folder / FunctionConstants.OUTPUT_OFFSET_FILE_NAME.value
        )

        data_folder = fn_root / FunctionConstants.DATA_FOLDER_NAME.value
        os.makedirs(data_folder, exist_ok=True)

        input_paths = save_tables(input_tables, input_folder)
        input_files = paths_to_strings(input_paths)

        output_paths = name_files(function, output_folder)
        output_files = paths_to_strings(output_paths)

        if input_initial_values is not None:
            save_offset(input_initial_values, input_offset_file)

        bin_folder = fn_root / FunctionConstants.BIN_FOLDER_NAME.value
        os.makedirs(bin_folder, exist_ok=True)

        log_folder = td_root / FunctionConstants.LOG_FOLDER_NAME.value

        write_v2_yaml_file(
            filename=str(request_file),
            bundle_archive_location=bundle_file,
            mock_dependency_location=input_files,
            mock_table_location=output_files,
            input_initial_values_path=(
                None if input_initial_values is None else str(input_offset_file)
            ),
            output_initial_values_path=(
                None if output_initial_values is None else str(output_offset_file)
            ),
            function_data_path=str(data_folder),
        )

        assert request_file.exists()

        environment, status = invoker.invoke(
            request_folder=request_folder,
            response_folder=str(response_folder),
            output_folder=str(output_folder),
            current_instance=FunctionConstants.INSTANCE.value,
            bin_folder=str(bin_folder),
            environment_prefix=FunctionConstants.ENVIRONMENT_PREFIX.value,
            locks_folder=FunctionConstants.LOCK_FOLDER.value,
            logs_folder=str(log_folder),
            work=FunctionConstants.WORK.value,
            temp_cwd=True,
        )

        fnout = FnOut(
            folder=fn_root,
            request=request_file,
            response=response_file,
            output=output_paths,
            offset=output_offset_file,
            environment=environment,
            status=status,
            output_initial_values=output_initial_values,
        )

        return fnout
