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
from typing import Any, Callable, Literal, TypeAlias, Union, cast
from unittest import mock

import pandas as pd
import polars as pl
import pytest

import tabsdata as td
from tabsdata import (
    TableInput,
    TableOutput,
    TabsdataFunction,
    publisher,
    subscriber,
    transformer,
)
from tabsdata._io.stage_trigger import (
    CallConfig,
    LocalFileStageTriggerContext,
    StageTrigger,
    StageTriggerContext,
    StageTriggerStatus,
)
from tabsdata._tabsserver import invoker
from tabsdata._tabsserver.function.global_utils import (
    DEFAULT_DEVELOPMENT_LOCKS_LOCATION,
)
from tabsdata._tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata._tabsserver.function.yaml_parsing import (
    ExceptionYaml,
    FunctionYaml,
    ResponseYaml,
    StageTriggerYaml,
    parse_exception_yaml,
    parse_request_yaml,
    parse_response_yaml,
)
from tabsdata._tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata._utils.bundle_utils import create_bundle_archive
from tabsdata._utils.constants import TRUE_VALUES
from tabsdata._utils.tableframe import _helpers as td_helpers
from tabsdata.exceptions import StageTriggerExecutionError
from tabsdata.expansions.foundation.symbols import EXCEPTION_FILE
from tabsdata.tableframe._actions.dataquality.processor import DataQualityProcessor
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder
from test_tabsdata.bootest import TDLOCAL_FOLDER
from test_tabsdata.conftest import (
    LOCAL_PACKAGES_LIST,
    MOCK_COLLECTION,
    write_v2_function_yaml_file,
)
from test_tabsdata.test_tableframe.common import pretty_pandas, pretty_polars

HTTP_METHODS: TypeAlias = Literal["GET", "POST", "PUT"]

EXCEPTION_FILE_NAME = EXCEPTION_FILE

TD_USE_CURRENT_VENV = "TD_USE_CURRENT_VENV"


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "tdfntrigger(venv=True): automatically add behaviours to 'tdfntrigger' tests. "
        "Set venv=False to use current virtual environment instead of creating a new "
        "one.",
    )
    config.addinivalue_line(
        "markers",
        "tdfnrun(venv=True): automatically add behaviours to 'tdfnrun' tests."
        "Set venv=False to use current virtual environment instead of creating a new"
        "one.",
    )
    config.addinivalue_line(
        "markers",
        "tdfnjob(venv=True): automatically add behaviours to 'tdfnjob' tests."
        "Set venv=False to use current virtual environment instead of creating a new"
        "one.",
    )


from contextlib import contextmanager


@contextmanager
def handle_venv(venv: bool):
    if not venv:
        os.environ[TD_USE_CURRENT_VENV] = "true"
    else:
        os.environ[TD_USE_CURRENT_VENV] = "false"
    try:
        yield
    finally:
        if not venv:
            os.environ.pop(TD_USE_CURRENT_VENV, None)


@contextmanager
def handle_stdin():
    with mock.patch("sys.stdin", StringIO("mock_key: mock_value\n")):
        yield


@contextmanager
def setup(marker):
    if marker:
        venv = marker.kwargs.get("venv", False)
        with handle_venv(venv), handle_stdin():
            yield
    else:
        yield


@pytest.fixture(autouse=True)
def tdfntrigger_fixture(request):
    with setup(request.node.get_closest_marker("tdfntrigger")):
        yield


@pytest.fixture(autouse=True)
def tdfnrun_fixture(request):
    with setup(request.node.get_closest_marker("tdfnrun")):
        yield


@pytest.fixture(autouse=True)
def tdfnjob_fixture(request):
    with setup(request.node.get_closest_marker("tdfnjob")):
        yield


class FunctionConstants(Enum):
    ENVIRONMENT_PREFIX = "tdtest"
    INSTANCE = "tdfntest"
    WORK = "0"
    COLLECTION = MOCK_COLLECTION
    BUNDLE_FOLDER_NAME = "bundle"
    REQUEST_FOLDER_NAME = "request"
    REQUEST_FILE_NAME = REQUEST_FILE_NAME
    RESPONSE_FOLDER_NAME = "response"
    RESPONSE_FILE_NAME = RESPONSE_FILE_NAME
    STAGING_FOLDER_NAME = "staging"
    WORKING_FOLDER_NAME = "working"
    EXCEPTION_FOLDER_NAME = "response"
    EXCEPTION_FILE_NAME = EXCEPTION_FILE_NAME
    INPUT_FOLDER_NAME = "input"
    OUTPUT_FOLDER_NAME = "output"
    OFFSET_FOLDER_NAME = "offset"
    INPUT_OFFSET_FILE_NAME = "offset.i.parquet"
    OUTPUT_OFFSET_FILE_NAME = "offset.o.parquet"
    DATA_FOLDER_NAME = "data"
    BIN_FOLDER_NAME = "bin"
    LOG_FOLDER_NAME = "log"
    LOCK_FOLDER = DEFAULT_DEVELOPMENT_LOCKS_LOCATION
    TABSDATA_PACKAGES = LOCAL_PACKAGES_LIST
    TEMP_ROOT = ".tdtest"
    TEMP_PREFIX = "td_"
    TEMP_SUFFIX = ".run"
    TRIGGER_HTTP_METHOD = "POST"
    TRIGGER_HTTP_URL = "http://127.0.0.1:2457"
    PARQUET_EXTENSION_PATTERN = "*.parquet"


MockTableFrame: TypeAlias = Union[
    td.TableFrame,
    pl.LazyFrame,
    pl.DataFrame,
    pd.DataFrame,
    Path,
    str,
    dict[
        str,
        list[
            Union[
                td.TableFrame,
                pl.LazyFrame,
                pl.DataFrame,
                pd.DataFrame,
                Path,
                str,
            ]
        ],
    ],
]

MockTableFrameSpec: TypeAlias = Union[
    None,
    MockTableFrame,
    list[Union[MockTableFrame, list]],
    tuple[Union[MockTableFrame, list], ...],
]


class FnError(Exception):
    def __init__(
        self,
        message: str,
        fntrigger: "FnTrigger | None" = None,
        fnrun: "FnRun | None" = None,
    ):
        super().__init__(message)
        self.fntrigger = fntrigger
        self.fnrun = fnrun


class FnRequest:

    def __init__(self, path: Path):
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def as_object(self) -> Union[FunctionYaml, StageTriggerYaml, None]:
        if self._path.exists():
            return parse_request_yaml(str(self._path))
        return None


class FnResponse:

    def __init__(self, path: Path):
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def as_object(self) -> Union[ResponseYaml, None]:
        if self._path.exists():
            return parse_response_yaml(str(self._path))
        return None


class FnException:

    def __init__(self, path: Path):
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def as_object(self) -> Union[ExceptionYaml, None]:
        if self._path.exists():
            return parse_exception_yaml(str(self._path))
        return None


def drop_system_columns_td_tf(tf: td.TableFrame) -> td.TableFrame:
    system_columns = list(td_helpers.SYSTEM_COLUMNS)
    existing_columns = set(tf._lf.collect_schema().names())
    columns_to_drop = [
        column for column in system_columns if column in existing_columns
    ]
    lf = tf._lf.drop(columns_to_drop)
    return td.TableFrame.__build__(
        df=lf,
        mode="raw",
        idx=tf._idx,
        properties=tf._properties,
    )


def drop_system_columns_pl_lf(lf: pl.LazyFrame) -> pl.LazyFrame:
    system_columns = list(td_helpers.SYSTEM_COLUMNS)
    existing_columns = set(lf.collect_schema().names())
    columns_to_drop = [
        column for column in system_columns if column in existing_columns
    ]
    return lf.drop(columns_to_drop)


def drop_system_columns_pl_df(df: pl.DataFrame) -> pl.DataFrame:
    system_columns = list(td_helpers.SYSTEM_COLUMNS)
    existing_columns = set(df.collect_schema().names())
    columns_to_drop = [
        column for column in system_columns if column in existing_columns
    ]
    return df.drop(columns_to_drop)


def drop_system_columns_pd_df(df: pd.DataFrame) -> pd.DataFrame:
    system_columns = list(td_helpers.SYSTEM_COLUMNS)
    columns_to_drop = [column for column in system_columns if column in df.columns]
    return df.drop(columns=columns_to_drop)


class FnPath:

    def __init__(self, path: Path):
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def as_os_fd(self) -> Path:
        return self._path

    def as_td_tf(self, with_sys_columns: bool = True) -> td.TableFrame:
        lf = pl.scan_parquet(self._path)
        tf = td.TableFrame.__build__(
            df=lf,
            mode="raw",
            idx=None,
            properties=TableFramePropertiesBuilder.empty(),
        )
        if not with_sys_columns:
            tf = drop_system_columns_td_tf(tf)
        return tf

    def as_pl_lf(self, with_sys_columns: bool = True) -> pl.LazyFrame:
        lf = pl.scan_parquet(self._path)
        if not with_sys_columns:
            lf = drop_system_columns_pl_lf(lf)
        return lf

    def as_pl_df(self, with_sys_columns: bool = True) -> pl.DataFrame:
        df = pl.read_parquet(self._path)
        if not with_sys_columns:
            df = drop_system_columns_pl_df(df)
        return df

    def as_pd_df(self, with_sys_columns: bool = True) -> pd.DataFrame:
        df = pd.read_parquet(self._path)
        if not with_sys_columns:
            df = drop_system_columns_pd_df(df)
        return df


class FnOut:

    def __init__(self, paths: list[Path]):
        self._paths = [FnPath(path) for path in paths]

    @property
    def files(self) -> list[FnPath]:
        return self._paths

    def __len__(self) -> int:
        return len(self._paths)

    def __getitem__(self, index: int) -> FnPath:
        return self._paths[index]

    def __iter__(self):
        return iter(self._paths)

    def as_os_fd(self) -> list[Path]:
        return [path.as_os_fd() for path in self._paths]

    def as_td_tf(self, with_sys_columns: bool = True) -> list[td.TableFrame]:
        return [path.as_td_tf(with_sys_columns) for path in self._paths]

    def as_pl_lf(self, with_sys_columns: bool = True) -> list[pl.LazyFrame]:
        return [path.as_pl_lf(with_sys_columns) for path in self._paths]

    def as_pl_df(self, with_sys_columns: bool = True) -> list[pl.DataFrame]:
        return [path.as_pl_df(with_sys_columns) for path in self._paths]

    def as_pd_df(self, with_sys_columns: bool = True) -> list[pd.DataFrame]:
        return [path.as_pd_df(with_sys_columns) for path in self._paths]


class FnTrigger:
    def __init__(
        self,
        staging: Path,
        context: StageTriggerContext,
        trigger: StageTrigger,
        status: StageTriggerStatus,
    ):
        self._staging = staging
        self._context = context
        self._trigger = trigger
        self._status = status

    def check(self):
        if self._status != StageTriggerStatus.SUCCESS:
            raise FnError(
                message=f"Status is not 0: '{self._status}'",
                fntrigger=self,
                fnrun=None,
            )

        if not self._staging.exists():
            raise FnError(
                message=f"Staging folder does not exist: '{self._staging}'",
                fntrigger=self,
                fnrun=None,
            )

    @property
    def staging(self) -> Path:
        return self._staging

    @property
    def context(self) -> StageTriggerContext:
        return self._context

    @property
    def trigger(self) -> StageTrigger:
        return self._trigger

    @property
    def status(self) -> StageTriggerStatus:
        return self._status

    @property
    def output(self) -> FnOut:
        files = (
            sorted(
                file
                for file in self._staging.glob(
                    FunctionConstants.PARQUET_EXTENSION_PATTERN.value
                )
                if file.is_file()
            )
            if self._staging.exists()
            else []
        )
        return FnOut(files)


class FnRun:
    def __init__(
        self,
        fntrigger: FnTrigger | None,
        folder: Path,
        request: FnRequest,
        response: FnResponse,
        exception: FnException,
        output: list[Path],
        offset: Path,
        environment: str,
        status: int,
        output_initial_values: bool = False,
    ):
        self._fntrigger = fntrigger
        self._folder = folder
        self._request = request
        self._response = response
        self._exception = exception
        self._output = FnOut(output)
        self._offset = offset
        self._environment = environment
        self._status = status
        self._output_initial_values = output_initial_values

    def check(self, require_output=True):
        if self._status != 0:
            raise FnError(
                message=f"Status is not 0: '{self._status}'",
                fntrigger=self.fntrigger,
                fnrun=self,
            )

        if not self._folder.exists():
            raise FnError(
                message=f"Function folder does not exist: '{self._folder}'",
                fntrigger=self.fntrigger,
                fnrun=self,
            )

        if not self._request.path.exists():
            raise FnError(
                message=f"Request file does not exist: '{self._request.path}'",
                fntrigger=self.fntrigger,
                fnrun=self,
            )

        if not self._response.path.exists():
            raise FnError(
                message=f"Response file does not exist: '{self._response.path}'",
                fntrigger=self.fntrigger,
                fnrun=self,
            )

        if require_output:
            for output_file in self._output:
                if not output_file.path.exists():
                    raise FnError(
                        message=f"Output file does not exist: '{output_file.path}'",
                        fntrigger=self.fntrigger,
                        fnrun=self,
                    )

        if self._output_initial_values and not self._offset.exists():
            raise FnError(
                message=f"Offset file does not exist: '{self._offset}'",
                fntrigger=self.fntrigger,
                fnrun=self,
            )

        if self._environment is None:
            raise FnError(
                message="Environment is None",
                fntrigger=self.fntrigger,
                fnrun=self,
            )

    @property
    def fntrigger(self) -> FnTrigger:
        return self._fntrigger

    @property
    def folder(self) -> Path:
        return self._folder

    @property
    def request(self) -> FnRequest:
        return self._request

    @property
    def response(self) -> FnResponse:
        return self._response

    @property
    def exception(self) -> FnException:
        return self._exception

    @property
    def output(self) -> FnOut:
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
        case Path() | str():
            return td.TableFrame.__build__(
                df=pl.scan_parquet(data),
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
    fntype = function.type
    if fntype == subscriber.__name__:
        paths = []
    elif fntype in (transformer.__name__, publisher.__name__):
        if isinstance(function.output, TableOutput):
            tdoutput = cast(TableOutput, function.output)
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
        cast(TableInput, function.input)._table_uris_map
        if isinstance(function.input, TableInput)
        else {}
    )
    output_tables = (
        cast(TableOutput, function.output)._table_list
        if isinstance(function.output, TableOutput)
        else []
    )
    input_actions = []
    output_actions = function.output_actions
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


def _trigger(
    fn_root: Path,
    trigger: StageTrigger,
) -> FnTrigger:
    logger = logging.getLogger(__name__)
    logger.debug("Performing trigger...")

    staging = fn_root / FunctionConstants.STAGING_FOLDER_NAME.value
    os.makedirs(staging, exist_ok=True)

    working_folder = fn_root / FunctionConstants.WORKING_FOLDER_NAME.value
    os.makedirs(working_folder, exist_ok=True)

    context = LocalFileStageTriggerContext(
        staging_folder=str(staging),
        trigger_config=CallConfig(
            method=cast(
                HTTP_METHODS,
                FunctionConstants.TRIGGER_HTTP_METHOD.value,
            ),
            url=FunctionConstants.TRIGGER_HTTP_URL.value,
            headers={},
            body=None,
        ),
        logger=logger,
        working_dir=str(fn_root),
        end=trigger.end,
    )
    context.trigger = lambda: True
    context.trigger_monitor = lambda: True

    trigger.return_on_end = True
    try:
        trigger.run(context)
        status = StageTriggerStatus.SUCCESS
    except StageTriggerExecutionError:
        status = StageTriggerStatus.ERROR

    return FnTrigger(
        staging=staging,
        context=context,
        trigger=trigger,
        status=status,
    )


def _run(
    fntrigger: FnTrigger | None,
    function: TabsdataFunction | Callable[..., Any],
    fn_root: Path,
    td_root: Path,
    input_tables: MockTableFrameSpec,
    input_initial_values: MockTableFrame,
    output_initial_values: bool,
    staging_files: MockTableFrameSpec,
    function_defined_on: float,
    require_output: bool,
    staging_folder: Path = None,
    staging_uri: str = None,
) -> FnRun:
    logger = logging.getLogger(__name__)
    logger.debug("Performing run...")

    pretty_polars()
    pretty_pandas()
    pl.Config.set_verbose(False)

    use_current_venv = os.getenv(TD_USE_CURRENT_VENV, "").lower() in TRUE_VALUES
    bundle_folder = fn_root / FunctionConstants.BUNDLE_FOLDER_NAME.value
    os.makedirs(bundle_folder, exist_ok=True)

    bundle_file = create_bundle_archive(
        function=function,
        local_packages=(
            None if use_current_venv else FunctionConstants.TABSDATA_PACKAGES.value
        ),
        save_location=bundle_folder,
    )

    if staging_folder is None:
        staging_folder = fn_root / FunctionConstants.STAGING_FOLDER_NAME.value
        os.makedirs(staging_folder, exist_ok=True)
        save_tables(staging_files, staging_folder)
        staging_uri = staging_folder.as_uri()
    elif staging_uri is None:
        staging_uri = staging_folder.as_uri()

    request_folder = fn_root / FunctionConstants.REQUEST_FOLDER_NAME.value
    os.makedirs(request_folder, exist_ok=True)
    response_folder = fn_root / FunctionConstants.RESPONSE_FOLDER_NAME.value
    os.makedirs(response_folder, exist_ok=True)

    request_file = request_folder / FunctionConstants.REQUEST_FILE_NAME.value
    response_file = response_folder / FunctionConstants.RESPONSE_FILE_NAME.value
    exception_file = response_folder / FunctionConstants.EXCEPTION_FILE_NAME.value

    input_folder = fn_root / FunctionConstants.INPUT_FOLDER_NAME.value
    os.makedirs(input_folder, exist_ok=True)
    output_folder = fn_root / FunctionConstants.OUTPUT_FOLDER_NAME.value
    os.makedirs(output_folder, exist_ok=True)

    offset_folder = fn_root / FunctionConstants.OFFSET_FOLDER_NAME.value
    os.makedirs(offset_folder, exist_ok=True)

    input_offset_file = offset_folder / FunctionConstants.INPUT_OFFSET_FILE_NAME.value
    output_offset_file = offset_folder / FunctionConstants.OUTPUT_OFFSET_FILE_NAME.value

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

    write_v2_function_yaml_file(
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
        staging_uri=staging_uri,
        function_defined_on=function_defined_on,
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

    fnrun = FnRun(
        fntrigger=fntrigger,
        folder=fn_root,
        request=FnRequest(request_file),
        response=FnResponse(response_file),
        exception=FnException(exception_file),
        output=output_paths,
        offset=output_offset_file,
        environment=environment,
        status=status,
        output_initial_values=output_initial_values,
    )

    fnrun.check(require_output=require_output)
    return fnrun


def tdfntrigger(
    trigger: StageTrigger,
) -> FnTrigger:
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
        logger.debug(f"Running test: {inspect.stack()[1].function} @ folder {root}")

        fn_root = Path(root)

        fn_trigger = _trigger(
            fn_root=fn_root,
            trigger=trigger,
        )

        return fn_trigger


def tdfnrun(
    function: TabsdataFunction | Callable[..., Any],
    input_tables: MockTableFrameSpec = None,
    input_initial_values: MockTableFrame = None,
    output_initial_values: bool = False,
    staging_files: MockTableFrameSpec = None,
    function_defined_on: float = None,
    require_output: bool = True,
) -> FnRun:
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

        return _run(
            fntrigger=None,
            function=function,
            fn_root=fn_root,
            td_root=td_root,
            input_tables=input_tables,
            input_initial_values=input_initial_values,
            output_initial_values=output_initial_values,
            staging_files=staging_files,
            function_defined_on=function_defined_on,
            require_output=require_output,
        )


def tdfnjob(
    trigger: StageTrigger,
    function: TabsdataFunction | Callable[..., Any],
    input_tables: MockTableFrameSpec = None,
    input_initial_values: MockTableFrame = None,
    output_initial_values: bool = False,
    staging_files: MockTableFrameSpec = None,
    function_defined_on: float = None,
    require_output: bool = True,
) -> tuple[FnTrigger, FnRun]:
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

        fn_trigger = _trigger(
            fn_root=fn_root,
            trigger=trigger,
        )

        fn_run = _run(
            fntrigger=fn_trigger,
            function=function,
            fn_root=fn_root,
            td_root=td_root,
            input_tables=input_tables,
            input_initial_values=input_initial_values,
            output_initial_values=output_initial_values,
            staging_files=staging_files,
            function_defined_on=function_defined_on,
            require_output=require_output,
            staging_folder=fn_trigger.staging,
        )

        return fn_trigger, fn_run
