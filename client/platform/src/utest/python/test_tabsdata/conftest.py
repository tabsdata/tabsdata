#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import inspect
import logging
import os
import pathlib
import re
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from tenacity import retry, stop_after_delay, wait_fixed
from typing import Any

import pytest
import yaml
from xdist.workermanage import WorkerController

try:
    from docutils import nodes
    from docutils.parsers.rst import roles

    def _sphinx_role_stub(  # noqa: PLR0913
        name,
        rawtext,
        text,
        lineno,
        inliner,
        options=None,
        content=None,
    ):
        return [nodes.literal(rawtext, text)], []

    for _role in ("func", "class", "meth", "mod", "attr", "data", "exc", "obj"):
        roles.register_local_role(_role, _sphinx_role_stub)
except ImportError:
    pass

try:
    import pytest_reporter_html1.plugin as _html1_plugin

    _publish_parts = _html1_plugin.publish_parts

    def _quiet_publish_parts(*args, **kwargs):
        overrides = dict(kwargs.get("settings_overrides") or {})
        overrides.setdefault("report_level", 3)  # 1=info 2=warning 3=error
        kwargs["settings_overrides"] = overrides
        return _publish_parts(*args, **kwargs)

    _html1_plugin.publish_parts = _quiet_publish_parts
except ImportError:
    pass

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._constants as td_constants
from tabsdata._tdfunction import TableInput, TableOutput, TabsdataFunction
from tabsdata._utils.constants import TABSDATA_TOOLKIT_PACKAGE_NAME

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._generators import _id
from tabsdata._utils.tdlogging import setup_tests_logging
from tabsdata._utils.temps import tabsdata_temp_folder
from tabsdata.api.apiserver import APIServer, obtain_connection
from tabsdata.api.status_utils.data_version import (
    DataVersionStatus,
    data_version_status_to_mapping,
)
from tabsdata.api.tabsdata_server import TabsdataServer

# Import fixtures from backend modules so pytest can discover them
# These imports make the fixtures available to all tests
from test_tabsdata.common.backend import (  # noqa: F401
    azure_client,
    azure_config,
    clickhouse_config,
    clickhouse_client,
    druid_config,
    gcs_client,
    gcs_config,
    kafka_config,
    db2_config,
    mariadb_config,
    mysql_config,
    oracle_config,
    postgres_config,
    s3_client,
    s3_config,
    starrocks_config,
    starrocks_client,
    testing_hashicorp_vault,
)
from test_tabsdata.common.constants import EXECUTION_MAXIMUM_TIMEOUT_SECONDS
from test_tabsdata.common.docker import clean_docker_containers
from test_tabsdata.common.venv import clean_python_virtual_environments

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from test_tabsdata.bootest import TESTING_RESOURCES_PATH, check_assets, enrich_sys_path

TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
check_assets()

pytest_plugins = ["test_tabsdata.fntest"]

os.environ["POLARS_VERBOSE"] = "0"
os.environ["POLARS_FORCE_ASYNC"] = "1"


def get_function_registration_path(fn: TabsdataFunction):
    return inspect.getfile(fn.original_function) + "::" + fn.original_function.__name__


def pytest_configure(config: pytest.Config):
    setup_tests_logging()
    if not hasattr(config, "workerinput"):
        setup_temp_folder(config)
        clean_everything()


def pytest_configure_node(node: WorkerController):
    setup_temp_folder_node(node)


def setup_temp_folder(config: pytest.Config) -> None:
    if getattr(config.option, "basetemp", None):
        return
    config.option.basetemp = tabsdata_temp_folder()


def setup_temp_folder_node(node: WorkerController) -> None:
    if not getattr(node.config.option, "basetemp", None):
        base_temp = tabsdata_temp_folder()
        worker_temp = os.path.join(base_temp, f"pyw_{node.workerinput['workerid']}")
        node.config.option.basetemp = worker_temp


TESTS_ROOT_FOLDER = os.path.dirname(__file__)

sys.path.insert(0, TESTS_ROOT_FOLDER)

# fmt: off
ABSOLUTE_TEST_FOLDER_LOCATION = str(
    Path(__file__).absolute()
    .parent  # test_tabsdata/
    .parent  # python/
)

ABSOLUTE_ROOT_FOLDER_LOCATION = str(
    Path(ABSOLUTE_TEST_FOLDER_LOCATION)
    .parent  # utest/
    .parent  # src/
    .parent  # platform/
    .parent  # client/
    .parent  # tabsdata-*
)
logger.debug(f"ABSOLUTE_ROOT_FOLDER_LOCATION: {ABSOLUTE_ROOT_FOLDER_LOCATION}")

CORRECT_SOURCE = TableInput("input")
CORRECT_DESTINATION = TableOutput("output")
CORRECT_INPUT_ACTIONS = {}
CORRECT_OUTPUT_ACTIONS = {}

DEFAULT_LOGS_FILE = "fn.log"

ROOT_PROJECT_DIR = str(
    Path(ABSOLUTE_TEST_FOLDER_LOCATION)
    .parent  # utest/
    .parent  # src/
    .parent  # platform/
    .parent  # client/
    .parent  # tabsdata-*
)
# fmt: on


class TestInstance:
    def __init__(self, values: dict[str, Any] | None = None):
        self._values = values or {}

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._values.get(key, default)

    def items(self):
        return self._values.items()

    def as_dict(self) -> dict[str, Any]:
        return dict(self._values)


class TestInstances:
    def __init__(self, instances: dict[str, TestInstance] | None = None):
        self._instances = instances or {}

    def __getitem__(self, name: str) -> TestInstance:
        return self._instances[name]

    def get(self, name: str, default: TestInstance = None) -> TestInstance:
        return self._instances.get(name, default)

    def items(self):
        return self._instances.items()

    def as_dict(self) -> dict[str, TestInstance]:
        return dict(self._instances)


def load_testing_supervisor_instances() -> TestInstances:
    testing_instances_config_path = (
        pathlib.Path(ROOT_PROJECT_DIR)
        / "variant"
        / "config"
        / "pytest"
        / "supervisor.yaml"
    )
    if not testing_instances_config_path.exists():
        logger.warning(
            f"Testing instances configuration '{testing_instances_config_path}' not"
            " found; continuing with no instance."
        )
        return TestInstances()

    with testing_instances_config_path.open("r", encoding="utf-8") as supervisor_file:
        testing_instances_config = yaml.safe_load(supervisor_file) or {}

    if not isinstance(testing_instances_config, dict):
        logger.warning(
            f"Testing instances configuration '{testing_instances_config_path}' is not"
            " a mapping; continuing with empty instances."
        )
        return TestInstances()

    testing_instances: dict[str, TestInstance] = {}
    for (
        testing_instance_name,
        testing_instance_settings,
    ) in testing_instances_config.items():
        if not isinstance(testing_instance_settings, dict):
            logger.warning(
                f"Configured testing instance '{testing_instance_name}' is not a"
                " mapping; storing empty configuration."
            )
            testing_instance_settings = {}
        testing_instances[testing_instance_name] = TestInstance(
            testing_instance_settings
        )

    return TestInstances(testing_instances)


TESTING_SUPERVISOR_INSTANCES = load_testing_supervisor_instances()


def gather_connectors() -> list[str]:
    connectors_folders = [
        os.path.join(ROOT_PROJECT_DIR, "connectors", "python"),
        # os.path.join(root, "connectors.ee", "python"),
    ]

    tabsdata_connectors: list[str] = []

    for connectors_folder in connectors_folders:
        if not os.path.isdir(connectors_folder):
            continue

        for entry in os.scandir(connectors_folder):
            if entry.is_dir():
                connector_name_parts = entry.name.split("-", 1)
                if (
                    len(connector_name_parts) != 2
                    or connector_name_parts[0] != "tabsdata"
                ):
                    logger.debug(f"⛔️ Invalid connector folder name: {entry.name}")
                else:
                    tabsdata_connectors.append(entry.name)
                    logger.debug(f"📦️ Inserting connector {entry}")
    return sorted(tabsdata_connectors)


APISERVER_URL = (
    TESTING_SUPERVISOR_INSTANCES.get("pytest").get("apiserver").get("address")
)

# The bootstrap seeds exactly one project, `sandbox`; tenant-scoped tests target
# it. The client never defaults a project (defaulting, if any, is the server's
# job), so test connections must select one explicitly.
TESTING_PROJECT = "sandbox"


@contextmanager
def scoped_project(connection: APIServer, project: str):
    """Temporarily target `project` on `connection`, restoring the prior one.

    The active project rides on the connection as plain mutable state, so a
    test can flip between tenants on a single connection. Use this to scope a
    block of tenant-scoped calls to `project`; the previous project (if any) is
    restored on exit even when the block raises.
    """
    previous = connection.project
    connection.project = project
    try:
        yield
    finally:
        connection.project = previous

LOCAL_PACKAGES_LIST = [
    ROOT_PROJECT_DIR,
    os.path.join(
        ROOT_PROJECT_DIR,
        "devutils",
        "automation",
        "tasks",
        "python",
        TABSDATA_TOOLKIT_PACKAGE_NAME,
    ),
]

for connector in gather_connectors():
    LOCAL_PACKAGES_LIST.append(
        os.path.join(
            ROOT_PROJECT_DIR,
            "connectors",
            "python",
            connector,
        )
    )

LOCAL_PACKAGES_LIST = []

MOCK_COLLECTION = "td_collection"
MOCK_COLLECTION_ID = 19
MOCK_PROJECT_ID = "td_project"
FAKE_EXECUTION_ID = 24
FAKE_FUNCTION_RUN_ID = 60
FAKE_SCHEDULED_TIME = 1248067890123
FAKE_TRANSACTION_ID = 42
FAKE_TRIGGERED_TIME = 1234567890123
FAKE_FUNCTION_DEFINED_ON = (
    datetime.fromtimestamp(0, tz=timezone.utc).timestamp() * 1000.0
)

# Expected output of the foo1 function when called with f1="o1" and f2="o2"
EXPECTED_FOO1_OUTPUT = ("o1", "o2")

FORMAT_TYPE_TO_CONFIG = {
    "avro": {"chunk_size": 50000},
    "csv": {
        "separator": ",",
        "quote_char": '"',
        "eol_char": "\n",
        "input_encoding": "Utf8",
        "input_null_values": None,
        "input_missing_is_null": True,
        "input_truncate_ragged_lines": False,
        "input_comment_prefix": None,
        "input_try_parse_dates": False,
        "input_decimal_comma": False,
        "input_has_header": True,
        "input_skip_rows": 0,
        "input_skip_rows_after_header": 0,
        "input_raise_if_empty": True,
        "input_ignore_errors": False,
        "output_include_header": True,
        "output_datetime_format": None,
        "output_date_format": None,
        "output_time_format": None,
        "output_float_scientific": None,
        "output_float_precision": None,
        "output_null_value": None,
        "output_quote_style": None,
        "output_maintain_order": True,
    },
    "json": {},
    "log": {},
    "parquet": {},
}

FUNCTION_DATA_FOLDER = "function_data_folder"

if not os.environ.get("TD_CLI_SHOW"):
    os.environ["TD_CLI_SHOW"] = "false"


def pytest_addoption(parser):
    # Check if option already exists to avoid conflicts when running multiple test
    # suites in parallel.
    try:
        parser.addoption(
            "--performance-size",
            help="Amount of records per table when running performance tests",
            default=25000,
            type=int,
        )
    except ValueError:
        pass


def pytest_generate_tests(metafunc):
    if "size" in metafunc.fixturenames:
        metafunc.parametrize("size", [metafunc.config.getoption("performance_size")])


def read_json_and_clean(path, sort=True):
    """Reads a JSON file and removes the td.id column. Also sorts it to make
    comparison more stable"""
    import polars as pl  # Lazy import

    df = pl.read_json(path)
    df = clean_polars_df(df, sort=sort)
    return df


def clean_polars_df(df, sort=True):
    """Removes the td column. Also sorts it to make comparison more stable"""
    import polars as pl  # Lazy import

    # TODO: change to quote

    df = df.select(pl.exclude(f"^{re.escape(td_constants.TD_COLUMN_PREFIX)}.*$"))
    if sort:
        df = df.select(sorted(df.columns))
        df = df.sort(df.columns)
    df.columns = [col.lower() for col in df.columns]
    return df


@pytest.fixture(scope="session")
def apiserver_connection() -> APIServer:
    connection = obtain_connection(APISERVER_URL, "admin", "tabsdata")
    # Tenant-scoped calls ride the active project on the connection; the tests
    # target the bootstrap-seeded `sandbox` project (the only one the server
    # seeds). Without it every tenant call fails with NoProjectSelectedError.
    connection.project = TESTING_PROJECT
    return connection


@pytest.fixture(scope="session")
def tabsserver_connection() -> TabsdataServer:
    server = TabsdataServer(APISERVER_URL, "admin", "tabsdata")
    server.connection.project = TESTING_PROJECT
    return server


def clean_everything():
    clean_python_virtual_environments()
    clean_docker_containers()


# noinspection PyUnusedLocal
def pytest_sessionfinish(session, exitstatus):
    # Based on the following discussion:
    # https://github.com/pytest-dev/pytest-xdist/issues/271
    if getattr(session.config, "workerinput", None) is not None:
        # No need to download, the master process has already done that.
        return
    clean_everything()


class V2FunctionContext:
    """Simple class to enable us to generate YAML files for testing that emulate the
    ones generated by the rust backend"""

    def __init__(self, content):
        self.content = content


class V1StageTriggerContext:
    """Simple class to enable us to generate YAML files for testing that emulate the
    ones generated by the rust backend"""

    def __init__(self, content):
        self.content = content


class MockTable:
    def __init__(self, content):
        self.content = content


class MockTableVersions:
    def __init__(self, content):
        self.content = content


def v2_function_context_format_representer(
    dumper: yaml.SafeDumper, v1_execution_context_format: V2FunctionContext
) -> yaml.nodes.MappingNode:
    """Represent a V2_YAML instance as a YAML mapping node."""
    dumper.add_representer(MockTable, v1_mock_table_representer)
    dumper.add_representer(MockTableVersions, v1_mock_table_versions_representer)
    return dumper.represent_mapping("!FunctionV2", v1_execution_context_format.content)


def v1_mock_table_representer(
    dumper: yaml.SafeDumper, mock_table: MockTable
) -> yaml.nodes.MappingNode:
    """Represent a MockTable instance as a YAML mapping node."""
    return dumper.represent_mapping("!Table", mock_table.content)


def v1_mock_table_versions_representer(
    dumper: yaml.SafeDumper, mock_table_versions: MockTableVersions
) -> yaml.nodes.MappingNode:
    """Represent a MockTable instance as a YAML mapping node."""
    return dumper.represent_mapping("!TableVersions", mock_table_versions.content)


def v1_listener_context_format_representer(
    dumper: yaml.SafeDumper, v1_listener_context: V1StageTriggerContext
) -> yaml.nodes.MappingNode:
    """Represent a V2_YAML instance as a YAML mapping node."""
    return dumper.represent_mapping("!StageTriggerV1", v1_listener_context.content)


def get_dumper():
    """Add representers to a YAML serializer."""
    safe_dumper = yaml.SafeDumper
    safe_dumper.add_representer(
        V2FunctionContext, v2_function_context_format_representer
    )
    safe_dumper.add_representer(
        V1StageTriggerContext, v1_listener_context_format_representer
    )
    return safe_dumper


def write_v2_function_yaml_file(
    filename: str,
    bundle_archive_location: str,
    mock_dependency_location: list[str] | None = None,
    mock_table_location: list[str] | None = None,
    input_initial_values_path: str | None = None,
    output_initial_values_path: str | None = None,
    function_data_path: str | None = None,
    staging_uri: str | Path | None = None,
    function_defined_on: float | None = None,
):
    # Using explicit None check as condition evaluates to False when == 0.0
    if function_defined_on is None:
        function_defined_on = datetime.now().timestamp() * 1000.0
    content: dict[str, Any] = {
        "info": {
            "project_id": MOCK_PROJECT_ID,
            "collection": MOCK_COLLECTION,
            "collection_id": MOCK_COLLECTION_ID,
            "execution_id": FAKE_EXECUTION_ID,
            "function_bundle": {
                "uri": pathlib.Path(bundle_archive_location).as_uri(),
                "env_prefix": None,
            },
            "function_data": {
                "uri": pathlib.Path(function_data_path).as_uri(),
                "env_prefix": None,
            },
            "function_defined_on": function_defined_on,
            "function_run_id": FAKE_FUNCTION_RUN_ID,
            "scheduled_on": FAKE_SCHEDULED_TIME,
            "transaction_id": FAKE_TRANSACTION_ID,
            "triggered_on": FAKE_TRIGGERED_TIME,
        },
        "input": [],
    }
    if staging_uri:
        content["info"]["function_stage"] = {
            "uri": staging_uri,
            "env_prefix": "STAGE",
        }
    if mock_dependency_location:
        for position, mocked_table in enumerate(mock_dependency_location):
            if isinstance(mocked_table, str):
                uri = (
                    pathlib.Path(mocked_table).as_uri()
                    if mocked_table != "null"
                    else None
                )
                table_content = {
                    "name": f"mocked_table_{position}",
                    "collection_id": f"mocked_collection_id_{position}",
                    "collection": f"mocked_collection_{position}",
                    "table_id": f"mocked_table_id_{position}",
                    "table_version_id": f"mocked_table_version_id_{position}",
                    "execution_id": f"mocked_execution_id_{position}",
                    "transaction_id": f"mocked_transaction_id_{position}",
                    "triggered_on": datetime.now(tz=timezone.utc),
                    "table_data_version_id": f"mocked_table_data_version_id_{position}",
                    "location": {
                        "uri": uri,
                        "env_prefix": None,
                    },
                    "input_idx": 314 + position,
                    "table_pos": 25 + position,
                    "version_pos": 19 + position,
                }
                content["input"].append(MockTable(table_content))
            elif isinstance(mocked_table, list):
                table_versions = []
                for index, table in enumerate(mocked_table):
                    uri = pathlib.Path(table).as_uri() if table != "null" else None
                    table_content = {
                        "name": f"mocked_table_{position}_{index}",
                        "collection_id": f"mocked_collection_id_{position}_{index}",
                        "collection": f"mocked_collection_{position}_{index}",
                        "table_id": f"mocked_table_id_{position}_{index}",
                        "table_version_id": (
                            f"mocked_table_version_id_{position}_{index}"
                        ),
                        "execution_id": f"mocked_execution_id_{position}_{index}",
                        "transaction_id": f"mocked_transaction_id_{position}_{index}",
                        "triggered_on": datetime.now(tz=timezone.utc),
                        "table_data_version_id": (
                            f"mocked_table_data_version_id_{position}_{index}"
                        ),
                        "location": {
                            "uri": uri,
                            "env_prefix": None,
                        },
                        "input_idx": 314 + position * 1000 + index,
                        "table_pos": 25 + position * 1000 + index,
                        "version_pos": 19 + position * 1000 + index,
                    }
                    table_versions.append(table_content)
                full_content = {
                    "is_empty": False,
                    "versions": table_versions,
                }
                content["input"].append(MockTableVersions(full_content))
            else:
                raise ValueError(
                    f"Unexpected type {type(mocked_table)} for mock_dependency_location"
                )
    location = None
    if input_initial_values_path:
        location = {
            "uri": pathlib.Path(input_initial_values_path).as_uri(),
            "env_prefix": None,
        }
    table_content = {"name": "td-initial-values", "location": location}
    content["system_input"] = [MockTable(table_content)]

    content["output"] = []
    if mock_table_location:
        content["output"] = add_mock_table_location(mock_table_location)
    location = None
    if output_initial_values_path:
        location = {
            "uri": pathlib.Path(output_initial_values_path).as_uri(),
            "env_prefix": None,
        }
    table_content = {
        "name": "td-initial-values",
        "location": location,
    }
    content["system_output"] = [MockTable(table_content)]
    v2_execution_context = V2FunctionContext(content)
    with open(filename, "w") as stream:
        stream.write(yaml.dump(v2_execution_context, Dumper=get_dumper()))


def write_v1_listener_yaml_file(
    filename: str,
    bundle_archive_location: str,
):
    content: dict[str, Any] = {
        "info": {
            "project_id": MOCK_PROJECT_ID,
            "collection": MOCK_COLLECTION,
            "collection_id": MOCK_COLLECTION_ID,
            "function_bundle": {
                "uri": pathlib.Path(bundle_archive_location).as_uri(),
                "env_prefix": None,
            },
            "scheduled_on": FAKE_SCHEDULED_TIME,
            "triggered_on": FAKE_TRIGGERED_TIME,
            "fn_trigger": {
                "method": "POST",
                "url": "http://localhost:2457/internal/fn_trigger",
                "headers": [],
                "body": None,
            },
            "fn_state": {
                "method": "GET",
                "url": "http://localhost:2457/internal/fn_state",
                "headers": [["content-type", "application/parquet"]],
                "body": None,
            },
            "fn_status": {
                "method": "GET",
                "url": "http://localhost:2457/internal/fn_status",
                "headers": [],
                "body": None,
            },
            "config": (
                '{"timeout_threshold_min":60,"size_threshold":10000,'
                '"start":1765377873840,"end":1765377873841}'
            ),
        },
    }

    v1_listener_context = V1StageTriggerContext(content)
    with open(filename, "w") as stream:
        stream.write(yaml.dump(v1_listener_context, Dumper=get_dumper()))


def add_mock_table_location(mock_table_location):
    output = []
    for position, mocked_table in enumerate(mock_table_location):
        if isinstance(mocked_table, str):
            uri = (
                pathlib.Path(mocked_table).as_uri() if mocked_table != "null" else None
            )
            table_content = {
                "name": f"mocked_table_{position}",
                "collection_id": f"mocked_collection_id_{position}",
                "collection": f"mocked_collection_{position}",
                "table_id": f"mocked_table_id_{position}",
                "table_version_id": f"mocked_table_version_id_{position}",
                "execution_id": f"mocked_execution_id_{position}",
                "transaction_id": f"mocked_transaction_id_{position}",
                "triggered_on": datetime.now(tz=timezone.utc),
                "table_data_version_id": f"mocked_table_data_version_id_{position}",
                "location": {
                    "uri": uri,
                    "env_prefix": None,
                },
                "input_idx": 314 + position,
                "table_pos": 25 + position,
                "version_pos": 19 + position,
            }
            output.append(MockTable(table_content))
        elif isinstance(mocked_table, list):
            table_versions = []
            for index, table in enumerate(mocked_table):
                uri = pathlib.Path(table).as_uri() if table != "null" else None
                table_content = {
                    "name": f"mocked_table_{position}_{index}",
                    "collection_id": f"mocked_collection_id_{position}_{index}",
                    "collection": f"mocked_collection_{position}_{index}",
                    "table_id": f"mocked_table_id_{position}_{index}",
                    "table_version_id": f"mocked_table_version_id_{position}_{index}",
                    "execution_id": f"mocked_execution_id_{position}_{index}",
                    "transaction_id": f"mocked_transaction_id_{position}_{index}",
                    "triggered_on": datetime.now(tz=timezone.utc),
                    "table_data_version_id": (
                        f"mocked_table_data_version_id_{position}_{index}"
                    ),
                    "location": {
                        "uri": uri,
                        "env_prefix": None,
                    },
                    "input_idx": 314 + position * 1000 + index,
                    "table_pos": 25 + position * 1000 + index,
                    "version_pos": 19 + position * 1000 + index,
                }
                table_versions.append(table_content)
            full_content = {
                "is_empty": False,
                "versions": table_versions,
            }
            output.append(MockTableVersions(full_content))
        else:
            raise ValueError(
                f"Unexpected type {type(mocked_table)} for mock_table_location"
            )
    return output


@pytest.fixture(scope="session")
def testing_collection_with_table(worker_id, tabsserver_connection):
    random_id = _id()
    collection_name = f"testing_collection_with_table_{worker_id}_{random_id}"
    file_path = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_file_csv_string_format",
        "example.py",
    )
    function_path = file_path + "::input_file_csv_string_format"
    tabsserver_connection.create_collection(collection_name, description="description")
    tabsserver_connection.register_function(
        collection_name, function_path, local_packages=LOCAL_PACKAGES_LIST
    )
    tabsserver_connection.trigger_function(
        collection_name, "input_file_csv_string_format"
    )
    table = tabsserver_connection.get_collection(collection_name).get_table("output")

    @retry(
        stop=stop_after_delay(EXECUTION_MAXIMUM_TIMEOUT_SECONDS),
        wait=wait_fixed(2),
        reraise=True,
    )
    def wait_for_committed():
        dataversion = table.dataversions[0] if table.dataversions else None
        if dataversion and dataversion.status == data_version_status_to_mapping(
            DataVersionStatus.COMMITTED.value
        ):
            logger.debug(
                f"Dataversion '{dataversion}' is in status '"
                f"{dataversion.status}, indicating that the data is ready "
                "to be consumed."
            )
            return
        if dataversion:
            logger.debug(
                f"Dataversion '{dataversion}' is in status '"
                f"{dataversion.status}, indicating that the data is NOT "
                "ready to be consumed yet."
            )
        else:
            logger.debug(f"No dataversions found for table yet: '{table.dataversions}'")
        logger.debug(f"Table '{random_id}' - '{file_path}' does not have data yet")
        raise ValueError(
            f"Data from the table is not yet available for"
            f" '{random_id}' - '{file_path}'."
        )

    wait_for_committed()
    try:
        yield collection_name
    finally:
        pass
        # ToDo: Deleting collections is not yet supported. In any case,
        #       as log as identifiers are unique, there seems to be no
        #       strong need to delete the collections after a test.
        # try:
        #     logger.debug(f"Deleting collection {collection_name}")
        #     tabsserver_connection.collection_delete(collection_name)
        # except APIServerError as error:
        #     logger.error(f"Failed to delete collection: {error}")


def get_lf(size: int, chunk_size=1000):
    import numpy as np  # Lazy import
    import polars as pl  # Lazy import

    final_lf = None
    for start in range(0, size, chunk_size):
        end = min(start + chunk_size, size)
        id_column = range(start, end)
        lf = pl.LazyFrame(
            {
                "id": id_column,
                "name": [f"name_{i}" for i in id_column],
                "value": np.random.rand(end - start),
            }
        )
        final_lf = pl.concat([final_lf, lf]) if final_lf is not None else lf
    return final_lf
