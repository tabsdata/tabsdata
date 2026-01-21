#
# Copyright 2024 Tabs Data Inc.
#

import os
import pathlib
import platform

from tabsdata._io.stage_trigger import CallConfig
from tabsdata._tabsserver.function.yaml_parsing import (
    DataListenerYaml,
    FunctionV2,
    Location,
    Table,
    TableVersions,
    parse_request_yaml,
)
from tests_tabsdata.conftest import (
    FAKE_EXECUTION_ID,
    FAKE_FUNCTION_DEFINED_ON,
    FAKE_FUNCTION_RUN_ID,
    FAKE_SCHEDULED_TIME,
    FAKE_TRANSACTION_ID,
    FAKE_TRIGGERED_TIME,
    FUNCTION_DATA_FOLDER,
    MOCK_COLLECTION,
    MOCK_COLLECTION_ID,
    TESTING_RESOURCES_FOLDER,
    write_v2_function_yaml_file,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_parse_input_yaml():
    input_yaml = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "example_file_input.yaml",
    )
    config = parse_request_yaml(input_yaml)
    assert isinstance(config, FunctionV2)
    assert config.info == {
        "function_bundle": {
            "uri": "file:///users/tucu/.tdserver/default/s/ID1/d/ID2/f/ID3.f",
            "env_prefix": None,
        }
    }
    assert config.function_bundle == Location(
        {
            "uri": "file:///users/tucu/.tdserver/default/s/ID1/d/ID2/f/ID3.f",
            "env_prefix": None,
        }
    )
    assert (
        config.function_bundle_uri
        == "file:///users/tucu/.tdserver/default/s/ID1/d/ID2/f/ID3.f"
    )
    assert config.function_bundle_env_prefix is None

    input = config.input
    assert isinstance(input, list)
    input_table = input[0]
    assert isinstance(input_table, Table)
    assert input_table.name == "$td.initial_values"
    assert not input_table.location
    assert input_table.location.uri is None
    assert input_table.location.env_prefix is None
    assert input_table.uri is None
    assert input_table.env_prefix is None
    assert input_table.table_id is None

    input_table_versions = input[1]
    assert isinstance(input_table_versions, TableVersions)
    assert len(input_table_versions.list_of_table_objects) == 3
    first_table = input_table_versions.list_of_table_objects[0]
    assert isinstance(first_table, Table)
    assert first_table.name == "users"
    assert first_table.table_id == "td://ID1/ID2/$td.initial_values/ID4"
    assert (
        first_table.location.uri
        == "file:///users/tucu/.tdserver/default/s/ID1/d/ID2/v/ID3/ID4/t/.initial_values.t"
    )
    assert first_table.location.env_prefix is None
    assert first_table.env_prefix is None
    assert (
        first_table.uri
        == "file:///users/tucu/.tdserver/default/s/ID1/d/ID2"
        "/v/ID3/ID4/t/.initial_values.t"
    )
    third_table = input_table_versions.list_of_table_objects[2]
    assert isinstance(third_table, Table)
    assert third_table.name == "users"
    assert third_table.table_id is None
    assert not third_table.location
    assert third_table.location.uri is None
    assert third_table.location.env_prefix is None

    output = config.output
    assert isinstance(output, list)
    assert len(output) == 2
    first_output_table = output[0]
    assert isinstance(first_output_table, Table)
    assert first_output_table.name == "users"
    assert (
        first_output_table.uri
        == "file:///users/tucu/.tdserver/default/s/ID1/d/ID2/v/ID3/IDA/t/users.f"
    )
    assert first_output_table.env_prefix is None
    second_output_table = output[1]
    assert isinstance(second_output_table, Table)
    assert second_output_table.name == ".initial_values"
    assert (
        second_output_table.uri
        == "file:///users/tucu/.tdserver/default/s/ID1/d/ID2/v/ID3/IDA/t/"
        ".initial_values.t"
    )
    assert second_output_table.env_prefix is None


def test_parse_data_listener_input_yaml():
    input_yaml = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "example_data_listener_input.yaml",
    )
    config: DataListenerYaml = parse_request_yaml(input_yaml)
    assert isinstance(config, DataListenerYaml)
    assert config.function_bundle == Location(
        {
            "uri": "file:///foo",
            "env_prefix": None,
        }
    )
    assert config.function_bundle_uri == "file:///foo"
    assert config.function_bundle_env_prefix is None

    assert config.collection == "collection"
    assert config.collection_id == "06DGHE4NM1RG3CK8OE7MA02V24"
    assert config.function == "function"
    assert config.function_version_id == "06DGHE4NMDQF3BKH73TLFLABAC"
    assert config.config == {
        "timeout_threshold_min": 60,
        "size_threshold": 10000,
        "start": 1765377873840,
        "end": 1765377873841,
    }

    fn_trigger = config.fn_trigger
    assert isinstance(fn_trigger, CallConfig)
    assert fn_trigger.method == "POST"
    assert fn_trigger.url == "http://localhost:2457/internal/fn_trigger"
    assert fn_trigger.headers == []
    assert fn_trigger.body is None

    fn_state = config.fn_state
    assert isinstance(fn_state, CallConfig)
    assert fn_state.method == "GET"
    assert fn_state.url == "http://localhost:2457/internal/fn_state"
    assert fn_state.headers == [["content-type", "application/parquet"]]
    assert fn_state.body is None

    fn_status = config.fn_status
    assert isinstance(fn_status, CallConfig)
    assert fn_status.method == "GET"
    assert fn_status.url == "http://localhost:2457/internal/fn_status"
    assert fn_status.headers == []
    assert fn_status.body is None


def test_parse_minimal_input_yaml(tmp_path):
    """Test parsing a minimal input yaml file. An error in this test might signal
    that the write_v1_yaml_file function in conftest.py is not working properly,
    instead of being a product issue."""
    tmp_yaml_file = os.path.join(tmp_path, "minimal_input.yaml")
    context_file = (
        "C:\\input_context"
        if platform.system() == "Windows"
        else "/minimal_input_context"
    )
    expected_uri = (
        "file:///C:/input_context"
        if platform.system() == "Windows"
        else f"file://{context_file}"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        tmp_yaml_file,
        context_file,
        function_data_path=function_data_folder,
        function_defined_on=FAKE_FUNCTION_DEFINED_ON,
    )
    config = parse_request_yaml(tmp_yaml_file)
    assert isinstance(config, FunctionV2)
    assert config.info == {
        "collection": MOCK_COLLECTION,
        "collection_id": MOCK_COLLECTION_ID,
        "execution_id": FAKE_EXECUTION_ID,
        "function_bundle": {
            "uri": expected_uri,
            "env_prefix": None,
        },
        "function_data": {
            "uri": pathlib.Path(function_data_folder).as_uri(),
            "env_prefix": None,
        },
        "function_run_id": FAKE_FUNCTION_RUN_ID,
        "scheduled_on": FAKE_SCHEDULED_TIME,
        "transaction_id": FAKE_TRANSACTION_ID,
        "triggered_on": FAKE_TRIGGERED_TIME,
        "function_defined_on": FAKE_FUNCTION_DEFINED_ON,
    }
    assert config.function_bundle == Location(
        {
            "uri": expected_uri,
            "env_prefix": None,
        }
    )
    assert config.function_bundle_uri == expected_uri
    assert config.function_bundle_env_prefix is None
