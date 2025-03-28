#
# Copyright 2024 Tabs Data Inc.
#

import os
import platform

from tests_tabsdata.conftest import (
    FAKE_SCHEDULED_TIME,
    FAKE_TRIGGERED_TIME,
    TESTING_RESOURCES_FOLDER,
    write_v1_yaml_file,
)

from tabsdata.tabsserver.function.yaml_parsing import (
    V1,
    Table,
    TableVersions,
    parse_request_yaml,
)


def test_parse_input_yaml():
    input_yaml = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "example_file_input.yaml",
    )
    config = parse_request_yaml(input_yaml)
    assert isinstance(config, V1)
    assert config.info == {
        "function_bundle": {
            "uri": "file:///users/tucu/.tdserver/default/s/ID1/d/ID2/f/ID3.f",
            "env_prefix": None,
        }
    }
    assert config.function_bundle == {
        "uri": "file:///users/tucu/.tdserver/default/s/ID1/d/ID2/f/ID3.f",
        "env_prefix": None,
    }
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
    assert input_table.location is None
    assert input_table.uri is None
    assert input_table.env_prefix is None
    assert input_table.table is None
    assert input_table.table_id is None

    input_table_versions = input[1]
    assert isinstance(input_table_versions, TableVersions)
    assert len(input_table_versions.list_of_table_objects) == 3
    first_table = input_table_versions.list_of_table_objects[0]
    assert isinstance(first_table, Table)
    assert first_table.name == "users"
    assert first_table.table == "td://eu/users/$td.initial_values/HEAD"
    assert first_table.table_id == "td://ID1/ID2/$td.initial_values/ID4"
    assert first_table.location == {
        "uri": (
            "file:///users/tucu/.tdserver/default/s/ID1"
            "/d/ID2/v/ID3/ID4/t/.initial_values.t"
        ),
        "env_prefix": None,
    }
    assert first_table.env_prefix is None
    assert (
        first_table.uri
        == "file:///users/tucu/.tdserver/default/s/ID1/d/ID2"
        "/v/ID3/ID4/t/.initial_values.t"
    )
    third_table = input_table_versions.list_of_table_objects[2]
    assert isinstance(third_table, Table)
    assert third_table.name == "users"
    assert third_table.table == "td://eu/users/$td.initial_values/HEAD^^"
    assert third_table.table_id is None
    assert third_table.location is None
    assert third_table.env_prefix is None
    assert third_table.uri is None

    output = config.output
    assert isinstance(output, list)
    assert len(output) == 2
    first_output_table = output[0]
    assert isinstance(first_output_table, Table)
    assert first_output_table.name == "users"
    assert first_output_table.table is None
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
    write_v1_yaml_file(tmp_yaml_file, context_file)
    config = parse_request_yaml(tmp_yaml_file)
    assert isinstance(config, V1)
    assert config.info == {
        "function_bundle": {
            "uri": expected_uri,
            "env_prefix": None,
        },
        "dataset_data_version": "fake_dataset_version",
        "triggered_on": FAKE_TRIGGERED_TIME,
        "execution_plan_triggered_on": FAKE_SCHEDULED_TIME,
    }
    assert config.function_bundle == {
        "uri": expected_uri,
        "env_prefix": None,
    }
    assert config.function_bundle_uri == expected_uri
    assert config.function_bundle_env_prefix is None
