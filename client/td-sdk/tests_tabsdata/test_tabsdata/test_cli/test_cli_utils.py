#
# Copyright 2024 Tabs Data Inc.
#

import os

import pytest

from tabsdata._api.status_utils.execution import user_execution_status_to_api
from tabsdata._cli.cli_utils import (
    beautify_list,
    cleanup_dot_files,
    generate_dot_image,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_beautify_list():
    assert beautify_list(["a", "b", "c"]) == "a\nb\nc"
    assert beautify_list(3) == "3"


def test_cleanup_dot_files():
    cleanup_dot_files()


def test_show_dot_file(tmp_path):
    sample_dot_string = (
        "digraph {\nsubgraph cluster_input_file_csv_string_format "
        '{\n               label = "input_file_csv_string_format"'
        ";\n               style = filled"
        ';\n               fillcolor = "#FFEECC"'
        ';\n               color = "#FBAF4F";0 '
        '[label = "td:///testing_collection_with_table_master_832/'
        'input_file_csv_string_format" fillcolor="#FBAF4F",style='
        'filled, group="\\#input_file_csv_string_format"];\n    }\n}'
    )
    full_path = os.path.join(tmp_path, "test.dot")
    with open(full_path, "w") as f:
        f.write(sample_dot_string)
    generate_dot_image(full_path)


def test_convert_user_provided_status_to_api_status():
    assert user_execution_status_to_api("S") == "S"
    assert user_execution_status_to_api("s") == "S"
    assert user_execution_status_to_api("sCheDulEd") == "S"
    assert user_execution_status_to_api(None) is None
    assert user_execution_status_to_api("") is None
    with pytest.raises(ValueError):
        user_execution_status_to_api("invalid_status")
