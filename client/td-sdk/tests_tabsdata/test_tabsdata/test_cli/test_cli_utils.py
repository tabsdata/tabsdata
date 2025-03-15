#
# Copyright 2024 Tabs Data Inc.
#

import os

import pytest

from tabsdata.cli.cli_utils import (
    beautify_list,
    cleanup_dot_files,
    complete_datetime,
    show_dot_file,
)


def test_beautify_list():
    assert beautify_list(["a", "b", "c"]) == "a\nb\nc"
    assert beautify_list(3) == "3"


def test_complete_datetime():
    assert complete_datetime("2025-01-16") == "2025-01-16T00:00:00.000Z"
    assert complete_datetime("2025-01-16T15Z") == "2025-01-16T15:00:00.000Z"
    assert complete_datetime("2025-01-16T15:30Z") == "2025-01-16T15:30:00.000Z"
    assert complete_datetime("2025-01-16T15:30:45Z") == "2025-01-16T15:30:45.000Z"
    assert complete_datetime("2025-01-16T15:05:38.137Z") == "2025-01-16T15:05:38.137Z"
    with pytest.raises(ValueError):
        complete_datetime("2025-01-16T15:05:38.137")
    assert complete_datetime(None) is None


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
    show_dot_file(full_path)
