#
# Copyright 2024 Tabs Data Inc.
#
import glob
import logging
import os
import pathlib
import re

# from tabsdata.tabsserver.function.store_results_utils import (
#     _extract_index,
#     _get_matching_files,
#
from tabsdata.tabsserver.utils import convert_uri_to_path

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)


def test_convert_uri_to_path():
    # We use current directory instead of a hardcoded value so that it properly
    # tests this function in every os
    path = os.getcwd()
    uri = pathlib.Path(path).as_uri()
    assert convert_uri_to_path(uri) == path


def test_extract_index():
    assert _extract_index("example_file_0.jsonl") == 0
    assert _extract_index("example_file_1.jsonl") == 1
    assert _extract_index("example_file_things_and_numbers_4732.jsonl") == 4732
    assert _extract_index("example_file_0.parquet") == 0
    assert _extract_index("example_file_1.parquet") == 1
    assert _extract_index("example_file_things_and_numbers_4732.parquet") == 4732


def test_get_matching_files(tmp_path):
    # Create some files
    files_generated = []
    for index in range(2000):
        file = tmp_path / f"example_file_{index}.jsonl"
        file.write_text("hi")
        files_generated.append(str(file))
    # Create some files that should not be matched
    for index in range(2000):
        file = tmp_path / f"example_file_{index}_potato.csv"
        file.write_text("hi")
    assert (
        _get_matching_files(os.path.join(tmp_path, "example_file_*.jsonl"))
        == files_generated
    )


# Provisionally duplicating this utility function as the original one is flaky.
def _get_matching_files(pattern):
    # Construct the full pattern
    # Use glob to get the list of matching files
    matching_files = glob.glob(pattern)
    ordered_files = sorted(matching_files, key=_extract_index)
    logger.debug(f"Matching files: {ordered_files}")
    return ordered_files


# Sort the files to ensure that they are processed in the correct order
def _extract_index(filename):
    # Extract just the base filename without the path to ensure ordinal extraction is
    # based only on it.
    base_filename = os.path.basename(filename)
    match = re.search(r"_(\d+)\.*", base_filename)
    # Instead of infinity, this should fail. Else, we cannot guarantee determinacy.
    return int(match.group(1)) if match else float("inf")
