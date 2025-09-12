#
# Copyright 2025 Tabs Data Inc.
#

import os
import tempfile
from pathlib import Path

import yaml

from tabsdata._tabsserver.function.yaml_parsing import (
    Data,
    NoData,
    store_response_as_yaml,
)
from tabsdata._utils.temps import tabsdata_temp_folder

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_store_response_as_yaml_generates_correct_yaml():
    tables = [
        Data("d01"),
        Data("d02"),
        NoData("nod01"),
        NoData("nod02"),
    ]

    with tempfile.TemporaryDirectory(dir=tabsdata_temp_folder()) as tmpdir:
        response_file = Path(tmpdir) / "response.yaml"

        store_response_as_yaml(tables, response_file)

        class TestResponseLoader(yaml.SafeLoader):
            pass

        def v2_constructor(loader, node):
            return {"!V2": loader.construct_mapping(node)}

        def v2_data_constructor(loader, node):
            return {"!Data": loader.construct_mapping(node)}

        def v2_no_data_constructor(loader, node):
            return {"!NoData": loader.construct_mapping(node)}

        TestResponseLoader.add_constructor("!V2", v2_constructor)
        TestResponseLoader.add_constructor("!Data", v2_data_constructor)
        TestResponseLoader.add_constructor("!NoData", v2_no_data_constructor)

        with open(response_file, "r") as f:
            content = yaml.load(f, Loader=TestResponseLoader)

        expected_structure = {
            "!V2": {
                "output": [
                    {"!Data": {"table": "d01"}},
                    {"!Data": {"table": "d02"}},
                    {"!NoData": {"table": "nod01"}},
                    {"!NoData": {"table": "nod02"}},
                ]
            }
        }

        assert content == expected_structure
