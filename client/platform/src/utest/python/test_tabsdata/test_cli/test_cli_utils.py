#
# Copyright 2024 Tabs Data Inc.
#

import os

import pytest

from tabsdata._cli.cli_utils import beautify_list
from tabsdata.api.status_utils.execution import user_execution_status_to_api

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_beautify_list():
    assert beautify_list(["a", "b", "c"]) == "a\nb\nc"
    assert beautify_list(3) == "3"


def test_convert_user_provided_status_to_api_status():
    assert user_execution_status_to_api("S") == "S"
    assert user_execution_status_to_api("s") == "S"
    assert user_execution_status_to_api("sCheDulEd") == "S"
    assert user_execution_status_to_api(None) is None
    assert user_execution_status_to_api("") is None
    with pytest.raises(ValueError):
        user_execution_status_to_api("invalid_status")
