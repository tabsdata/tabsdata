#
# Copyright 2026 Tabs Data Inc.
#

"""
Unit tests for offset_utils.py bounds checks on system_input/system_output.
"""

from unittest.mock import MagicMock

import pytest

from tabsdata._tabsserver.function.offset_utils import Offset

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def _make_request(system_input=None, system_output=None):
    request = MagicMock()
    request.system_input = system_input
    request.system_output = system_output
    return request


class TestLoadCurrentOffsetBoundsCheck:
    """Verify that load_current_offset raises clear errors for missing data."""

    def test_raises_on_none_system_input(self):
        offset = Offset()
        request = _make_request(system_input=None)
        execution_context = MagicMock()
        with pytest.raises(ValueError, match="System input is missing"):
            offset.load_current_offset(request, execution_context)

    def test_raises_on_empty_system_input(self):
        offset = Offset()
        request = _make_request(system_input=[])
        execution_context = MagicMock()
        with pytest.raises(ValueError, match="System input is missing.*0 entries"):
            offset.load_current_offset(request, execution_context)

    def test_succeeds_with_valid_system_input(self):
        offset = Offset()
        table = MagicMock()
        table.uri = None
        table.name = "td-initial-values"
        request = _make_request(system_input=[table])
        execution_context = MagicMock()
        # Should not raise — URI is None so it just skips loading
        offset.load_current_offset(request, execution_context)
        assert offset.loaded_offset == {}


class TestStoreOffsetBoundsCheck:
    """Verify that store raises clear errors for missing data."""

    def test_raises_on_none_system_output(self):
        offset = Offset()
        request = _make_request(system_output=None)
        execution_context = MagicMock()
        with pytest.raises(ValueError, match="System output is missing"):
            offset.store(request, execution_context)

    def test_raises_on_empty_system_output(self):
        offset = Offset()
        request = _make_request(system_output=[])
        execution_context = MagicMock()
        with pytest.raises(ValueError, match="System output is missing.*0 entries"):
            offset.store(request, execution_context)
