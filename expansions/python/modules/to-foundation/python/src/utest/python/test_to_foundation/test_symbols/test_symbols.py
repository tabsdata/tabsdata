#
# Copyright 2025 Tabsdata Inc.
#

import pytest

from tabsdata.expansions.foundation.symbols import (
    REQUEST_FILE,
    RESPONSE_FILE,
    _py,
)


@pytest.mark.pyo3
def test_request_file():
    assert REQUEST_FILE is not None


@pytest.mark.pyo3
def test_response_file():
    assert RESPONSE_FILE is not None


@pytest.mark.pyo3
def test_py_function():
    result = _py()
    assert result is None
