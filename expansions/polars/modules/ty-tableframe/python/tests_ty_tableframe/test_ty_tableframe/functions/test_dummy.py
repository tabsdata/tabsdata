#
# Copyright 2025 Tabs Data Inc.
#

import logging

from tabsdata.expansions.tableframe.functions.mockup import dummy_fn

logger = logging.getLogger(__name__)


def test_dummy_fn():
    assert dummy_fn("aaa") == "aaa"
    assert dummy_fn("AAA") == "aaa"
    assert dummy_fn("AaA") == "aaa"
    assert dummy_fn("aAa") == "aaa"
