#
# Copyright 2026 Tabs Data Inc.
#

import pytest


@pytest.fixture
def helper(instance_aitest):
    return instance_aitest


@pytest.mark.tabsdata_instance("tdtest")
def test_via_helper(helper):
    pass
