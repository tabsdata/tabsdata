#
# Copyright 2026 Tabsdata Inc.
#

import pytest


@pytest.fixture
def helper(instance_tdtest):
    return instance_tdtest


@pytest.mark.tabsdata_instance("tdtest")
def test_unlisted(helper):
    pass
