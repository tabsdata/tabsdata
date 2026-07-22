#
# Copyright 2026 Tabsdata Inc.
#

import pytest


@pytest.fixture
def helper(instance):
    return instance


@pytest.mark.tabsdata_instance("tdtest")
def test_meta_relay(instance_tdtest, helper):
    pass
