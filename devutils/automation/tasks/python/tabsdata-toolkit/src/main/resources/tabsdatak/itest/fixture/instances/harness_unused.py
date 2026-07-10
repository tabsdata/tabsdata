#
# Copyright 2026 Tabs Data Inc.
#

import pytest


@pytest.mark.tabsdata_instance("tdtest")
@pytest.mark.tabsdata_instance("aitest")
def test_unused(instance_tdtest):
    pass
