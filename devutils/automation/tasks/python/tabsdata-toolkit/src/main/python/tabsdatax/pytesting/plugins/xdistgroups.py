#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest
from _pytest.nodeid import NodeId

LOADGROUP_OPTION = "loadgroup"
GROUP_MARKER = "xdist_group"
GROUP_SEPARATOR = "@"
DEFAULT_GROUP = "default"


def group_suffix(item: pytest.Item) -> str | None:
    names = {
        str(mark.args[0] if mark.args else mark.kwargs.get("name", DEFAULT_GROUP))
        for mark in item.iter_markers(GROUP_MARKER)
    }
    if not names:
        return None
    return GROUP_SEPARATOR + "_".join(sorted(names))


# ToDo: remove once an xdist release carries pytest-dev/pytest-xdist#1388. pytest 9.2
# derives `nodeid` from `Node._id`, while pytest-xdist 3.8.0 still tags groups in
# `item._nodeid`, so `--dist=loadgroup` ignores `xdist_group`.
@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]):
    if not config.getoption(LOADGROUP_OPTION, default=False):
        return
    for item in items:
        suffix = group_suffix(item)
        if suffix is None or item.nodeid.endswith(suffix):
            continue
        item._id = NodeId.parse(item.nodeid + suffix)
