#
# Copyright 2026 Tabs Data Inc.
#
"""Pytest plumbing for live extractor integration tests.

Every test in this subtree uses the shared
:func:`tabsdata_testutils.external.external` framework — the
:fixture:`envs` fixture and the ``pytest_runtest_makereport`` hook
(for the "export GATE=skip" hint on failure) are re-exported here
so pytest picks them up for tests under this directory.
"""

from __future__ import annotations

from tabsdata_testutils.external import (  # noqa: F401
    envs,
    pytest_runtest_makereport,
)
