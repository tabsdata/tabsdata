#
# Copyright 2026 Tabs Data Inc.
#

import os
import shutil
import sys

import pytest
from case import STATE_BOTH_STOPPED, STATE_CLUSTER_ONLY
from constant import INSTANCE_FOLDER, KEEP_ON_FAILURE_ENV
from helper import (
    drive_to_state,
    ensure_instance_exists,
    ensure_secrets_populated,
    observed_state,
)


def pytest_addoption(parser):
    parser.addoption(
        "--tdkserver",
        action="store",
        default="tdkserver",
        help=(
            "Path to the tdkserver binary. "
            "Defaults to 'tdkserver' (resolved via $PATH)."
        ),
    )


@pytest.fixture(scope="module")
def instance(pytestconfig):
    ensure_instance_exists(pytestconfig)
    state = observed_state()
    assert state == STATE_CLUSTER_ONLY, (
        f"Post-create state should be {STATE_CLUSTER_ONLY.label()}, "
        f"observed {state.label()}"
    )
    ensure_secrets_populated(pytestconfig)
    session = pytestconfig.pluginmanager.getplugin("terminalreporter")
    yield
    keep_on_failure = os.environ.get(KEEP_ON_FAILURE_ENV) == "1"
    any_failed = bool(session.stats.get("failed")) if session else False
    if keep_on_failure and any_failed:
        print(
            f"\n{KEEP_ON_FAILURE_ENV}=1 and a case failed; "
            f"leaving {INSTANCE_FOLDER} and live cluster in place for inspection.",
            file=sys.stderr,
        )
        return
    try:
        drive_to_state(pytestconfig, STATE_BOTH_STOPPED)
    finally:
        if INSTANCE_FOLDER.exists():
            shutil.rmtree(INSTANCE_FOLDER, ignore_errors=True)
