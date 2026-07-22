#
# Copyright 2026 Tabsdata Inc.
#

import time

import pytest
from case import CASES, Case
from constant import (
    INSTANCE_NAME,
    PROVIDER,
    SETTLE_SECONDS,
    START_TIMEOUT_SECONDS,
)
from helper import (
    assert_substrings,
    drive_to_state,
    run_tdkserver,
    wait_for_state,
)


@pytest.mark.parametrize("case", CASES, ids=[case.id() for case in CASES])
def test_lifecycle(instance, pytestconfig, case: Case) -> None:
    drive_to_state(pytestconfig, case.pre_state)
    proc = run_tdkserver(
        pytestconfig,
        case.noun,
        case.verb,
        "--provider",
        PROVIDER,
        "--name",
        INSTANCE_NAME,
        "--preview",
        "--yes",
        expect_exit_zero=case.expect_exit_zero,
        timeout=START_TIMEOUT_SECONDS,
    )
    assert_substrings(proc.stdout + proc.stderr, case.expect_substrings)
    time.sleep(SETTLE_SECONDS)
    actual = wait_for_state(case.post_state, timeout=START_TIMEOUT_SECONDS)
    assert actual == case.post_state, (
        f"After {case.id()}: expected state {case.post_state.label()}, "
        f"observed {actual.label()}"
    )
