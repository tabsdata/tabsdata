#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for the five status-mapping modules under
``tabsdatak.sdk.status_utils``. They all follow the same shape:
a status enum + ``*_STATUS_MAPPING`` dict + ``*_status_to_mapping``
function + final-status sets + a ``user_*_status_to_api`` translator
that accepts the user's short or long form, case-insensitively."""

from __future__ import annotations

import pytest

from tabsdatak.sdk.status_utils import (
    data_version,
    execution,
    function_run,
    transaction,
    worker,
)


# Per-module: (module, mapping_dict, to_mapping_fn, valid_str_const,
#             user_to_api_fn, failed_set, ok_set, final_set, enum_cls)
_MODULES = [
    (
        data_version,
        data_version.DATA_VERSION_STATUS_MAPPING,
        data_version.data_version_status_to_mapping,
        data_version.DATA_VERSION_VALID_USER_PROVIDED_STATUSES,
        data_version.user_data_version_status_to_api,
        data_version.DATA_VERSION_FAILED_FINAL_STATUSES,
        data_version.DATA_VERSION_SUCCESSFUL_FINAL_STATUSES,
        data_version.DATA_VERSION_FINAL_STATUSES,
        data_version.DataVersionStatus,
    ),
    (
        execution,
        execution.EXECUTION_STATUS_MAPPING,
        execution.execution_status_to_mapping,
        execution.EXECUTION_VALID_USER_PROVIDED_STATUSES,
        execution.user_execution_status_to_api,
        execution.EXECUTION_FAILED_FINAL_STATUSES,
        execution.EXECUTION_SUCCESSFUL_FINAL_STATUSES,
        execution.EXECUTION_FINAL_STATUSES,
        execution.ExecutionStatus,
    ),
    (
        function_run,
        function_run.FUNCTION_RUN_STATUS_MAPPING,
        function_run.function_run_status_to_mapping,
        function_run.FUNCTION_RUN_VALID_USER_PROVIDED_STATUSES,
        function_run.user_function_run_status_to_api,
        function_run.FUNCTION_RUN_FAILED_FINAL_STATUSES,
        function_run.FUNCTION_RUN_SUCCESSFUL_FINAL_STATUSES,
        function_run.FUNCTION_RUN_FINAL_STATUSES,
        function_run.FunctionRunStatus,
    ),
    (
        transaction,
        transaction.TRANSACTION_STATUS_MAPPING,
        transaction.transaction_status_to_mapping,
        transaction.TRANSACTION_VALID_USER_PROVIDED_STATUSES,
        transaction.user_transaction_status_to_api,
        transaction.TRANSACTION_FAILED_FINAL_STATUSES,
        transaction.TRANSACTION_SUCCESSFUL_FINAL_STATUSES,
        transaction.TRANSACTION_FINAL_STATUSES,
        transaction.TransactionStatus,
    ),
    (
        worker,
        worker.WORKER_STATUS_MAPPING,
        worker.worker_status_to_mapping,
        worker.WORKER_VALID_USER_PROVIDED_STATUSES,
        worker.user_worker_status_to_api,
        worker.WORKER_FAILED_FINAL_STATUSES,
        worker.WORKER_SUCCESSFUL_FINAL_STATUSES,
        worker.WORKER_FINAL_STATUSES,
        worker.WorkerStatus,
    ),
]

_IDS = ["data_version", "execution", "function_run", "transaction", "worker"]


@pytest.fixture(params=_MODULES, ids=_IDS)
def mod(request):
    """One test parameter set per status module."""
    return request.param


def test_status_mapping_covers_every_enum_value(mod):
    _, mapping, _, _, _, _, _, _, enum_cls = mod
    for member in enum_cls:
        assert member.value in mapping, (
            f"{enum_cls.__name__}.{member.name}={member.value} missing from mapping"
        )


def test_status_to_mapping_returns_long_form_for_known_short(mod):
    _, mapping, to_mapping, _, _, _, _, _, _ = mod
    for short, long_form in mapping.items():
        assert to_mapping(short) == long_form


def test_status_to_mapping_passes_through_unknown_status(mod):
    _, _, to_mapping, _, _, _, _, _, _ = mod
    assert to_mapping("not-a-status") == "not-a-status"


def test_failed_and_successful_final_sets_are_disjoint(mod):
    _, _, _, _, _, failed, ok, _, _ = mod
    assert failed.isdisjoint(ok)


def test_final_set_is_union_of_failed_and_successful(mod):
    _, _, _, _, _, failed, ok, final, _ = mod
    assert final == failed | ok


def test_valid_user_provided_statuses_string_lists_every_pair(mod):
    _, mapping, _, valid_str, _, _, _, _, _ = mod
    for short, long_form in mapping.items():
        assert f"{long_form}/{short}" in valid_str


def test_user_to_api_returns_none_for_empty_input(mod):
    _, _, _, _, user_to_api, _, _, _, _ = mod
    assert user_to_api(None) is None
    assert user_to_api("") is None


def test_user_to_api_accepts_short_form_case_insensitive(mod):
    _, mapping, _, _, user_to_api, _, _, _, _ = mod
    short = next(iter(mapping.keys()))
    assert user_to_api(short.lower()) == short
    assert user_to_api(short.upper()) == short


def test_user_to_api_accepts_long_form_case_insensitive(mod):
    _, mapping, _, _, user_to_api, _, _, _, _ = mod
    short, long_form = next(iter(mapping.items()))
    assert user_to_api(long_form.lower()) == short
    assert user_to_api(long_form.upper()) == short


def test_user_to_api_raises_on_unknown_status(mod):
    _, _, _, valid_str, user_to_api, _, _, _, _ = mod
    with pytest.raises(ValueError) as ei:
        user_to_api("this-is-not-a-status")
    # The error message lists the valid statuses.
    assert valid_str in str(ei.value)


# ---------------------------------------------------------------------------
# Module-specific spot checks (canonical values)
# ---------------------------------------------------------------------------


def test_data_version_known_short_codes():
    assert data_version.DataVersionStatus.COMMITTED.value == "C"
    assert data_version.DataVersionStatus.FAILED.value == "F"
    assert (
        data_version.data_version_status_to_mapping("C")
        == data_version.DATA_VERSION_STATUS_MAPPING["C"]
    )


def test_execution_has_trigger_type_mapping_too():
    # Only `execution` carries the trigger-type alongside execution
    # statuses. Spot-check that the helper is present and round-trips.
    assert hasattr(execution, "TriggerTypeStatus")
    assert hasattr(execution, "EXECUTION_TRIGGER_TYPE_MAPPING")
    for member in execution.TriggerTypeStatus:
        assert (
            execution.execution_trigger_type_to_mapping(member.value)
            == execution.EXECUTION_TRIGGER_TYPE_MAPPING[member.value]
        )
