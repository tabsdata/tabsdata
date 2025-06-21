#
# Copyright 2025 Tabs Data Inc.
#

from enum import Enum


class ExecutionStatus(Enum):
    FINISHED = "F"
    STALLED = "L"
    RUNNING = "R"
    SCHEDULED = "S"
    UNEXPECTED = "U"


EXECUTION_STATUS_MAPPING = {
    ExecutionStatus.FINISHED.value: "Finished",
    ExecutionStatus.STALLED.value: "Stalled",
    ExecutionStatus.RUNNING.value: "Running",
    ExecutionStatus.SCHEDULED.value: "Scheduled",
    ExecutionStatus.UNEXPECTED.value: "Unexpected",
}


def execution_status_to_mapping(status: str) -> str:
    """
    Function to convert a status to a mapping. While currently it
    only accesses the dictionary and returns the corresponding value, it could get
    more difficult in the future.
    """
    return EXECUTION_STATUS_MAPPING.get(status, status)


EXECUTION_FAILED_FINAL_STATUSES = {
    execution_status_to_mapping(ExecutionStatus.STALLED.value),
    execution_status_to_mapping(ExecutionStatus.UNEXPECTED.value),
}

# Final classification of statuses is pending consideration of the counters. For now,
# successful just means "the user does not have to examine the logs to see what went
# wrongs, and doesn't have to take any action".
EXECUTION_SUCCESSFUL_FINAL_STATUSES = {
    execution_status_to_mapping(ExecutionStatus.FINISHED.value),
}


EXECUTION_FINAL_STATUSES = (
    EXECUTION_FAILED_FINAL_STATUSES | EXECUTION_SUCCESSFUL_FINAL_STATUSES
)

EXECUTION_VALID_USER_PROVIDED_STATUSES = ", ".join(
    [f"{long}/{short}" for short, long in EXECUTION_STATUS_MAPPING.items()]
)


def user_execution_status_to_api(
    user_provided_status: str | None,
) -> str | None:
    """
    Convert a user-provided status string to the API status string.
    :param user_provided_status: The user-provided status string.
    :return: The API status string.
    """
    if not user_provided_status:
        return None

    user_provided_status = user_provided_status.lower()
    for key, value in EXECUTION_STATUS_MAPPING.items():
        if user_provided_status == key.lower() or user_provided_status == value.lower():
            return key

    raise ValueError(
        f"Invalid status: '{user_provided_status}'. "
        "Valid statuses are: "
        f"{EXECUTION_VALID_USER_PROVIDED_STATUSES}. Statuses are case-insensitive."
    )
