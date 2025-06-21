#
# Copyright 2025 Tabs Data Inc.
#

from enum import Enum


class WorkerStatus(Enum):
    DONE = "D"
    ERROR = "E"
    FAILED = "F"
    RUNNING = "R"
    RUN_REQUESTED = "RR"
    CANCELED = "X"


WORKER_STATUS_MAPPING = {
    WorkerStatus.DONE.value: "Done",
    WorkerStatus.ERROR.value: "Error",
    WorkerStatus.FAILED.value: "Failed",
    WorkerStatus.RUNNING.value: "Running",
    WorkerStatus.RUN_REQUESTED.value: "Run Requested",
    WorkerStatus.CANCELED.value: "Canceled",
}


def worker_status_to_mapping(status: str) -> str:
    """
    Function to convert a status to a mapping. While currently it
    only accesses the dictionary and returns the corresponding value, it could get
    more difficult in the future.
    """
    return WORKER_STATUS_MAPPING.get(status, status)


WORKER_FAILED_FINAL_STATUSES = {
    worker_status_to_mapping(WorkerStatus.FAILED.value),
}

# Final classification of statuses is pending consideration of the counters. For now,
# successful just means "the user does not have to examine the logs to see what went
# wrongs, and doesn't have to take any action".
WORKER_SUCCESSFUL_FINAL_STATUSES = {
    worker_status_to_mapping(WorkerStatus.CANCELED.value),
}


WORKER_FINAL_STATUSES = WORKER_FAILED_FINAL_STATUSES | WORKER_SUCCESSFUL_FINAL_STATUSES

WORKER_VALID_USER_PROVIDED_STATUSES = ", ".join(
    [f"{long}/{short}" for short, long in WORKER_STATUS_MAPPING.items()]
)


def user_worker_status_to_api(
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
    for key, value in WORKER_STATUS_MAPPING.items():
        if user_provided_status == key.lower() or user_provided_status == value.lower():
            return key

    raise ValueError(
        f"Invalid status: '{user_provided_status}'. "
        "Valid statuses are: "
        f"{WORKER_VALID_USER_PROVIDED_STATUSES}. Statuses are case-insensitive."
    )
