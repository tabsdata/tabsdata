#
# Copyright 2025 Tabs Data Inc.
#

from enum import Enum


class DataVersionStatus(Enum):
    COMMITTED = "C"
    DONE = "D"
    ERROR = "E"
    FAILED = "F"
    ON_HOLD = "H"
    RUNNING = "R"
    RUN_REQUESTED = "RR"
    RESCHEDULED = "RS"
    SCHEDULED = "S"
    CANCELED = "X"
    YANKED = "Y"
    UNEXPECTED = "U"


DATA_VERSION_STATUS_MAPPING = {
    DataVersionStatus.COMMITTED.value: "Committed",
    DataVersionStatus.DONE.value: "Done",
    DataVersionStatus.ERROR.value: "Error",
    DataVersionStatus.FAILED.value: "Failed",
    DataVersionStatus.ON_HOLD.value: "On Hold",
    DataVersionStatus.RUNNING.value: "Running",
    DataVersionStatus.RUN_REQUESTED.value: "Run Requested",
    DataVersionStatus.RESCHEDULED.value: "Rescheduled",
    DataVersionStatus.SCHEDULED.value: "Scheduled",
    DataVersionStatus.CANCELED.value: "Canceled",
    DataVersionStatus.YANKED.value: "Yanked",
    DataVersionStatus.UNEXPECTED.value: "Unexpected",
}


def data_version_status_to_mapping(status: str) -> str:
    """
    Function to convert a status to a mapping. While currently it
    only accesses the dictionary and returns the corresponding value, it could get
    more difficult in the future.
    """
    return DATA_VERSION_STATUS_MAPPING.get(status, status)


DATA_VERSION_FAILED_FINAL_STATUSES = {
    data_version_status_to_mapping(DataVersionStatus.FAILED.value),
    data_version_status_to_mapping(DataVersionStatus.ON_HOLD.value),
    data_version_status_to_mapping(DataVersionStatus.UNEXPECTED.value),
}

# Final classification of statuses is pending consideration of the counters. For now,
# successful just means "the user does not have to examine the logs to see what went
# wrongs, and doesn't have to take any action".
DATA_VERSION_SUCCESSFUL_FINAL_STATUSES = {
    data_version_status_to_mapping(DataVersionStatus.CANCELED.value),
    data_version_status_to_mapping(DataVersionStatus.COMMITTED.value),
    data_version_status_to_mapping(DataVersionStatus.YANKED.value),
}


DATA_VERSION_FINAL_STATUSES = (
    DATA_VERSION_FAILED_FINAL_STATUSES | DATA_VERSION_SUCCESSFUL_FINAL_STATUSES
)

DATA_VERSION_VALID_USER_PROVIDED_STATUSES = ", ".join(
    [f"{long}/{short}" for short, long in DATA_VERSION_STATUS_MAPPING.items()]
)


def user_data_version_status_to_api(
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
    for key, value in DATA_VERSION_STATUS_MAPPING.items():
        if user_provided_status == key.lower() or user_provided_status == value.lower():
            return key

    raise ValueError(
        f"Invalid status: '{user_provided_status}'. "
        "Valid statuses are: "
        f"{DATA_VERSION_VALID_USER_PROVIDED_STATUSES}. Statuses are case-insensitive."
    )
