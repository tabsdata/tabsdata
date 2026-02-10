#
# Copyright 2025 Tabs Data Inc.
#

from enum import Enum


class TransactionStatus(Enum):
    COMMITTED = "C"
    STALLED = "L"
    RUNNING = "R"
    SCHEDULED = "S"
    UNEXPECTED = "U"
    CANCELED = "X"
    YANKED = "Y"


TRANSACTION_STATUS_MAPPING = {
    TransactionStatus.COMMITTED.value: "Committed",
    TransactionStatus.STALLED.value: "Stalled",
    TransactionStatus.RUNNING.value: "Running",
    TransactionStatus.SCHEDULED.value: "Scheduled",
    TransactionStatus.UNEXPECTED.value: "Unexpected",
    TransactionStatus.CANCELED.value: "Canceled",
    TransactionStatus.YANKED.value: "Yanked",
}


def transaction_status_to_mapping(status: str) -> str:
    """
    Function to convert a status to a mapping. While currently it
    only accesses the dictionary and returns the corresponding value, it could get
    more difficult in the future.
    """
    return TRANSACTION_STATUS_MAPPING.get(status, status)


TRANSACTION_FAILED_FINAL_STATUSES = {
    transaction_status_to_mapping(TransactionStatus.STALLED.value),
    transaction_status_to_mapping(TransactionStatus.UNEXPECTED.value),
}

# Final classification of statuses is pending consideration of the counters. For now,
# successful just means "the user does not have to examine the logs to see what went
# wrongs, and doesn't have to take any action".
TRANSACTION_SUCCESSFUL_FINAL_STATUSES = {
    transaction_status_to_mapping(TransactionStatus.CANCELED.value),
    transaction_status_to_mapping(TransactionStatus.COMMITTED.value),
    transaction_status_to_mapping(TransactionStatus.YANKED.value),
}


TRANSACTION_FINAL_STATUSES = (
    TRANSACTION_FAILED_FINAL_STATUSES | TRANSACTION_SUCCESSFUL_FINAL_STATUSES
)

TRANSACTION_VALID_USER_PROVIDED_STATUSES = ", ".join(
    [f"{long}/{short}" for short, long in TRANSACTION_STATUS_MAPPING.items()]
)


def user_transaction_status_to_api(
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
    for key, value in TRANSACTION_STATUS_MAPPING.items():
        if user_provided_status == key.lower() or user_provided_status == value.lower():
            return key

    raise ValueError(
        f"Invalid status: '{user_provided_status}'. "
        "Valid statuses are: "
        f"{TRANSACTION_VALID_USER_PROVIDED_STATUSES}. Statuses are case-insensitive."
    )
