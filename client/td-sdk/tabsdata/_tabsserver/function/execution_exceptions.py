#
# Copyright 2025 Tabs Data Inc.
#

GENERAL_ERROR_EXIT_STATUS = 201
TABSDATA_ERROR_EXIT_STATUS = 202


class CustomException(Exception):
    """Custom exception for execution errors."""

    def __init__(self, message: str, error_code=None):
        super().__init__(message)
        self.error_code = error_code
