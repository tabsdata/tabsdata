#
# Copyright 2026 Tabs Data Inc.
#

"""Shared constants for test fixtures and utility functions."""

import os

# Docker configuration
PYTEST_DEFAULT_ENVIRONMENT_PREFIX = "tdpytest_"
DOCKER_REGULAR_CONTAINER_INFIX = "c_"
DOCKER_COMPOSE_CONTAINER_INFIX = "k_"

# Retry configuration
CONTAINER_MAXIMUM_RETRY_COUNT = int(os.environ.get("TD_PYTEST_CONTAINER_MAXIMUM_RETRY_COUNT", "36"))
EXECUTION_MAXIMUM_RETRY_COUNT = int(os.environ.get("TD_PYTEST_EXECUTION_MAXIMUM_RETRY_COUNT", "18"))

# Database configuration
DB_HOST = "127.0.0.1"
DB_NAME = "testing"
DB_PASSWORD = "p@ssw0rd#"
DB_USER = "@dmIn"
