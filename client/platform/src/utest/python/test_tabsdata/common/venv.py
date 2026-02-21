#
# Copyright 2026 Tabs Data Inc.
#

"""Python virtual environments utility functions."""

import logging
import os

from tabsdata._utils.constants import TRUE_VALUES
from tabsdata._tabsserver.pyenv_creation import (
    DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER,
    delete_virtual_environment,
    read_testimony,
)
from test_tabsdata.common.constants import ON_GA, PYTEST_DEFAULT_ENVIRONMENT_PREFIX

logger = logging.getLogger(__name__)


def clean_python_virtual_environments():
    if os.environ.get(ON_GA) in TRUE_VALUES:
        logger.info("Skipping virtual environment cleanup on GA.")
        return

    if not os.path.exists(DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER):
        logger.info("No virtual environments to clean up.")
        return

    try:
        existing_virtual_environments = sorted(
            os.listdir(DEFAULT_ENVIRONMENT_TESTIMONY_FOLDER)
        )
        logger.debug("Existing virtual environments:")
        for environment in existing_virtual_environments:
            logger.debug(f"· {environment}")
        for environment in existing_virtual_environments:
            if PYTEST_DEFAULT_ENVIRONMENT_PREFIX in environment:
                testimony_data = read_testimony(environment)
                real_name = testimony_data.real_name
                logger.info(
                    f"Cleaning up environment {environment} with real name {real_name}"
                )
                delete_virtual_environment(environment, real_name)
    except Exception as e:
        logger.warning(f"Error cleaning up the virtual environments: {e}.")
        logger.warning("Ignoring this exception as it is not critical.")
