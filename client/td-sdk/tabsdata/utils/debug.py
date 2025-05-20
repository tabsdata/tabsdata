#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os

import pydevd_pycharm

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

REMOTE_DEBUG = "TD_REMOTE_DEBUG"
REMOTE_DEBUG_HOST = "127.0.0.1"
REMOTE_DEBUG_PORT = 5678

TRUE_1 = "1"
TRUE_TRUE = "true"
TRUE_YES = "yes"
TRUE_Y = "y"
TRUE_ON = "on"

FALSE_0 = "0"
FALSE_FALSE = "false"
FALSE_NO = "no"
FALSE_N = "n"
FALSE_OFF = "off"

TRUE_VALUES = {TRUE_1, TRUE_TRUE, TRUE_YES, TRUE_Y, TRUE_ON}
FALSE_VALUES = {FALSE_0, FALSE_FALSE, FALSE_NO, FALSE_N, FALSE_OFF}


def remote_debug() -> bool:
    remote_debug_enabled = os.getenv(REMOTE_DEBUG, FALSE_FALSE).lower() in TRUE_VALUES
    if remote_debug_enabled:
        pydevd_pycharm.settrace(
            host=REMOTE_DEBUG_HOST,
            port=REMOTE_DEBUG_PORT,
            stdoutToServer=True,
            stderrToServer=True,
        )
    logger.info("Remote debug enabled...")
    return remote_debug_enabled
