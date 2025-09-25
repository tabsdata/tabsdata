#
#  Copyright 2025 Tabs Data Inc.
#

import logging
import os

import colorlog


def get_logger() -> logging.Logger:
    log_colors_config = {
        "CRITICAL": "bold_red",
        "FATAL": "bold_red",
        "ERROR": "red",
        "WARNING": "yellow",
        "INFO": "green",
        "DEBUG": "cyan",
    }

    log_level = os.getenv("PYTHON_LOG_LEVEL", "INFO").upper()
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s[%(levelname)s] %(message)s",
        log_colors=log_colors_config,
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    logger.addHandler(handler)

    logger.debug(f"📌 Python 'log' library loaded with '{log_level}' level")

    return logger
