#
#  Copyright 2025 Tabs Data Inc.
#

import logging
import os

import colorlog

CRITICAL = 60
FATAL = 50
TRACE = 5


logging.addLevelName(CRITICAL, "CRITICAL")
logging.addLevelName(FATAL, "FATAL")
logging.addLevelName(TRACE, "TRACE")


def get_logger() -> logging.Logger:
    log_colors_config = {
        "CRITICAL": "bold_red",
        "FATAL": "purple",
        "ERROR": "red",
        "WARNING": "yellow",
        "INFO": "green",
        "DEBUG": "cyan",
        "TRACE": "blue",
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

    def critical(self, message, *args, **kwargs):
        self.log(CRITICAL, message, args, **kwargs)

    def fatal(self, message, *args, **kwargs):
        self.log(FATAL, message, args, **kwargs)

    def trace(self, message, *args, **kwargs):
        self.log(TRACE, message, args, **kwargs)

    logger.trace = trace.__get__(logger, logging.Logger)
    logger.fatal = fatal.__get__(logger, logging.Logger)
    logger.critical = critical.__get__(logger, logging.Logger)

    logger.log(TRACE, f"📌 Python 'log' library loaded with '{log_level}' level")

    return logger
