#
# Copyright 2024 Tabs Data Inc.
#

"""The place for fixtures and shared testing configuration."""
import logging
import os
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
            "..",
            "..",
            "client",
            "td-sdk",
            "tabsdata",
        )
    ),
)
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
            "..",
            "..",
            "client",
            "td-sdk",
            "tabsserver",
        )
    ),
)
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__),
                    "..",
                    "..",
                    "..",
                    "..",
                    "..",
                    "client",
                    "td-lib",
                    "td_features",
                )
            ),
        )
    ),
)
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__),
                    "..",
                    "..",
                    "..",
                    "..",
                    "..",
                    "client",
                    "td-lib",
                    "ta_interceptor",
                )
            ),
        )
    ),
)
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__),
                    "..",
                    "..",
                    "..",
                    "..",
                    "..",
                    "client",
                    "td-lib",
                )
            ),
        )
    ),
)
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__),
                    "..",
                    "..",
                    "..",
                    "..",
                    "..",
                    "client",
                    "td-lib",
                )
            ),
        )
    ),
)
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
            "..",
            "..",
            "client",
            "td-sdk",
        )
    ),
)

logger.info("")
logger.info("Using sys.path entries.for td_interceptor tests..:")
for path in sys.path:
    logger.info(f"   - {path}")
logger.info("")
