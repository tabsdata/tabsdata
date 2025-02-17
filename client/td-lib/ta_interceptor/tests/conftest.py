#
# Copyright 2024 Tabs Data Inc.
#

"""The place for fixtures and shared testing configuration."""
import logging
import os
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.getLogger("filelock").setLevel(logging.INFO)

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
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
            "td-sdk",
            "tabsserver",
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
            "td_features",
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
            "plugins",
            "python",
            "td-lib",
            "td_interceptor",
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
            "plugins",
            "python",
            "td-lib",
        )
    ),
)
# td-lib
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
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
            "td-sdk",
        )
    ),
)

logger.info("")
logger.info("Using sys.path entries.for ta_interceptor tests..:")
for path in sys.path:
    logger.info(f"   - {path}")
logger.info("")
