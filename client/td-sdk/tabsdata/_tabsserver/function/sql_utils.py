#
# Copyright 2024 Tabs Data Inc.
#

import logging

logger = logging.getLogger(__name__)

MARIADB_COLLATION = "utf8mb4_unicode_520_ci"


def add_mariadb_collation(uri: str) -> str:
    # Note: if the user has not provided a collation parameter, we must add it
    # to ensure that the driver works properly.
    if "collation" not in uri:
        logger.debug("Adding collation parameter to the MariaDB URI")
        if "?" in uri:
            uri = f"{uri}&collation={MARIADB_COLLATION}"
        else:
            uri = f"{uri}?collation={MARIADB_COLLATION}"
    else:
        logger.debug("Collation parameter already exists in the MariaDB URI")

    return uri
