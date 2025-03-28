#
# Copyright 2024 Tabs Data Inc.
#

import json
import logging
import os

from tabsdata.utils.bundle_utils import CONFIG_FILE_NAME

logger = logging.getLogger(__name__)


def load_configuration(bundle_folder: str | bytes) -> dict:
    logger.info(f"Loading the configuration from the bundle folder '{bundle_folder}'")
    configuration_file = os.path.join(bundle_folder, CONFIG_FILE_NAME)
    with open(configuration_file) as f:
        configuration = json.load(f)
    logger.info(
        f"Loaded the configuration '{configuration}' from the bundle folder"
        f" '{bundle_folder}'"
    )
    return configuration
