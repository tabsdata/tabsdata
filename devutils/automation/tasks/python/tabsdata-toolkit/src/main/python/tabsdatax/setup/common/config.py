#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

logger = logging.getLogger(__name__)

REQUIRE_SERVER_BINARIES = "REQUIRE_SERVER_BINARIES"
REQUIRE_THIRD_PARTY = "REQUIRE_THIRD_PARTY"
TD_IGNORE_CONNECTOR_REQUIREMENTS = "TD_IGNORE_CONNECTOR_REQUIREMENTS"
TD_SKIP_NON_EXISTING_ASSETS = "TD_SKIP_NON_EXISTING_ASSETS"

TD_USE_MUSLLINUX = "TD_USE_MUSLLINUX"

TRUE_VALUES = {"1", "true", "yes", "y", "on"}

@dataclass
class SetupConfiguration:
    require_server_binaries: bool
    require_third_party: bool
    ignore_connector_requirements: bool
    skip_non_existing_assets: bool


def load_setup_configuration() -> SetupConfiguration:
    return SetupConfiguration(
        require_server_binaries=(
            os.getenv(REQUIRE_SERVER_BINARIES, "False").lower() in TRUE_VALUES
        ),
        require_third_party=(
            os.getenv(REQUIRE_THIRD_PARTY, "False").lower() in TRUE_VALUES
        ),
        ignore_connector_requirements=(
            os.getenv(TD_IGNORE_CONNECTOR_REQUIREMENTS, "True").lower() in TRUE_VALUES
        ),
        skip_non_existing_assets=(
            os.getenv(TD_SKIP_NON_EXISTING_ASSETS, "True").lower() in TRUE_VALUES
        ),
    )


def get_build_profile() -> str:
    profile = os.getenv("profile") or os.getenv("PROFILE", "debug")
    if profile in ("", "dev"):
        profile = "debug"
    logger.debug(f"Using Rust profile: '{profile}'")
    return profile


def get_td_target() -> str:
    td_target = os.getenv("td-target", "")
    logger.debug(f"Using tabsdata target: '{td_target}'")
    return td_target
