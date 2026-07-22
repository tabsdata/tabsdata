#
#  Copyright 2026 Tabsdata Inc.
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
TD_SETUP_GENERATE_LOCKS = "TD_SETUP_GENERATE_LOCKS"

TD_USE_MUSLLINUX = "TD_USE_MUSLLINUX"

TRUE_VALUES = {"1", "true", "yes", "y", "on"}


@dataclass
class SetupConfiguration:
    require_server_binaries: bool
    require_third_party: bool
    ignore_connector_requirements: bool
    skip_non_existing_assets: bool
    setup_generate_locks: bool


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
        setup_generate_locks=(
            os.getenv(TD_SETUP_GENERATE_LOCKS, "False").lower() in TRUE_VALUES
        ),
    )


def get_build_profile() -> str:
    logger.debug(
        f"Profile build environment variables:\n"
        f"profile='{os.getenv('profile', 'unset')}\n"
        f"PROFILE='{os.getenv('PROFILE', 'unset')}'"
    )
    profile = os.getenv("profile") or os.getenv("PROFILE", "debug")
    if profile in ("", "dev"):
        profile = "debug"
    logger.debug(f"Using Rust profile: '{profile}'")
    return profile


def get_td_target() -> str:
    logger.debug(
        f"Target build environment variables:\n"
        f"td-target='{os.getenv('td-target', 'unset')}'\n"
        f"TD_TARGET='{os.getenv('TD_TARGET', 'unset')}'"
    )
    td_target = os.getenv("td-target") or os.getenv("TD_TARGET", "")
    logger.debug(f"Using tabsdata target: '{td_target}'")
    return td_target
