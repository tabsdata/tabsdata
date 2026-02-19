#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import shutil

logger = logging.getLogger(__name__)

BANNER = "BANNER"
LICENSE = "LICENSE"


def copy_variant_assets(
    variant_assets_folder: str,
    package_assets_folder: str,
    skip_non_existing_assets: bool,
) -> None:
    logger.debug(
        f"Copying contents of {variant_assets_folder} to {package_assets_folder}"
    )
    try:
        shutil.copytree(
            variant_assets_folder,
            package_assets_folder,
            dirs_exist_ok=True,
            symlinks=False,
        )
    except Exception as e:
        logger.warning(
            f"🦠 Warning: Failed to copy {variant_assets_folder} to"
            f" {package_assets_folder}: {e}"
        )
        if not skip_non_existing_assets:
            logger.error(
                "🦠 Raising error as 'TD_SKIP_NON_EXISTING_ASSETS' is set to"
                f" {skip_non_existing_assets}"
            )
            raise
        else:
            logger.debug(
                "🦠 Ignoring error as 'TD_SKIP_NON_EXISTING_ASSETS' is set to"
                f" {skip_non_existing_assets}"
            )


def copy_single_asset(
    source: str,
    target: str,
    skip_non_existing_assets: bool = True,
) -> None:
    logger.debug(f"Copying {source} ({os.path.abspath(source)}) to {target}")
    try:
        shutil.copy(source, target)
    except Exception as e:
        logger.warning(f"🦠 Warning: Failed to copy {source} to {target}: {e}")
        if not skip_non_existing_assets:
            logger.error(
                "🦠 Raising error as 'TD_SKIP_NON_EXISTING_ASSETS' is set to"
                f" {skip_non_existing_assets}"
            )
            raise
        else:
            logger.debug(
                "🦠 Ignoring error as 'TD_SKIP_NON_EXISTING_ASSETS' is set to"
                f" {skip_non_existing_assets}"
            )


def copy_manifest_assets(
    variant_manifest_folder: str,
    package_manifest_folder: str,
    skip_non_existing_assets: bool = True,
) -> None:
    banner_file = os.path.join(variant_manifest_folder, BANNER)
    copy_single_asset(banner_file, package_manifest_folder, skip_non_existing_assets)

    license_file = os.path.join(variant_manifest_folder, LICENSE)
    copy_single_asset(license_file, package_manifest_folder, skip_non_existing_assets)
