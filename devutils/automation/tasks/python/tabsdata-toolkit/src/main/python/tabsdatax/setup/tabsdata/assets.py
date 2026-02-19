#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

THIRD_PARTY = "THIRD-PARTY"


def check_third_party(
    variant_assets_folder: str,
    require_third_party: bool,
) -> None:
    third_party_path = os.path.join(variant_assets_folder, "manifest", THIRD_PARTY)
    if not os.path.exists(third_party_path) and require_third_party:
        raise FileNotFoundError(
            f"The THIRD-PARTY file is missing in {variant_assets_folder}."
        )
