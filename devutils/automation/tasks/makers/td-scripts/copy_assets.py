#
# Copyright 2025 Tabs Data Inc.
#

import os
import shutil
from pathlib import Path


def copy_assets():
    require_third_party = os.getenv("REQUIRE_THIRD_PARTY", "False").lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )
    print(f"Current path in copy assets is {Path.cwd()}")
    variant_assets_folder = os.path.join("variant", "assets")
    client_assets_folder = os.path.join("client", "td-sdk", "tabsdata", "assets")
    print(f"Copying contents of {variant_assets_folder} to {client_assets_folder}")
    if (
        not os.path.exists(
            os.path.join(variant_assets_folder, "manifest", "THIRD-PARTY")
        )
        and require_third_party
    ):
        raise FileNotFoundError(
            f"The THIRD-PARTY file is missing in {client_assets_folder}."
        )
    shutil.copytree(variant_assets_folder, client_assets_folder, dirs_exist_ok=True)


copy_assets()
