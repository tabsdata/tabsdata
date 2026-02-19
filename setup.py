#
# Copyright 2024 Tabs Data Inc.
#

import logging
import os
import tomllib
from pathlib import Path

from setuptools import find_namespace_packages, setup
from tabsdatax.setup import tdabout, tdlock
from tabsdatax.setup.common.assets import copy_variant_assets
from tabsdatax.setup.common.commands import (
    create_custom_bdist_egg_class,
    create_custom_bdist_wheel_class,
    create_custom_build_class,
    create_custom_sdist_class,
    ensure_egg_folder,
)
from tabsdatax.setup.common.config import load_setup_configuration
from tabsdatax.setup.common.logging import global_spinner, init_logging
from tabsdatax.setup.common.paths import (
    get_variant_assets_folder,
    get_variant_manifest_folder,
    read_file,
    root_folder,
)
from tabsdatax.setup.common.platform import (
    check_cpython,
    get_platname,
    get_python_tags,
    get_target_release_folder,
)
from tabsdatax.setup.common.requirements import read_requirements
from tabsdatax.setup.tabsdata.assets import check_third_party
from tabsdatax.setup.tabsdata.binaries import create_datafiles, detect_binaries
from tabsdatax.setup.tabsdata.build import (
    create_custom_build_py_class,
    create_main_custom_build_hook,
)
from tabsdatax.setup.tabsdata.clean import create_custom_clean_class
from tabsdatax.setup.tabsdata.editable import (
    EDITABLE_EXTRA_PATHS,
    create_editable_binary_symlinks,
    create_editable_extra_pth_file,
)
from tabsdatax.setup.tabsdata.requirements import build_extras_require
from tabsdatax.setup.tabsdata.scripts import load_console_scripts

# noinspection DuplicatedCode
init_logging()
logger = logging.getLogger(__name__)

check_cpython()

ROOT = root_folder(__file__)
logger.debug(f"Current path in setup is {ROOT}")

configuration = load_setup_configuration()

python_version, python_implementation, python_abi = get_python_tags(
    ROOT,
    os.path.join(
        "client",
        "platform",
        "src",
        "main",
        "python",
        "tabsdata",
        "__spec.py",
    ),
    __file__,
)
python_version_spec = f">={python_version}"
python_version_tag = f"{python_implementation}{python_version.replace('.', '')}"
python_version_abi = python_abi

variant_assets_folder = get_variant_assets_folder(ROOT)
variant_manifest_folder = get_variant_manifest_folder(ROOT)
package_assets_folder = os.path.join(
    "client",
    "platform",
    "src",
    "main",
    "python",
    "tabsdata",
    "assets",
)
package_resources_folder = os.path.join(
    "variant",
    "resources",
)
check_third_party(
    variant_assets_folder,
    configuration.require_third_party,
)
copy_variant_assets(
    variant_assets_folder,
    package_assets_folder,
    configuration.skip_non_existing_assets,
)

target_release_folder = get_target_release_folder()
with open(os.path.join(ROOT, "setup.toml"), "rb") as setup_toml:
    setup_toml = tomllib.load(setup_toml)
    base_binaries = setup_toml["setup"]["binaries"]
existing_binaries, missing_binaries = detect_binaries(
    base_binaries,
    target_release_folder,
    configuration.require_server_binaries,
)
datafiles = create_datafiles(
    existing_binaries,
    target_release_folder,
)

yaml_files = []
yaml_root_folder = Path(
    os.path.join(
        "client",
        "platform",
        "src",
        "main",
        "python",
        "tabsdata",
    )
)
for path in yaml_root_folder.rglob("*.yaml"):
    relative_path = path.relative_to(yaml_root_folder)
    yaml_files.append(str(relative_path))

console_scripts: list[str] = [
    "td = tabsdata._cli.cli:cli",
    "tdorachk = tabsdata._tabsserver.tools.x_oracle_check:cli",
    "td_cfgrsv = tabsdata._tabsserver.tools.config_resolver:main",
    "td_invoker = tabsdata._tabsserver.invoker:main",
    "td_function = tabsdata._tabsserver.invoker:main",
    "td_stagetrigger = tabsdata._tabsserver.invoker:main",
    "td_janitor = tabsdata._tabsserver.tools.janitor:main",
    "td_mntext = tabsdata._tabsserver.tools.mount_extractor:main",
    "td_srvinf = tabsdata._tabsserver.tools.server_info:main",
    "td_upgrader = tabsdata._tabsserver.server.upgrader.upgrader:main",
    "td_venv = tabsdata._tabsserver.pyenv_creation:main",
]
console_scripts.extend(load_console_scripts())

create_editable_extra_pth_file(__file__, EDITABLE_EXTRA_PATHS)
create_editable_binary_symlinks(datafiles)
ensure_egg_folder()

build_manifest_path = os.path.join(package_assets_folder, "manifest", "BUILD")
tdabout.write_build_manifest(Path(ROOT), build_manifest_path)

requirements_lock_path = os.path.join(
    package_resources_folder, "lock", "requirements.txt"
)
tdlock.write_requirements_lock(Path(ROOT), requirements_lock_path)

TABSDATA_VERSION = read_file(
    "assets",
    "manifest",
    "VERSION",
    base_path=".",
)
TABSDATA_LICENSE = read_file(
    variant_manifest_folder,
    "LICENSE.setup",
    base_path=".",
)
PACKAGE_RESOURCES = {
    Path(ROOT) / "variant" / "resources": Path(".") / "tabsdata" / "resources",
}

with global_spinner("Building package 'tabsdata' (TabsData)..."):
    setup(
        version=TABSDATA_VERSION,
        long_description=read_file(
            "variant",
            "assets",
            "manifest",
            "README-PyPi.md",
            base_path=".",
        ),
        long_description_content_type="text/markdown",
        license=TABSDATA_LICENSE,
        license_files=(
            os.path.join(
                "variant",
                "assets",
                "manifest",
                "LICENSE",
            ).replace(os.sep, "/"),
        ),
        python_requires=python_version_spec,
        install_requires=read_requirements("requirements.txt"),
        extras_require=build_extras_require(os.getcwd(), TABSDATA_VERSION),
        cmdclass={
            "build": create_custom_build_class(create_main_custom_build_hook(ROOT)),
            "bdist_egg": create_custom_bdist_egg_class(),
            "build_py": create_custom_build_py_class(PACKAGE_RESOURCES),
            "bdist_wheel": create_custom_bdist_wheel_class(
                python_version_tag,
                python_version_abi,
                get_platname,
            ),
            "clean": create_custom_clean_class(
                ROOT,
                "setup.toml",
            ),
            "sdist": create_custom_sdist_class(),
        },
        packages=[
            *find_namespace_packages(
                where=os.path.join(
                    "client",
                    "platform",
                    "src",
                    "main",
                    "python",
                ),
                exclude=[
                    "tabsdata.assets",
                    "tabsdata.assets*",
                    "tabsdata.resources",
                    "tabsdata.resources*",
                ],
            ),
            *find_namespace_packages(
                where=os.path.join(
                    "client",
                    "libraries",
                    "ta_features",
                    "src",
                    "main",
                    "python",
                ),
                include=["tabsdata.extensions*"],
            ),
            *find_namespace_packages(
                where=os.path.join(
                    "client",
                    "libraries",
                    "ta_tableframe",
                    "src",
                    "main",
                    "python",
                ),
                include=["tabsdata.extensions*"],
            ),
            *find_namespace_packages(
                where=os.path.join(
                    "extensions",
                    "python",
                    "libraries",
                    "te_examples",
                    "src",
                    "main",
                    "python",
                ),
                include=["tabsdata.extensions*"],
            ),
            *find_namespace_packages(
                where=os.path.join(
                    "extensions",
                    "python",
                    "libraries",
                    "te_tableframe",
                    "src",
                    "main",
                    "python",
                ),
                include=["tabsdata.extensions*"],
            ),
            *find_namespace_packages(
                where=os.path.join(
                    "extensions",
                    "python",
                    "libraries",
                    "te_variant",
                    "src",
                    "main",
                    "python",
                ),
                include=["tabsdata.extensions*"],
            ),
            *find_namespace_packages(
                where=os.path.join(
                    "expansions",
                    "python",
                    "modules",
                    "to-foundation",
                    "python",
                    "src",
                    "main",
                    "python",
                ),
                include=["tabsdata.expansions*"],
            ),
            *find_namespace_packages(
                where=os.path.join(
                    "expansions",
                    "polars",
                    "modules",
                    "ty-tableframe",
                    "python",
                    "src",
                    "main",
                    "python",
                ),
                include=["tabsdata.expansions*"],
            ),
        ],
        package_dir={
            "": os.path.join(
                "client",
                "platform",
                "src",
                "main",
                "python",
            ),
            "tabsdata": os.path.join(
                "client",
                "platform",
                "src",
                "main",
                "python",
                "tabsdata",
            ),
            "tabsdata.expansions": os.path.join(
                "expansions",
                "python",
                "modules",
                "to-foundation",
                "python",
                "src",
                "main",
                "python",
                "tabsdata",
                "expansions",
            ),
            "tabsdata.expansions.foundation": os.path.join(
                "expansions",
                "python",
                "modules",
                "to-foundation",
                "python",
                "src",
                "main",
                "python",
                "tabsdata",
                "expansions",
                "foundation",
            ),
            "tabsdata.expansions.tableframe": os.path.join(
                "expansions",
                "polars",
                "modules",
                "ty-tableframe",
                "python",
                "src",
                "main",
                "python",
                "tabsdata",
                "expansions",
                "tableframe",
            ),
            "tabsdata.extensions": os.path.join(
                "client",
                "libraries",
                "ta_features",
                "src",
                "main",
                "python",
                "tabsdata",
                "extensions",
            ),
            "tabsdata.extensions._examples": os.path.join(
                "extensions",
                "python",
                "libraries",
                "te_examples",
                "src",
                "main",
                "python",
                "tabsdata",
                "extensions",
                "_examples",
            ),
            "tabsdata.extensions._features": os.path.join(
                "client",
                "libraries",
                "ta_features",
                "src",
                "main",
                "python",
                "tabsdata",
                "extensions",
                "_features",
            ),
            "tabsdata.extensions._features.api": os.path.join(
                "client",
                "libraries",
                "ta_features",
                "src",
                "main",
                "python",
                "tabsdata",
                "extensions",
                "_features",
                "api",
            ),
            "tabsdata.extensions._tableframe": os.path.join(
                "extensions",
                "python",
                "libraries",
                "te_tableframe",
                "src",
                "main",
                "python",
                "tabsdata",
                "extensions",
                "_tableframe",
            ),
            "tabsdata.extensions._tableframe.api": os.path.join(
                "client",
                "libraries",
                "ta_tableframe",
                "src",
                "main",
                "python",
                "tabsdata",
                "extensions",
                "_tableframe",
                "api",
            ),
            "tabsdata.extensions.variant": os.path.join(
                "extensions",
                "python",
                "libraries",
                "te_variant",
                "src",
                "main",
                "python",
                "tabsdata",
                "extensions",
                "variant",
            ),
        },
        package_data={
            "tabsdata": [
                os.path.join(
                    "assets",
                    "manifest",
                    "*",
                ),
                os.path.join(
                    "resources",
                    "**",
                    "*",
                ),
                *yaml_files,
            ],
            "tabsdata.extensions._examples": [
                os.path.join(
                    "guides",
                    "book",
                    "**",
                    "*",
                ),
                os.path.join(
                    "cases",
                    "**",
                    "*",
                ),
            ],
            "tabsdata.expansions.foundation": [os.path.join("_*")],
            "tabsdata.expansions.tableframe": [os.path.join("_*")],
        },
        data_files=datafiles,
        entry_points={"console_scripts": console_scripts},
        include_package_data=True,
        exclude_package_data={
            "": [
                ".gitkeep",
                "*.pyc",
                "__pycache__/*",
            ]
        },
    )
