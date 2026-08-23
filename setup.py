#
# Copyright 2024 Tabsdata Inc.
#

import logging
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

from setuptools import find_namespace_packages, setup
from tabsdatax.setup import tdabout, tdlock
from tabsdatax.setup.common.assets import copy_variant_assets
from tabsdatax.setup.common.commands import (
    create_custom_bdist_egg_class,
    create_custom_bdist_wheel_class,
    create_custom_build_class,
    create_custom_editable_wheel_class,
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
from tabsdatax.setup.tabsdata.binaries import collect_group_datafiles
from tabsdatax.setup.tabsdata.build import (
    create_custom_build_py_class,
    create_main_custom_build_hook,
)
from tabsdatax.setup.tabsdata.clean import create_custom_clean_class
from tabsdatax.setup.tabsdata.discovery import discover_tabsdata_commands
from tabsdatax.setup.tabsdata.editable import (
    EDITABLE_EXTRA_PATHS,
    create_editable_binary_symlinks,
    create_editable_libexec_links,
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
package_assets_folder_tabsdata = os.path.join(
    "client",
    "platform",
    "src",
    "main",
    "python",
    "tabsdata",
    "assets",
)
package_assets_folder_tabsdatak = os.path.join(
    "client",
    "platform",
    "src",
    "main",
    "python",
    "tabsdatak",
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
    package_assets_folder_tabsdata,
    configuration.skip_non_existing_assets,
)

target_release_folder = get_target_release_folder()
with open(os.path.join(ROOT, "setup.toml"), "rb") as setup_toml:
    setup_toml = tomllib.load(setup_toml)
    binary_groups = setup_toml["binary_groups"]
datafiles = collect_group_datafiles(
    binary_groups,
    target_release_folder,
    require_server_binaries=configuration.require_server_binaries,
    root=ROOT,
)

PYTHON_SOURCE_ROOT = os.path.join(
    ROOT,
    "client",
    "platform",
    "src",
    "main",
    "python",
)


@dataclass(frozen=True)
class LibexecCommandsSource:
    source_code_root: str
    root_module_path: str


LIBEXEC_COMMANDS_SOURCES: list[LibexecCommandsSource] = [
    LibexecCommandsSource(
        source_code_root=os.path.join(
            ROOT,
            "connectors",
            "python",
            "tabsdata-server",
            "src",
            "main",
            "python",
        ),
        root_module_path=os.path.join(
            "tabsdatak",
            "server",
            "_tdrunner",
            "function",
            "commands",
        ),
    ),
]
LIBEXEC_BUILD_FOLDER = os.path.join("target", "python", "libexec", "tabsdata")
LIBEXEC_INSTALL_FOLDER = os.path.join("libexec", "tabsdata")


def discover_libexec_specs(
    commands_sources: list[LibexecCommandsSource],
) -> list[tuple[str, str]]:
    specs: list[tuple[str, str]] = []
    for commands_source in commands_sources:
        specs.extend(
            discover_tabsdata_commands(
                os.path.join(
                    commands_source.source_code_root,
                    commands_source.root_module_path,
                ),
                commands_source.source_code_root,
            )
        )
    return sorted(specs)


libexec_specs = discover_libexec_specs(LIBEXEC_COMMANDS_SOURCES)
# libexec_launchers = build_launchers(libexec_specs, LIBEXEC_BUILD_FOLDER)
libexec_launchers: list[str] = []
libexec_datafiles: list[tuple[str, list[str]]] = (
    [(LIBEXEC_INSTALL_FOLDER, libexec_launchers)] if libexec_launchers else []
)
create_editable_libexec_links(LIBEXEC_INSTALL_FOLDER, libexec_launchers)

yaml_files_tabsdata = []
yaml_root_folder_tabsdata = Path(
    os.path.join(
        "client",
        "platform",
        "src",
        "main",
        "python",
        "tabsdata",
    )
)
for path in yaml_root_folder_tabsdata.rglob("*.yaml"):
    relative_path = path.relative_to(yaml_root_folder_tabsdata)
    yaml_files_tabsdata.append(str(relative_path))

yaml_files_tabsdatak = []
yaml_root_folder_tabsdatak = Path(
    os.path.join(
        "client",
        "platform",
        "src",
        "main",
        "python",
        "tabsdatak",
    )
)
for path in yaml_root_folder_tabsdatak.rglob("*.yaml"):
    relative_path = path.relative_to(yaml_root_folder_tabsdatak)
    yaml_files_tabsdatak.append(str(relative_path))

console_scripts: list[str] = [
    "tdk = tabsdatak.cli.td.cli:cli",
    "td_apiready = tabsdatak.server._tdserver.workers.apiready:main",
    "td_conn_resources_extractor = "
    "tabsdatak.server._tdserver.workers.conn_resources_extractor:main",
    "td_dbready = tabsdatak.server._tdserver.workers.dbready:main",
    "td_resource = tabsdatak.common._resources.reader:main",
    "td_pyvenv = tabsdatak.common._interpreter.python.pyvenv:main",
    "tdk_check = tabsdatak.server._tdserver.render.check:main",
    "tdk_table = tabsdatak.server._tdserver.render.table:main",
    "tdk_yaml = tabsdatak.server._tdserver.render.yaml:main",
]
console_scripts.extend(load_console_scripts())
console_scripts.extend(
    f"{script_name} = {module}:main" for script_name, module in libexec_specs
)

create_editable_binary_symlinks(datafiles)
ensure_egg_folder()

build_manifest_path = os.path.join(package_assets_folder_tabsdata, "manifest", "BUILD")
tdabout.write_build_manifest(Path(ROOT), build_manifest_path)

tdlock.generate_requirements_lock(ROOT, package_resources_folder)

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
    Path(ROOT) / "variant" / "resources": [
        Path(".") / "tabsdata" / "resources",
        Path(".") / "tabsdatak" / "resources",
    ],
    Path(ROOT) / "client" / "platform" / "src" / "main" / "resources" / "tdresources": [
        Path(".") / "tabsdatak" / "tdresources",
    ],
}

with global_spinner("Building package 'tabsdata' (Tabsdata)..."):
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
            "editable_wheel": create_custom_editable_wheel_class(
                EDITABLE_EXTRA_PATHS,
                ROOT,
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
                    "tabsdatak.assets",
                    "tabsdatak.assets*",
                    "tabsdatak.resources",
                    "tabsdatak.resources*",
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
                    "extensions",
                    "python",
                    "libraries",
                    "te_tableframe",
                    "src",
                    "main",
                    "python",
                ),
                include=["tabsdatak.extensions.tableframe*"],
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
                include=["tabsdatak.extensions.variant*"],
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
            *find_namespace_packages(
                where=os.path.join(
                    "expansions",
                    "polars",
                    "extensions",
                    "tz-tableframe",
                    "python",
                    "src",
                    "main",
                    "python",
                ),
                include=["tabsdata.expansions.extensions*"],
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
            "tabsdatak": os.path.join(
                "client",
                "platform",
                "src",
                "main",
                "python",
                "tabsdatak",
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
            "tabsdata.expansions.extensions": os.path.join(
                "expansions",
                "polars",
                "extensions",
                "tz-tableframe",
                "python",
                "src",
                "main",
                "python",
                "tabsdata",
                "expansions",
                "extensions",
            ),
            "tabsdata.expansions.extensions.tableframe": os.path.join(
                "expansions",
                "polars",
                "extensions",
                "tz-tableframe",
                "python",
                "src",
                "main",
                "python",
                "tabsdata",
                "expansions",
                "extensions",
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
            "tabsdatak.extensions.variant": os.path.join(
                "extensions",
                "python",
                "libraries",
                "te_variant",
                "src",
                "main",
                "python",
                "tabsdatak",
                "extensions",
                "variant",
            ),
            "tabsdatak.extensions.tableframe": os.path.join(
                "extensions",
                "python",
                "libraries",
                "te_tableframe",
                "src",
                "main",
                "python",
                "tabsdatak",
                "extensions",
                "tableframe",
            ),
        },
        package_data={
            "tabsdata": [
                "py.typed",
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
                *yaml_files_tabsdata,
            ],
            "tabsdatak": [
                "py.typed",
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
                os.path.join(
                    "tdresources",
                    "**",
                    "*",
                ),
                *yaml_files_tabsdatak,
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
            "tabsdata.expansions.extensions.tableframe": [os.path.join("_*")],
        },
        data_files=[*datafiles, *libexec_datafiles],
        entry_points={"console_scripts": console_scripts},
        include_package_data=True,
        exclude_package_data={
            "": [
                ".gitkeep",
                "*.pyc",
                "__pycache__/*",
                ".DS_Store",
                "Thumbs.db",
                "desktop.ini",
            ]
        },
    )
