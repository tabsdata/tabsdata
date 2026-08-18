#
#  Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
from pathlib import Path

from setuptools import find_namespace_packages, setup

from tabsdatax.setup.common.assets import copy_manifest_assets
from tabsdatax.setup.common.binaries import create_empty_datafiles
from tabsdatax.setup.common.commands import (
    create_custom_bdist_egg_class,
    create_custom_bdist_wheel_class,
    create_custom_build_class,
    create_custom_sdist_class,
    ensure_egg_folder,
)
from tabsdatax.setup.common.config import load_setup_configuration
from tabsdatax.setup.common.descriptor import load_package_descriptor
from tabsdatax.setup.common.groups import load_binary_groups
from tabsdatax.setup.common.logging import global_spinner, init_logging
from tabsdatax.setup.common.paths import (
    CONNECTOR_TO_ROOT,
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
from tabsdatax.setup.tabsdata.binaries import collect_group_datafiles
from tabsdatax.setup.tabsdata.editable import create_editable_binary_symlinks


def build(setup_file: str) -> None:
    init_logging()
    check_cpython()

    root = root_folder(setup_file, fallback=CONNECTOR_TO_ROOT)
    descriptor = load_package_descriptor(setup_file)
    configuration = load_setup_configuration()

    python_version, python_implementation, python_abi = get_python_tags(
        root,
        os.path.join(
            "client",
            "platform",
            "src",
            "main",
            "python",
            "tabsdata",
            "__spec.py",
        ),
        setup_file,
    )
    python_version_spec = f">={python_version}"
    python_version_tag = f"{python_implementation}{python_version.replace('.', '')}"
    python_version_abi = python_abi

    variant_manifest_folder = get_variant_manifest_folder(root)
    for ns_root in ("tabsdata", "tabsdatak"):
        manifest_path = descriptor.manifest_path(ns_root)
        if not os.path.isdir(manifest_path):
            continue
        copy_manifest_assets(
            variant_manifest_folder,
            manifest_path,
            configuration.skip_non_existing_assets,
        )

    binary_groups = load_binary_groups(os.path.dirname(os.path.abspath(setup_file)))
    if binary_groups:
        datafiles = collect_group_datafiles(
            binary_groups,
            os.path.join(root, get_target_release_folder()),
            require_server_binaries=configuration.require_server_binaries,
            root=root,
        )
    else:
        datafiles = create_empty_datafiles()
    create_editable_binary_symlinks(datafiles)
    ensure_egg_folder()

    python_source_root = Path("src") / "main" / "python"

    def collect_yaml_files(namespace: str) -> list[str]:
        namespace_root = python_source_root / namespace
        if not namespace_root.is_dir():
            return []
        return [
            str(path.relative_to(namespace_root))
            for path in namespace_root.rglob("*.yaml")
        ]

    def collect_root_modules(root: str) -> list[str]:
        namespace_root = python_source_root / root
        if not namespace_root.is_dir():
            return []
        return [
            f"{root}.{path.stem}"
            for path in namespace_root.glob("*.py")
            if path.name != "__init__.py"
        ]

    def collect_root_yaml(root: str) -> list[str]:
        namespace_root = python_source_root / root
        if not namespace_root.is_dir():
            return []
        return [path.name for path in namespace_root.glob("*.yaml")]

    yaml_files_tabsdata = collect_yaml_files(descriptor.namespace("tabsdata"))
    yaml_files_tabsdatak = collect_yaml_files(descriptor.namespace("tabsdatak"))

    root_modules = collect_root_modules("tabsdata") + collect_root_modules("tabsdatak")
    root_yaml_tabsdata = collect_root_yaml("tabsdata")
    root_yaml_tabsdatak = collect_root_yaml("tabsdatak")

    # noinspection PyBroadException
    try:
        tabsdata_license = read_file(
            variant_manifest_folder,
            "LICENSE.setup",
            base_path=".",
        )
    except Exception:
        tabsdata_license = "Not Found"

    with global_spinner(descriptor.spinner_message):
        setup(
            version=read_file(
                descriptor.primary_manifest_path,
                "VERSION",
                base_path=".",
            ),
            long_description=read_file(
                descriptor.primary_manifest_path,
                "README-PyPi.md",
                base_path=".",
            ),
            long_description_content_type="text/markdown",
            license=tabsdata_license,
            license_files=(
                os.path.join(
                    descriptor.primary_manifest_path,
                    "LICENSE",
                ),
            ),
            python_requires=python_version_spec,
            install_requires=[],
            extras_require={"deps": read_requirements("requirements.txt")},
            cmdclass={
                "build": create_custom_build_class(),
                "bdist_egg": create_custom_bdist_egg_class(),
                "bdist_wheel": create_custom_bdist_wheel_class(
                    python_version_tag,
                    python_version_abi,
                    get_platname,
                ),
                "sdist": create_custom_sdist_class(skip_readme_check=True),
            },
            packages=find_namespace_packages(
                where=os.path.join(
                    "src",
                    "main",
                    "python",
                ),
                include=[
                    "tabsdata",
                    "tabsdatak",
                    f"{descriptor.namespace('tabsdata')}*",
                    f"{descriptor.namespace('tabsdatak')}*",
                ],
            ),
            py_modules=root_modules,
            package_dir={
                "": os.path.join(
                    "src",
                    "main",
                    "python",
                )
            },
            package_data={
                "tabsdata": root_yaml_tabsdata,
                "tabsdatak": root_yaml_tabsdatak,
                descriptor.namespace("tabsdata"): [
                    "py.typed",
                    os.path.join(
                        "assets",
                        "manifest",
                        "*",
                    ),
                    # Connector resources (e.g. icon SVGs referenced by
                    # a plugin def's ``icon=`` relative path).
                    os.path.join(
                        "resources",
                        "*",
                    ),
                    *yaml_files_tabsdata,
                ],
                descriptor.namespace("tabsdatak"): [
                    "py.typed",
                    os.path.join(
                        "assets",
                        "manifest",
                        "*",
                    ),
                    # Connector resources (e.g. icon SVGs referenced by
                    # a plugin def's ``icon=`` relative path).
                    os.path.join(
                        "resources",
                        "*",
                    ),
                    *yaml_files_tabsdatak,
                ],
            },
            data_files=datafiles,
            entry_points={},
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
