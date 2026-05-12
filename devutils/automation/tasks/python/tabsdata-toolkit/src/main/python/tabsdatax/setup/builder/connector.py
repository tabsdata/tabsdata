#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import os

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
from tabsdatax.setup.common.logging import global_spinner, init_logging
from tabsdatax.setup.common.paths import (
    CONNECTOR_TO_ROOT,
    get_variant_manifest_folder,
    read_file,
    root_folder,
)
from tabsdatax.setup.common.platform import check_cpython, get_platname, get_python_tags
from tabsdatax.setup.common.requirements import read_requirements


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

    datafiles = create_empty_datafiles()
    ensure_egg_folder()

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
                    f"{descriptor.namespace('tabsdata')}*",
                    f"{descriptor.namespace('tabsdatak')}*",
                ],
            ),
            package_dir={
                "": os.path.join(
                    "src",
                    "main",
                    "python",
                )
            },
            package_data={
                descriptor.namespace("tabsdata"): [
                    os.path.join(
                        "assets",
                        "manifest",
                        "*",
                    )
                ],
                descriptor.namespace("tabsdatak"): [
                    os.path.join(
                        "assets",
                        "manifest",
                        "*",
                    )
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
                ]
            },
        )
