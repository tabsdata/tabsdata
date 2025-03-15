#
#  Copyright 2025 Tabs Data Inc.
#

import io
import os
import platform
from sysconfig import get_platform

from setuptools import find_packages, setup

# noinspection DuplicatedCode
if platform.python_implementation() != "CPython":
    raise RuntimeError("The Tabsdata package requires CPython to function correctly.")


def read(*paths, **kwargs):
    with io.open(
        os.path.join(os.path.dirname(__file__), *paths),
        encoding=kwargs.get("encoding", "utf8"),
    ) as open_file:
        content = open_file.read().strip()
    return content


def get_platname():
    system = platform.system()
    architecture = platform.machine().lower()

    if system == "Linux":
        if architecture in ["x86_64", "amd64"]:
            return "manylinux1_x86_64"
        elif architecture in ["aarch64", "arm64"]:
            return "manylinux1_aarch64"
        else:
            return f"manylinux1_{architecture}"
    elif system == "Darwin":
        if architecture in ["aarch64", "arm64"]:
            return "macosx_11_0_arm64"
        elif architecture == "x86_64":
            return "macosx_10_15_x86_64"
        else:
            return f"macosx_11_0_{architecture}"
    else:
        return get_platform().replace("-", "_").replace(".", "_")


def read_requirements(path):
    return [
        line.strip()
        for line in read(path).split("\n")
        if not line.startswith(('"', "#", "-", "git+"))
    ]


setup(
    name="tabsdata_salesforce",
    version=read("tabsdata_salesforce/VERSION"),
    description="Tabsdata plugin to access Salesforce data.",
    long_description=read("tabsdata_salesforce/README-PyPi.md"),
    long_description_content_type="text/markdown",
    license_files=(
        os.path.join("..", "..", "..", "variant", "assets", "manifest", "LICENSE"),
    ),
    author="Tabs Data Inc.",
    python_requires=">=3.12",
    install_requires=[],
    extras_require={"deps": read_requirements("requirements.txt")},
    options={
        "bdist_wheel": {
            "python_tag": "py312",
            "plat_name": get_platname(),
        }
    },
    packages=find_packages(
        where=os.getcwd(),
    ),
    package_dir={
        "tabsdata_salesforce": "tabsdata_salesforce",
    },
)
