#
#  Copyright 2025 Tabs Data Inc.
#

import os

from setuptools import find_packages, setup

setup(
    name="td_setup",
    version="0",
    description="Tabsdata setup commons",
    author="Tabs Data Inc.",
    packages=find_packages(
        where=os.path.join(
            "src",
            "main",
            "python",
        ),
    ),
    package_dir={
        "": os.path.join(
            "src",
            "main",
            "python",
        ),
    },
)
