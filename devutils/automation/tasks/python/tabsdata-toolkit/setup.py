#
# Copyright 2025 Tabs Data Inc.
#

from pathlib import Path

from setuptools import setup


def read_version() -> str:
    root = Path(__file__).parent
    while root != root.parent:
        version_file = root / "assets" / "manifest" / "VERSION"
        if version_file.exists():
            return version_file.read_text(encoding="utf-8").strip()
        root = root.parent
    return "0.0.1   "


setup(version=read_version())
