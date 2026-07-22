#
# Copyright 2025 Tabsdata Inc.
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


target_folder = Path(__file__).parent / "target"
target_folder.mkdir(exist_ok=True)
(target_folder / "build").mkdir(exist_ok=True)
(target_folder / "egg").mkdir(exist_ok=True)

setup(version=read_version())
