#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import sys
from importlib.metadata import PackageNotFoundError, version as package_version

import yaml

from tabsdatak.common._resources.reader import td_resource

PROG = "td_tdinfo"
DESCRIPTION = (
    "Report the running distribution's version and build provenance as YAML on stdout."
)

DISTRIBUTION = "tabsdata"
SOURCETRACK_RESOURCE = "assets/manifest/SOURCETRACK.yaml"

VERSION_KEY = "version"
SOURCETRACK_KEY = "sourcetrack"

UNKNOWN_VERSION = "-unknown-"


def server_version() -> str:
    try:
        return package_version(DISTRIBUTION)
    except PackageNotFoundError:
        return UNKNOWN_VERSION


def sourcetrack() -> dict | None:
    try:
        resource = td_resource(SOURCETRACK_RESOURCE)
    except FileNotFoundError:
        return None
    try:
        loaded = yaml.safe_load(resource.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return None
    return loaded or None


def runtime_info() -> dict:
    return {
        VERSION_KEY: server_version(),
        SOURCETRACK_KEY: sourcetrack(),
    }


def build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(prog=PROG, description=DESCRIPTION)


def main() -> int:
    build_parser().parse_args()
    yaml.safe_dump(runtime_info(), sys.stdout, sort_keys=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
