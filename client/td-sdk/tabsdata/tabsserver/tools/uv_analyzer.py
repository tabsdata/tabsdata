#
#  Copyright 2025 Tabs Data Inc.
#

import argparse
import json
import os
import subprocess
from dataclasses import asdict, dataclass
from typing import Optional

import yaml


@dataclass
class PythonVersionParts:
    major: int
    minor: int
    patch: int


@dataclass
class PythonInstallation:
    key: str
    version: str
    version_parts: PythonVersionParts
    path: Optional[str]
    symlink: Optional[str]
    url: Optional[str]
    os: str
    variant: str
    implementation: str
    arch: str
    libc: str


class IndentDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, indentless=False)


def resolve_uv_versions() -> list[PythonInstallation]:
    result = subprocess.run(
        ["uv", "python", "list", "--all-versions", "--output-format", "json"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True,
    )

    raw_json = json.loads(result.stdout)
    installations = []

    for entry in raw_json:
        if entry["implementation"] != "cpython":
            continue
        installation = PythonInstallation(
            key=entry["key"],
            version=entry["version"],
            version_parts=PythonVersionParts(**entry["version_parts"]),
            path=entry.get("path"),
            symlink=entry.get("symlink"),
            url=entry.get("url"),
            os=entry["os"],
            variant=entry["variant"],
            implementation=entry["implementation"],
            arch=entry["arch"],
            libc=entry["libc"],
        )
        installations.append(installation)

    installations.sort(
        key=lambda i: (
            i.version_parts.major,
            i.version_parts.minor,
            i.version_parts.patch,
        ),
        reverse=False,
    )
    return installations


def main():
    parser = argparse.ArgumentParser(
        description="Generate Python installations info from uv."
    )
    parser.add_argument(
        "--output",
        choices=["versions", "objects"],
        help=(
            "Type of output to generate:\n"
            "- 'versions' = yaml file with a bare listing of versions\n"
            "- 'objects' = json file with a full listing of installations."
        ),
    )
    parser.add_argument(
        "--file",
        type=str,
        help="Path to output file (it will be overwritten if it exists).",
    )
    args = parser.parse_args()

    if os.path.isdir(args.file):
        raise ValueError(
            f"File is an existing directory. Please specify a file path. '{args.file}'"
        )

    installations = resolve_uv_versions()
    if args.output == "versions":
        with open(args.file, "w", encoding="utf-8") as f:
            yaml.dump(
                {
                    "python-versions": [
                        installation.version for installation in installations
                    ]
                },
                f,
                Dumper=IndentDumper,
                sort_keys=False,
                default_flow_style=False,
            )
    elif args.output == "objects":
        with open(args.file, "w", encoding="utf-8") as f:
            json.dump(
                [asdict(installation) for installation in installations],
                f,
                indent=2,
            )
    else:
        raise ValueError(
            f"Invalid output type: '{args.output}'. Use 'versions' or 'objects'."
        )


if __name__ == "__main__":
    main()
