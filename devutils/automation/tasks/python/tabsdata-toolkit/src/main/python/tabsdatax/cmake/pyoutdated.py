#
# Copyright 2026 Tabs Data Inc.
#

import argparse
import concurrent.futures
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.error import URLError
from urllib.request import urlopen

from packaging.version import InvalidVersion, Version
from rich.console import Console
from rich.table import Table

RENDER_WIDTH = 1024


@dataclass
class Requirement:
    name: str
    specifier: str
    source_file: Path


@dataclass
class OutdatedPackage:
    name: str
    declared: str
    latest: str


def tokenize(requirement: str) -> Optional[tuple[str, str]]:
    requirement = requirement.strip()

    if not requirement or requirement.startswith("#") or requirement.startswith("-"):
        return None

    match = re.match(
        r"^([a-zA-Z0-9][-a-zA-Z0-9._]*)"  # package name
        r"(?:\[[^]]+])?"  # optional extras
        r"([<>=!~][^;#]*)?",  # version specifier
        requirement,
    )

    if match:
        name = match.group(1).lower().replace("_", "-")
        specifier = (match.group(2) or "").strip()
        return name, specifier

    return None


def parse(
    file: Path,
    visited: Optional[set[Path]] = None,
) -> list[Requirement]:
    if visited is None:
        visited = set()

    file = file.resolve()

    if file in visited:
        return []

    visited.add(file)

    if not file.exists():
        print(f"Warning: File not found: {file}", file=sys.stderr)
        return []

    requirements: list[Requirement] = []

    with open(file) as f:
        for line in f:
            line = line.strip()

            if line.startswith("-r ") or line.startswith("--requirement "):
                parts = line.split(maxsplit=1)
                if len(parts) == 2:
                    included_path = file.parent / parts[1]
                    requirements.extend(parse(included_path, visited))
                continue

            parsed = tokenize(line)
            if parsed:
                name, specifier = parsed
                requirements.append(Requirement(name, specifier, file))

    return requirements


def get_declared_version(specifier: str) -> Optional[str]:
    if not specifier:
        return None

    match = re.match(r"^[=~<>!]+\s*([0-9][0-9a-zA-Z._-]*)", specifier)
    if match:
        return match.group(1)

    return None


def get_latest_version(package_name: str) -> Optional[str]:
    url = f"https://pypi.org/pypi/{package_name}/json"

    try:
        with urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())
            return data.get("info", {}).get("version")
    except (URLError, json.JSONDecodeError, TimeoutError):
        return None


def is_outdated(declared: str, latest: str) -> bool:
    try:
        declared_version = Version(declared)
        latest_version = Version(latest)
        return declared_version < latest_version
    except InvalidVersion:
        return False


def check_package(requirement: Requirement) -> Optional[OutdatedPackage]:
    if requirement.name.startswith("tabsdata"):
        return None

    declared = get_declared_version(requirement.specifier)
    if not declared:
        return None

    latest = get_latest_version(requirement.name)
    if not latest:
        return None

    if is_outdated(declared, latest):
        return OutdatedPackage(name=requirement.name, declared=declared, latest=latest)

    return None


def check_outdated(
    requirements_file: Path,
    max_workers: int = 10,
) -> list[OutdatedPackage]:
    requirements = parse(requirements_file)

    seen: dict[str, Requirement] = {}
    for requirement in requirements:
        if requirement.name not in seen:
            seen[requirement.name] = requirement

    requirements = list(seen.values())

    outdated: list[OutdatedPackage] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(check_package, requirements): requirements
            for requirements in requirements
        }

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                outdated.append(result)

    return sorted(outdated, key=lambda p: p.name)


def display_outdate(outdated: list[OutdatedPackage]) -> None:
    console = Console(width=RENDER_WIDTH)

    if not outdated:
        console.print("[green]All packages are up to date![/green]")
        return

    table = Table(
        title="Outdated Packages",
        show_header=True,
        header_style="bold magenta",
    )

    table.add_column("Package", style="cyan", no_wrap=True)
    table.add_column("Declared", style="yellow", justify="right")
    table.add_column("Latest", style="green", justify="right")

    for package in outdated:
        table.add_row(package.name, package.declared, package.latest)

    console.print()
    console.print(table)
    console.print()
    console.print(f"[bold red]{len(outdated)}[/bold red] outdated package(s) found")
    console.print()


def main():
    parser = argparse.ArgumentParser(
        description="Check for outdated packages in requirements files.",
    )
    parser.add_argument(
        "file",
        type=Path,
        help="Path to the requirements file to check",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=16,
        help="Number of parallel workers for PyPI queries (default: 16)",
    )

    args = parser.parse_args()

    if not args.file.exists():
        print(f"Error: File not found: {args.file}", file=sys.stderr)
        sys.exit(1)

    console = Console(width=RENDER_WIDTH)
    console.print(f"[dim]Checking {args.file}...[/dim]")

    outdated = check_outdated(args.file, args.workers)
    display_outdate(outdated)


if __name__ == "__main__":
    main()
