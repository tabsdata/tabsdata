#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tabsdatak.common._interpreter.python.binary import binaries_from_setup_toml
from tabsdatak.common._interpreter.python.recipe import (
    InstallRecipe,
    PackageRecipe,
    TabsdataEdition,
    VenvRecipe,
)
from tabsdatak.common._interpreter.python.venv import (
    CONNECTORS_RELATIVE_PATH,
    TABSDATA_AGENT_PACKAGE,
    TABSDATA_CONNECTORS_NAMES,
    TABSDATA_PACKAGE,
    EnvironmentProfile,
    PythonEnvironment,
)

PYTHON_ROOT: str = "Python Root"
PYTHON_VERSION: str = "Python Version"
TABSDATA_EDITION: str = "Tabsdata Edition"
TABSDATA_CORE_PACKAGES: str = "Tabsdata Core Packages"

COMMAND_RESOLVE: str = "resolve"
COMMAND_CREATE: str = "create"
COMMAND_PYTHON: str = "python"
COMMAND_SCRIPTS: str = "scripts"

VENV_TD: str = "td"
VENV_AG: str = "ag"
VENV_FN: str = "fn"


def main() -> int:
    arguments = parse_arguments()
    if arguments.command == COMMAND_PYTHON:
        return python(arguments.target)
    if arguments.command == COMMAND_SCRIPTS:
        return scripts(arguments.target)
    try:
        profile = PythonEnvironment(Path(sys.prefix)).resolve()
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    for line in render(profile):
        print(line)
    if arguments.command == COMMAND_CREATE:
        return create(
            profile,
            arguments.target,
            archive=arguments.archive,
            verbose=arguments.verbose,
        )
    return 0


def python(target: Path) -> int:
    try:
        path = PythonEnvironment(target).python()
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    print(path)
    return 0


def scripts(target: Path) -> int:
    try:
        path = PythonEnvironment(target).scripts()
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    print(path)
    return 0


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="td_pyvenv")
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Show the output of every subprocess this command runs.",
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser(
        COMMAND_RESOLVE,
        help="Print metadata of the current Python environment",
    )
    create_parser = subparsers.add_parser(
        COMMAND_CREATE,
        help="Create tabsdata Python virtual environment templates under <target>",
    )
    create_parser.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Folder where the Python virtual environment templates will be created",
    )
    create_parser.add_argument(
        "--archive",
        action="store_true",
        default=False,
        help=(
            "Compress each Python virtual environment template into a transferable"
            " image next to it"
        ),
    )
    python_parser = subparsers.add_parser(
        COMMAND_PYTHON,
        help=(
            "Print the absolute path of the Python binary of the Python"
            " virtual environment at <target>"
        ),
    )
    python_parser.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Root folder of the Python virtual environment",
    )
    scripts_parser = subparsers.add_parser(
        COMMAND_SCRIPTS,
        help=(
            "Print the absolute path of the scripts folder of the Python"
            " virtual environment at <target>"
        ),
    )
    scripts_parser.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Root folder of the Python virtual environment",
    )
    arguments = parser.parse_args()
    if arguments.command is None:
        arguments.command = COMMAND_RESOLVE
    return arguments


def create(
    profile: EnvironmentProfile,
    target: Path,
    *,
    archive: bool = False,
    verbose: bool = False,
) -> int:
    try:
        target.mkdir(parents=True, exist_ok=True)
        PythonEnvironment.install(profile.python_version, verbose=verbose)
        recipes = build_recipes(profile)
        with ThreadPoolExecutor(max_workers=len(recipes)) as executor:
            futures = [
                executor.submit(
                    create_recipe,
                    recipe,
                    profile,
                    target,
                    archive=archive,
                    verbose=verbose,
                )
                for recipe in recipes
            ]
            for future in futures:
                future.result()
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


def create_recipe(
    recipe: VenvRecipe,
    profile: EnvironmentProfile,
    target: Path,
    *,
    archive: bool = False,
    verbose: bool = False,
) -> None:
    environment = PythonEnvironment(target / recipe.name)
    environment.create(recipe, python_version=profile.python_version, verbose=verbose)
    if archive:
        environment.archive(target / f"{recipe.name}.tar.zst")


def build_recipes(profile: EnvironmentProfile) -> tuple[VenvRecipe, ...]:
    connector_recipes = build_connector_recipes(profile.packages)
    server_recipes = (
        tuple(
            package
            for package in profile.packages
            if package.name != TABSDATA_AGENT_PACKAGE
        )
        + connector_recipes
    )
    server = VenvRecipe(name=VENV_TD, packages=server_recipes)
    function = VenvRecipe(name=VENV_FN, packages=server_recipes)
    if profile.edition != TabsdataEdition.ENTERPRISE:
        return server, function
    agent_recipes = profile.packages + connector_recipes
    agent = VenvRecipe(name=VENV_AG, packages=agent_recipes)
    return server, function, agent


def build_connector_recipes(
    packages: tuple[PackageRecipe, ...],
) -> tuple[PackageRecipe, ...]:
    tabsdata = next(
        (package for package in packages if package.name == TABSDATA_PACKAGE),
        None,
    )
    if tabsdata is None:
        return ()
    if tabsdata.recipe == InstallRecipe.WHEEL:
        return ()
    return tuple(
        build_connector_recipe(name, tabsdata) for name in TABSDATA_CONNECTORS_NAMES
    )


def build_connector_recipe(name: str, tabsdata: PackageRecipe) -> PackageRecipe:
    if tabsdata.recipe == InstallRecipe.REGISTRY:
        return PackageRecipe(
            name=name,
            recipe=InstallRecipe.REGISTRY,
            version=tabsdata.version,
            location=None,
        )
    location = (
        tabsdata.location / CONNECTORS_RELATIVE_PATH / name
        if tabsdata.location is not None
        else None
    )
    return PackageRecipe(
        name=name,
        recipe=tabsdata.recipe,
        version=tabsdata.version,
        location=location,
        binaries=binaries_from_setup_toml(location) if location is not None else (),
    )


def render(profile: EnvironmentProfile) -> list[str]:
    lines = [
        f"- {PYTHON_ROOT}: {profile.root}",
        f"- {PYTHON_VERSION}: {profile.python_version}",
        f"- {TABSDATA_EDITION}: {profile.edition.value}",
        f"- {TABSDATA_CORE_PACKAGES}:",
    ]
    for recipe in profile.packages:
        lines.append(render_package(recipe))
    return lines


def render_package(recipe) -> str:
    fields = [f"name={recipe.name}", f"recipe={recipe.recipe.value}"]
    if recipe.version:
        fields.append(f"version={recipe.version}")
    if recipe.location is not None:
        fields.append(f"location={recipe.location}")
    return "    * " + ", ".join(fields)


if __name__ == "__main__":
    sys.exit(main())
