#
# Copyright 2025 Tabs Data Inc.
#

import argparse
import importlib.util
import io
import os
import shutil
from datetime import datetime
from os import getcwd
from pathlib import Path
from types import ModuleType

# noinspection PyPackageRequirements
from tomlkit import array, dumps, parse, table


def read(*paths, **kwargs):
    with io.open(
        os.path.join(
            getcwd(),
            *paths,
        ),
        encoding=kwargs.get("encoding", "utf8"),
    ) as open_file:
        content = open_file.read().strip()
    return content


TABSDATA_VERSION = read(os.path.join("assets", "manifest", "VERSION"))


# noinspection DuplicatedCode
def load(module_name) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        module_name,
        os.path.join(
            os.getenv("MAKE_LIBRARIES_PATH"),
            f"{module_name}.py",
        ),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


logger = load("log").get_logger()


def gather_connectors(root: str) -> list[tuple[str, str]]:
    connectors_folders = [
        ("connectors", os.path.join(root, "connectors", "python")),
        # ("connectors.ee", os.path.join(root, "connectors.ee", "python")),
    ]

    connectors: list[tuple[str, str]] = []

    for connectors_root, connectors_folder in connectors_folders:
        if not os.path.isdir(connectors_folder):
            continue

        for entry in os.scandir(connectors_folder):
            if entry.is_dir():
                connector_name_parts = entry.name.split("_", 1)
                if (
                    len(connector_name_parts) != 2
                    or connector_name_parts[0] != "tabsdata"
                ):
                    logger.debug(f"⛔️ Invalid connector folder name: {entry.name}")
                else:
                    connectors.append((connectors_root, entry.name))
                    logger.debug(
                        f"📦️ Inserting connector {entry.name} from {connectors_root}"
                    )
    return sorted(connectors)


def gather_polars_modules(root: str) -> list[tuple[str, str]]:
    polars_modules_folders = [
        ("expansions", os.path.join(root, "expansions", "polars", "modules")),
    ]

    polars_modules: list[tuple[str, str]] = []

    for polars_modules_root, polars_modules_folder in polars_modules_folders:
        if not os.path.isdir(polars_modules_folder):
            continue

        for entry in os.scandir(polars_modules_folder):
            if entry.is_dir():
                if not entry.name.startswith("ty-"):
                    raise ValueError(
                        f"⛔️ Invalid polars module folder name: {entry.name}"
                    )
                polars_modules.append((polars_modules_root, entry.name))
                logger.debug(
                    f"📦️ Inserting polars module {entry.name} from"
                    f" {polars_modules_root}"
                )
    return sorted(polars_modules)


def lock_requirements_dev(root: str, connectors: list[tuple[str, str]], year: int):
    dev_lines = [
        "#\n",
        f"# Copyright {year} Tabs Data Inc.\n",
        "#\n",
        "\n",
        (
            "# Required packages to develop, test and build the project. They should"
            " not be installed by the client.\n"
        ),
        "\n",
        "# Third-party dev dependencies\n",
        "\n",
        "-r requirements/requirements-dev-third-party.txt\n",
    ]

    for c_r, c_n in sorted(connectors):
        dev_lines.append(
            f"-r {c_r}/python/{c_n}/requirements/requirements-dev-third-party.txt\n"
        )

    dev_lines.extend(
        [
            "\n",
            "# Third-party dependencies\n",
            "\n",
            "-r requirements/requirements-third-party.txt\n",
        ]
    )

    for c_r, c_n in sorted(connectors):
        dev_lines.append(
            f"-r {c_r}/python/{c_n}/requirements/requirements-third-party.txt\n"
        )

    with open(os.path.join(root, "requirements-dev.txt"), "w") as f:
        f.write("".join(dev_lines))

    logger.info("🔐️ File requirements-dev.txt locked!")


def lock_requirements_test(root: str, connectors: list[tuple[str, str]], year: int):
    test_lines = [
        "#\n",
        f"# Copyright {year} Tabs Data Inc.\n",
        "#\n",
        "\n",
        (
            "# Required packages to develop, test and build the project. They should"
            " not be installed by the client.\n"
        ),
        "\n",
        "-r requirements.txt\n",
    ]

    for c_r, c_n in sorted(connectors):
        test_lines.append(f"-r {c_r}/python/{c_n}/requirements.txt\n")

    test_lines.extend(
        [
            "\n",
            "# Third-party dependencies\n",
            "\n",
            "-r requirements/requirements-dev-third-party.txt\n",
            "\n",
            "# First-party dependencies\n",
            "\n",
            "-r requirements/requirements-dev-first-party.txt\n",
        ]
    )

    with open(os.path.join(root, "requirements-test.txt"), "w") as f:
        f.write("".join(test_lines))

    logger.info("🔐️ File requirements-test.txt locked!")


def lock_requirements_dev_first_party(
    root: str, connectors: list[tuple[str, str]], year: int
):
    dev_lines = [
        "#\n",
        f"# Copyright {year} Tabs Data Inc.\n",
        "#\n",
        "\n",
    ]

    for c_r, c_n in sorted(connectors):
        dev_lines.append(f"{c_n}[deps]=={TABSDATA_VERSION}\n")

    with open(
        os.path.join(root, "requirements", "requirements-dev-first-party.txt"), "w"
    ) as f:
        f.write("".join(dev_lines))

    logger.info("🔐️ File requirements/requirements-dev-first-party.txt locked!")


def lock_requirements_first_party(
    root: str, connectors: list[tuple[str, str]], year: int
):
    dev_lines = [
        "#\n",
        f"# Copyright {year} Tabs Data Inc.\n",
        "#\n",
        "\n",
    ]

    for c_r, c_n in sorted(connectors):
        dev_lines.append(f"{c_n}=={TABSDATA_VERSION}\n")

    with open(
        os.path.join(root, "requirements", "requirements-first-party.txt"), "w"
    ) as f:
        f.write("".join(dev_lines))

    logger.info("🔐️ File requirements/requirements-first-party.txt locked!")


def lock_requirements_third_party_all(
    root: str, connectors: list[tuple[str, str]], year: int
):
    test_lines = [
        "#\n",
        f"# Copyright {year} Tabs Data Inc.\n",
        "#\n",
        "\n",
        "-r requirements-third-party.txt\n",
    ]

    for c_r, c_n in sorted(connectors):
        test_lines.append(
            f"-r ../connectors/python/{c_n}/requirements/requirements-third-party.txt\n"
        )

    with open(
        os.path.join(root, "requirements", "requirements-third-party-all.txt"), "w"
    ) as f:
        f.write("".join(test_lines))

    logger.info("🔐️ File requirements/requirements-third-party-all.txt locked!")


def lock_pyprojects(
    root: Path,
    connectors: list[tuple[str, str]],
    polars_modules: list[tuple[str, str]],
):
    def lock_pyproject(module_folder: Path, connector_modules: set[str]):
        pyproject_file = Path(os.path.join(module_folder, "pyproject.toml"))
        if not pyproject_file.exists():
            raise ValueError("⛔️ File pyproject.toml does not exist")

        document = parse(pyproject_file.read_text())
        isort_entry = document.setdefault("tool", {}).setdefault("isort", table())

        known_first_party_entry = isort_entry.get("known_first_party", [])
        known_first_party_entry_set = {
            v for v in known_first_party_entry if isinstance(v, str)
        }

        merged = known_first_party_entry_set | connector_modules

        tabsdata = sorted(name for name in merged if name.startswith("tabsdata"))
        others = sorted(name for name in merged if not name.startswith("tabsdata"))

        sorted_array = array()
        sorted_array.multiline(True)
        for name in tabsdata + others:
            sorted_array.append(name)

        isort_entry["known_first_party"] = sorted_array
        pyproject_file.write_text(dumps(document))

        logger.info(f"🔐️ File pyproject.toml @ {module_folder} locked!")

    connector_names = {name for _, name in connectors}

    lock_pyproject(root, connector_names)

    for base, name in connectors:
        connector_folder = Path(
            os.path.join(
                root,
                base,
                "python",
                name,
            )
        )
        lock_pyproject(connector_folder, connector_names)

    for base, name in polars_modules:
        polars_module_folder = Path(
            os.path.join(
                root,
                base,
                "polars",
                "modules",
                name,
            )
        )
        lock_pyproject(polars_module_folder, connector_names)


def lock_pytest_ini(root: str) -> None:
    tabsdata_pytest_ini_file = os.path.join(root, "client", "td-sdk", "pytest.ini")
    tabsdata_pytest_ini_path = Path(tabsdata_pytest_ini_file).resolve()
    if not tabsdata_pytest_ini_path.is_file():
        raise FileNotFoundError(
            f"Base tabsdata pytest.ini not found: {tabsdata_pytest_ini_path}"
        )
    pytest_ini_name = tabsdata_pytest_ini_path.name
    for path in Path(root).rglob(pytest_ini_name):
        if path.is_file() and path.resolve() != tabsdata_pytest_ini_path:
            logger.info(f"🔏 Locking pytest.ini file: {path}")
            shutil.copy2(tabsdata_pytest_ini_path, path)
    for path in Path(os.path.join(root, "..", "tabsdata-ag")).rglob(pytest_ini_name):
        if path.is_file() and path.resolve() != tabsdata_pytest_ini_path:
            logger.info(f"🔏 Locking pytest.ini file: {path}")
            shutil.copy2(tabsdata_pytest_ini_path, path)

def lock(root: str):
    year = datetime.now().year
    connectors = gather_connectors(root)
    polars_modules = gather_polars_modules(root)
    lock_requirements_dev(root, connectors, year)
    lock_requirements_test(root, connectors, year)
    lock_requirements_dev_first_party(root, connectors, year)
    lock_requirements_first_party(root, connectors, year)
    lock_requirements_third_party_all(root, connectors, year)
    lock_pyprojects(Path(root), connectors, polars_modules)
    lock_pytest_ini(root)

    logger.info("🔑 Python connectors, dependencies and requirements locked!")


def main():
    parser = argparse.ArgumentParser(description="Lock connectors requirements")
    parser.add_argument("root", type=str, help="Root folder")
    args = parser.parse_args()

    lock(args.root)


if __name__ == "__main__":
    main()
