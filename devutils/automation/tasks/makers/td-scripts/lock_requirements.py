#
# Copyright 2025 Tabs Data Inc.
#

import argparse
import configparser
import importlib.util
import io
import os
import re
import shutil
from datetime import datetime
from os import getcwd
from pathlib import Path
from types import ModuleType

# noinspection PyPackageRequirements
from packaging.utils import canonicalize_name

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
def load(module_name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        module_name,
        os.path.join(
            os.getenv("MAKE_LIBRARIES_PATH", ""),
            f"{module_name}.py",
        ),
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


logger = load("log").get_logger()


def is_under_templates(path: Path, root: str) -> bool:
    templates_folder = Path(root) / "templates"
    try:
        path.resolve().relative_to(templates_folder.resolve())
        return True
    except ValueError:
        return False


def is_under_tabsdata_toolkit(path: Path, root: str) -> bool:
    toolkit_folder = (
        Path(root) / "devutils" / "automation" / "tasks" / "python" / "tabsdata-toolkit"
    )
    try:
        path.resolve().relative_to(toolkit_folder.resolve())
        return True
    except ValueError:
        return False


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
                connector_name_parts = entry.name.split("-", 1)
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


# noinspection DuplicatedCode
def gather_python_modules(root: str) -> list[tuple[str, str]]:
    polars_modules_folders = [
        ("expansions", os.path.join(root, "expansions", "python", "modules")),
    ]

    polars_modules: list[tuple[str, str]] = []

    for polars_modules_root, polars_modules_folder in polars_modules_folders:
        if not os.path.isdir(polars_modules_folder):
            continue

        for entry in os.scandir(polars_modules_folder):
            if entry.is_dir():
                if not entry.name.startswith("to-"):
                    raise ValueError(
                        f"⛔️ Invalid python module folder name: {entry.name}"
                    )
                polars_modules.append((polars_modules_root, entry.name))
                logger.debug(
                    f"📦️ Inserting python module {entry.name} from"
                    f" {polars_modules_root}"
                )
    return sorted(polars_modules)


def gather_test_modules(root: str) -> set[str]:
    test_modules: set[str] = set()
    for utest_dir in Path(root).rglob("utest"):
        utest_python = utest_dir / "python"
        if not utest_python.is_dir():
            continue
        if is_under_templates(utest_python, root):
            continue
        if is_under_tabsdata_toolkit(utest_python, root):
            continue
        for entry in utest_python.iterdir():
            if entry.is_dir() and entry.name.startswith("test_"):
                test_modules.add(entry.name)
                logger.debug(f"🧪 Found test module {entry.name} at {entry}")
    return test_modules


# noinspection DuplicatedCode
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

    dev_lines.extend([
        "\n",
        "# Third-party dependencies\n",
        "\n",
        "-r requirements/requirements-third-party.txt\n",
    ])

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

    test_lines.extend([
        "\n",
        "# Third-party dependencies\n",
        "\n",
        "-r requirements/requirements-dev-third-party.txt\n",
        "\n",
        "# First-party dependencies\n",
        "\n",
        "-r requirements/requirements-dev-first-party.txt\n",
    ])

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
        dev_lines.append(f"{canonicalize_name(c_n)}[deps]=={TABSDATA_VERSION}\n")

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
        dev_lines.append(f"{canonicalize_name(c_n)}=={TABSDATA_VERSION}\n")

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
    python_modules: list[tuple[str, str]],
    polars_modules: list[tuple[str, str]],
):
    def lock_pyproject(module_folder: Path, test_modules: set[str]):
        pyproject_file = Path(os.path.join(module_folder, "pyproject.toml"))
        if not pyproject_file.exists():
            raise ValueError("⛔️ File pyproject.toml does not exist")

        document = parse(pyproject_file.read_text())

        tool = document.get("tool", {})
        ruff = tool.get("ruff", {})
        lint = ruff.get("lint", {})
        isort_entry = lint.get("isort", None)

        if isort_entry is None:
            logger.debug(
                f"⏭️ Skipping {module_folder}: no [tool.ruff.lint.isort] section"
            )
            return

        known_first_party_entry = isort_entry.get("known-first-party", [])
        known_first_party_entry_set = {
            entry for entry in known_first_party_entry if isinstance(entry, str)
        }

        merged = known_first_party_entry_set | {"tabsdata", "tabsdatak"} | test_modules

        tabsdata_packages = sorted(
            name for name in merged if name.startswith("tabsdata")
        )
        tests_tabsdata_packages = sorted(
            name for name in merged if name.startswith("test_tabsdata")
        )
        tabsdata_libraries = sorted(
            name
            for name in merged
            if not (
                name.startswith("tabsdata")
                or name.startswith("test_tabsdata")
                or name.startswith("test_")
            )
        )
        tests_tabsdata_libraries = sorted(
            name
            for name in merged
            if (name.startswith("test_") and not name.startswith("test_tabsdata"))
        )

        sorted_array = array()
        sorted_array.multiline(True)
        for name in (
            tabsdata_packages
            + tabsdata_libraries
            + tests_tabsdata_packages
            + tests_tabsdata_libraries
        ):
            sorted_array.append(name)

        isort_entry["known-first-party"] = sorted_array
        pyproject_file.write_text(dumps(document))

        logger.info(f"🔐️ File pyproject.toml @ {module_folder} locked!")

    test_modules = gather_test_modules(str(root))

    lock_pyproject(root, test_modules)

    for base, name in connectors:
        connector_folder = Path(
            os.path.join(
                root,
                base,
                "python",
                name,
            )
        )
        lock_pyproject(connector_folder, test_modules)

    for base, name in python_modules:
        polars_module_folder = Path(
            os.path.join(
                root,
                base,
                "python",
                "modules",
                name,
            )
        )
        lock_pyproject(polars_module_folder, test_modules)

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
        lock_pyproject(polars_module_folder, test_modules)


def lock_connectors_constants(root: str, connectors: list[tuple[str, str]]) -> None:
    constants_py_file = os.path.join(
        root,
        "client",
        "platform",
        "src",
        "main",
        "python",
        "tabsdata",
        "_utils",
        "constants.py",
    )
    constants_py_path = Path(constants_py_file)
    if not constants_py_path.is_file():
        logger.warning(f"⚠️ constants.py not found: {constants_py_path}")
        return

    content = constants_py_path.read_text(encoding="utf-8")

    connector_names = sorted(name.replace("tabsdata-", "") for _, name in connectors)

    names_list = ",\n".join(f'    "{name}"' for name in connector_names)
    new_list = f"TABSDATA_CONNECTORS_NAMES = [\n{names_list},\n]"

    pattern = r"TABSDATA_CONNECTORS_NAMES\s*=\s*\[[^\]]*\]"
    new_content = re.sub(pattern, new_list, content)

    if new_content != content:
        constants_py_path.write_text(new_content, encoding="utf-8")
        logger.info("🔐️ File constants.py TABSDATA_CONNECTORS_NAMES locked!")
    else:
        logger.debug("⚠️️ constants.py TABSDATA_CONNECTORS_NAMES already up to date")


def lock_connectors_venv(root: str, connectors: list[tuple[str, str]]) -> None:
    venv_py_file = os.path.join(
        root,
        "client",
        "platform",
        "src",
        "main",
        "python",
        "tabsdatak",
        "_tdruntime",
        "python",
        "venv.py",
    )
    venv_py_path = Path(venv_py_file)
    if not venv_py_path.is_file():
        logger.warning(f"⚠️ File venv.py not found: {venv_py_path}")
        return

    content = venv_py_path.read_text(encoding="utf-8")

    connector_names = sorted(name for _, name in connectors)

    if connector_names:
        names_list = ",\n".join(f'    "{name}"' for name in connector_names)
        new_list = f"TABSDATA_CONNECTORS_NAMES: tuple[str, ...] = (\n{names_list},\n)"
    else:
        new_list = "TABSDATA_CONNECTORS_NAMES: tuple[str, ...] = ()"

    pattern = r"TABSDATA_CONNECTORS_NAMES: tuple\[str, \.\.\.\] = \([^)]*\)"
    new_content = re.sub(pattern, new_list, content)

    if new_content != content:
        venv_py_path.write_text(new_content, encoding="utf-8")
        logger.info("🔐️ File venv.py TABSDATA_CONNECTORS_NAMES locked!")
    else:
        logger.debug("⚠️️ File venv.py TABSDATA_CONNECTORS_NAMES already up to date")


def read_markers_from_pytest_ini(pytest_ini_path: Path) -> list[str]:
    config = configparser.RawConfigParser()
    config.read(pytest_ini_path, encoding="utf-8")

    markers_value = config.get("pytest", "markers", fallback="")
    markers: list[str] = []
    for line in markers_value.strip().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(("#", "@")):
            continue
        colon_pos = stripped.find(":")
        if colon_pos > 0 and stripped[:colon_pos].strip().isidentifier():
            markers.append(stripped)
        elif markers:
            markers[-1] = markers[-1] + " " + stripped

    return markers


def lock_pyproject_markers(root: str) -> None:
    tabsdata_pytest_ini_path = Path(
        os.path.join(
            root,
            "client",
            "platform",
            "src",
            "utest",
            "python",
            "test_tabsdata",
            "pytest.ini",
        )
    ).resolve()
    if not tabsdata_pytest_ini_path.is_file():
        logger.warning(
            f"⚠️ Main pytest.ini not found at {tabsdata_pytest_ini_path}, "
            f"skipping marker sync"
        )
        return

    markers = read_markers_from_pytest_ini(tabsdata_pytest_ini_path)
    if not markers:
        logger.warning("⚠️ No markers found in main pytest.ini")
        return

    def sync_markers(pyproject_file: Path):
        document = parse(pyproject_file.read_text())

        tool = document.get("tool")
        if tool is None:
            tool = table()
            document.add("tool", tool)

        pytest_section = tool.get("pytest")
        if pytest_section is None:
            pytest_section = table()
            tool.add("pytest", pytest_section)

        ini_options = pytest_section.get("ini_options")
        if ini_options is None:
            ini_options = table()
            pytest_section.add("ini_options", ini_options)

        markers_array = array()
        markers_array.multiline(True)
        for marker_line in markers:
            markers_array.append(marker_line)

        ini_options["markers"] = markers_array
        pyproject_file.write_text(dumps(document))
        logger.info(f"🔏 Synced {len(markers)} markers to {pyproject_file}")

    for path in Path(root).rglob("pyproject.toml"):
        if (
            path.is_file()
            and not is_under_templates(path, root)
            and not is_under_tabsdata_toolkit(path, root)
        ):
            sync_markers(path)

    tabsdata_ag_root = Path(root).parent / "tabsdata-ag"
    if tabsdata_ag_root.is_dir():
        for path in tabsdata_ag_root.rglob("pyproject.toml"):
            if path.is_file() and not is_under_templates(path, str(tabsdata_ag_root)):
                sync_markers(path)


# noinspection DuplicatedCode
def lock_pytest_ini(root: str) -> None:
    tabsdata_pytest_ini_file = os.path.join(
        root,
        "client",
        "platform",
        "src",
        "utest",
        "python",
        "test_tabsdata",
        "pytest.ini",
    )
    tabsdata_pytest_ini_path = Path(tabsdata_pytest_ini_file).resolve()
    if not tabsdata_pytest_ini_path.is_file():
        logger.warning(
            f"Base tabsdata pytest.ini not found: {tabsdata_pytest_ini_path}"
        )
        tabsdata_pytest_ini_file = os.path.join(
            root,
            "..",
            "tabsdata-os",
            "client",
            "platform",
            "src",
            "utest",
            "python",
            "test_tabsdata",
            "pytest.ini",
        )
        tabsdata_pytest_ini_path = Path(tabsdata_pytest_ini_file).resolve()
        if not tabsdata_pytest_ini_path.is_file():
            logger.warning(
                f"Base tabsdata pytest.ini not found: {tabsdata_pytest_ini_path}"
            )
            tabsdata_pytest_ini_file = os.path.join(
                root,
                "..",
                "tabsdata",
                "client",
                "platform",
                "src",
                "utest",
                "python",
                "test_tabsdata",
                "pytest.ini",
            )
            tabsdata_pytest_ini_path = Path(tabsdata_pytest_ini_file).resolve()
            if not tabsdata_pytest_ini_path.is_file():
                raise FileNotFoundError(
                    f"Base tabsdata pytest.ini not found: {tabsdata_pytest_ini_path}"
                )

    pytest_ini_name = tabsdata_pytest_ini_path.name

    for path in Path(root).rglob(pytest_ini_name):
        if path.is_file() and path.resolve() != tabsdata_pytest_ini_path:
            if is_under_templates(path, root):
                logger.debug(f"⏭️ Skipping templates file: {path}")
                continue
            if is_under_tabsdata_toolkit(path, root):
                logger.debug(f"⏭️ Skipping tabsdata-toolkit file: {path}")
                continue
            logger.info(f"🔏 Locking pytest.ini file: {path}")
            shutil.copy2(tabsdata_pytest_ini_path, path)

    tabsdata_ag_root = os.path.join(root, "..", "tabsdata-ag")
    for path in Path(tabsdata_ag_root).rglob(pytest_ini_name):
        if path.is_file() and path.resolve() != tabsdata_pytest_ini_path:
            if is_under_templates(path, tabsdata_ag_root):
                logger.debug(f"⏭️ Skipping templates file: {path}")
                continue
            logger.info(f"🔏 Locking pytest.ini file: {path}")
            shutil.copy2(tabsdata_pytest_ini_path, path)


def lock_requirements_first_party_versions(root: str):
    for requirements_file in Path(root).rglob("requirements*.txt"):
        if not requirements_file.is_file():
            continue

        content = requirements_file.read_text(encoding="utf-8")

        updated_content = re.sub(
            r"^(tabsdata[\w-]*)==.+$",
            rf"\1=={TABSDATA_VERSION}",
            content,
            flags=re.MULTILINE,
        )

        if updated_content != content:
            requirements_file.write_text(updated_content, encoding="utf-8")
            logger.info(f"🔐️ File {requirements_file} locked!")


def lock(root: str):
    year = datetime.now().year
    connectors = gather_connectors(root)
    python_modules = gather_python_modules(root)
    polars_modules = gather_polars_modules(root)
    lock_requirements_dev(root, connectors, year)
    lock_requirements_test(root, connectors, year)
    if os.path.basename(os.path.normpath(root)) != "tabsdata-ag":
        lock_requirements_dev_first_party(root, connectors, year)
        lock_requirements_first_party(root, connectors, year)
        lock_requirements_third_party_all(root, connectors, year)
        lock_connectors_constants(root, connectors)
        lock_connectors_venv(root, connectors)
    lock_requirements_first_party_versions(root)
    lock_pyprojects(Path(root), connectors, python_modules, polars_modules)
    lock_pyproject_markers(root)
    lock_pytest_ini(root)

    logger.info("🔑 Python connectors, dependencies and requirements locked!")


def main():
    parser = argparse.ArgumentParser(description="Lock connectors requirements")
    parser.add_argument("root", type=str, help="Root folder")
    args = parser.parse_args()

    lock(args.root)


if __name__ == "__main__":
    main()
