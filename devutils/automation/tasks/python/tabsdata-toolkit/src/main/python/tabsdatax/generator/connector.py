#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path

import rich_click as click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

PLACEHOLDER_PACKAGE = "{c.package}"
PLACEHOLDER_BACKEND = "{c.backend}"
PLACEHOLDER_MODULE = "{c.module}"
PLACEHOLDER_ALIAS = "{c.alias}"
PLACEHOLDER_VERSION = "{c.version}"
PLACEHOLDER_YEAR = "{c.year}"

VALID_PROJECT_NAMES = {"tabsdata", "tabsdata-os"}

TEMPLATE_FOLDER = "templates/connector/tabsdata-{c.package}"

CONNECTORS_FOLDER = "connectors/python"

ROOT_PYPROJECT = "pyproject.toml"
PLATFORM_PYTEST_INI = "client/platform/src/utest/python/test_tabsdata/pytest.ini"

console = Console()

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}

STYLE_SUCCESS = "bold green"
STYLE_ERROR = "bold red"
STYLE_INFO = "cyan"
STYLE_HIGHLIGHT = "bold cyan"
STYLE_DIM = "dim"


@dataclass(frozen=True)
class Placeholder:
    key: str
    caption: str
    value: str = ""
    path_value: str = ""
    compute: Callable[[Placeholders], str] | None = None
    path_compute: Callable[[Placeholders], str] | None = None


Placeholders = dict[str, Placeholder]


def build_placeholders(package: str, backend: str, root: Path) -> Placeholders:
    version_file = root / "assets" / "manifest" / "VERSION"
    version = version_file.read_text(encoding="utf-8").strip()
    year = str(datetime.now().year)

    specs = [
        Placeholder(
            key=PLACEHOLDER_PACKAGE,
            caption="Package",
            value=package,
        ),
        Placeholder(
            key=PLACEHOLDER_BACKEND,
            caption="Backend",
            value=backend,
        ),
        Placeholder(
            key=PLACEHOLDER_MODULE,
            caption="Module",
            compute=lambda ps: ps[PLACEHOLDER_PACKAGE].value.replace("-", "."),
            path_compute=lambda ps: os.sep.join(
                ps[PLACEHOLDER_PACKAGE].value.split("-")
            ),
        ),
        Placeholder(
            key=PLACEHOLDER_ALIAS,
            caption="Alias",
            compute=lambda ps: ps[PLACEHOLDER_PACKAGE].value.replace("-", "_"),
        ),
        Placeholder(
            key=PLACEHOLDER_VERSION,
            caption="Version",
            value=version,
        ),
        Placeholder(
            key=PLACEHOLDER_YEAR,
            caption="Year",
            value=year,
        ),
    ]

    placeholders: Placeholders = {}
    for spec in specs:
        placeholder = spec
        if placeholder.compute is not None:
            placeholder = replace(placeholder, value=placeholder.compute(placeholders))
        if placeholder.path_compute is not None:
            placeholder = replace(
                placeholder, path_value=placeholder.path_compute(placeholders)
            )
        placeholders[placeholder.key] = placeholder
    return placeholders


def show_header() -> None:
    header = Text()
    header.append("Tabsdata ", style="bold cyan")
    header.append("Connector Generator", style="bold white")
    console.print(Panel(header, border_style="cyan", padding=(0, 2)))
    console.print()


def show_settings(placeholders: Placeholders, root: Path) -> None:
    settings = Table(show_header=False, box=None, padding=(0, 2))
    settings.add_column("Key", style=STYLE_DIM)
    settings.add_column("Value", style=STYLE_HIGHLIGHT)
    for placeholder in placeholders.values():
        settings.add_row(placeholder.caption, placeholder.value)
    settings.add_row("Root", str(root))
    console.print(settings)
    console.print()


def show_report(
    success: bool,
    placeholders: Placeholders,
    result: str,
    root: Path,
) -> None:
    console.print()
    if success:
        package = placeholders[PLACEHOLDER_PACKAGE].value
        alias = placeholders[PLACEHOLDER_ALIAS].value
        show_success(f"Generated connector [bold]tabsdata-{package}[/bold]")
        console.print()

        summary = Table(
            title="Summary",
            show_header=True,
            header_style="bold",
            box=None,
        )
        summary.add_column("Item", style=STYLE_DIM)
        summary.add_column("Path/Value", style=STYLE_INFO)
        summary.add_row("Connector", result)
        for placeholder in placeholders.values():
            summary.add_row(placeholder.caption, placeholder.value)
        summary.add_row(
            "pyproject.toml",
            f"Added test_tabsdata_{alias} to known-first-party",
        )
        summary.add_row("pytest.ini", f"Added {alias} marker")
        console.print(summary)
        console.print()

        show_info("Next steps:")
        console.print(
            f"  1. Run [cyan]cargo make lock_py[/cyan] in [cyan]{root}[/cyan]"
        )
        tabsdata_ee = root.parent / "tabsdata-ee"
        if tabsdata_ee.is_dir():
            console.print(
                f"  2. Run [cyan]cargo make lock_py[/cyan] in [cyan]"
                f"{tabsdata_ee}[/cyan]"
            )
            step = 3
        else:
            step = 2
        console.print(f"  {step}. Review the generated files in [cyan]{result}[/cyan]")
        console.print(
            f"  {step + 1}. Implement the connector logic in [cyan]"
            f"src/main/python/[/cyan]"
        )
        console.print(f"  {step + 2}. Add tests in [cyan]src/utest/python/[/cyan]")
        console.print()
    else:
        show_error(result)
        console.print()
        console.print(
            f"[{STYLE_DIM}]Run with --help for usage information.[/{STYLE_DIM}]"
        )
        sys.exit(1)


def show_success(message: str) -> None:
    console.print(f"[{STYLE_SUCCESS}]✓[/{STYLE_SUCCESS}] {message}")


def show_error(message: str) -> None:
    console.print(f"[{STYLE_ERROR}]✗ Error:[/{STYLE_ERROR}] {message}")


def show_info(message: str) -> None:
    console.print(f"[{STYLE_INFO}]ℹ[/{STYLE_INFO}] {message}")


def show_step(step: int, total: int, message: str) -> None:
    console.print(f"[{STYLE_DIM}][{step}/{total}][/{STYLE_DIM}] {message}")


def get_git_project_name(root: Path) -> str | None:
    # noinspection PyBroadException
    try:
        result = subprocess.run(
            [
                "git",
                "remote",
                "get-url",
                "origin",
            ],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            return None

        remote_url = result.stdout.strip()
        match = re.search(r"[/:]([^/]+)\.git$", remote_url)
        if match:
            return match.group(1)
        match = re.search(r"[/:]([^/]+)$", remote_url)
        if match:
            return match.group(1)
    except Exception:
        pass
    return None


def validate_project_root(path: Path) -> tuple[bool, str]:
    root_file = path / ".root"
    if not root_file.exists():
        return False, f"Missing [bold].root[/bold] file at [cyan]{path}[/cyan]"

    git_folder = path / ".git"
    if not git_folder.is_dir():
        return False, f"Missing [bold].git[/bold] folder at [cyan]{path}[/cyan]"

    project_name = get_git_project_name(path)
    if project_name is None:
        return False, "Could not determine project name from git remote"

    if project_name not in VALID_PROJECT_NAMES:
        valid_names = ", ".join(
            f"[cyan]{name}[/cyan]" for name in sorted(VALID_PROJECT_NAMES)
        )
        return (
            False,
            f"Invalid project: [bold]{project_name}[/bold]. "
            f"Must be one of: {valid_names}",
        )

    return True, ""


def validate_module_name(module: str) -> tuple[bool, str]:
    if not module:
        return False, "Module name cannot be empty"

    if not re.match(r"^[a-z][a-z0-9-]*$", module):
        return (
            False,
            f"Invalid module name [bold]{module}[/bold]. "
            "Must start with lowercase letter and contain only "
            "lowercase letters, digits, and hyphens.",
        )

    return True, ""


def replace_placeholders(text: str, placeholders: Placeholders) -> str:
    result = text
    for placeholder in placeholders.values():
        result = result.replace(placeholder.key, placeholder.value)
    return result


def replace_placeholders_in_path(name: str, placeholders: Placeholders) -> str:
    result = name
    for placeholder in placeholders.values():
        result = result.replace(
            placeholder.key,
            placeholder.path_value if placeholder.path_value else placeholder.value,
        )
    return result


def add_namespace_inits(parent: Path, new_path: Path) -> None:
    relative = new_path.relative_to(parent)
    intermediate_parts = relative.parts[:-1]
    if not intermediate_parts:
        return
    init_source = parent / "__init__.py"
    if not init_source.exists():
        return
    current = parent
    for part in intermediate_parts:
        current = current / part
        init_file = current / "__init__.py"
        if not init_file.exists():
            shutil.copy2(init_source, init_file)


def process_template(
    template_path: Path,
    target_path: Path,
    placeholders: Placeholders,
) -> None:
    shutil.copytree(template_path, target_path)

    for root, _, files in os.walk(target_path):
        top = Path(root)
        for filename in files:
            file_path = top / filename

            if is_binary_file(file_path):
                continue

            try:
                content = file_path.read_text(encoding="utf-8")
                new_content = replace_placeholders(content, placeholders)
                if content != new_content:
                    file_path.write_text(new_content, encoding="utf-8")
            except UnicodeDecodeError:
                continue

    placeholder_keys = set(placeholders.keys())
    paths_to_rename: list[Path] = []

    for root, folders, files in os.walk(target_path):
        top = Path(root)

        for folder in folders:
            if any(key in folder for key in placeholder_keys):
                paths_to_rename.append(top / folder)

        for file in files:
            if any(key in file for key in placeholder_keys):
                paths_to_rename.append(top / file)

    paths_to_rename.sort(key=lambda parts: len(parts.parts), reverse=True)

    for old_path in paths_to_rename:
        if old_path.exists():
            is_dir = old_path.is_dir()
            new_name = replace_placeholders_in_path(old_path.name, placeholders)
            new_path = old_path.parent / new_name
            if old_path != new_path:
                new_path.parent.mkdir(parents=True, exist_ok=True)
                old_path.rename(new_path)
                if is_dir and f"{os.sep}src{os.sep}main{os.sep}" in str(old_path):
                    add_namespace_inits(old_path.parent, new_path)


def is_binary_file(file: Path) -> bool:
    # noinspection PyBroadException
    try:
        with open(file, "rb") as f:
            chunk = f.read(8192)
            return b"\x00" in chunk
    except Exception:
        return True


def update_main_pyproject_toml(
    root: Path, placeholders: Placeholders
) -> tuple[bool, str]:
    alias = placeholders[PLACEHOLDER_ALIAS].value
    pyproject_toml_path = root / ROOT_PYPROJECT
    if not pyproject_toml_path.exists():
        return False, f"pyproject.toml not found at {pyproject_toml_path}"

    try:
        content = pyproject_toml_path.read_text(encoding="utf-8")
        new_entry = f"test_tabsdata_{alias}"

        if f'"{new_entry}"' in content:
            return True, f"Entry '{new_entry}' already exists in pyproject.toml"

        pattern = (
            r"(\[tool\.ruff\.lint\.isort\].*?known-first-party\s*=\s*\[)([^\]]*?)(\])"
        )
        match = re.search(pattern, content, re.DOTALL)

        if not match:
            return (
                False,
                "Could not find [tool.ruff.lint.isort] "
                "known-first-party in pyproject.toml",
            )

        prefix = match.group(1)
        entries_string = match.group(2)
        suffix = match.group(3)

        entries = re.findall(r'"([^"]+)"', entries_string)

        entries.append(new_entry)
        entries.sort()

        new_entries_string = "\n" + "\n".join(f'    "{e}",' for e in entries) + "\n"

        new_content = (
            content[: match.start()]
            + prefix
            + new_entries_string
            + suffix
            + content[match.end() :]
        )

        pyproject_toml_path.write_text(new_content, encoding="utf-8")
        return True, f"Added '{new_entry}' to pyproject.toml known-first-party"

    except Exception as e:
        return False, f"Failed to update pyproject.toml: {e}"


def update_main_pytest_ini(root: Path, placeholders: Placeholders) -> tuple[bool, str]:
    package = placeholders[PLACEHOLDER_PACKAGE].value
    alias = placeholders[PLACEHOLDER_ALIAS].value
    pytest_ini_path = root / PLATFORM_PYTEST_INI
    if not pytest_ini_path.exists():
        return False, f"pytest.ini not found at {pytest_ini_path}"

    try:
        content = pytest_ini_path.read_text(encoding="utf-8")
        new_marker = f"{alias}: mark tests that require the tabsdata-{package} package."

        marker_pattern = rf"^\s*{re.escape(alias)}:"
        if re.search(marker_pattern, content, re.MULTILINE):
            return True, f"Marker '{alias}' already exists in pytest.ini"

        lines = content.split("\n")
        markers_start = None
        marker_lines: list[tuple[str, int]] = []

        in_markers = False
        i = 0
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()

            if stripped == "markers =":
                markers_start = i
                in_markers = True
                i += 1
                continue

            if in_markers:
                if stripped and not stripped.startswith(("#", "@")) and ":" in stripped:
                    marker_name = stripped.split(":")[0].strip()
                    marker_lines.append((marker_name, i))
                elif (
                    stripped and not line.startswith(" ") and not line.startswith("\t")
                ):
                    break
                elif not stripped:
                    j = i + 1
                    while j < len(lines) and not lines[j].strip():
                        j += 1
                    if j < len(lines) and not (
                        lines[j].startswith(" ") or lines[j].startswith("\t")
                    ):
                        break
            i += 1

        if markers_start is None:
            return False, "Could not find 'markers =' section in pytest.ini"

        insert_after_index = markers_start
        for marker_name, line_index in marker_lines:
            if marker_name < alias:
                insert_after_index = line_index

        new_marker_line = f"    {new_marker}"
        lines.insert(insert_after_index + 1, new_marker_line)

        new_content = "\n".join(lines)
        pytest_ini_path.write_text(new_content, encoding="utf-8")
        return True, f"Added marker '{alias}' to pytest.ini"

    except Exception as e:
        return False, f"Failed to update pytest.ini: {e}"


def generate_connector(
    placeholders: Placeholders,
    root: Path,
    verbose: bool = False,
) -> tuple[bool, str]:
    total_steps = 5

    if verbose:
        show_step(1, total_steps, "Validating project root...")
    valid_root, error_message = validate_project_root(root)
    if not valid_root:
        return False, error_message

    package = placeholders[PLACEHOLDER_PACKAGE].value

    if verbose:
        show_step(2, total_steps, "Validating module name...")
    valid_module, error_message = validate_module_name(package)
    if not valid_module:
        return False, error_message

    template_folder = root / TEMPLATE_FOLDER
    if not template_folder.is_dir():
        return False, f"Template folder not found: [cyan]{template_folder}[/cyan]"

    connector_name = f"tabsdata-{package}"
    target_folder = root / CONNECTORS_FOLDER / connector_name
    if target_folder.exists():
        return (
            False,
            f"Connector [bold]{connector_name}[/bold] already exists at "
            f"[cyan]{target_folder}[/cyan]",
        )

    connectors_folder = root / CONNECTORS_FOLDER
    if not connectors_folder.exists():
        return False, f"Connectors folder not found: [cyan]{connectors_folder}[/cyan]"

    if verbose:
        show_step(3, total_steps, "Copying and processing template...")
    try:
        process_template(template_folder, target_folder, placeholders)
    except Exception as exception:
        if target_folder.exists():
            shutil.rmtree(target_folder)
        return False, f"Failed to generate connector: {exception}"

    if verbose:
        show_step(4, total_steps, "Updating pyproject.toml...")
    success, message = update_main_pyproject_toml(root, placeholders)
    if not success:
        return False, message

    if verbose:
        show_step(5, total_steps, "Updating pytest.ini...")
    success, message = update_main_pytest_ini(root, placeholders)
    if not success:
        return False, message

    return True, str(target_folder)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option(
    "-p",
    "--package",
    required=True,
    help="Package name (lowercase, e.g., 'mysql', 'postgres')",
)
@click.option(
    "-b",
    "--backend",
    required=True,
    help="Backend display name (e.g., 'MySQL', 'PostgreSQL')",
)
@click.option(
    "--root",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=None,
    help="Project root path (default: current working folder)",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Show detailed progress messages",
)
def main(package: str, backend: str, root: Path | None, verbose: bool) -> None:
    """
    Generate a new connector package from template.

    This tool creates a new connector package in connectors/python/ by copying
    and processing the template from templates/connector/. It replaces all
    placeholders with the provided module and backend names.

    \b
    Examples:
        tdx_gconnector -p mysql -b MySQL
        tdx_gconnector --package postgres --backend PostgreSQL
        tdx_gconnector -p oracle -b Oracle --verbose
    """
    current = Path(root).resolve() if root else Path.cwd()
    placeholders = build_placeholders(package, backend, current)
    show_header()
    show_settings(placeholders, current)
    success, location = generate_connector(
        placeholders=placeholders,
        root=current,
        verbose=verbose,
    )
    show_report(success, placeholders, location, current)


if __name__ == "__main__":
    main()
