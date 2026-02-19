#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import rich_click as click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

PLACEHOLDER_MODULE = "{c.module}"
PLACEHOLDER_BACKEND = "{c.backend}"
PLACEHOLDER_YEAR = "{c.year}"

VALID_PROJECT_NAMES = {"tabsdata", "tabsdata-os"}

TEMPLATE_FOLDER = "templates/connector/tabsdata-{c.module}"

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


def show_header() -> None:
    header = Text()
    header.append("Tabsdata ", style="bold cyan")
    header.append("Connector Generator", style="bold white")
    console.print(Panel(header, border_style="cyan", padding=(0, 2)))
    console.print()


def show_settings(module: str, backend: str, root: Path | None) -> None:
    settings = Table(show_header=False, box=None, padding=(0, 2))
    settings.add_column("Key", style=STYLE_DIM)
    settings.add_column("Value", style=STYLE_HIGHLIGHT)
    settings.add_row("Module", module)
    settings.add_row("Backend", backend)
    settings.add_row("Root", str(root or Path.cwd()))
    console.print(settings)
    console.print()


def show_report(success: bool, module: str, result: str, root: Path) -> None:
    console.print()
    if success:
        show_success(f"Generated connector [bold]tabsdata-{module}[/bold]")
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
        summary.add_row(
            "pyproject.toml",
            f"Added test_tabsdata_{module} to known-first-party",
        )
        summary.add_row("pytest.ini", f"Added {module} marker")
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

    if not re.match(r"^[a-z][a-z0-9_]*$", module):
        return (
            False,
            f"Invalid module name [bold]{module}[/bold]. "
            "Must start with lowercase letter and contain only "
            "lowercase letters, digits, and underscores.",
        )

    return True, ""


def replace_placeholders_in_content(
    text: str,
    module: str,
    backend: str,
    year: str,
) -> str:
    result = text
    result = result.replace(PLACEHOLDER_MODULE, module)
    result = result.replace(PLACEHOLDER_BACKEND, backend)
    result = result.replace(PLACEHOLDER_YEAR, year)
    return result


def replace_placeholders_in_name(
    name: str,
    module: str,
    backend: str,
    year: str,
) -> str:
    return replace_placeholders_in_content(name, module, backend, year)


def process_template(
    template_path: Path,
    target_path: Path,
    module: str,
    backend: str,
    year: str,
) -> None:
    shutil.copytree(template_path, target_path)

    paths_to_rename: list[Path] = []

    for root, folders, files in os.walk(target_path):
        top = Path(root)

        for folder in folders:
            if any(
                placeholder in folder
                for placeholder in [
                    PLACEHOLDER_MODULE,
                    PLACEHOLDER_BACKEND,
                    PLACEHOLDER_YEAR,
                ]
            ):
                paths_to_rename.append(top / folder)

        for file in files:
            if any(
                placeholder in file
                for placeholder in [
                    PLACEHOLDER_MODULE,
                    PLACEHOLDER_BACKEND,
                    PLACEHOLDER_YEAR,
                ]
            ):
                paths_to_rename.append(top / file)

    paths_to_rename.sort(key=lambda parts: len(parts.parts), reverse=True)

    for old_path in paths_to_rename:
        if old_path.exists():
            new_name = replace_placeholders_in_name(
                old_path.name,
                module,
                backend,
                year,
            )
            new_path = old_path.parent / new_name
            if old_path != new_path:
                old_path.rename(new_path)

    for root, _, files in os.walk(target_path):
        top = Path(root)
        for filename in files:
            file_path = top / filename

            if is_binary_file(file_path):
                continue

            try:
                content = file_path.read_text(encoding="utf-8")
                new_content = replace_placeholders_in_content(
                    content,
                    module,
                    backend,
                    year,
                )
                if content != new_content:
                    file_path.write_text(new_content, encoding="utf-8")
            except UnicodeDecodeError:
                continue


def is_binary_file(file: Path) -> bool:
    # noinspection PyBroadException
    try:
        with open(file, "rb") as f:
            chunk = f.read(8192)
            return b"\x00" in chunk
    except Exception:
        return True


def update_main_pyproject_toml(root: Path, module: str) -> tuple[bool, str]:
    pyproject_toml_path = root / ROOT_PYPROJECT
    if not pyproject_toml_path.exists():
        return False, f"pyproject.toml not found at {pyproject_toml_path}"

    try:
        content = pyproject_toml_path.read_text(encoding="utf-8")
        new_entry = f"test_tabsdata_{module}"

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


def update_main_pytest_ini(root: Path, module: str) -> tuple[bool, str]:
    pytest_ini_path = root / PLATFORM_PYTEST_INI
    if not pytest_ini_path.exists():
        return False, f"pytest.ini not found at {pytest_ini_path}"

    try:
        content = pytest_ini_path.read_text(encoding="utf-8")
        new_marker = f"{module}: mark tests that require the tabsdata-{module} package."

        marker_pattern = rf"^\s*{re.escape(module)}:"
        if re.search(marker_pattern, content, re.MULTILINE):
            return True, f"Marker '{module}' already exists in pytest.ini"

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
            if marker_name < module:
                insert_after_index = line_index

        new_marker_line = f"    {new_marker}"
        lines.insert(insert_after_index + 1, new_marker_line)

        new_content = "\n".join(lines)
        pytest_ini_path.write_text(new_content, encoding="utf-8")
        return True, f"Added marker '{module}' to pytest.ini"

    except Exception as e:
        return False, f"Failed to update pytest.ini: {e}"


def generate_connector(
    module: str,
    backend: str,
    root: Path | None = None,
    verbose: bool = False,
) -> tuple[bool, str]:
    if root is None:
        top = Path.cwd()
    else:
        top = Path(root).resolve()

    total_steps = 5

    if verbose:
        show_step(1, total_steps, "Validating project root...")
    valid_root, error_message = validate_project_root(top)
    if not valid_root:
        return False, error_message

    if verbose:
        show_step(2, total_steps, "Validating module name...")
    valid_module, error_message = validate_module_name(module)
    if not valid_module:
        return False, error_message

    template_folder = top / TEMPLATE_FOLDER
    if not template_folder.is_dir():
        return False, f"Template folder not found: [cyan]{template_folder}[/cyan]"

    connector_name = f"tabsdata-{module}"
    target_folder = top / CONNECTORS_FOLDER / connector_name
    if target_folder.exists():
        return (
            False,
            f"Connector [bold]{connector_name}[/bold] already exists at "
            f"[cyan]{target_folder}[/cyan]",
        )

    connectors_folder = top / CONNECTORS_FOLDER
    if not connectors_folder.exists():
        return False, f"Connectors folder not found: [cyan]{connectors_folder}[/cyan]"

    year = str(datetime.now().year)

    if verbose:
        show_step(3, total_steps, "Copying and processing template...")
    try:
        process_template(template_folder, target_folder, module, backend, year)
    except Exception as exception:
        if target_folder.exists():
            shutil.rmtree(target_folder)
        return False, f"Failed to generate connector: {exception}"

    if verbose:
        show_step(4, total_steps, "Updating pyproject.toml...")
    success, message = update_main_pyproject_toml(top, module)
    if not success:
        return False, message

    if verbose:
        show_step(5, total_steps, "Updating pytest.ini...")
    success, message = update_main_pytest_ini(top, module)
    if not success:
        return False, message

    return True, str(target_folder)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option(
    "-m",
    "--module",
    required=True,
    help="Module name (lowercase, e.g., 'mysql', 'postgres')",
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
def main(module: str, backend: str, root: Path | None, verbose: bool) -> None:
    """
    Generate a new connector package from template.

    This tool creates a new connector package in connectors/python/ by copying
    and processing the template from templates/connector/. It replaces all
    placeholders with the provided module and backend names.

    \b
    Examples:
        tdx_gconnector -m mysql -b MySQL
        tdx_gconnector --module postgres --backend PostgreSQL
        tdx_gconnector -m oracle -b Oracle --verbose
    """
    current = Path(root).resolve() if root else Path.cwd()
    show_header()
    show_settings(module, backend, current)
    success, location = generate_connector(
        module=module,
        backend=backend,
        root=current,
        verbose=verbose,
    )
    show_report(success, module, location, current)


if __name__ == "__main__":
    main()
