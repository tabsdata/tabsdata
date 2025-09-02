#
#  Copyright 2025 Tabs Data Inc.
#

import ast
import importlib.util
import json
import os
import sys
from pathlib import Path
from types import ModuleType

from rich import box
from rich.console import Console
from rich.table import Table


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


sys.stdout.reconfigure(encoding="utf-8")


def safe_read_text(p: Path) -> str | None:
    # noinspection PyBroadException
    try:
        return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None


def count_defs(py_src: str) -> tuple[int, int]:
    # noinspection PyBroadException
    try:
        tree = ast.parse(py_src)
    except Exception:
        return 0, 0
    funcs = sum(
        isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) for n in ast.walk(tree)
    )
    classes = sum(isinstance(n, ast.ClassDef) for n in ast.walk(tree))
    return funcs, classes


def coverage_report(coverage_json: Path, project_root: Path):  # noqa: C901
    coverage_data = json.loads(coverage_json.read_text(encoding="utf-8"))

    totals = coverage_data.get("totals", {}) or {}
    files = coverage_data.get("files", {}) or {}
    covered_lines = int(totals.get("covered_lines", 0) or 0)
    missing_lines = int(totals.get("missing_lines", 0) or 0)
    excluded_lines = int(totals.get("excluded_lines", 0) or 0)
    num_statements = int(totals.get("num_statements", 0) or 0)
    percent_covered = totals.get("percent_covered")
    percent_covered_display = totals.get("percent_covered_display") or (
        f"{percent_covered:.2f}%"
        if isinstance(percent_covered, (int, float))
        else "N/A"
    )
    percent_covered_numeric: float | None = None
    if isinstance(percent_covered, (int, float)):
        percent_covered_numeric = round(float(percent_covered), 2)
    else:
        if isinstance(
            percent_covered_display, str
        ) and percent_covered_display.endswith("%"):
            # noinspection PyBroadException
            try:
                percent_covered_numeric = round(
                    float(percent_covered_display.rstrip("%")), 2
                )
            except Exception:
                percent_covered_numeric = None

    covered_lines_percent = (
        round(100.0 * covered_lines / num_statements, 2) if num_statements else None
    )
    excluded_lines_percent = (
        round(100.0 * excluded_lines / num_statements, 2) if num_statements else None
    )
    missing_lines_percent = (
        round(100.0 * missing_lines / num_statements, 2) if num_statements else None
    )

    num_branches = int(totals.get("num_branches", 0) or 0)
    covered_branches = int(totals.get("covered_branches", 0) or 0)
    missing_branches = int(totals.get("missing_branches", 0) or 0)
    covered_branches_percent_numeric = (
        round(100.0 * covered_branches / num_branches, 2) if num_branches else None
    )

    files_dictionary = coverage_data.get("files", {}) or {}
    files_statistics: list[tuple[str, int, int, float | None, int]] = []
    for path, info in files_dictionary.items():
        file_summary = info.get("summary") or {}
        file_num_statements = int(file_summary.get("num_statements", 0) or 0)
        file_missing_lines = int(file_summary.get("missing_lines", 0) or 0)
        file_covered_lines = int(
            file_summary.get(
                "covered_lines",
                (
                    (file_num_statements - file_missing_lines)
                    if file_num_statements
                    else 0
                ),
            )
            or 0
        )
        file_covered_lines_percent = (
            round(100.0 * file_covered_lines / file_num_statements, 2)
            if file_num_statements
            else None
        )
        files_statistics.append(
            (
                path,
                file_num_statements,
                file_missing_lines,
                file_covered_lines_percent,
                file_covered_lines,
            )
        )

    files_covered = sum(
        1
        for _, _, _, _, file_covered_lines in files_statistics
        if file_covered_lines > 0
    )
    files_covered_100 = sum(
        1
        for _, file_num_statements, file_missing_lines, *_ in files_statistics
        if file_num_statements > 0 and file_missing_lines == 0
    )
    files_covered_0 = sum(
        1
        for _, file_num_statements, file_missing_lines, *_ in files_statistics
        if file_num_statements > 0 and file_num_statements == file_missing_lines
    )

    file_covered_lines_percent_values = [
        file_covered_lines_percent
        for *_, file_covered_lines_percent, _ in files_statistics
        if file_covered_lines_percent is not None
    ]
    nonzero_file_covered_lines_percent = [
        value for value in file_covered_lines_percent_values if value > 0
    ]
    worst_file_covered_lines_percent = (
        min(nonzero_file_covered_lines_percent)
        if nonzero_file_covered_lines_percent
        else None
    )
    average_file_covered_lines_percent = (
        round(
            sum(file_covered_lines_percent_values)
            / len(file_covered_lines_percent_values),
            2,
        )
        if file_covered_lines_percent_values
        else None
    )

    files_keys = list(files.keys())
    total_files = len(files_keys)

    total_functions = 0
    total_classes = 0
    for key in files_keys:
        file_path = Path(key)
        if not file_path.is_absolute():
            file_path = (project_root / file_path).resolve()
        if file_path.suffix != ".py" or not file_path.exists():
            continue
        file_source = safe_read_text(file_path)
        if not file_source:
            continue
        functions, classes = count_defs(file_source)
        total_functions += functions
        total_classes += classes

    # noinspection PyBroadException
    try:
        console = Console()
        table = Table(title="Coverage Summary", box=box.ROUNDED, expand=False)
        table.add_column("Metric", style="cyan", no_wrap=True, justify="left")
        table.add_column("Value", style="white", justify="right")

        table.add_section()
        table.add_row("Total files", str(total_files))
        table.add_row("Total classes", str(total_classes))
        table.add_row("Total functions", str(total_functions))

        table.add_section()
        table.add_row("Files with coverage", str(files_covered))
        table.add_row("Files with 0% coverage", str(files_covered_0))
        table.add_row("Files with 100% coverage", str(files_covered_100))

        if worst_file_covered_lines_percent is not None:
            table.add_row(
                "Lowest file coverage (%)", f"{worst_file_covered_lines_percent:.2f}"
            )
        if average_file_covered_lines_percent is not None:
            table.add_row(
                "Average file coverage (%)", f"{average_file_covered_lines_percent:.2f}"
            )

        table.add_section()
        table.add_row("Lines executed", str(covered_lines))
        table.add_row("Lines excluded", str(excluded_lines))
        table.add_row("Lines not executed", str(missing_lines))
        table.add_row("Total Lines", str(num_statements))

        table.add_row(
            "Lines executed (%)",
            "N/A" if covered_lines_percent is None else f"{covered_lines_percent:.2f}",
        )
        table.add_row(
            "Lines excluded (%)",
            (
                "N/A"
                if excluded_lines_percent is None
                else f"{excluded_lines_percent:.2f}"
            ),
        )
        table.add_row(
            "Lines not executed (%)",
            "N/A" if missing_lines_percent is None else f"{missing_lines_percent:.2f}",
        )

        table.add_section()
        table.add_row("Branches executed", str(covered_branches))
        table.add_row("Branches not executed", str(missing_branches))
        table.add_row("Total branches", str(num_branches))

        if covered_branches_percent_numeric is not None:
            bstyle = (
                "bold green"
                if covered_branches_percent_numeric >= 90
                else (
                    "yellow" if covered_branches_percent_numeric >= 80 else "bold red"
                )
            )
            table.add_row(
                "Branches executed (%)",
                f"[{bstyle}]{covered_branches_percent_numeric:.2f}[/{bstyle}]",
            )
        else:
            table.add_row("Branches executed (%)", "N/A")

        table.add_section()
        style = (
            "bold green"
            if (
                isinstance(percent_covered_numeric, (int, float))
                and percent_covered_numeric >= 90
            )
            else (
                "yellow"
                if (
                    isinstance(percent_covered_numeric, (int, float))
                    and percent_covered_numeric >= 80
                )
                else "bold red"
            )
        )
        coverage_cell = (
            "N/A" if percent_covered_numeric is None else f"{percent_covered_numeric}"
        )
        table.add_row(
            "Overall lines coverage",
            f"[{style}]{coverage_cell}[/{style}]",
        )

        console.print("")
        console.print(table)
        console.print("")

    except Exception:
        logger.error("Error generating the coverage summary", file=sys.stderr)
        sys.exit(1)


def main():
    if len(sys.argv) < 2:
        logger.error(
            "Usage: report_coverage.py <coverage json> <project_root>", file=sys.stderr
        )
        sys.exit(1)

    coverage_json = Path(sys.argv[1])
    if not coverage_json.exists():
        logger.error(f"Coverage json file not found: {coverage_json}")
        sys.exit(1)

    project_root = Path(sys.argv[2]) if len(sys.argv) > 2 else Path.cwd()

    coverage_report(coverage_json, project_root)


if __name__ == "__main__":
    main()
