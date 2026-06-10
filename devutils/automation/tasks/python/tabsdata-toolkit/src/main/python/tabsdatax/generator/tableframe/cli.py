#
# Copyright 2026 Tabs Data Inc.
#

"""Command-line interface for the Polars delegation generator (``tdx_gtableframe``).

A rich-click app with three subcommands:

* ``manifest`` --root-module M [--out FILE]
* ``compare``  --old-manifest A.json --new-manifest B.json [--out FILE]
* ``generate`` --root-module M --root-dir D --manifest FILE [--testdir DIR]

All subcommands print a styled summary; ``manifest`` / ``compare`` also write
JSON.
"""

from __future__ import annotations

from pathlib import Path

import rich_click as click
from rich.console import Console

console = Console()

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT_SETTINGS)
def main() -> None:
    """Generate a delegating wrapper API over the Polars API."""


@main.command()
@click.option(
    "--root-module",
    required=True,
    help="root module for the generated code",
)
@click.option(
    "--out",
    type=click.Path(dir_okay=False, path_type=Path),
    help="write the JSON manifest here (default: stdout)",
)
def manifest(root_module: str, out: Path | None) -> None:
    """Emit the JSON signature manifest (the whitelist)."""
    from .scan.report import build_report
    from .styling import render_report

    report, skipped = build_report(root_module)
    render_report(report, console, skipped=skipped)
    if out:
        out.write_text(report.to_json())
        console.print(f"[green]wrote[/] {out}")
    else:
        print(report.to_json())


@main.command()
@click.option(
    "--old-manifest",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="the earlier manifest JSON",
)
@click.option(
    "--new-manifest",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="the later manifest JSON",
)
@click.option(
    "--out",
    type=click.Path(dir_okay=False, path_type=Path),
    help="write the JSON diff here (default: stdout)",
)
def compare(old_manifest: Path, new_manifest: Path, out: Path | None) -> None:
    """Diff two signature manifests."""
    from .model.compare import compute_diff
    from .model.model import Report
    from .styling import render_diff

    old = Report.from_json(old_manifest.read_text())
    new = Report.from_json(new_manifest.read_text())
    diff = compute_diff(old, new)
    render_diff(diff, console)
    if out:
        out.write_text(diff.to_json())
        console.print(f"[green]wrote[/] {out}")


@main.command()
@click.option(
    "--root-module",
    required=True,
    help="root module for the generated code",
)
@click.option(
    "--root-dir",
    required=True,
    type=click.Path(file_okay=False, path_type=Path),
    help="root dir to generate the code into",
)
@click.option(
    "--manifest",
    "manifest_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="the signature manifest JSON to generate from",
)
@click.option(
    "--testdir",
    type=click.Path(file_okay=False, path_type=Path),
    help="also generate delegation tests here (overwrites same-named files)",
)
def generate(
    root_module: str,
    root_dir: Path,
    manifest_path: Path,
    testdir: Path | None,
) -> None:
    """Generate .py delegation and .pyi stubs."""
    from .emit.generate import generate as run_generate
    from .model.model import Report
    from .styling import render_generate

    report = Report.from_json(manifest_path.read_text())
    result = run_generate(
        report,
        root_module,
        str(root_dir),
        testdir=str(testdir) if testdir else None,
    )
    render_generate(result, console)


if __name__ == "__main__":
    main()
