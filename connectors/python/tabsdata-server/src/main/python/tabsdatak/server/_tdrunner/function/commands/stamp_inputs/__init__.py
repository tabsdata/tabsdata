#
# Copyright 2026 Tabs Data Inc.
#

"""stamp-inputs command.

Sits between `run-source-plugin` and `run-function` in the publisher
chain. Reads the raw parquet files produced by `run-source-plugin`,
stamps each one with the framework's system columns (per-row UUID
identifier plus execution/transaction/version/timestamp metadata
broadcast from the request envelope), and writes the enriched
parquets under `--enriched-dir`. Emits an updated inputs-manifest at
`--enriched-inputs-manifest` for `run-function` to consume.

Legacy counterpart: `tabsdata._tabsserver.function.execution_utils.
store_source_raw_data` — the legacy `_tabsserver` ran this in-process
as part of the function-execution wrapper. We extract it into its own
runner step so the data-shape boundary between "plugin output" and
"framework-managed input to user function" is explicit.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

from tabsdatak.server._tdrunner.function._lib.cli import build_parser, command
from tabsdatak.server._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak.server._tdrunner.function._lib.inputs_manifest import (
    MANIFEST_FILES_KEY,
    MANIFEST_NAME_KEY,
    load_inputs_manifest_or_fail,
    write_inputs_manifest,
)
from tabsdatak.server._tdrunner.function._lib.request import load_request_or_fail
from tabsdatak.server._tdrunner.function._lib.system_columns import (
    EMPTY_VERSION,
    stamp_system_columns,
)

# TODO: After all commands are done, version will be unified in a single
#  source of truth file.
VERSION = "0.1.0"
INFO = (
    "stamp-inputs — stamp framework system columns onto the parquets "
    "produced by run-source-plugin"
)
MAN = (
    "stamp-inputs --request <PATH> --inputs-manifest <PATH> "
    "--enriched-dir <PATH> --enriched-inputs-manifest <PATH> "
    "--liveness <PATH>\n\n"
    "Reads the raw inputs manifest, stamps each parquet with the "
    "framework's system columns ($td.id per row plus $td.ver.execution"
    " / .transaction / .version / .timestamp broadcast across rows), "
    "writes enriched parquets under --enriched-dir, and emits an "
    "updated inputs manifest at --enriched-inputs-manifest for "
    "run-function to consume."
)

# Command-specific exit codes (>= 10 per docs/01-CommandBasics.md).
# TODO: Pending error code rework.
EXIT_BAD_REQUEST = 10
EXIT_BAD_MANIFEST = 11
EXIT_BAD_INPUT_PARQUET = 12
EXIT_STAMP_FAILED = 13
EXIT_MANIFEST_WRITE_FAILED = 14


def _build_parser():
    parser = build_parser(
        prog="stamp-inputs",
        description=(
            "Stamp framework system columns onto run-source-plugin's parquets."
        ),
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--inputs-manifest",
        required=True,
        help="Path to the YAML inputs manifest written by run-source-plugin",
    )
    parser.add_argument(
        "--enriched-dir",
        required=True,
        help="Directory where stamped parquets are written",
    )
    parser.add_argument(
        "--enriched-inputs-manifest",
        required=True,
        help=(
            "Path where the enriched inputs manifest is written "
            "(consumed by run-function)"
        ),
    )
    return parser


def _stamp_file(
    src_path: Path,
    dest_path: Path,
    *,
    execution: str,
    transaction: str,
    version: str,
    timestamp,
) -> None:
    """Read a raw parquet, stamp framework system columns, write to dest."""
    try:
        df = pl.read_parquet(src_path)
    except Exception as exc:
        raise CommandSpecificError(
            EXIT_BAD_INPUT_PARQUET,
            f"failed to read input parquet {src_path}: {exc}",
        ) from exc

    try:
        stamped = stamp_system_columns(
            df,
            execution=execution,
            transaction=transaction,
            version=version,
            timestamp=timestamp,
        )
    except Exception as exc:
        raise CommandSpecificError(
            EXIT_STAMP_FAILED,
            f"failed to stamp system columns on {src_path}: {exc}",
        ) from exc

    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        stamped.write_parquet(dest_path)
    except OSError as exc:
        raise CommandSpecificError(
            EXIT_STAMP_FAILED,
            f"failed to write enriched parquet {dest_path}: {exc}",
        ) from exc


@command(name="td_stamp_inputs", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    inputs_manifest = Path(args.inputs_manifest)
    enriched_dir = Path(args.enriched_dir)
    enriched_manifest = Path(args.enriched_inputs_manifest)

    request = load_request_or_fail(request_path, EXIT_BAD_REQUEST)
    entries = load_inputs_manifest_or_fail(inputs_manifest, EXIT_BAD_MANIFEST)

    execution = str(request.info.execution_id)
    transaction = str(request.info.transaction_id)
    timestamp = request.info.triggered_on_datetime

    enriched_entries: list[dict] = []
    for slot_idx, entry in enumerate(entries):
        name = entry.get(MANIFEST_NAME_KEY, f"slot{slot_idx}")
        files = entry.get(MANIFEST_FILES_KEY) or []

        slot_dir = enriched_dir / f"slot{slot_idx}"
        enriched_files: list[str] = []
        for src in files:
            src_path = Path(src)
            dest_path = slot_dir / src_path.name
            _stamp_file(
                src_path,
                dest_path,
                execution=execution,
                transaction=transaction,
                version=EMPTY_VERSION,
                timestamp=timestamp,
            )
            enriched_files.append(str(dest_path.absolute()))

        enriched_entries.append({
            MANIFEST_NAME_KEY: name,
            MANIFEST_FILES_KEY: enriched_files,
        })

    try:
        write_inputs_manifest(enriched_entries, enriched_manifest)
    except OSError as exc:
        raise CommandSpecificError(
            EXIT_MANIFEST_WRITE_FAILED,
            f"failed to write enriched inputs manifest {enriched_manifest}: {exc}",
        ) from exc
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
