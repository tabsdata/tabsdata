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

-------------------------------------------------------------------
TODO (MIGRATION GAP — DO NOT FORGET): triggered_on missing from request.yaml
-------------------------------------------------------------------
`request.yaml`'s `info` block does not yet carry a `triggered_on` (or
similarly-named) timestamp. The framework's `$td.ver.timestamp`
system column is therefore stamped with `EMPTY_TIMESTAMP` here. The
same gap also forces `_SrcContext.exec_plan_ts` in `run_source_plugin`
to raise `NotImplementedError`.

When the request schema is extended to carry the trigger timestamp:
  1. Add the field to `tabsdatak._tdrunner.function._lib.request.Info`
     (matching whatever name the API server emits).
  2. Replace `timestamp = EMPTY_TIMESTAMP` below with
     `timestamp = request.info.<that_field>`.
  3. Drop this TODO.
  4. Resolve the matching TODO inside `run_source_plugin._SrcContext.
     exec_plan_ts` at the same time.
-------------------------------------------------------------------
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

from tabsdatak._tdrunner.function._lib.cli import build_parser, run_command
from tabsdatak._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak._tdrunner.function._lib.inputs_manifest import (
    MANIFEST_FILES_KEY,
    MANIFEST_NAME_KEY,
    InvalidManifestError,
    load_inputs_manifest,
    write_inputs_manifest,
)
from tabsdatak._tdrunner.function._lib.liveness import Liveness
from tabsdatak._tdrunner.function._lib.request import (
    InvalidRequestError,
    load_request,
)
from tabsdatak._tdrunner.function._lib.system_columns import (
    EMPTY_TIMESTAMP,
    EMPTY_VERSION,
    stamp_system_columns,
)
from tabsdatak._tdrunner.function.commands import tabsdata_command

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


def _load_request_or_fail(path: Path):
    try:
        return load_request(path)
    except (FileNotFoundError, InvalidRequestError) as exc:
        raise CommandSpecificError(
            EXIT_BAD_REQUEST, f"unreadable request.yaml: {exc}"
        ) from exc


def _load_inputs_manifest_or_fail(path: Path) -> list[dict]:
    """Wrap the shared `load_inputs_manifest` to map its exceptions to
    this command's `EXIT_BAD_MANIFEST` code."""
    try:
        return load_inputs_manifest(path)
    except (FileNotFoundError, InvalidManifestError) as exc:
        raise CommandSpecificError(EXIT_BAD_MANIFEST, str(exc)) from exc


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


def _run() -> int:
    args = _build_parser().parse_args()
    request_path = Path(args.request)
    inputs_manifest = Path(args.inputs_manifest)
    enriched_dir = Path(args.enriched_dir)
    enriched_manifest = Path(args.enriched_inputs_manifest)
    liveness_path = Path(args.liveness)

    with Liveness(liveness_path):
        request = _load_request_or_fail(request_path)
        entries = _load_inputs_manifest_or_fail(inputs_manifest)

        execution = str(request.info.execution_id)
        transaction = str(request.info.transaction_id)
        # TODO: replace EMPTY_TIMESTAMP with `request.info.triggered_on`
        # (or whatever the field ends up being named) once it lands in
        # request.yaml's schema. See module docstring for the full
        # cleanup plan.
        timestamp = EMPTY_TIMESTAMP

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


@tabsdata_command(name="td_stamp_inputs")
def main() -> int:
    return run_command(_run)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
