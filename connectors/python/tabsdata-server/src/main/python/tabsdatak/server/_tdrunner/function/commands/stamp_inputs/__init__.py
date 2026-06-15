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
import logging
import sys
from pathlib import Path

import polars as pl

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function._lib.inputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_INPUT_IDX_KEY,
    MANIFEST_NAME_KEY,
    MANIFEST_TABLES_KEY,
    load_inputs_manifest_or_raise,
    write_inputs_manifest,
)
from tabsdatak.server._tdrunner.function._lib.request import load_request_or_raise
from tabsdatak.server._tdrunner.function.commands.stamp_inputs.error import (
    StampInputsErrorCode,
)
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._engine.column.generator import IdxGenerator
from tabsdatak.tableframe._engine.constant.emptiness import EMPTY_VERSION
from tabsdatak.tableframe._engine.model.frame import DataVersion
from tabsdatak.tableframe._engine.taxonomy.frame import CreationMode

logger = logging.getLogger(__name__)

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
    version: DataVersion,
    idx: int,
) -> None:
    """Read a raw parquet, stamp framework system columns, write to dest."""
    try:
        lf = pl.scan_parquet(src_path)
    except Exception as exc:
        raise StampInputsErrorCode.SI_3.exception(
            cause=exc, path=src_path, detail=str(exc)
        )

    try:
        stamped = TableFrame._build(
            lf, CreationMode.RAW, idx=idx, version=version
        ).to_polars_lf(drop_system_columns=False)
    except Exception as exc:
        raise StampInputsErrorCode.SI_4.exception(
            cause=exc, path=src_path, detail=str(exc)
        )

    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        stamped.sink_parquet(dest_path)
    except OSError as exc:
        raise StampInputsErrorCode.SI_5.exception(
            cause=exc, path=dest_path, detail=str(exc)
        )


@command(name="td_stamp_inputs", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    inputs_manifest = Path(args.inputs_manifest)
    enriched_dir = Path(args.enriched_dir)
    enriched_manifest = Path(args.enriched_inputs_manifest)

    logger.info("Stamping framework system columns onto inputs")
    request = load_request_or_raise(request_path, StampInputsErrorCode.SI_1)
    entries = load_inputs_manifest_or_raise(inputs_manifest, StampInputsErrorCode.SI_2)
    logger.debug(
        "Loaded inputs manifest with %d slot(s): %s from %s",
        len(entries),
        entries,
        inputs_manifest,
    )

    version = (
        DataVersion
        .builder()
        .with_execution(str(request.info.execution_id))
        .with_transaction(str(request.info.transaction_id))
        .with_version(EMPTY_VERSION)
        .with_timestamp(request.info.triggered_on_datetime)
        .build()
    )
    logger.debug(
        "Stamping with execution=%s transaction=%s timestamp=%s",
        version.execution,
        version.transaction,
        version.timestamp,
    )

    # One IdxGenerator shared across every stamped file so idx assignment is
    # globally unique across all slots, matching legacy store_sources_raw_data.
    # The assigned idx is recorded on each table record so downstream steps
    # read it from the manifest instead of regenerating it.
    idx = IdxGenerator()
    enriched_entries: list[dict] = []
    total_stamped = 0
    for slot_idx, entry in enumerate(entries):
        name = entry.get(MANIFEST_NAME_KEY, f"slot{slot_idx}")
        records = entry.get(MANIFEST_TABLES_KEY) or []
        logger.debug(
            "Slot %d (%s): %d table record(s) to stamp: %s",
            slot_idx,
            name,
            len(records),
            records,
        )

        slot_dir = enriched_dir / f"slot{slot_idx}"
        enriched_records: list[dict] = []
        for record in records:
            src = record.get(MANIFEST_FILE_KEY)
            if src is None:
                enriched_records.append(record)
                continue
            src_path = Path(src)
            dest_path = slot_dir / src_path.name
            assigned = idx()
            _stamp_file(src_path, dest_path, version=version, idx=assigned)
            enriched_records.append({
                **record,
                MANIFEST_FILE_KEY: str(dest_path.absolute()),
                MANIFEST_INPUT_IDX_KEY: assigned,
            })
            total_stamped += 1

        enriched_entries.append({
            **entry,
            MANIFEST_TABLES_KEY: enriched_records,
        })

    try:
        write_inputs_manifest(enriched_entries, enriched_manifest)
    except OSError as exc:
        raise StampInputsErrorCode.SI_6.exception(
            cause=exc, path=enriched_manifest, detail=str(exc)
        )
    logger.info(
        "Stamped %d input parquet(s) across %d slot(s); enriched manifest at %s",
        total_stamped,
        len(enriched_entries),
        enriched_manifest,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
