#
# Copyright 2026 Tabsdata Inc.
#

"""stamp-inputs runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies a pre-built ``Namespace``.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import polars as pl

from tabsdatak.server._tdrunner.function._lib.inputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_INPUT_IDX_KEY,
    MANIFEST_METADATA_KEY,
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


def run(args: argparse.Namespace) -> int:
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
            # TODO Dimas, a dict with @td. columns coming from source plugin
            _metadata = record.get(MANIFEST_METADATA_KEY)
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
