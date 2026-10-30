#
# Copyright 2026 Tabsdata Inc.
#

"""stamp-inputs runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies pre-built ``StampInputsArguments``.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from pathlib import Path

import polars as pl

from tabsdatak.server._tdrunner.function._commons.document.request import Request
from tabsdatak.server._tdrunner.function._commons.manifests.input import (
    InputEntry,
    InputManifest,
    InputRecord,
)
from tabsdatak.server._tdrunner.function._commons.manifests.manifest import (
    ManifestError,
)
from tabsdatak.server._tdrunner.function._handlers.landing import (
    raw_version,
    stamp,
)
from tabsdatak.server._tdrunner.function.commands.stamp_inputs.error import (
    StampInputsErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.stamp_inputs.options import (
    StampInputsArguments,
)
from tabsdatak.spi import MetadataSpec
from tabsdatak.tableframe._engine.column.generator import IdxGenerator
from tabsdatak.tableframe._engine.model.frame import DataVersion

logger = logging.getLogger(__name__)


def _stamp_file(
    src_path: Path,
    dest_path: Path,
    *,
    version: DataVersion,
    idx: int,
    metadata: MetadataSpec | None = None,
) -> None:
    """Read a raw parquet, stamp framework system columns, write to dest."""
    try:
        lf = pl.scan_parquet(src_path)
    except Exception as exc:
        raise StampInputsErrorCode.SI_3.exception(
            cause=exc, path=src_path, detail=str(exc)
        )

    try:
        stamped = stamp(
            lf,
            idx=idx,
            version=version,
            metadata=metadata,
        )
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


def run(arguments: StampInputsArguments) -> int:
    request_path = arguments.request
    inputs_manifest = arguments.inputs_manifest
    enriched_folder = arguments.enriched_folder
    enriched_manifest = arguments.enriched_inputs_manifest

    logger.info("Stamping framework system columns onto inputs")
    request = Request.load(request_path, StampInputsErrorCode.SI_1)
    try:
        manifest = InputManifest.load(inputs_manifest)
    except ManifestError as error:
        raise StampInputsErrorCode.SI_2.exception(
            cause=error, path=inputs_manifest, detail=str(error)
        )
    logger.debug(
        "Loaded inputs manifest with %d slot(s): %s from %s",
        len(manifest.inputs),
        manifest.inputs,
        inputs_manifest,
    )

    version = raw_version(request.info)
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
    enriched_entries: list[InputEntry] = []
    total_stamped = 0
    for slot_idx, entry in enumerate(manifest.inputs):
        logger.debug(
            "Slot %d (%s): %d table record(s) to stamp: %s",
            slot_idx,
            entry.name,
            len(entry.tables),
            entry.tables,
        )

        slot_folder = enriched_folder / f"slot{slot_idx}"
        enriched_records: list[InputRecord] = []
        for record in entry.tables:
            if record.file is None:
                enriched_records.append(record)
                continue
            src_path = Path(record.file)
            dest_path = slot_folder / src_path.name
            assigned = idx()
            _stamp_file(
                src_path,
                dest_path,
                version=version,
                idx=assigned,
                metadata=record.metadata,
            )
            enriched_records.append(
                replace(
                    record,
                    file=str(dest_path.absolute()),
                    input_idx=assigned,
                )
            )
            total_stamped += 1

        enriched_entries.append(replace(entry, tables=tuple(enriched_records)))

    try:
        InputManifest(inputs=tuple(enriched_entries)).write(enriched_manifest)
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
