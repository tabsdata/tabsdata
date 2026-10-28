#
# Copyright 2026 Tabsdata Inc.
#

"""prep-localize-state runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies a pre-built ``Namespace``.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from tabsdatak.server._tdrunner.function._commons.document.request import (
    InputTableVersions,
    Request,
)
from tabsdatak.server._tdrunner.function._commons.manifests.input import (
    InputEntry,
    InputManifest,
    InputRecord,
)
from tabsdatak.server._tdrunner.function.commands.prep_localize_state.error import (
    PrepLocalizeStateErrorCode,
)

logger = logging.getLogger(__name__)


def build_inputs_manifest(request: Request) -> InputManifest:
    entries: list[InputEntry] = []
    for entry in request.input:
        is_empty = isinstance(entry, InputTableVersions) and entry.is_empty
        records: list[InputRecord] = []
        if not is_empty:
            for member in entry.tables:
                records.append(
                    InputRecord(
                        location=member.location,
                        input_idx=member.input_idx,
                        version_pos=member.version_pos,
                        execution_id=as_text(member.execution_id),
                        transaction_id=as_text(member.transaction_id),
                        physical_table_data_version_id=as_text(
                            member.physical_table_data_version_id
                        ),
                        triggered_on=member.triggered_on,
                    )
                )
        entries.append(
            InputEntry(
                name=entry.tables[0].table,
                table_pos=entry.tables[0].table_pos,
                is_empty=is_empty,
                tables=tuple(records),
            )
        )
    return InputManifest(inputs=tuple(entries))


def as_text(value: str | int | None) -> str | None:
    return None if value is None else str(value)


def run(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    inputs_manifest_path = Path(args.inputs_manifest)

    logger.info("Preparing the inputs manifest")
    request = Request.load(request_path, PrepLocalizeStateErrorCode.PLS_1)
    logger.debug(
        "Request has %d input slot(s): %s",
        len(request.input),
        request.input,
    )

    inputs_manifest = build_inputs_manifest(request)
    try:
        inputs_manifest.write(inputs_manifest_path)
    except OSError as exc:
        raise PrepLocalizeStateErrorCode.PLS_4.exception(
            cause=exc, path=inputs_manifest_path, detail=str(exc)
        )
    logger.debug(
        "Wrote inputs manifest with %d entr%s: %s -> %s",
        len(inputs_manifest.inputs),
        "y" if len(inputs_manifest.inputs) == 1 else "ies",
        inputs_manifest.inputs,
        inputs_manifest_path,
    )
    logger.info("Inputs manifest preparation complete")
    return 0
