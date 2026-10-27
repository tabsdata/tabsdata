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
from tabsdatak.server._tdrunner.function._commons.manifests.state import (
    StateEntry,
    StateManifest,
)
from tabsdatak.server._tdrunner.function._lib.transporter import (
    build_localize_state_request,
    write_transporter_request,
)
from tabsdatak.server._tdrunner.function.commands.prep_localize_state.error import (
    PrepLocalizeStateErrorCode,
)

logger = logging.getLogger(__name__)


def _build_inputs_manifest(request: Request) -> InputManifest:
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
                        execution_id=_text(member.execution_id),
                        transaction_id=_text(member.transaction_id),
                        physical_table_data_version_id=_text(
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


def _build_state_manifest(request: Request, state_dir: Path) -> StateManifest:
    return StateManifest(
        state=tuple(
            StateEntry(
                name=table.table,
                files=()
                if table.location is None
                else (str((state_dir / f"{table.table}.parquet").absolute()),),
            )
            for table in request.system_input
        )
    )


def _text(value: str | int | None) -> str | None:
    return None if value is None else str(value)


def run(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    state_dir = Path(args.state_dir)
    out_path = Path(args.out)
    inputs_manifest_path = Path(args.inputs_manifest)
    state_manifest_path = Path(args.state_manifest)
    storage_config = Path(args.storage_config)

    logger.info("Preparing localize-state transporter request and manifests")
    request = Request.load(request_path, PrepLocalizeStateErrorCode.PLS_1)
    logger.debug(
        "Request has %d input slot(s): %s; %d system_input slot(s): %s",
        len(request.input),
        request.input,
        len(request.system_input),
        request.system_input,
    )

    copy_req = build_localize_state_request(request, state_dir, storage_config)
    logger.debug(
        "Localize-state copy request has %d pair(s): %s; writing to %s",
        len(copy_req.pairs),
        copy_req.pairs,
        out_path,
    )

    try:
        write_transporter_request(copy_req, out_path)
    except OSError as exc:
        raise PrepLocalizeStateErrorCode.PLS_3.exception(
            cause=exc, path=out_path, detail=str(exc)
        )

    inputs_manifest = _build_inputs_manifest(request)
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

    state_manifest = _build_state_manifest(request, state_dir)
    try:
        state_manifest.write(state_manifest_path)
    except OSError as exc:
        raise PrepLocalizeStateErrorCode.PLS_5.exception(
            cause=exc, path=state_manifest_path, detail=str(exc)
        )
    logger.debug(
        "Wrote state manifest with %d entr%s: %s -> %s",
        len(state_manifest.state),
        "y" if len(state_manifest.state) == 1 else "ies",
        state_manifest.state,
        state_manifest_path,
    )
    logger.info("Localize-state preparation complete")
    return 0
