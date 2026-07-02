#
# Copyright 2026 Tabs Data Inc.
#

"""prep-localize-state command.

Step 4a of the function-runner chain. Reads request.yaml, builds the
transporter copy request that pulls input + state-in tables from cloud
storage to local disk, and writes it to ``--out``. Step 4b
(`run-transporter`) consumes it.

Also writes two manifests describing what was localized:

  * ``--inputs-manifest``: one entry per ``request.input`` table, with
    ``files`` pointing at ``<in_dir>/<name>.parquet``. The transformer
    path of `run-function` consumes this; the publisher path
    overwrites it with the source-plugin-produced manifest.
  * ``--state-manifest``: one entry per ``request.system_input``
    table, with ``files`` pointing at ``<state_dir>/<name>.parquet``.
    No consumer reads this in the current chain — it exists so future
    steps (manifest-driven prep-upload-state, source-plugin context
    loader, debugging) have a structured handle on framework state-in.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function._lib.inputs_manifest import (
    build_entry,
    build_table_record,
    write_inputs_manifest,
)
from tabsdatak.server._tdrunner.function._lib.request import (
    InputTableVersions,
    Request,
    input_tables,
    load_request_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.state_manifest import (
    MANIFEST_FILES_KEY,
    MANIFEST_NAME_KEY,
    write_state_manifest,
)
from tabsdatak.server._tdrunner.function._lib.transporter import (
    build_localize_state_request,
    localized_input_files,
    write_transporter_request,
)
from tabsdatak.server._tdrunner.function.commands.prep_localize_state.error import (
    PrepLocalizeStateErrorCode,
)

logger = logging.getLogger(__name__)

VERSION = "0.1.0"
INFO = (
    "prep-localize-state — generate the transporter request that pulls "
    "input + state-in tables to local, plus the inputs and state manifests"
)
MAN = (
    "prep-localize-state --request <PATH> --in-dir <PATH> "
    "--state-dir <PATH> --out <PATH> --inputs-manifest <PATH> "
    "--state-manifest <PATH> --liveness <PATH>\n\n"
    "Reads request.yaml, generates the transporter copy YAML for "
    "localising the input and system-input tables (writes to --out). "
    "Writes an inputs manifest at --inputs-manifest with one entry per "
    "request.input table (files at <in_dir>/<name>.parquet), and a "
    "state manifest at --state-manifest with one entry per "
    "request.system_input table (files at <state_dir>/<name>.parquet)."
)


def _build_parser():
    parser = build_parser(
        prog="prep-localize-state",
        description="Generate the localize-state transporter request.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--in-dir",
        required=True,
        dest="in_dir",
        help="Local destination dir for input tables",
    )
    parser.add_argument(
        "--state-dir",
        required=True,
        dest="state_dir",
        help="Local destination dir for state-in tables",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Path to write the transporter request YAML",
    )
    parser.add_argument(
        "--inputs-manifest",
        required=True,
        dest="inputs_manifest",
        help=(
            "Path to write the inputs manifest (one entry per "
            "request.input table, pointing at the localized parquets)"
        ),
    )
    parser.add_argument(
        "--state-manifest",
        required=True,
        dest="state_manifest",
        help=(
            "Path to write the state manifest (one entry per "
            "request.system_input table, pointing at the localized "
            "state-in parquets)"
        ),
    )
    parser.add_argument(
        "--storage-config",
        required=True,
        dest="storage_config",
        help="Path to the storage configuration file.",
    )
    return parser


def _build_inputs_manifest_entries(request: Request, in_dir: Path) -> list[dict]:
    """One manifest entry per `request.input` entry (positional argument).

    A plain table entry yields one table record; a multi-version
    dependency yields one record per member, in `version_pos` order. A
    member with `location is None` keeps its record with a null `file`,
    so downstream `_materialize_input_slot` preserves its position as a
    `None` value. An `is_empty` dependency yields no records.
    """
    entries: list[dict] = []
    for entry in request.input:
        is_empty = isinstance(entry, InputTableVersions) and entry.is_empty
        members = input_tables(entry)
        records: list[dict] = []
        if not is_empty:
            for member, path in localized_input_files(in_dir, entry):
                if member.location is None:
                    file = None
                else:
                    file = str(path.absolute())
                records.append(
                    build_table_record(
                        file=file,
                        input_idx=member.input_idx,
                        version_pos=member.version_pos,
                        execution_id=(
                            None
                            if member.execution_id is None
                            else str(member.execution_id)
                        ),
                        transaction_id=(
                            None
                            if member.transaction_id is None
                            else str(member.transaction_id)
                        ),
                        table_data_version_id=(
                            None
                            if member.table_data_version_id is None
                            else str(member.table_data_version_id)
                        ),
                        triggered_on=member.triggered_on,
                    )
                )
        entries.append(
            build_entry(
                name=members[0].name,
                table_pos=members[0].table_pos,
                tables=records,
                is_empty=is_empty,
            )
        )
    return entries


def _build_state_manifest_entries(request: Request, state_dir: Path) -> list[dict]:
    """One `{name, files}` entry per `request.system_input` table."""
    entries: list[dict] = []
    for table in request.system_input:
        if table.location is None:
            files: list[str] = []
        else:
            files = [str((state_dir / f"{table.name}.parquet").absolute())]
        entries.append({
            MANIFEST_NAME_KEY: table.name,
            MANIFEST_FILES_KEY: files,
        })
    return entries


@command(name="td_prep_localize_state", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    in_dir = Path(args.in_dir)
    state_dir = Path(args.state_dir)
    out_path = Path(args.out)
    inputs_manifest_path = Path(args.inputs_manifest)
    state_manifest_path = Path(args.state_manifest)
    storage_config = Path(args.storage_config)

    logger.info("Preparing localize-state transporter request and manifests")
    request = load_request_or_raise(request_path, PrepLocalizeStateErrorCode.PLS_1)
    logger.debug(
        "Request has %d input slot(s): %s; %d system_input slot(s): %s",
        len(request.input),
        request.input,
        len(request.system_input),
        request.system_input,
    )

    copy_req = build_localize_state_request(request, in_dir, state_dir, storage_config)
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

    inputs_entries = _build_inputs_manifest_entries(request, in_dir)
    try:
        write_inputs_manifest(inputs_entries, inputs_manifest_path)
    except OSError as exc:
        raise PrepLocalizeStateErrorCode.PLS_4.exception(
            cause=exc, path=inputs_manifest_path, detail=str(exc)
        )
    logger.debug(
        "Wrote inputs manifest with %d entr%s: %s -> %s",
        len(inputs_entries),
        "y" if len(inputs_entries) == 1 else "ies",
        inputs_entries,
        inputs_manifest_path,
    )

    state_entries = _build_state_manifest_entries(request, state_dir)
    try:
        write_state_manifest(state_entries, state_manifest_path)
    except OSError as exc:
        raise PrepLocalizeStateErrorCode.PLS_5.exception(
            cause=exc, path=state_manifest_path, detail=str(exc)
        )
    logger.debug(
        "Wrote state manifest with %d entr%s: %s -> %s",
        len(state_entries),
        "y" if len(state_entries) == 1 else "ies",
        state_entries,
        state_manifest_path,
    )
    logger.info("Localize-state preparation complete")
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
