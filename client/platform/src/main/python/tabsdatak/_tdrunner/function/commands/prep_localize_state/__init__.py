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

from pathlib import Path

from tabsdatak._tdrunner.function._lib.cli import build_parser, run_command
from tabsdatak._tdrunner.function._lib.endpoint import UnsupportedSchemeError
from tabsdatak._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak._tdrunner.function._lib.inputs_manifest import (
    MANIFEST_FILES_KEY,
    MANIFEST_NAME_KEY,
    write_inputs_manifest,
)
from tabsdatak._tdrunner.function._lib.liveness import Liveness
from tabsdatak._tdrunner.function._lib.request import (
    InvalidRequestError,
    Request,
    load_request,
)
from tabsdatak._tdrunner.function._lib.state_manifest import (
    write_state_manifest,
)
from tabsdatak._tdrunner.function._lib.transporter_request import (
    build_localize_state_request,
    write_transporter_request,
)
from tabsdatak._tdrunner.function.commands import tabsdata_command

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

# Command-specific exit codes (>= 10).
EXIT_BAD_REQUEST = 10
EXIT_UNSUPPORTED_SCHEME = 11
EXIT_WRITE_FAILED = 12
EXIT_INPUTS_MANIFEST_WRITE_FAILED = 13
EXIT_STATE_MANIFEST_WRITE_FAILED = 14


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
    return parser


def _build_table_manifest_entries(tables, parquet_dir: Path) -> list[dict]:
    """One entry per `tables` element.

    Tables with a non-null `location` get a single-element `files` list
    pointing at where the transporter is going to put the localized
    parquet (`<parquet_dir>/<name>.parquet`). Tables with
    `location is None` get an empty `files` list — downstream
    `_materialise_input_slot` turns that into a None / [] value for
    the user function depending on the slot's mode.

    Used to build both the inputs manifest (from `request.input` +
    `in_dir`) and the state manifest (from `request.system_input` +
    `state_dir`) since the per-entry shape is identical.
    """
    entries: list[dict] = []
    for table in tables:
        if table.location is None:
            files: list[str] = []
        else:
            files = [str((parquet_dir / f"{table.name}.parquet").absolute())]
        entries.append({
            MANIFEST_NAME_KEY: table.name,
            MANIFEST_FILES_KEY: files,
        })
    return entries


def _build_inputs_manifest_entries(request: Request, in_dir: Path) -> list[dict]:
    return _build_table_manifest_entries(request.input, in_dir)


def _build_state_manifest_entries(request: Request, state_dir: Path) -> list[dict]:
    return _build_table_manifest_entries(request.system_input, state_dir)


def _run() -> int:
    args = _build_parser().parse_args()
    request_path = Path(args.request)
    in_dir = Path(args.in_dir)
    state_dir = Path(args.state_dir)
    out_path = Path(args.out)
    inputs_manifest_path = Path(args.inputs_manifest)
    state_manifest_path = Path(args.state_manifest)
    liveness_path = Path(args.liveness)

    with Liveness(liveness_path):
        try:
            request = load_request(request_path)
        except (FileNotFoundError, InvalidRequestError) as exc:
            raise CommandSpecificError(
                EXIT_BAD_REQUEST, f"unreadable request.yaml: {exc}"
            ) from exc

        try:
            copy_req = build_localize_state_request(request, in_dir, state_dir)
        except UnsupportedSchemeError as exc:
            raise CommandSpecificError(EXIT_UNSUPPORTED_SCHEME, str(exc)) from exc

        try:
            write_transporter_request(copy_req, out_path)
        except OSError as exc:
            raise CommandSpecificError(
                EXIT_WRITE_FAILED, f"failed to write {out_path}: {exc}"
            ) from exc

        inputs_entries = _build_inputs_manifest_entries(request, in_dir)
        try:
            write_inputs_manifest(inputs_entries, inputs_manifest_path)
        except OSError as exc:
            raise CommandSpecificError(
                EXIT_INPUTS_MANIFEST_WRITE_FAILED,
                f"failed to write {inputs_manifest_path}: {exc}",
            ) from exc

        state_entries = _build_state_manifest_entries(request, state_dir)
        try:
            write_state_manifest(state_entries, state_manifest_path)
        except OSError as exc:
            raise CommandSpecificError(
                EXIT_STATE_MANIFEST_WRITE_FAILED,
                f"failed to write {state_manifest_path}: {exc}",
            ) from exc
    return 0


@tabsdata_command(name="td_prep_localize_state")
def main() -> int:
    return run_command(_run)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
