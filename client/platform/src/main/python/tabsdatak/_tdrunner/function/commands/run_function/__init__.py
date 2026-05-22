#
# Copyright 2026 Tabs Data Inc.
#

"""run-function command.

The publisher chain's penultimate step. Reads the enriched inputs
manifest produced by `stamp-inputs`, materialises each input slot as
a polars `LazyFrame`, invokes the user-decorated publisher function,
and sinks each returned `LazyFrame` to a parquet under `--out-dir`.
Emits an outputs manifest at `--outputs-manifest` mapping each
declared output table to the parquet it produced (or `null` if the
function returned `None` for that table — "not modified this run").

Currently handles `publisherDef` only. `AutoPublisherInfo`,
`TransformerInfo`, and `SubscriberInfo` use different input/output
topologies and will get their own command paths when their flows land
in the migration plan.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import polars as pl
from pydantic import ValidationError

from tabsdatak._bundle import BUNDLE_MANIFEST_FILENAME, BUNDLE_USER_CODE_DIRNAME
from tabsdatak._connectors import get_src
from tabsdatak._info import PublisherInfo, _unwrap_info
from tabsdatak._tdrunner.function._lib.cli import build_parser, run_command
from tabsdatak._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak._tdrunner.function._lib.inputs_manifest import (
    MANIFEST_FILES_KEY,
    InvalidManifestError,
    load_inputs_manifest,
)
from tabsdatak._tdrunner.function._lib.liveness import Liveness
from tabsdatak._tdrunner.function._lib.outputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_NAME_KEY,
    write_outputs_manifest,
)
from tabsdatak._tdrunner.function._lib.request import (
    InvalidRequestError,
    load_request,
)
from tabsdatak._tdrunner.function.commands import tabsdata_command
from tabsdatak.spi import TableMode

# TODO: After all commands are done, version will be unified in a single
#  source of truth file.
VERSION = "0.1.0"
INFO = (
    "run-function — invoke the publisher's decorated function on the "
    "enriched inputs and sink the returned tables to parquet"
)
MAN = (
    "run-function --request <PATH> --bundle-dir <PATH> "
    "--inputs-manifest <PATH> --out-dir <PATH> "
    "--outputs-manifest <PATH> --liveness <PATH>\n\n"
    "Loads function_def.json from --bundle-dir, materialises each "
    "input slot from --inputs-manifest as a polars LazyFrame "
    "(single-mode slots → one LazyFrame; array-mode slots → list of "
    "LazyFrames), invokes the user function, and sinks each returned "
    "table to <out-dir>/slot<i>/<table_name>.parquet. Writes an "
    "outputs manifest at --outputs-manifest mapping each output "
    "table to its parquet (or null if the function returned None)."
)

# Command-specific exit codes (>= 10 per docs/01-CommandBasics.md).
EXIT_BAD_REQUEST = 10
EXIT_BAD_BUNDLE = 11
EXIT_BAD_USER_CODE = 12
EXIT_BAD_MANIFEST = 14
EXIT_BAD_INPUT_PARQUET = 15
EXIT_NO_PLUGIN = 16
EXIT_USER_FUNCTION_FAILED = 17
EXIT_OUTPUT_SHAPE_MISMATCH = 18
EXIT_OUTPUT_WRITE_FAILED = 19
EXIT_OUTPUTS_MANIFEST_WRITE_FAILED = 20


def _build_parser():
    parser = build_parser(
        prog="run-function",
        description="Invoke the publisher's user-defined function.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--bundle-dir", required=True, help="Extracted bundle directory"
    )
    parser.add_argument(
        "--inputs-manifest",
        required=True,
        help="Path to the YAML inputs manifest produced by stamp-inputs",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Directory where the function's output parquets are written",
    )
    parser.add_argument(
        "--outputs-manifest",
        required=True,
        help=(
            "Path where this command writes the YAML outputs manifest "
            "mapping each output table to its parquet file (or null if "
            "the function returned None). Consumed by prep-upload-state."
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


def _load_function_def(bundle_dir: Path) -> PublisherInfo:
    """Same bundle-loading shape as `run-source-plugin`: prepend
    `<bundle_dir>/user_code/` to `sys.path`, then deserialise."""
    manifest = bundle_dir / BUNDLE_MANIFEST_FILENAME
    if not manifest.is_file():
        raise CommandSpecificError(
            EXIT_BAD_BUNDLE,
            f"{BUNDLE_MANIFEST_FILENAME} not found in {bundle_dir}",
        )

    user_code_path = str((bundle_dir / BUNDLE_USER_CODE_DIRNAME).resolve())
    if user_code_path not in sys.path:
        sys.path.insert(0, user_code_path)

    try:
        text = manifest.read_text(encoding="utf-8")
    except OSError as exc:
        raise CommandSpecificError(
            EXIT_BAD_BUNDLE, f"unreadable {manifest}: {exc}"
        ) from exc

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise CommandSpecificError(
            EXIT_BAD_BUNDLE, f"malformed JSON in {manifest}: {exc}"
        ) from exc

    try:
        info = PublisherInfo.from_dict(data)
    except ValidationError as exc:
        raise CommandSpecificError(EXIT_BAD_USER_CODE, str(exc)) from exc
    except Exception as exc:
        raise CommandSpecificError(EXIT_BAD_BUNDLE, str(exc)) from exc

    return info


def _load_inputs_manifest_or_fail(path: Path) -> list[dict]:
    try:
        return load_inputs_manifest(path)
    except (FileNotFoundError, InvalidManifestError) as exc:
        raise CommandSpecificError(EXIT_BAD_MANIFEST, str(exc)) from exc


def _resolve_src_def(info: PublisherInfo):
    """Look up the SrcDef for this publisher's source, so we know
    whether each input slot is SINGLE or ARRAY mode."""
    try:
        return get_src(type(info.source))
    except KeyError as exc:
        raise CommandSpecificError(EXIT_NO_PLUGIN, str(exc)) from exc


def _scan_parquet_or_fail(path: Path) -> pl.LazyFrame:
    """Open `path` as a `pl.LazyFrame`, validating the file eagerly.

    `pl.scan_parquet` itself is lazy and won't touch the file until
    you ask for its schema or collect — so a bad parquet would
    silently slip past us and only blow up inside the user function.
    We fetch the schema here as a cheap header read to surface
    corruption / wrong-format failures with the right exit code at
    the right layer.
    """
    try:
        lf = pl.scan_parquet(path)
        lf.collect_schema()
    except Exception as exc:
        raise CommandSpecificError(
            EXIT_BAD_INPUT_PARQUET,
            f"failed to scan input parquet {path}: {exc}",
        ) from exc
    return lf


def _materialise_input_slot(files: list[str], *, table_mode: TableMode):
    """Turn one inputs-manifest entry into the value the user function
    expects for that input parameter.

    SINGLE mode:
      * empty `files` → `None`
      * one file     → `LazyFrame`
      * many files   → contract violation (plugin should have returned
        exactly one for a SINGLE slot)

    ARRAY mode:
      * any files (incl. empty) → `list[LazyFrame]`

    !!! WARNING — INTERIM STUB !!!
    The user-function contract (see `tabsdatak._api`) declares input
    parameters as `TableFrame | None` / `list[TableFrame | None] | None`,
    but this helper currently hands back raw `polars.LazyFrame` because
    `tabsdatak.tableframe.TableFrame` is still an empty placeholder class
    (no real wrapper implementation yet). When the real TableFrame lands
    in tabsdatak, every `LazyFrame` produced here must be wrapped — e.g.
    `return TableFrame(scans[0])` in SINGLE mode and
    `return [TableFrame(s) for s in scans]` in ARRAY mode — and the
    `LazyFrame` mentions above should be replaced with `TableFrame`.

    TODO(T2-migration): wrap LazyFrame outputs in TableFrame once the
    real TableFrame implementation is available in tabsdatak.
    """
    scans = [_scan_parquet_or_fail(Path(f)) for f in files]
    if table_mode is TableMode.SINGLE:
        if len(scans) == 0:
            return None
        if len(scans) > 1:
            raise CommandSpecificError(
                EXIT_BAD_MANIFEST,
                f"SINGLE-mode slot has {len(scans)} files; expected 0 or 1",
            )
        return scans[0]
    # ARRAY mode
    return scans


def _invoke_user_function(func, call_args: list, output_tables: list[str]) -> tuple:
    """Call the user function and validate its return shape.

    Expected return: a tuple with exactly `len(output_tables)` elements.
    Each element must be a `LazyFrame` or `None`.
    """
    try:
        result = func(*call_args)
    except Exception as exc:
        raise CommandSpecificError(
            EXIT_USER_FUNCTION_FAILED, f"user function raised: {exc}"
        ) from exc

    if not isinstance(result, tuple):
        raise CommandSpecificError(
            EXIT_OUTPUT_SHAPE_MISMATCH,
            f"user function must return a tuple, got {type(result).__name__}",
        )
    if len(result) != len(output_tables):
        raise CommandSpecificError(
            EXIT_OUTPUT_SHAPE_MISMATCH,
            f"user function returned {len(result)} value(s); "
            f"output_tables declares {len(output_tables)} "
            f"({list(output_tables)})",
        )
    for i, value in enumerate(result):
        if value is not None and not isinstance(value, pl.LazyFrame):
            raise CommandSpecificError(
                EXIT_OUTPUT_SHAPE_MISMATCH,
                f"output {i} ({output_tables[i]!r}) must be a "
                f"polars.LazyFrame or None, got "
                f"{type(value).__name__}",
            )
    return result


def _sink_outputs(
    result: tuple,
    output_tables: list[str],
    out_dir: Path,
) -> list[dict]:
    """Sink each non-None output to `<out_dir>/slot<i>/<table>.parquet`
    and build the outputs-manifest entries."""
    entries: list[dict] = []
    for i, (table_name, value) in enumerate(zip(output_tables, result)):
        if value is None:
            entries.append({
                MANIFEST_NAME_KEY: table_name,
                MANIFEST_FILE_KEY: None,
            })
            continue
        slot_dir = out_dir / f"slot{i}"
        dest = slot_dir / f"{table_name}.parquet"
        try:
            slot_dir.mkdir(parents=True, exist_ok=True)
            value.sink_parquet(dest)
        except Exception as exc:
            raise CommandSpecificError(
                EXIT_OUTPUT_WRITE_FAILED,
                f"failed to write output {table_name!r} to {dest}: {exc}",
            ) from exc
        entries.append({
            MANIFEST_NAME_KEY: table_name,
            MANIFEST_FILE_KEY: str(dest.absolute()),
        })
    return entries


def _run() -> int:
    args = _build_parser().parse_args()
    request_path = Path(args.request)
    bundle_dir = Path(args.bundle_dir)
    inputs_manifest_path = Path(args.inputs_manifest)
    out_dir = Path(args.out_dir)
    outputs_manifest_path = Path(args.outputs_manifest)
    liveness_path = Path(args.liveness)

    with Liveness(liveness_path):
        _load_request_or_fail(request_path)  # validates shape, not used otherwise yet
        info = _load_function_def(bundle_dir)
        entries = _load_inputs_manifest_or_fail(inputs_manifest_path)
        src_def = _resolve_src_def(info)
        table_mode = src_def.table_mode

        # Build the positional args for the user function from the
        # inputs manifest. Order is the manifest's declaration order
        # (which mirrors function parameter order, since stamp-inputs
        # preserved that from run-source-plugin).
        call_args = []
        for entry in entries:
            files = entry.get(MANIFEST_FILES_KEY) or []
            call_args.append(_materialise_input_slot(files, table_mode=table_mode))

        func = _unwrap_info(info.func)
        output_tables = list(info.output_tables)
        result = _invoke_user_function(func, call_args, output_tables)

        out_dir.mkdir(parents=True, exist_ok=True)
        entries_out = _sink_outputs(result, output_tables, out_dir)

        try:
            write_outputs_manifest(entries_out, outputs_manifest_path)
        except OSError as exc:
            raise CommandSpecificError(
                EXIT_OUTPUTS_MANIFEST_WRITE_FAILED,
                f"failed to write outputs manifest {outputs_manifest_path}: {exc}",
            ) from exc

    return 0


@tabsdata_command(name="td_run_function")
def main() -> int:
    return run_command(_run)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
