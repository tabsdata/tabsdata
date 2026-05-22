#
# Copyright 2026 Tabs Data Inc.
#

"""run-function command.

The penultimate step of the function-runner chain. Reads the inputs
manifest, materialises each input slot as a polars `LazyFrame` (or a
list of LazyFrames for ARRAY-mode slots), invokes the user-decorated
function, and sinks each returned `LazyFrame` to a parquet under
`--out-dir`. Emits an outputs manifest at `--outputs-manifest` mapping
each declared output table to the parquet it produced (or `null` if
the function returned `None` for that table — "not modified this run").

Kind-agnostic: handles `publisherDef` and `transformerDef`. The input
slot mode (SINGLE vs ARRAY) is inferred per-parameter from the user
function's type annotations, so the command does not need to know
which kind it is running.
"""

from __future__ import annotations

import inspect
import json
import sys
import typing
from pathlib import Path
from typing import Any, Callable

import polars as pl
from pydantic import ValidationError

from tabsdatak._api import _is_array_slot, _is_single_slot
from tabsdatak._bundle import BUNDLE_MANIFEST_FILENAME, BUNDLE_USER_CODE_DIRNAME
from tabsdatak._info import (
    KIND_PUBLISHER_DEF,
    KIND_TRANSFORMER_DEF,
    PublisherInfo,
    TransformerInfo,
    _unwrap_info,
)
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

# The Info classes this command knows how to invoke. The runner is
# agnostic of *what* the function does — it just needs the call shape
# (input modes, output names) which is shared across both.
FunctionInfo = PublisherInfo | TransformerInfo

# TODO: After all commands are done, version will be unified in a single
#  source of truth file.
VERSION = "0.1.0"
INFO = (
    "run-function — invoke the decorated function on the prepared inputs "
    "and sink the returned tables to parquet"
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
    "table to its parquet (or null if the function returned None). "
    "Handles both publisherDef and transformerDef bundles."
)

# Command-specific exit codes (>= 10 per docs/01-CommandBasics.md).
EXIT_BAD_REQUEST = 10
EXIT_BAD_BUNDLE = 11
EXIT_BAD_USER_CODE = 12
EXIT_BAD_MANIFEST = 14
EXIT_BAD_INPUT_PARQUET = 15
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


_INFO_BY_KIND: dict[str, type[FunctionInfo]] = {
    KIND_PUBLISHER_DEF: PublisherInfo,
    KIND_TRANSFORMER_DEF: TransformerInfo,
}


def _load_function_def(bundle_dir: Path) -> FunctionInfo:
    """Same bundle-loading shape as `run-source-plugin`: prepend
    `<bundle_dir>/user_code/` to `sys.path`, then deserialise.

    The envelope's `kind` field decides which Info class to use; bundles
    with an unknown or missing kind raise EXIT_BAD_BUNDLE.
    """
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

    kind = data.get("kind") if isinstance(data, dict) else None
    info_cls = _INFO_BY_KIND.get(kind) if kind else None
    if info_cls is None:
        raise CommandSpecificError(
            EXIT_BAD_BUNDLE,
            f"unsupported function_def kind {kind!r}; expected one of "
            f"{sorted(_INFO_BY_KIND)}",
        )

    try:
        info = info_cls.from_dict(data)
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


def _infer_table_modes(func: Callable) -> list[TableMode]:
    """Derive the table-mode (SINGLE / ARRAY) for each of the user
    function's parameters from its type annotations.

    The count of returned modes equals `len(inspect.signature(func).
    parameters)` — the function's parameter count *is* the input slot
    count, by construction. Both `@publisher` and `@transformer`
    validate at decoration time that the function has exactly the
    expected number of slot-annotated parameters and nothing else, so
    iterating all params here is correct for both kinds.

    Each parameter must be annotated as either `TableFrameSpec`
    (single-mode) or `TableFramesSpec` (array-mode). Because the
    decorator's `_check_callable_signature` already enforces that, the
    EXIT_BAD_USER_CODE branches below are defensive fallbacks for
    runtime invariant violations rather than realistic user errors.
    """
    try:
        hints = typing.get_type_hints(func)
    except Exception as exc:
        raise CommandSpecificError(
            EXIT_BAD_USER_CODE,
            f"cannot resolve type hints for user function: {exc}",
        ) from exc
    params = list(inspect.signature(func).parameters.values())
    modes: list[TableMode] = []
    for p in params:
        hint = hints.get(p.name)
        if hint is not None and _is_single_slot(hint):
            modes.append(TableMode.SINGLE)
        elif hint is not None and _is_array_slot(hint):
            modes.append(TableMode.ARRAY)
        else:
            raise CommandSpecificError(
                EXIT_BAD_USER_CODE,
                f"parameter {p.name!r} of user function has an "
                f"unrecognised slot annotation; expected TableFrameSpec "
                f"or TableFramesSpec",
            )
    return modes


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

        func = _unwrap_info(info.func)
        table_modes = _infer_table_modes(func)
        if len(entries) != len(table_modes):
            raise CommandSpecificError(
                EXIT_BAD_MANIFEST,
                f"inputs manifest has {len(entries)} entries but the "
                f"user function declares {len(table_modes)} input slots",
            )

        # Build the positional args for the user function from the
        # inputs manifest. Manifest order mirrors function parameter
        # order.
        call_args: list[Any] = []
        for entry, table_mode in zip(entries, table_modes):
            files = entry.get(MANIFEST_FILES_KEY) or []
            call_args.append(_materialise_input_slot(files, table_mode=table_mode))

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
