#
# Copyright 2026 Tabs Data Inc.
#

"""run-source-plugin command.

Reads the function definition from the extracted bundle, imports the
user's code, instantiates the matching SrcPlugin via the connector
registry exposed by `tabsdatak._connectors`, and invokes read_in() to
materialise parquet files in --out-dir.
"""

from __future__ import annotations

import argparse
import inspect
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

from tabsdatak._connectors import get_src
from tabsdatak._info import KIND_PUBLISHER_DEF, PublisherInfo, _unwrap_info
from tabsdatak.server._tdrunner.function._lib.cli import build_parser, command
from tabsdatak.server._tdrunner.function._lib.function_def import (
    load_function_def_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.initial_values import (
    drain_ctx_to_state_or_raise,
    hydrate_ctx_from_state_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.inputs_manifest import (
    MANIFEST_FILES_KEY,
    MANIFEST_NAME_KEY,
    write_inputs_manifest,
)
from tabsdatak.server._tdrunner.function._lib.request import load_request_or_raise
from tabsdatak.server._tdrunner.function._lib.request_ctx import FromRequestCtx
from tabsdatak.server._tdrunner.function.commands.run_source_plugin.error import (
    RunSourcePluginErrorCode,
)
from tabsdatak.spi import SrcPluginCtx

# TODO: After all commands are done, version will be unified in a single source of
#  truth file.
VERSION = "0.1.0"
INFO = (
    "run-source-plugin — invoke the publisher's source plugin to produce parquet files"
)
MAN = (
    "run-source-plugin --request <PATH> --bundle-dir <PATH> "
    "--out-dir <PATH> --inputs-manifest <PATH> --liveness <PATH>\n\n"
    "Loads function_def.json from --bundle-dir, instantiates the SrcPlugin "
    "matching the deserialised source spec, and invokes read_in() to "
    "materialise parquet files under --out-dir. Writes a YAML manifest "
    "at --inputs-manifest that maps each function input parameter (in "
    "declaration order) to the parquet files the plugin produced for "
    "it; consumed by the function runner."
)


def _build_parser():
    parser = build_parser(
        prog="run-source-plugin",
        description="Invoke the publisher's source plugin.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--bundle-dir", required=True, help="Extracted bundle directory"
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Directory where read_in writes parquet files",
    )
    parser.add_argument(
        "--inputs-manifest",
        required=True,
        help=(
            "Path where this command writes the YAML manifest mapping "
            "each function input parameter to the parquet files produced "
            "by the source plugin. Consumed by the function runner."
        ),
    )
    parser.add_argument(
        "--state-dir",
        required=True,
        dest="state_dir",
        help=(
            "Local directory where state-in tables are read from "
            "(populated by prep-localize-state + run-transporter) and "
            "state-out tables are written to (consumed by "
            "prep-upload-state). The convention is "
            "<state-dir>/<table.name>.parquet, mirroring the wire "
            "format prep-localize-state / prep-upload-state already use."
        ),
    )
    return parser


def _load_function_def(bundle_dir: Path) -> PublisherInfo:
    """Load the publisher's ``function_def.json`` -- thin wrapper over
    :func:`load_function_def_or_raise` pinning the accepted kind set
    and the command's per-site error codes."""
    return load_function_def_or_raise(
        bundle_dir,
        allowed_kinds={KIND_PUBLISHER_DEF: PublisherInfo},
        bad_bundle_code=RunSourcePluginErrorCode.RSP_2,
        bad_user_code_code=RunSourcePluginErrorCode.RSP_3,
        wrong_kind_code=RunSourcePluginErrorCode.RSP_4,
    )


class _SrcContext(FromRequestCtx, SrcPluginCtx):
    """Concrete :class:`SrcPluginCtx` for the run-source-plugin command.

    All behaviour comes from :class:`FromRequestCtx`; this class
    only fixes the abstract base so ``isinstance(ctx, SrcPluginCtx)``
    holds at the call boundary.
    """


def _resolve_plugin(info: PublisherInfo):
    try:
        src_def = get_src(type(info.source))
    except KeyError as exc:
        raise RunSourcePluginErrorCode.RSP_5.exception(
            cause=exc, src_type=type(info.source).__name__, detail=str(exc)
        )
    return src_def.plugin()


def _invoke_read_in(plugin, ctx: SrcPluginCtx, conn, src) -> list[list[Path]]:
    try:
        return plugin.read_in(ctx, conn, src)
    except Exception as exc:
        raise RunSourcePluginErrorCode.RSP_6.exception(cause=exc, detail=str(exc))


def _normalise_table_file(p) -> str:
    """Render a `TableFileSpec` as an absolute filesystem path string.

    `TableFileSpec` is declared as `ParquetFile` (a `Path` subclass),
    which should mean a real filesystem path. In practice some
    connector plugins (e.g. `LocalFileSrcPlugin`) return the path
    parsed verbatim from the transporter report's `to:` field — i.e.
    `Path("file:///...")` (a URI wrapped in `Path`, not an actual
    filesystem path). `Path.absolute()` on such a value mistakes the
    `file:` prefix for a path segment and prepends the CWD.

    Accept both shapes here so the manifest always contains a clean
    absolute filesystem path.

    TODO: clarify the `TableFileSpec` contract and unwrap at the
    plugin layer; this normalisation should then become unnecessary.
    """
    s = str(p)
    if s[:5].lower().startswith("file:"):
        # `urlparse` tolerates both `file:///foo` and the
        # `Path`-collapsed `file:/foo` form. `url2pathname` then
        # converts the URI path to an OS-native filesystem path: on
        # POSIX it passes `/foo/bar` through; on Windows it turns
        # `/C:/Users/x` into `C:\Users\x`. URL-encoded characters
        # (`%20` etc.) are decoded by `url2pathname`. Same pair of
        # primitives the legacy `convert_uri_to_path` uses.
        return url2pathname(urlparse(s).path)
    return str(Path(s).absolute())


def _write_inputs_manifest(
    info: PublisherInfo,
    produced: list[list[Path]],
    path: Path,
) -> None:
    """Write the YAML inputs-manifest to `path`.

    The manifest maps each *function input parameter* (in declaration
    order) to the parquet files the source plugin produced for it.
    Input parameters are unrelated to `info.output_tables` — the
    output-tables list describes what the function *returns*, not what
    feeds in to it.

    Schema:

        inputs:
          - name: <function param name>          # e.g. "in_1"
            files:
              - <absolute path>
              - ...

    Plugin-contract check (→ `PluginFailedError`): the plugin must
    have produced exactly `cardinality(source)` outer slots. The
    function-signature side (function must declare at least that many
    parameters) is owned by `PublisherInfo._check_func_signature` and
    is already guaranteed by the time this code runs; not re-checked
    here.

    A negative `cardinality` (the `-1` sentinel) means the source's
    slot count is decided dynamically at read time -- e.g. a
    stream-stage manifest source, whose slot count comes from the
    manifest, not from static config -- so the produced-vs-declared
    check is skipped and the plugin's output is trusted as-is.
    """
    src_def = get_src(type(info.source))
    cardinality = src_def.cardinality(info.source)
    func = _unwrap_info(info.func)
    parameters = list(inspect.signature(func).parameters)

    if cardinality < 0:
        # Dynamic slot count (-1 sentinel): trust whatever the plugin
        # produced rather than validating against a declared count.
        cardinality = len(produced)
    elif len(produced) != cardinality:
        raise RunSourcePluginErrorCode.RSP_7.exception(
            returned=len(produced), expected=cardinality
        )

    entries = [
        {
            MANIFEST_NAME_KEY: parameters[i],
            MANIFEST_FILES_KEY: [_normalise_table_file(f) for f in produced[i]],
        }
        for i in range(cardinality)
    ]

    try:
        write_inputs_manifest(entries, path)
    except OSError as exc:
        raise RunSourcePluginErrorCode.RSP_8.exception(
            cause=exc, path=path, detail=str(exc)
        )


@command(name="td_run_source_plugin", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    bundle_dir = Path(args.bundle_dir)
    out_dir = Path(args.out_dir)
    inputs_manifest = Path(args.inputs_manifest)
    state_dir = Path(args.state_dir)

    request = load_request_or_raise(request_path, RunSourcePluginErrorCode.RSP_1)
    info = _load_function_def(bundle_dir)
    plugin = _resolve_plugin(info)
    ctx = _SrcContext(
        work_dir=out_dir,
        exec_plan_id=str(request.info.execution_id),
        trx_id=str(request.info.transaction_id),
        exec_plan_ts=request.info.triggered_on_iso,
        collection_id=str(request.info.collection_id),
        collection_name=request.info.collection,
        function_id=str(request.info.function_run_id),
        function_name=request.info.function,
    )
    hydrate_ctx_from_state_or_raise(
        ctx, request, state_dir, error_code=RunSourcePluginErrorCode.RSP_9
    )
    produced = _invoke_read_in(plugin, ctx, info.conn, info.source)
    drain_ctx_to_state_or_raise(
        ctx, request, state_dir, error_code=RunSourcePluginErrorCode.RSP_10
    )
    _write_inputs_manifest(info, produced, inputs_manifest)
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
