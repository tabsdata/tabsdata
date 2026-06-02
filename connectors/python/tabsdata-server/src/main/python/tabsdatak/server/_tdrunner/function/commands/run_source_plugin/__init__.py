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
from tabsdatak.server._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak.server._tdrunner.function._lib.function_def import load_function_def
from tabsdatak.server._tdrunner.function._lib.inputs_manifest import (
    MANIFEST_FILES_KEY,
    MANIFEST_NAME_KEY,
    write_inputs_manifest,
)
from tabsdatak.server._tdrunner.function._lib.request import load_request_or_fail
from tabsdatak.server._tdrunner.function._lib.request_ctx import FromRequestCtx
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

# Command-specific exit codes (>= 10 per docs/01-CommandBasics.md).
# TODO: Implementing this for now for easy errors, awaiting end of conversation
#  regarding errors to refactor
EXIT_BAD_REQUEST = 10
EXIT_BAD_BUNDLE = 11
EXIT_BAD_USER_CODE = 12
EXIT_WRONG_KIND = 13
EXIT_NO_PLUGIN = 14
EXIT_PLUGIN_FAILED = 15
EXIT_MANIFEST_WRITE_FAILED = 16


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
    return parser


def _load_function_def(bundle_dir: Path) -> PublisherInfo:
    """Load the publisher's ``function_def.json`` -- thin wrapper over
    :func:`load_function_def` pinning the accepted kind set and the
    command's exit codes."""
    return load_function_def(
        bundle_dir,
        allowed_kinds={KIND_PUBLISHER_DEF: PublisherInfo},
        exit_bad_bundle=EXIT_BAD_BUNDLE,
        exit_bad_user_code=EXIT_BAD_USER_CODE,
        exit_wrong_kind=EXIT_WRONG_KIND,
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
        raise CommandSpecificError(EXIT_NO_PLUGIN, str(exc)) from exc
    return src_def.plugin()


def _invoke_read_in(plugin, ctx: SrcPluginCtx, src) -> list[list[Path]]:
    try:
        return plugin.read_in(ctx, src)
    except Exception as exc:
        raise CommandSpecificError(
            EXIT_PLUGIN_FAILED, f"plugin read_in failed: {exc}"
        ) from exc


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

    Plugin-contract check (→ `EXIT_PLUGIN_FAILED`): the plugin must
    have produced exactly `cardinality(source)` outer slots. The
    function-signature side (function must declare at least that many
    parameters) is owned by `PublisherInfo._check_func_signature` and
    is already guaranteed by the time this code runs; not re-checked
    here.
    """
    src_def = get_src(type(info.source))
    cardinality = src_def.cardinality(info.source)
    func = _unwrap_info(info.func)
    parameters = list(inspect.signature(func).parameters)

    if len(produced) != cardinality:
        raise CommandSpecificError(
            EXIT_PLUGIN_FAILED,
            f"plugin returned {len(produced)} input slot(s); source "
            f"cardinality is {cardinality}",
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
        raise CommandSpecificError(
            EXIT_MANIFEST_WRITE_FAILED,
            f"failed to write inputs manifest {path}: {exc}",
        ) from exc


@command(name="td_run_source_plugin", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    bundle_dir = Path(args.bundle_dir)
    out_dir = Path(args.out_dir)
    inputs_manifest = Path(args.inputs_manifest)

    request = load_request_or_fail(request_path, EXIT_BAD_REQUEST)
    info = _load_function_def(bundle_dir)
    plugin = _resolve_plugin(info)
    ctx = _SrcContext(
        work_dir=out_dir,
        exec_plan_id=str(request.info.execution_id),
        trx_id=str(request.info.transaction_id),
        collection_id=str(request.info.collection_id),
        collection_name=request.info.collection,
        function_id=str(request.info.function_run_id),
    )
    produced = _invoke_read_in(plugin, ctx, info.source)
    _write_inputs_manifest(info, produced, inputs_manifest)
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
