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
import json
import sys
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

from pydantic import ValidationError

from tabsdatak._bundle import BUNDLE_MANIFEST_FILENAME, BUNDLE_USER_CODE_DIRNAME
from tabsdatak._connectors import get_src
from tabsdatak._info import KIND_PUBLISHER_DEF, PublisherInfo, _unwrap_info
from tabsdatak._tdrunner.function._lib.cli import build_parser, command
from tabsdatak._tdrunner.function._lib.errors import CommandSpecificError
from tabsdatak._tdrunner.function._lib.inputs_manifest import (
    MANIFEST_FILES_KEY,
    MANIFEST_NAME_KEY,
    write_inputs_manifest,
)
from tabsdatak._tdrunner.function._lib.request import load_request_or_fail
from tabsdatak.api import ContextKeySpec, ContextValueSpec
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
    """Load `function_def.json` from `bundle_dir` and return the
    deserialised `PublisherInfo`.

    `<bundle_dir>/user_code/` is prepended to `sys.path` *before*
    deserialisation — `PublisherInfo`'s `func` validator (`_val_func`
    in `tabsdatak._info`) calls `callable_from_str`, which does an
    `importlib.import_module` against the user's package. The bundle
    producer (`tabsdatak._bundle.build_bundle`) stages the user's
    project tree under that subdir, so it's the predictable sys.path
    entry that makes `info.func` importable.
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

    # JSON parsing is a bundle concern (producer wrote garbage);
    # schema validation is a separate failure mode below. Pydantic's
    # `validate_json` collapses both into `ValidationError`, so we
    # split them by pre-parsing here.
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise CommandSpecificError(
            EXIT_BAD_BUNDLE, f"malformed JSON in {manifest}: {exc}"
        ) from exc

    try:
        info = PublisherInfo.from_dict(data)
    except ValidationError as exc:
        # ImportError / AttributeError from callable_from_str surface
        # here as ValidationError; user-code failure, not bundle shape.
        raise CommandSpecificError(EXIT_BAD_USER_CODE, str(exc)) from exc
    except Exception as exc:
        # Anything else (unexpected pydantic internals, IO oddities)
        # — surface as a bundle problem.
        raise CommandSpecificError(EXIT_BAD_BUNDLE, str(exc)) from exc

    if info.__kind__ != KIND_PUBLISHER_DEF:
        raise CommandSpecificError(
            EXIT_WRONG_KIND,
            f"expected {KIND_PUBLISHER_DEF} in {manifest}, got {info.__kind__!r}",
        )
    return info


class _SrcContext(SrcPluginCtx):
    """Concrete :class:`SrcPluginCtx` for the run-source-plugin command.

    Populated from ``request.yaml`` -- ``collection_id`` /
    ``collection_name`` / ``exec_plan_id`` / ``trx_id`` come straight
    off ``request.info``. ``function_id`` borrows ``function_run_id``
    from the same block (closest available identifier today).

    Surfaces not yet plumbed through:
      * ``function_name`` and ``exec_plan_ts`` raise
        ``NotImplementedError`` -- they're missing from ``request.yaml``
        and plugins that touch them should surface the gap loudly
        rather than silently consume a placeholder.
      * ``get_attr`` returns ``None`` and ``store_attr`` /
        ``add_metadata`` are no-ops -- the per-flow scratch store and
        the observability sink aren't wired through tdrunner yet, but
        plugins that use them (e.g. progress-marker writes) shouldn't
        crash. Values just don't persist across trx invocations.
    """

    def __init__(
        self,
        work_dir: Path,
        exec_plan_id: str,
        trx_id: str,
        collection_id: str,
        collection_name: str,
        function_id: str,
    ):
        self._work_dir = work_dir
        self._exec_plan_id = exec_plan_id
        self._trx_id = trx_id
        self._collection_id = collection_id
        self._collection_name = collection_name
        self._function_id = function_id

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def collection_id(self) -> str:
        return self._collection_id

    @property
    def collection_name(self) -> str:
        return self._collection_name

    @property
    def function_id(self) -> str:
        return self._function_id

    @property
    def function_name(self) -> str:
        # TODO: thread function_name through request.yaml's info block
        # and populate at construction. Raising rather than returning
        # a placeholder so consumers surface the gap.
        raise NotImplementedError(
            "function_name is not yet plumbed through request.yaml; "
            "see run_source_plugin._SrcContext"
        )

    @property
    def exec_plan_id(self) -> str:
        return self._exec_plan_id

    @property
    def trx_id(self) -> str:
        return self._trx_id

    @property
    def exec_plan_ts(self) -> str:
        # TODO: add `exec_plan_ts` to request.yaml's `info` block and
        # plumb it through here. Raising rather than returning a
        # placeholder so any consumer reading it gets pointed at the gap.
        raise NotImplementedError(
            "exec_plan_ts is not yet plumbed through request.yaml; "
            "see run_source_plugin._SrcContext"
        )

    def get_attr(self, attr: ContextKeySpec) -> ContextValueSpec | None:
        # TODO: wire the per-flow attribute store through the runner.
        # Until then we honour the documented "returns None if unset"
        # contract -- plugins that touch get_attr on a cold runtime
        # see no prior state, which is the same surface they'd see
        # on the first call of a brand-new flow anyway.
        return None

    def store_attr(self, attr: ContextKeySpec, val: ContextValueSpec | None):
        # TODO: wire the per-flow attribute store. No-op for now so
        # plugins that store progress markers don't crash; the values
        # simply don't persist across trx invocations.
        pass

    def add_metadata(self, attr: ContextKeySpec, val: ContextValueSpec | None):
        # TODO: wire the trx-level observability sink. No-op for now.
        pass


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
    sys.exit(main())
