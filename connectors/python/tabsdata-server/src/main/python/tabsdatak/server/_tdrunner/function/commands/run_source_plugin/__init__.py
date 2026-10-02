#
# Copyright 2026 Tabsdata Inc.
#

"""run-source-plugin command.

Reads the function definition from the extracted bundle, imports the
user's code, instantiates the matching SrcPlugin via the connector
registry exposed by `tabsdatak._connectors`, and invokes read_in() to
materialise parquet files in --out-dir.

Supports two function-definition kinds:

* ``publisherDef`` -- a regular ``@publisher``. The user-declared
  ``conn`` and ``source`` go to ``plugin.read_in(...)`` verbatim.
* ``streamPublisherDef`` -- a ``@stream_publisher``. The user-declared
  ``conn`` (e.g. ``KafkaStreamSrcConn``) and ``source`` (e.g.
  ``KafkaStreamSrc``) are for the long-running streamer pod only;
  on the publisher side we read from the manifests the streamer
  wrote into ``info.function_stage``. We substitute:
    - source -> ``StreamSrcDef.publisher_src()`` (default
      ``StreamStageSrc()``)
    - conn -> a duck-typed ``ManifestStageConn`` whose populated
      backend matches the ``function_stage`` URI's scheme (today:
      ``file://``)
  Then we invoke the substituted plugin's ``read_in`` exactly like a
  regular publisher would. The publisher-side plugin's wildcard is
  ``manifests/stream-manifest-*.json`` (relative to the conn's
  ``base_path``, which we set to the stage root), matching the
  layout the streamer pod's :class:`StreamerPluginCtx` writes.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._commons.run.command import (
    build_parser,
    command,
)
from tabsdatak.server._tdrunner.function.commands.run_source_plugin.runner import (
    _build_manifest_stage_conn,
    _invoke_read_in,
    _load_function_def,
    _normalise_table_file,
    _resolve_plugin_and_io,
    _SrcContext,
    _write_inputs_manifest,
    run,
)

__all__ = [
    "_SrcContext",
    "_build_manifest_stage_conn",
    "_invoke_read_in",
    "_load_function_def",
    "_normalise_table_file",
    "_resolve_plugin_and_io",
    "_write_inputs_manifest",
    "main",
]

VERSION = "2.2.0"
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
        program="run-source-plugin",
        description="Invoke the publisher's source plugin.",
        version=VERSION,
        info=INFO,
        manual=MAN,
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
    parser.add_argument(
        "--connection-file",
        required=True,
        dest="connection_file",
        help="Path to the connection definition file.",
    )
    return parser


@command(name="td_run_source_plugin", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
