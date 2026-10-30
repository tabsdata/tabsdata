#
# Copyright 2026 Tabsdata Inc.
#

"""run-source-plugin command.

Reads the function definition from the extracted bundle, imports the
user's code, instantiates the matching SrcPlugin via the connector
registry exposed by `tabsdatak._connectors`, and invokes read_in() to
materialise parquet files in --output-folder.

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

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin.arguments import (  # noqa: E501
    RunSourcePluginArguments,
)
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin.runner import (
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

VERSION = version()
GENERATION = 1
INFORMATION = (
    "run-source-plugin — invoke the publisher's source plugin to produce parquet files"
)
MANUAL = (
    "run-source-plugin --request <PATH> --bundle-folder <PATH> "
    "--output-folder <PATH> --inputs-manifest <PATH> --state-folder <PATH> "
    "--connection-file <PATH> --liveness <PATH>\n\n"
    "Loads function_def.json from --bundle-folder, instantiates the SrcPlugin "
    "matching the deserialised source spec, and invokes read_in() to "
    "materialise parquet files under --output-folder. Writes a YAML manifest "
    "at --inputs-manifest that maps each function input parameter (in "
    "declaration order) to the parquet files the plugin produced for "
    "it; consumed by the function runner."
)


def build_command_parser() -> ArgumentsParser:
    return RunSourcePluginArguments.parser(
        program="run-source-plugin",
        description="Invoke the publisher's source plugin.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td_run_source_plugin", parser=build_command_parser)
def main(arguments: RunSourcePluginArguments) -> int:
    return run(arguments)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
