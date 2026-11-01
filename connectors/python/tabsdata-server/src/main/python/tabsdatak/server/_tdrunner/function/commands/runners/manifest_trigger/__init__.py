#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.runners.manifest_trigger.options import (  # noqa: E501
    RunManifestTriggerArguments,
)
from tabsdatak.server._tdrunner.function.commands.runners.manifest_trigger.runner import (  # noqa: E501
    ManifestTrigger,
)

VERSION = version()
GENERATION = 2
INFORMATION = (
    "manifest-trigger — long-running sidecar of the streamer; triggers the "
    "stream publisher once per staged batch and deletes the batches it consumed"
)
MANUAL = (
    "manifest-trigger --request <PATH> --trigger-url <URL | env:VAR | val:LITERAL> "
    "--trigger-method <METHOD | env:VAR | val:LITERAL> "
    "--trigger-authorization <AUTH | env:VAR | val:LITERAL> "
    "[--back-off-seconds <INT>] --liveness <PATH>\n\n"
    "Opens the function stage --request names and walks its manifests oldest "
    "first, posting a StageTrigger execution request keyed by each manifest's "
    "batch. The apiserver answers Scheduled, Busy (a run is in flight), Held (an "
    "execution holds the batch but has not committed) or Consumed (it "
    "committed). Only Consumed deletes the batch and moves on to the next "
    "manifest; any other answer, an error status or a connection failure keeps "
    "the manifest for the next poll. An outcome this sidecar does not know, or "
    "an answer without one, stops it. Between polls it deletes the consumed "
    "batches for --back-off-seconds. SIGTERM or SIGINT stops it."
)


def build_command_parser() -> ArgumentsParser:
    return RunManifestTriggerArguments.parser(
        program="manifest-trigger",
        description="Trigger the stream publisher once per staged batch.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td-t-src-manifest", parser=build_command_parser)
def main(arguments: RunManifestTriggerArguments) -> int:
    return ManifestTrigger(arguments).run()


if __name__ == "__main__":
    import sys

    sys.exit(main())
