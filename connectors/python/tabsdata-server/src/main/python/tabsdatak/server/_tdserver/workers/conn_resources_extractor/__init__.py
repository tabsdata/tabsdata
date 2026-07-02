#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from pathlib import Path

from tabsdatak import _connectors
from tabsdatak._logging import setup_logging
from tabsdatak.common._resources.reader import td_resource
from tabsdatak.common._runtime.worker import Worker
from tabsdatak.common._runtime.worker.error import ExitCode
from tabsdatak.server._tdserver.workers.conn_resources_extractor.error import (
    ConnResourcesExtractorErrorCode,
)

logger = logging.getLogger(__name__)

PROG = "td_conn_resources_extractor"
DESCRIPTION = "Extract every available connector plugin's resources into a directory."

ARG_OUTPUT_DIR = "--output-dir"
ARG_LOG_CONFIG = "--log-config"
OUTPUT_DIR_HELP = (
    "Directory the connector resources are extracted into (created if "
    "absent). Each connector's icon ('.svg') and a metadata file ('.json', "
    "holding the connector's 'system') are written directly into it."
)
LOG_CONFIG_HELP = "Path to a logging configuration YAML for this worker."
METAVAR_PATH = "PATH"

SVG_SUFFIX = ".svg"
JSON_SUFFIX = ".json"


def plugin_class(definition: object) -> type:
    """The plugin's user-facing class for a registry def.

    ``SrcDef`` / ``StreamSrcDef`` expose it as ``src``; ``DestDef`` as
    ``dest``. This class' fully-qualified ``<module>.<qualname>`` is the
    identity the extracted icon file is named after.
    """
    return getattr(definition, "src", None) or getattr(definition, "dest", None)


def resource_stem(plugin: type) -> str:
    """``<module>.<class>`` of the plugin with dots turned into dashes
    (e.g. ``tabsdatak-conn-mysql-MySQLSrc``). The extracted icon and its
    metadata file share this stem, differing only by suffix."""
    return f"{plugin.__module__}.{plugin.__qualname__}".replace(".", "-")


def icon_filename(plugin: type) -> str:
    """Icon file name for the plugin
    (e.g. ``tabsdatak-conn-mysql-MySQLSrc.svg``)."""
    return resource_stem(plugin) + SVG_SUFFIX


def metadata_filename(plugin: type) -> str:
    """Metadata file name for the plugin
    (e.g. ``tabsdatak-conn-mysql-MySQLSrc.json``)."""
    return resource_stem(plugin) + JSON_SUFFIX


def available_definitions() -> list:
    """Every registered connector def -- batch sources, stream sources and
    destinations -- discovered through the ``tabsdatak.connectors`` registry."""
    definitions: list = []
    definitions.extend(_connectors.avail_srcs().values())
    definitions.extend(_connectors.avail_stream_srcs().values())
    definitions.extend(_connectors.avail_dests().values())
    return definitions


class ConnResourcesExtractor(Worker):
    """Extract each available connector plugin's declared icon into
    ``output_dir``, alongside a ``.json`` metadata file holding the
    connector's ``system``. Both files are named after the plugin's full
    ``<module>.<class>`` (dots -> dashes), differing only by suffix
    (``.svg`` / ``.json``). Defs that declare no icon
    (``def.icon is None``) are skipped."""

    def __init__(self, output_dir: Path) -> None:
        super().__init__()
        self.output_dir = output_dir
        self.copied = 0
        self.skipped = 0
        self.failures: list[str] = []

    def perform(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        for definition in available_definitions():
            plugin = plugin_class(definition)
            name = f"{plugin.__module__}.{plugin.__qualname__}"
            icon = getattr(definition, "icon", None)
            if icon is None:
                self.skipped += 1
                logger.debug(f"No icon declared for {name}; skipping.")
                continue
            icon_target = self.output_dir / icon_filename(plugin)
            metadata_target = self.output_dir / metadata_filename(plugin)
            try:
                source = td_resource(icon)
                shutil.copyfile(source, icon_target)
                metadata = {"system": getattr(definition, "system", None)}
                metadata_target.write_text(json.dumps(metadata, indent=2) + "\n")
            except (FileNotFoundError, OSError) as error:
                self.failures.append(f"{name} ('{icon}'): {error}")
                logger.error(
                    f"Could not extract the resources for {name} from '{icon}': "
                    f"{error}."
                )
                continue
            self.copied += 1
            logger.info(
                f"Extracted '{icon}' -> {icon_target.name} "
                f"(+ {metadata_target.name})."
            )
        if self.failures:
            raise ConnResourcesExtractorErrorCode.CONN_RESOURCES_1.exception(
                count=len(self.failures),
                details="; ".join(self.failures),
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=PROG, description=DESCRIPTION)
    parser.add_argument(
        ARG_OUTPUT_DIR,
        required=True,
        metavar=METAVAR_PATH,
        help=OUTPUT_DIR_HELP,
    )
    parser.add_argument(
        ARG_LOG_CONFIG,
        required=False,
        metavar=METAVAR_PATH,
        help=LOG_CONFIG_HELP,
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.log_config:
        setup_logging(args.log_config)
    worker = ConnResourcesExtractor(output_dir=Path(args.output_dir))
    logger.info(f"Extracting connector resources into {worker.output_dir}.")
    code = worker.run()
    if code == ExitCode.SUCCESS:
        logger.info(
            f"Extracted resources for {worker.copied} connector(s); skipped "
            f"{worker.skipped} def(s) with no icon."
        )
    else:
        logger.warning(
            f"Connector resource extraction finished with status {code}: copied "
            f"{worker.copied}, skipped {worker.skipped}, failed {len(worker.failures)}."
        )
    return int(code)


if __name__ == "__main__":
    sys.exit(main())
