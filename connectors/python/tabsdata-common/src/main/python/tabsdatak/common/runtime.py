#
# Copyright 2026 Tabsdata Inc.
#

"""What a Tabsdata service process outside this distribution starts with: its
logging, its PATH, its configuration's ``${env:...}`` references and
materialized secrets, and the name of the distribution whose version it reports
(``NO_VERSION`` when it is not installed)."""

from tabsdatak._logging import setup_logging
from tabsdatak.common._configuration.materializer.secret import (
    materialize as materialize_secrets,
)
from tabsdatak.common._configuration.resolver.env import resolve as resolve_env
from tabsdatak.common._constants import NO_VERSION, TABSDATA_MODULE_NAME
from tabsdatak.common._runtime.pathenv import extend_path

__all__ = [
    "NO_VERSION",
    "TABSDATA_MODULE_NAME",
    "extend_path",
    "materialize_secrets",
    "resolve_env",
    "setup_logging",
]
