#
#  Copyright 2026 Tabs Data Inc.
#

"""A mypy plugin that forbids logging configuration calls inside ``tabsdatak``.

The ``tabsdatak`` package must not configure logging: log levels, handlers, and filters
are owned by the host process. This plugin flags calls such as ``logger.setLevel(...)``
or ``handler.addHandler(...)`` whose receiver mypy has resolved to a
``logging.Logger`` / ``logging.Handler`` (so unrelated classes with a same-named method
are not flagged).

**Suppressions**

Individual call sites may opt out with the standard mypy idiom::

    logger.setLevel(logging.DEBUG)  # type: ignore[tabsdatax-logging]

Whole files may be allowlisted via a dedicated config section ``[tabsdatax.logging]``
using glob patterns matched against the checked file path::

    [tabsdatax.logging]
    allowlist =
        **/tabsdatak/_logging.py

The allowlist is empty by default so the rule fails loudly on its first real target;
known offenders should be added deliberately.
"""

from __future__ import annotations

import configparser
import fnmatch
from typing import Callable

from mypy.checker import TypeChecker
from mypy.errorcodes import ErrorCode
from mypy.options import Options
from mypy.plugin import MethodContext, Plugin
from mypy.types import Instance, Type as MypyType

LOG_CONFIG_CODE = ErrorCode(
    "tabsdatax-logging",
    "Code from 'tabsdatak' module must not configure logging "
    "(levels, handlers, filters)",
    "tabsdatax",
)

FORBIDDEN_BY_BASE: dict[str, frozenset[str]] = {
    "logging.Logger": frozenset({
        "setLevel",
        "addHandler",
        "removeHandler",
        "addFilter",
        "removeFilter",
    }),
    "logging.Handler": frozenset({
        "setLevel",
        "setFormatter",
        "addFilter",
        "removeFilter",
    }),
    "logging.Manager": frozenset({"disable"}),
}

ALL_FORBIDDEN_NAMES: frozenset[str] = frozenset().union(*FORBIDDEN_BY_BASE.values())

TABSDATAK_SEGMENT = "/tabsdatak/"

CONFIG_SECTION = "tabsdatax.logging"


def is_tabsdatak_path(path: str) -> bool:
    normalized = path.replace("\\", "/")
    return (
        TABSDATAK_SEGMENT in normalized
        or normalized.startswith("tabsdatak/")
        or normalized == "tabsdatak"
        or normalized.endswith("/tabsdatak")
    )


def load_allowlist(config_file: str | None) -> tuple[str, ...]:
    if not config_file:
        return ()
    parser = configparser.ConfigParser()
    try:
        parser.read(config_file)
    except configparser.Error:
        return ()
    if not parser.has_section(CONFIG_SECTION):
        return ()
    raw = parser.get(CONFIG_SECTION, "allowlist", fallback="")
    patterns = [line.strip() for line in raw.splitlines()]
    return tuple(pattern for pattern in patterns if pattern)


def path_is_allowlisted(path: str, patterns: tuple[str, ...]) -> bool:
    if not patterns:
        return False
    normalized = path.replace("\\", "/")
    return any(fnmatch.fnmatchcase(normalized, pattern) for pattern in patterns)


class LoggingPlugin(Plugin):
    def __init__(self, options: Options) -> None:
        super().__init__(options)
        self.allowlist = load_allowlist(options.config_file)

    def get_method_hook(
        self, fullname: str
    ) -> Callable[[MethodContext], MypyType] | None:
        method_name = fullname.rsplit(".", 1)[-1]
        if method_name not in ALL_FORBIDDEN_NAMES:
            return None
        return self.check_log_config_call

    def check_log_config_call(self, context: MethodContext) -> MypyType:
        receiver = context.type
        if not isinstance(receiver, Instance):
            return context.default_return_type
        matched_base, forbidden_for_type = self.match_forbidden_base(receiver)
        if matched_base is None or forbidden_for_type is None:
            return context.default_return_type
        api = context.api
        if not isinstance(api, TypeChecker):
            return context.default_return_type
        method_name = self.method_name_from_context(context, forbidden_for_type)
        if method_name is None:
            return context.default_return_type
        current_path = api.path
        if not is_tabsdatak_path(current_path):
            return context.default_return_type
        if path_is_allowlisted(current_path, self.allowlist):
            return context.default_return_type
        api.fail(
            f"Code from tabsdatak must not configure logging: "
            f"{matched_base}.{method_name}() is forbidden here",
            context.context,
            code=LOG_CONFIG_CODE,
        )
        return context.default_return_type

    @staticmethod
    def match_forbidden_base(
        receiver: Instance,
    ) -> tuple[str | None, frozenset[str] | None]:
        for base_type in receiver.type.mro:
            forbidden_for_type = FORBIDDEN_BY_BASE.get(base_type.fullname)
            if forbidden_for_type is not None:
                return base_type.fullname, forbidden_for_type
        return None, None

    @staticmethod
    def method_name_from_context(
        context: MethodContext, allowed: frozenset[str]
    ) -> str | None:
        callee = getattr(context.context, "callee", None)
        attribute = getattr(callee, "name", None)
        if attribute in allowed:
            return attribute
        return None


def plugin(_version: str) -> type[LoggingPlugin]:
    return LoggingPlugin
