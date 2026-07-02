from __future__ import annotations

from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
    TabsdataException,
)


class CommonException(TabsdataException):
    """Top-level base exception for the ``tabsdatak.common`` package."""


class CommonRuntimeException(CommonException):
    """Runtime failure in shared helpers (e.g. malformed inputs to the
    JSONPath walker)."""


class CommonInitException(CommonException):
    """Configuration / construction-time failure in shared helpers (e.g.
    a format that cannot be resolved before any work starts)."""


class CommonErrorCode(ErrorCode):
    # --- JSONPath parsing (tabsdatak.common._json_serde) ------------
    COMMON_1 = ErrorDef(
        CommonRuntimeException,
        "path must start with '$': {path!r}",
    )  # _parse_path: missing root marker
    COMMON_2 = ErrorDef(
        CommonRuntimeException,
        "empty key at {rest!r} in {path!r}",
    )  # _parse_path: '.' not followed by a key
    COMMON_3 = ErrorDef(
        CommonRuntimeException,
        "unclosed '[' in {path!r}",
    )  # _parse_path: '[' without matching ']'
    COMMON_4 = ErrorDef(
        CommonRuntimeException,
        "unexpected char {ch!r} at index {index} in {path!r}",
    )  # _parse_path: token doesn't start with '.' or '['
    COMMON_5 = ErrorDef(
        CommonRuntimeException,
        "cannot replace the root node via '$'",
    )  # replace_at: empty token list (path is just '$')

    # --- secret/var hydration (tabsdatak.common._json_serde) -------
    COMMON_6 = ErrorDef(
        CommonRuntimeException,
        "values missing for keys: {keys!r}",
    )  # hydrate / env_hydrate: one or more values is None / env var unset
    COMMON_7 = ErrorDef(
        CommonRuntimeException,
        "keys mismatch (missing={missing!r}, extra={extra!r})",
    )  # hydrate: provided keys differ from keys derived from data

    # --- connection hydration (tabsdatak.common._json_serde) -------
    COMMON_8 = ErrorDef(
        CommonRuntimeException,
        "no connection definition supplied for name {name!r}",
    )  # hydrate_connections: connectionRef name has no entry in conn_by_name

    # --- bundle production (tabsdatak.common._archive.bundle) -------
    # Raised by `build_bundle` and its helpers at registration time
    # when filesystem or environment preconditions aren't met. These
    # are runtime conditions (path doesn't exist, version mismatch),
    # not type / value bugs at object construction.
    COMMON_9 = ErrorDef(
        CommonRuntimeException,
        "project_root is not a directory: {path}",
    )  # build_bundle: project_root argument doesn't resolve to a directory
    COMMON_10 = ErrorDef(
        CommonRuntimeException,
        "local package is not a directory: {path}",
    )  # collect_local_packages: entry in local_packages isn't a directory
    COMMON_11 = ErrorDef(
        CommonRuntimeException,
        "python {version} not in allowed set: {allowed}",
    )  # validate_python_version: producer's interpreter not whitelisted
