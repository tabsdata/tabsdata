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

    # --- secret hydration (tabsdatak.common._json_serde) -----------
    COMMON_6 = ErrorDef(
        CommonRuntimeException,
        "secret values missing for keys: {keys!r}",
    )  # hydrate_secrets: one or more KeyValue.value is None
    COMMON_7 = ErrorDef(
        CommonRuntimeException,
        "secret keys mismatch (missing={missing!r}, extra={extra!r})",
    )  # hydrate_secrets: provided keys differ from keys derived from data

    # --- connection hydration (tabsdatak.common._json_serde) -------
    COMMON_8 = ErrorDef(
        CommonRuntimeException,
        "no connection definition supplied for name {name!r}",
    )  # hydrate_connections: connectionRef name has no entry in conn_by_name
    COMMON_9 = ErrorDef(
        CommonRuntimeException,
        "connectionRef name env var {var!r} is not set",
    )  # _resolve_connection_name: envRef-typed name references an unset var
    COMMON_10 = ErrorDef(
        CommonRuntimeException,
        "connectionRef name has unsupported shape: {name!r}",
    )  # _resolve_connection_name: name is neither str nor envRef/envLiteral envelope
