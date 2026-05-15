#
# Copyright 2026 Tabs Data Inc.
#

"""Publisher `request.yaml` model.

Loads the resolved function-run request that the API Server writes for a
publisher run and exposes only the fields the publisher commands actually
read. The shape is intentionally narrower than the legacy tabsdata
`FunctionV2` definition — telemetry-only and tabsdata-internal fields are
not exposed.

The YAML is plain (no custom tags); `yaml.safe_load` is used directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


class InvalidRequestError(ValueError):
    """The request.yaml is malformed or missing required fields."""


@dataclass(frozen=True)
class Location:
    uri: str
    env_prefix: str | None = None


@dataclass(frozen=True)
class Table:
    name: str
    location: Location | None = None


@dataclass(frozen=True)
class Request:
    """Publisher-shaped subset of the resolved request.yaml."""

    collection: str
    collection_id: str | int
    execution_id: str | int
    transaction_id: str | int
    function_run_id: str | int
    function_bundle: Location
    input: tuple[Table, ...]
    output: tuple[Table, ...]
    system_input: tuple[Table, ...]
    system_output: tuple[Table, ...]


def load_request(path: Path) -> Request:
    """Load and validate a publisher request.yaml.

    Raises:
        FileNotFoundError: if `path` does not exist.
        InvalidRequestError: if the YAML is malformed, the document is
            not a mapping, or any required field is missing or has the
            wrong type.
    """
    text = path.read_text(encoding="utf-8")
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise InvalidRequestError(f"malformed YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise InvalidRequestError("request must be a mapping")
    info = _require(raw, "info", dict, dotted_path="info")
    return Request(
        collection=_require(
            info, "collection", str, dotted_path="info.collection"
        ),
        collection_id=_require(
            info, "collection_id", str, int, dotted_path="info.collection_id"
        ),
        execution_id=_require(
            info, "execution_id", str, int, dotted_path="info.execution_id"
        ),
        transaction_id=_require(
            info, "transaction_id", str, int, dotted_path="info.transaction_id"
        ),
        function_run_id=_require(
            info, "function_run_id", str, int, dotted_path="info.function_run_id"
        ),
        function_bundle=_parse_location(
            _require(
                info, "function_bundle", dict, dotted_path="info.function_bundle"
            ),
            "info.function_bundle",
        ),
        input=_parse_tables(raw.get("input", []), "input"),
        output=_parse_tables(raw.get("output", []), "output"),
        system_input=_parse_tables(raw.get("system_input", []), "system_input"),
        system_output=_parse_tables(raw.get("system_output", []), "system_output"),
    )


def _require(parent: dict, key: str, *types: type, dotted_path: str) -> Any:
    if key not in parent:
        raise InvalidRequestError(f"missing required field {dotted_path!r}")
    val = parent[key]
    if not isinstance(val, types):
        names = " or ".join(t.__name__ for t in types)
        raise InvalidRequestError(
            f"field {dotted_path!r} must be {names}, "
            f"got {type(val).__name__}"
        )
    return val


def _parse_location(raw: dict, dotted_path: str) -> Location:
    uri = _require(raw, "uri", str, dotted_path=f"{dotted_path}.uri")
    env_prefix = raw.get("env_prefix")
    if env_prefix is None:
        return Location(uri=uri, env_prefix=None)
    if not isinstance(env_prefix, str):
        raise InvalidRequestError(
            f"{dotted_path}.env_prefix must be a string or null, "
            f"got {type(env_prefix).__name__}"
        )
    return Location(uri=uri, env_prefix=env_prefix)


def _parse_tables(raw: object, dotted_path: str) -> tuple[Table, ...]:
    if not isinstance(raw, list):
        raise InvalidRequestError(
            f"{dotted_path!r} must be a list, got {type(raw).__name__}"
        )
    return tuple(
        _parse_table(item, f"{dotted_path}[{idx}]")
        for idx, item in enumerate(raw)
    )


def _parse_table(raw: object, dotted_path: str) -> Table:
    if not isinstance(raw, dict):
        raise InvalidRequestError(
            f"{dotted_path} must be a mapping, got {type(raw).__name__}"
        )
    name = _require(raw, "name", str, dotted_path=f"{dotted_path}.name")
    location_raw = raw.get("location")
    if location_raw is None:
        return Table(name=name, location=None)
    if not isinstance(location_raw, dict):
        raise InvalidRequestError(
            f"{dotted_path}.location must be a mapping or null, "
            f"got {type(location_raw).__name__}"
        )
    return Table(
        name=name,
        location=_parse_location(location_raw, f"{dotted_path}.location"),
    )
