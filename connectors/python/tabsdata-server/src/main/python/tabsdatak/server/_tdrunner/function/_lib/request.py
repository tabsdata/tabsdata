#
# Copyright 2026 Tabs Data Inc.
#

"""Publisher `request.yaml` model.

Loads the resolved function-run request that the API Server writes for a
publisher run. Pydantic handles validation; the YAML is plain (no custom
tags) and parsed with `yaml.safe_load`.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

from tabsdatak.server._tdrunner.function._lib.errors import CommandSpecificError


class InvalidRequestError(ValueError):
    """The request.yaml is malformed or fails schema validation."""


class Location(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    uri: str
    env_prefix: str | None = None


class Table(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    location: Location | None = None


class Info(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    collection: str
    collection_id: str | int
    execution_id: str | int
    transaction_id: str | int
    function_run_id: str | int
    function_bundle: Location


class Request(BaseModel):
    """Publisher-shaped subset of the resolved request.yaml."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    info: Info
    input: tuple[Table, ...] = ()
    output: tuple[Table, ...] = ()
    system_input: tuple[Table, ...] = ()
    system_output: tuple[Table, ...] = ()


def load_request(path: Path) -> Request:
    """Load and validate a publisher request.yaml.

    Raises:
        FileNotFoundError: if `path` does not exist.
        InvalidRequestError: if the YAML is malformed or the structure
            fails schema validation (missing required fields, wrong
            types, unknown fields).
    """
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise InvalidRequestError(f"malformed YAML in {path}: {exc}") from exc
    try:
        return Request.model_validate(raw)
    except ValidationError as exc:
        raise InvalidRequestError(
            f"invalid request structure in {path}: {exc}"
        ) from exc


def load_request_or_fail(path: Path, exit_code: int) -> Request:
    """:func:`load_request` with the canonical command-side error wrap.

    Every command that takes a ``--request`` flag has the same try/except
    boilerplate around :func:`load_request`: catch ``FileNotFoundError``
    and :class:`InvalidRequestError`, and re-raise as
    :class:`~tabsdatak.server._tdrunner.function._lib.errors.CommandSpecificError`
    so :func:`~tabsdatak.server._tdrunner.function._lib.cli.run_command` can map
    it to an exit code. This helper folds that boilerplate into one call.

    ``exit_code`` is passed by the caller so each command keeps its own
    numbering scheme (the convention is ``EXIT_BAD_REQUEST = 10``, but
    the helper doesn't enforce it).
    """
    try:
        return load_request(path)
    except (FileNotFoundError, InvalidRequestError) as exc:
        raise CommandSpecificError(
            exit_code, f"unreadable request.yaml: {exc}"
        ) from exc
