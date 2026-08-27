#
# Copyright 2026 Tabsdata Inc.
#

"""Function-run `request.yaml` model.

Loads the resolved function-run request that the API Server writes for
function run, then validates it against the pydantic models below.

The wire format wraps the payload in two variant envelopes (single-key
dicts whose key is the Rust enum variant tag):

* a top-level envelope (the request variant);
* a per-table envelope on every entry in
  ``input``/``output``/``system_input``/``system_output``.

For each envelope we track two sets:

* a ``*_SUPPORTED`` tuple: variants this code actually handles today.
  Anything outside it is rejected.
* a ``*_KNOWN`` tuple: variants the wire format defines (mirrors the
  Rust enum). A tag in ``KNOWN`` but not in ``SUPPORTED`` is rejected
  with a "recognized but not yet supported" message; a tag in neither
  is rejected with "unknown variant; expected one of: ...".

Today the supported sets are ``Function``, and per-table ``Table``
everywhere plus ``TableVersions`` on ``input`` (a multi-version
dependency: one entry per positional argument whose ``versions`` list
carries one Table-shaped member per resolved version, ordered by
``version_pos``).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

from tabsdatak.error import ErrorCode

REQUEST_ENVELOPE_SUPPORTED = ("Function", "Streamer")
REQUEST_ENVELOPE_KNOWN = REQUEST_ENVELOPE_SUPPORTED

TABLE_VARIANT_SUPPORTED = ("Table",)
INPUT_TABLE_VARIANT_SUPPORTED = ("Table", "TableVersions")
TABLE_VARIANT_KNOWN = (
    "Table",
    "TableVersions",
    "PartitionedTable",
    "PartitionedTableVersions",
)


class InvalidRequestError(ValueError):
    """The request.yaml is malformed or fails schema validation."""


RAW_SUFFIX = ".t"


class Location(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    uri: str
    env_prefix: str | None = None


class InputTable(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    collection_id: str | int
    collection: str
    table_id: str | int
    table_version_id: str | int
    execution_id: str | int | None = None
    transaction_id: str | int | None = None
    function_run_id: str | int | None = None
    triggered_on: int | None = None
    table_data_version_id: str | int | None = None
    location: Location | None = None
    input_idx: int
    table_pos: int
    version_pos: int


class InputTableVersions(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    is_empty: bool
    versions: tuple[InputTable, ...]


def input_tables(entry: InputTable | InputTableVersions) -> tuple[InputTable, ...]:
    if isinstance(entry, InputTableVersions):
        return entry.versions
    return (entry,)


class OutputTable(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    collection_id: str | int
    collection: str
    table_id: str | int
    table_version_id: str | int
    table_data_version_id: str | int
    location: Location
    table_pos: int


class Info(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    project_id: str | int
    collection_id: str | int
    collection: str
    function_version_id: str | int
    function_defined_on: int
    function: str
    function_run_id: str | int
    function_bundle: Location
    function_stage: Location
    triggered_on: int
    transaction_id: str | int
    execution_id: str | int
    execution_name: str | None = None
    function_data: Location
    function_raw: Location | None = None
    scheduled_on: int

    @property
    def triggered_on_datetime(self) -> datetime:
        """`triggered_on` as a timezone-aware UTC datetime.

        The wire format is epoch milliseconds; consumers that need a
        Python datetime (e.g. parquet timestamp columns) read this
        instead of converting at every call site.
        """
        return datetime.fromtimestamp(self.triggered_on / 1000, tz=timezone.utc)

    @property
    def triggered_on_iso(self) -> str:
        """`triggered_on` as an ISO-8601 string (UTC).

        Used to satisfy the ``TrxCtx.exec_plan_ts`` contract.
        """
        return self.triggered_on_datetime.isoformat()


class Request(BaseModel):
    """Resolved request.yaml for a function-run."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    info: Info
    input: tuple[InputTable | InputTableVersions, ...] = ()
    output: tuple[OutputTable, ...] = ()
    system_input: tuple[InputTable, ...] = ()
    system_output: tuple[OutputTable, ...] = ()


class StreamerInfo(BaseModel):
    """Streamer-pod request envelope shape.

    Deliberately leaner than :class:`Info`: a streamer is a long-lived
    worker that produces batches (each becoming its own function run
    downstream via ``fn_trigger``), so the per-execution identifiers
    (``execution_id`` / ``transaction_id`` / ``function_run_id`` /
    ``triggered_on``) on :class:`Info` do not represent real entities
    at the streamer's lifecycle. They are intentionally absent here.

    The apiserver's resolve endpoint builds this payload from DB state;
    the stream-publisher pod fetches it (like the function pod) and
    writes it to ``request.yaml``. Callbacks (``fn_trigger`` etc.) are
    not part of this payload — the streamer gets them via env from the
    pod Secret.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    project_id: str | int
    collection_id: str | int
    collection: str
    function_version_id: str | int
    function_defined_on: int
    function: str
    function_bundle: Location
    function_stage: Location
    scheduled_on: int
    worker_id: str | int


class StreamerRequest(BaseModel):
    """Resolved request.yaml for a streamer pod (``Streamer`` envelope)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    info: StreamerInfo


def _strip_variant(
    value: object,
    supported: tuple[str, ...],
    known: tuple[str, ...],
    where: str,
) -> dict:
    if not isinstance(value, dict) or len(value) != 1:
        raise InvalidRequestError(
            f"{where}: expected single-key variant envelope, got {type(value).__name__}"
        )
    [(tag, payload)] = value.items()
    if tag not in supported:
        if tag in known:
            raise InvalidRequestError(
                f"{where}: variant {tag!r} is recognized but not yet supported"
            )
        raise InvalidRequestError(
            f"{where}: unknown variant {tag!r}; expected one of: {', '.join(known)}"
        )
    if not isinstance(payload, dict):
        raise InvalidRequestError(f"{where}: variant {tag!r} payload must be a mapping")
    return payload


def _strip_variants(
    entries: object,
    slot: str,
    supported: tuple[str, ...],
) -> list[dict]:
    if entries is None:
        return []
    if not isinstance(entries, list):
        raise InvalidRequestError(
            f"{slot}: expected a list, got {type(entries).__name__}"
        )
    return [
        _strip_variant(
            entry,
            supported,
            TABLE_VARIANT_KNOWN,
            f"{slot}[{index}]",
        )
        for index, entry in enumerate(entries)
    ]


def load_request(path: Path) -> Request:
    """Load and validate a function-run request.yaml.

    Validates and strips the request envelope and each per-table
    envelope (see the module docstring for the supported/known model)
    before validating the inner payload against :class:`Request`.

    Raises:
        FileNotFoundError: if `path` does not exist.
        InvalidRequestError: if the YAML is malformed or the structure
            fails schema validation (missing required fields, wrong
            types, unknown fields, unsupported variant tags).
    """
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise InvalidRequestError(f"malformed YAML in {path}: {exc}") from exc

    try:
        payload = _strip_variant(
            raw,
            ("Function",),
            REQUEST_ENVELOPE_KNOWN,
            "request",
        )
        if "input" in payload:
            payload["input"] = _strip_variants(
                payload["input"], "input", INPUT_TABLE_VARIANT_SUPPORTED
            )
        for slot in ("output", "system_input", "system_output"):
            if slot in payload:
                payload[slot] = _strip_variants(
                    payload[slot], slot, TABLE_VARIANT_SUPPORTED
                )
    except InvalidRequestError as exc:
        raise InvalidRequestError(
            f"invalid request structure in {path}: {exc}"
        ) from exc

    try:
        return Request.model_validate(payload)
    except ValidationError as exc:
        raise InvalidRequestError(
            f"invalid request structure in {path}: {exc}"
        ) from exc


def load_streamer_request(path: Path) -> StreamerRequest:
    """Load and validate a streamer-pod request.yaml (``Streamer``
    envelope).

    Sibling of :func:`load_request` for the streamer pod: a different
    top-level envelope (``Streamer`` instead of ``Function``) and a
    leaner :class:`StreamerInfo` payload (no input/output tables, no
    per-execution identifiers).

    Raises:
        FileNotFoundError: if `path` does not exist.
        InvalidRequestError: if the YAML is malformed or fails schema
            validation.
    """
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise InvalidRequestError(f"malformed YAML in {path}: {exc}") from exc

    try:
        payload = _strip_variant(
            raw,
            ("Streamer",),
            REQUEST_ENVELOPE_KNOWN,
            "request",
        )
    except InvalidRequestError as exc:
        raise InvalidRequestError(
            f"invalid streamer request structure in {path}: {exc}"
        ) from exc

    try:
        return StreamerRequest.model_validate(payload)
    except ValidationError as exc:
        raise InvalidRequestError(
            f"invalid streamer request structure in {path}: {exc}"
        ) from exc


def load_streamer_request_or_raise(
    path: Path, error_code: ErrorCode
) -> StreamerRequest:
    """:func:`load_streamer_request` with a per-command typed-error wrap.
    Sibling of :func:`load_request_or_raise` for the streamer pod."""
    try:
        return load_streamer_request(path)
    except (FileNotFoundError, InvalidRequestError) as exc:
        raise error_code.exception(cause=exc, path=path, detail=str(exc))


class _AnyRequestInfo(BaseModel):
    """Just-enough-fields view of either request envelope's ``info``.

    Used by tdrunner commands that are shared between the
    function-publisher pod (``Function`` envelope) and the
    stream-publisher pod (``Streamer`` envelope) and only need the
    handful of fields both shapes expose -- e.g.

    * ``localize_fn_bundle`` reads ``info.function_bundle.uri``;
    * ``run_manifest_trigger`` reads ``info.function_stage``.

    ``extra="ignore"`` lets either envelope's full payload validate
    against this slim view without touching the per-envelope models.
    """

    model_config = ConfigDict(frozen=True, extra="ignore")

    function_bundle: Location
    function_stage: Location


class AnyRequest(BaseModel):
    """Envelope-agnostic request view exposing only ``info.function_bundle``.

    Loaded by :func:`load_any_request` when the caller is shared between
    function-publisher and stream-publisher pods.
    """

    model_config = ConfigDict(frozen=True, extra="ignore")

    info: _AnyRequestInfo


def load_any_request(path: Path) -> AnyRequest:
    """Load either a ``Function`` or a ``Streamer`` envelope and
    return the envelope-agnostic slim view :class:`AnyRequest`.

    For tdrunner commands that participate in both pod profiles and
    only need a small subset of the envelope (today: just
    ``function_bundle`` for ``localize_fn_bundle``).
    """
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise InvalidRequestError(f"malformed YAML in {path}: {exc}") from exc

    try:
        payload = _strip_variant(
            raw,
            ("Function", "Streamer"),
            REQUEST_ENVELOPE_KNOWN,
            "request",
        )
    except InvalidRequestError as exc:
        raise InvalidRequestError(
            f"invalid request structure in {path}: {exc}"
        ) from exc

    try:
        return AnyRequest.model_validate(payload)
    except ValidationError as exc:
        raise InvalidRequestError(
            f"invalid request structure in {path}: {exc}"
        ) from exc


def load_any_request_or_raise(path: Path, error_code: ErrorCode) -> AnyRequest:
    """:func:`load_any_request` with a per-command typed-error wrap."""
    try:
        return load_any_request(path)
    except (FileNotFoundError, InvalidRequestError) as exc:
        raise error_code.exception(cause=exc, path=path, detail=str(exc))


def load_request_or_raise(path: Path, error_code: ErrorCode) -> Request:
    """:func:`load_request` with a per-command typed-error wrap.

    Catches ``FileNotFoundError`` and :class:`InvalidRequestError` and
    re-raises via ``error_code.exception(cause=exc, path=path,
    detail=str(exc))``. The bound exception (a
    :class:`~tabsdatak.server._tdrunner.function._lib.errors.CommandException`
    subclass) carries the per-command ``exit_code`` on its class;
    :func:`~tabsdatak.server._tdrunner.function._lib.command.run_command`
    maps it to the process exit code.

    Templates that bind ``error_code`` can reference ``{path}`` and
    ``{detail}`` as placeholders.
    """
    try:
        return load_request(path)
    except (FileNotFoundError, InvalidRequestError) as exc:
        raise error_code.exception(cause=exc, path=path, detail=str(exc))
