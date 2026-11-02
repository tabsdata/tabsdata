#
# Copyright 2026 Tabsdata Inc.
#

"""probe runner: one connector Explorer call against one connection.

The request (``--request``, JSON) is::

    {"probe_id": str, "kind": "check" | "metadata" | "sample",
     "params": {"entity": fqn, "limit": int},   # sample only
     "entity_doc": {...},                        # sample only: the catalog doc
     "entity_docs": [{...}, ...],                # a sample of several entities
     "max_entities": int,                        # metadata only
     "max_entities_bytes": int}                  # metadata only

The outcome is the ``output`` of the ``Done`` responder callback::

    {"probe_id": str, "kind": str,
     "status": "succeeded" | "failed" | "timeout",
     "result": {...},                            # succeeded only
     "error": {"code": str, "message": str}}     # failed / timeout only

with ``result`` per kind:

* ``check``: ``{"ok": bool, "error": str}`` (``error`` only when not ok). A
  refused connection is a succeeded probe that reports ``ok: false``.
* ``metadata``: ``{"schema_hash", "entity_count", "truncated", "entities"}``,
  each entity ``{"fqn", "entity_kind", "field_names", "field_count", "doc"}``.
  ``schema_hash`` covers every entity; past ``max_entities``, or past
  ``max_entities_bytes`` of serialized rows, only the first entities by fqn
  are kept, ``entity_count`` counts the kept ones and ``truncated`` is set.
* ``sample``: ``{"entity", "columns", "rows", "truncated"}``; with
  ``entity_docs``, ``{"samples": [...]}``, one such object per entity (or
  ``{"entity", "error"}`` for one that failed), all within
  :data:`MAX_SAMPLE_BYTES`.

A connection whose connector has no explorer, or a sample the explorer cannot
serve, succeeds with ``{"supported": false}``.

Every error text leaving the pod has the connection's secret values
(``SECRET_CONN__*``, also URL-encoded and JSON-escaped; values shorter than
:data:`MIN_SECRET_CHARS` are left alone, they would scrub ordinary words)
scrubbed and is capped at :data:`MAX_ERROR_BYTES`. Sample values that are not
finite floats are sent as strings: the outcome is strict JSON.
"""

from __future__ import annotations

import argparse
import contextvars
import json
import logging
import math
import os
import threading
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from tabsdatak._connectors import explorer_for_conn
from tabsdatak.common._catalog import schema_fingerprint

# noinspection PyProtectedMember
from tabsdatak.common._json_serde import _SECRET_KEY_PREFIX
from tabsdatak.server._tdrunner.function._commons.run.command import notify_status_done
from tabsdatak.server._tdrunner.function._lib.connection import load_connection
from tabsdatak.server._tdrunner.function._lib.journal import Journal
from tabsdatak.server._tdrunner.probe.error import ProbeErrorCode
from tabsdatak.spi import Conn, Entity, Explorer

logger = logging.getLogger(__name__)

KIND_CHECK = "check"
KIND_METADATA = "metadata"
KIND_SAMPLE = "sample"

TIMEOUT_SECS = {KIND_CHECK: 20.0, KIND_METADATA: 90.0, KIND_SAMPLE: 30.0}

STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_TIMEOUT = "timeout"

SCRUBBED = "***"
MAX_ERROR_BYTES = 2048
MIN_SECRET_CHARS = 4

DEFAULT_MAX_ENTITIES = 20_000
DEFAULT_SAMPLE_ROWS = 20
MAX_SAMPLE_ROWS = 100
MAX_SAMPLE_BYTES = 1024 * 1024


class ProbeTimeout(Exception):
    pass


def run(args: argparse.Namespace) -> int:
    journal_path = Path(args.journal_file)
    journal = Journal(start=now_ms())
    journal.save(journal_path)
    request = load_request(Path(args.request))
    outcome = run_probe(request, Path(args.connection_file))
    logger.info(
        "%s probe %s: %s", outcome["kind"], outcome["probe_id"], outcome["status"]
    )
    journal.end = now_ms()
    journal.save(journal_path)
    notify_status_done(args, output=outcome)
    return 0


def run_probe(request: dict, connection_file: Path) -> dict:
    kind = request["kind"]
    timeout = TIMEOUT_SECS[kind]
    outcome: dict[str, Any] = {"probe_id": request.get("probe_id"), "kind": kind}
    try:
        conn = load_connection(connection_file)
        explorer_cls = explorer_for_conn(conn)
        if explorer_cls is None:
            result = {"supported": False}
        elif kind == KIND_CHECK:
            result = _check(explorer_cls(), conn, timeout)
        elif kind == KIND_METADATA:
            max_entities = int(request.get("max_entities") or DEFAULT_MAX_ENTITIES)
            max_bytes = request.get("max_entities_bytes")
            result = _metadata(
                explorer_cls(),
                conn,
                timeout,
                max_entities,
                None if max_bytes is None else int(max_bytes),
            )
        else:
            result = _sample(explorer_cls(), conn, timeout, request)
    except ProbeTimeout as error:
        outcome["status"] = STATUS_TIMEOUT
        outcome["error"] = {"code": STATUS_TIMEOUT, "message": _describe(error)}
        return outcome
    except BaseException as error:  # noqa: BLE001 - reported, scrubbed, never raised
        logger.error("%s probe failed: %s", kind, _describe(error))
        outcome["status"] = STATUS_FAILED
        outcome["error"] = {"code": f"{kind}_failed", "message": _describe(error)}
        return outcome
    outcome["status"] = STATUS_SUCCEEDED
    outcome["result"] = result
    return outcome


def load_request(path: Path) -> dict:
    try:
        request = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ProbeErrorCode.PRB_1.exception(cause=error, path=path, detail=error)
    kind = request.get("kind") if isinstance(request, dict) else None
    if kind not in TIMEOUT_SECS:
        raise ProbeErrorCode.PRB_1.exception(
            path=path, detail=f"unknown probe kind {kind!r}"
        )
    return request


def now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _secret_forms(secret: str) -> set[str]:
    return {
        secret,
        urllib.parse.quote(secret, safe=""),
        urllib.parse.quote_plus(secret),
        json.dumps(secret)[1:-1],
        json.dumps(secret, ensure_ascii=False)[1:-1],
    }


def scrub(text: str, environ: Mapping[str, str] | None = None) -> str:
    if environ is None:
        environ = os.environ
    forms = {
        form
        for key, value in environ.items()
        if key.startswith(_SECRET_KEY_PREFIX) and len(value) >= MIN_SECRET_CHARS
        for form in _secret_forms(value)
    }
    for form in sorted(forms, key=len, reverse=True):
        text = text.replace(form, SCRUBBED)
    encoded = text.encode("utf-8")
    if len(encoded) <= MAX_ERROR_BYTES:
        return text
    return encoded[:MAX_ERROR_BYTES].decode("utf-8", errors="ignore")


def _describe(error: BaseException) -> str:
    return scrub(f"{type(error).__name__}: {error}")


def call_with_timeout(fn: Callable[[], Any], timeout: float) -> Any:
    """Run ``fn`` on a daemon thread, abandoning it past ``timeout``: a driver
    with no socket timeout would otherwise hold the pod until its deadline."""
    outcome: list[tuple[bool, Any]] = []

    def run() -> None:
        try:
            outcome.append((True, fn()))
        except BaseException as error:  # noqa: BLE001 - re-raised by the caller
            outcome.append((False, error))

    worker = threading.Thread(
        target=contextvars.copy_context().run, args=(run,), daemon=True
    )
    worker.start()
    worker.join(timeout)
    if worker.is_alive():
        raise ProbeTimeout(f"exceeded {timeout:g}s")
    ok, value = outcome[0]
    if not ok:
        raise value
    return value


def entity_row(entity: Entity, entity_kind: str) -> dict:
    field_names = entity.field_names()
    return {
        "fqn": entity.fqn(),
        "entity_kind": entity_kind,
        "field_names": " ".join(name.lower() for name in field_names),
        "field_count": len(field_names),
        "doc": entity.to_dict(),
    }


def _check(explorer: Explorer, conn: Conn, timeout: float) -> dict:
    try:
        call_with_timeout(lambda: explorer.check_connect(conn), timeout)
    except ProbeTimeout:
        raise
    except Exception as error:
        return {"ok": False, "error": _describe(error)}
    return {"ok": True}


def _metadata(
    explorer: Explorer,
    conn: Conn,
    timeout: float,
    max_entities: int,
    max_bytes: int | None,
) -> dict:
    entities = call_with_timeout(lambda: explorer.metadata(conn), timeout) or []
    ordered = sorted(entities, key=lambda e: e.fqn())
    entity_kind = getattr(explorer.meta_model, "entity_level", "") or "entity"
    rows = []
    used = 0
    for entity in ordered[:max_entities]:
        row = _json_safe(entity_row(entity, entity_kind))
        used += len(json.dumps(row).encode("utf-8")) + 1
        if max_bytes is not None and used > max_bytes:
            break
        rows.append(row)
    return {
        "schema_hash": schema_fingerprint(entities),
        "entity_count": len(rows),
        "truncated": len(rows) < len(ordered),
        "entities": rows,
    }


def _finite(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {k: _finite(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite(v) for v in value]
    return value


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(_finite(value), default=str, allow_nan=False))


def _sample(explorer: Explorer, conn: Conn, timeout: float, request: dict) -> dict:
    params = request.get("params") or {}
    limit = max(
        1, min(int(params.get("limit") or DEFAULT_SAMPLE_ROWS), MAX_SAMPLE_ROWS)
    )
    docs = request.get("entity_docs")
    if docs is None:
        return _sample_one(explorer, conn, timeout, request["entity_doc"], limit)
    deadline = time.monotonic() + timeout
    samples = []
    for doc in docs:
        left = deadline - time.monotonic()
        fqn = doc.get("fqn", "")
        if left <= 0:
            samples.append({"entity": fqn, "error": "not sampled: out of time"})
            continue
        try:
            sample = _sample_one(explorer, conn, left, doc, limit)
        except ProbeTimeout:
            sample = {"entity": fqn, "error": "not sampled: out of time"}
        except Exception as error:  # noqa: BLE001 - one entity's failure stays its own
            sample = {"entity": fqn, "error": _describe(error)}
        if sample.get("supported") is False:
            return {"supported": False}
        samples.append(sample)
    while _size(samples) > MAX_SAMPLE_BYTES and _drop_row(samples):
        pass
    return {"samples": samples}


def _sample_one(
    explorer: Explorer, conn: Conn, timeout: float, doc: dict, limit: int
) -> dict:
    entity = explorer.entity_type.from_dict(doc, explorer.meta_model)
    records = call_with_timeout(lambda: explorer.data(conn, entity, limit), timeout)
    if records is None:
        return {"supported": False}
    columns: list[str] = []
    for record in records:
        columns.extend(k for k in record if k not in columns)
    rows = _json_safe([[record.get(c) for c in columns] for record in records[:limit]])
    truncated = len(records) > limit
    while rows and _size(rows) > MAX_SAMPLE_BYTES:
        rows.pop()
        truncated = True
    return {
        "entity": entity.fqn(),
        "columns": columns,
        "rows": rows,
        "truncated": truncated,
    }


def _size(value: Any) -> int:
    return len(json.dumps(value).encode("utf-8"))


def _drop_row(samples: list[dict]) -> bool:
    """Drops the last row of the sample holding the most rows; ``False`` when
    none is left."""
    holding = [s for s in samples if s.get("rows")]
    if not holding:
        return False
    largest = max(holding, key=lambda s: len(s["rows"]))
    largest["rows"].pop()
    largest["truncated"] = True
    return True
