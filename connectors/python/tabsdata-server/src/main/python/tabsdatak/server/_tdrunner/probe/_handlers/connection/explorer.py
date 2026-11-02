#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import logging
import math
import time
from pathlib import Path
from typing import Any

from tabsdatak._connectors import explorer_for_conn
from tabsdatak.common._catalog import schema_fingerprint
from tabsdatak.server._tdrunner.function._commons.document.connection import (
    ConnectionDocument,
)
from tabsdatak.server._tdrunner.probe._handlers.connection.request import (
    ConnectionProbeRequest,
    Kind,
)
from tabsdatak.server._tdrunner.probe._handlers.connection.secrets import describe
from tabsdatak.server._tdrunner.probe._handlers.connection.timeout import (
    ProbeTimeout,
    call_with_timeout,
)
from tabsdatak.spi import Conn, Entity, Explorer

logger = logging.getLogger(__name__)

TIMEOUT_SECONDS = {Kind.CHECK: 20.0, Kind.METADATA: 90.0, Kind.SAMPLE: 30.0}

STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_TIMEOUT = "timeout"

DEFAULT_SAMPLE_ROWS = 20
MAX_SAMPLE_ROWS = 100
MAX_SAMPLE_BYTES = 1024 * 1024
OUT_OF_TIME = "not sampled: out of time"
ENCODING = "utf-8"

Outcome = dict[str, Any]


class ConnectionExplorerProbe:
    connection_file: Path

    def __init__(self, connection_file: Path) -> None:
        self.connection_file = connection_file

    def run(self, request: ConnectionProbeRequest) -> Outcome:
        outcome: Outcome = {"probe_id": request.probe_id, "kind": request.kind.value}
        try:
            result = self.result(request)
        except ProbeTimeout as error:
            outcome["status"] = STATUS_TIMEOUT
            outcome["error"] = {"code": STATUS_TIMEOUT, "message": describe(error)}
            return outcome
        except BaseException as error:  # noqa: BLE001 - reported scrubbed, never raised
            logger.error(f"The {request.kind.value} probe failed: {describe(error)}")
            outcome["status"] = STATUS_FAILED
            outcome["error"] = {
                "code": f"{request.kind.value}_failed",
                "message": describe(error),
            }
            return outcome
        outcome["status"] = STATUS_SUCCEEDED
        outcome["result"] = result
        return outcome

    def result(self, request: ConnectionProbeRequest) -> dict[str, Any]:
        timeout = TIMEOUT_SECONDS[request.kind]
        conn = ConnectionDocument(self.connection_file).load()
        explorer_class = explorer_for_conn(conn)
        if explorer_class is None:
            return {"supported": False}
        explorer = explorer_class()
        match request.kind:
            case Kind.CHECK:
                return check(explorer, conn, timeout)
            case Kind.METADATA:
                return metadata(
                    explorer,
                    conn,
                    timeout,
                    request.max_entities,
                    request.max_entities_bytes,
                )
            case Kind.SAMPLE:
                return sample(explorer, conn, timeout, request)


def check(explorer: Explorer, conn: Conn, timeout: float) -> dict[str, Any]:
    try:
        call_with_timeout(lambda: explorer.check_connect(conn), timeout)
    except ProbeTimeout:
        raise
    except Exception as error:
        return {"ok": False, "error": describe(error)}
    return {"ok": True}


def entity_row(entity: Entity, entity_kind: str) -> dict[str, Any]:
    field_names = entity.field_names()
    return {
        "fqn": entity.fqn(),
        "entity_kind": entity_kind,
        "field_names": " ".join(name.lower() for name in field_names),
        "field_count": len(field_names),
        "doc": entity.to_dict(),
    }


def metadata(
    explorer: Explorer,
    conn: Conn,
    timeout: float,
    max_entities: int,
    max_bytes: int | None,
) -> dict[str, Any]:
    entities = call_with_timeout(lambda: explorer.metadata(conn), timeout) or []
    ordered = sorted(entities, key=lambda entity: entity.fqn())
    entity_kind = getattr(explorer.meta_model, "entity_level", "") or "entity"
    rows = []
    used = 0
    for entity in ordered[:max_entities]:
        row = json_safe(entity_row(entity, entity_kind))
        used += size(row) + 1
        if max_bytes is not None and used > max_bytes:
            break
        rows.append(row)
    return {
        "schema_hash": schema_fingerprint(entities),
        "entity_count": len(rows),
        "truncated": len(rows) < len(ordered),
        "entities": rows,
    }


def sample(
    explorer: Explorer,
    conn: Conn,
    timeout: float,
    request: ConnectionProbeRequest,
) -> dict[str, Any]:
    limit = int(request.params.get("limit") or DEFAULT_SAMPLE_ROWS)
    limit = max(1, min(limit, MAX_SAMPLE_ROWS))
    if request.entity_docs is None:
        return sample_one(explorer, conn, timeout, request.entity_doc or {}, limit)
    deadline = time.monotonic() + timeout
    samples = []
    for doc in request.entity_docs:
        left = deadline - time.monotonic()
        fqn = doc.get("fqn", "")
        if left <= 0:
            samples.append({"entity": fqn, "error": OUT_OF_TIME})
            continue
        try:
            entity_sample = sample_one(explorer, conn, left, doc, limit)
        except ProbeTimeout:
            entity_sample = {"entity": fqn, "error": OUT_OF_TIME}
        except Exception as error:  # noqa: BLE001 - one entity's failure stays its own
            entity_sample = {"entity": fqn, "error": describe(error)}
        if entity_sample.get("supported") is False:
            return {"supported": False}
        samples.append(entity_sample)
    while size(samples) > MAX_SAMPLE_BYTES and drop_row(samples):
        pass
    return {"samples": samples}


def sample_one(
    explorer: Explorer,
    conn: Conn,
    timeout: float,
    doc: dict[str, Any],
    limit: int,
) -> dict[str, Any]:
    entity = explorer.entity_type.from_dict(doc, explorer.meta_model)
    records = call_with_timeout(lambda: explorer.data(conn, entity, limit), timeout)
    if records is None:
        return {"supported": False}
    columns: list[str] = []
    for record in records:
        columns.extend(key for key in record if key not in columns)
    rows = json_safe([
        [record.get(column) for column in columns] for record in records[:limit]
    ])
    truncated = len(records) > limit
    while rows and size(rows) > MAX_SAMPLE_BYTES:
        rows.pop()
        truncated = True
    return {
        "entity": entity.fqn(),
        "columns": columns,
        "rows": rows,
        "truncated": truncated,
    }


def drop_row(samples: list[dict[str, Any]]) -> bool:
    holding = [entity_sample for entity_sample in samples if entity_sample.get("rows")]
    if not holding:
        return False
    largest = max(holding, key=lambda entity_sample: len(entity_sample["rows"]))
    largest["rows"].pop()
    largest["truncated"] = True
    return True


# The outcome is strict JSON: floats that are not finite travel as strings.
def finite(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {key: finite(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [finite(item) for item in value]
    return value


def json_safe(value: Any) -> Any:
    return json.loads(json.dumps(finite(value), default=str, allow_nan=False))


def size(value: Any) -> int:
    return len(json.dumps(value).encode(ENCODING))
