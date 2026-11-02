#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

ENCODING = "utf-8"
DEFAULT_MAX_ENTITIES = 20_000


class Kind(StrEnum):
    CHECK = "check"
    METADATA = "metadata"
    SAMPLE = "sample"


class InvalidRequestError(ValueError):
    detail: str

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


@dataclass(frozen=True)
class ConnectionProbeRequest:
    kind: Kind
    probe_id: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    entity_doc: dict[str, Any] | None = None
    entity_docs: list[dict[str, Any]] | None = None
    max_entities: int = DEFAULT_MAX_ENTITIES
    max_entities_bytes: int | None = None

    @classmethod
    def load(cls, path: Path) -> ConnectionProbeRequest:
        try:
            document = json.loads(path.read_text(encoding=ENCODING))
        except (OSError, ValueError) as error:
            raise InvalidRequestError(str(error)) from error
        return cls.of(document)

    @classmethod
    def of(cls, document: Any) -> ConnectionProbeRequest:
        kind = document.get("kind") if isinstance(document, dict) else None
        if kind not in tuple(Kind):
            raise InvalidRequestError(f"unknown probe kind {kind!r}")
        max_bytes = document.get("max_entities_bytes")
        return cls(
            kind=Kind(kind),
            probe_id=document.get("probe_id"),
            params=document.get("params") or {},
            entity_doc=document.get("entity_doc"),
            entity_docs=document.get("entity_docs"),
            max_entities=int(document.get("max_entities") or DEFAULT_MAX_ENTITIES),
            max_entities_bytes=None if max_bytes is None else int(max_bytes),
        )
