#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Mapping

SecretsSpec = Mapping[str, Mapping[str, str | None]]


def to_payload(secrets: SecretsSpec) -> list[dict]:
    return [
        {
            "name": name,
            "entries": [{"key": key, "value": value} for key, value in entries.items()],
        }
        for name, entries in secrets.items()
    ]


def from_read(data: dict) -> dict[str, list[str]]:
    return {secret["name"]: list(secret["keys"]) for secret in data.get("secrets", [])}
