#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import os
import urllib.parse
from collections.abc import Mapping

# noinspection PyProtectedMember
from tabsdatak.common._json_serde import _SECRET_KEY_PREFIX

SCRUBBED = "***"
MAX_ERROR_BYTES = 2048
# Shorter values would scrub ordinary words out of every message.
MIN_SECRET_CHARS = 4
ENCODING = "utf-8"


def forms(secret: str) -> set[str]:
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
    secrets = {
        form
        for key, value in environ.items()
        if key.startswith(_SECRET_KEY_PREFIX) and len(value) >= MIN_SECRET_CHARS
        for form in forms(value)
    }
    for secret in sorted(secrets, key=len, reverse=True):
        text = text.replace(secret, SCRUBBED)
    encoded = text.encode(ENCODING)
    if len(encoded) <= MAX_ERROR_BYTES:
        return text
    return encoded[:MAX_ERROR_BYTES].decode(ENCODING, errors="ignore")


def describe(error: BaseException) -> str:
    return scrub(f"{type(error).__name__}: {error}")
