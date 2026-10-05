#
# Copyright 2026 Tabsdata Inc.
#

"""Names for everything a monkey run creates. Each run, a replay included,
draws a fresh namespace, and every physical name embeds it, so runs on the
same instance never touch each other's objects, whether they overlap in time
or follow one another. Plans and descriptors keep only logical names."""

from __future__ import annotations

import hashlib
import os
import random
import socket
import string
import time

ALPHABET = string.digits + string.ascii_lowercase
PREFIX = "mk"
SUFFIX_LENGTH = 4


def base36(value: int) -> str:
    digits = ""
    while True:
        value, digit = divmod(value, len(ALPHABET))
        digits = ALPHABET[digit] + digits
        if value == 0:
            return digits


def run_namespace() -> str:
    host = hashlib.sha1(socket.gethostname().encode()).hexdigest()[:3]
    suffix = "".join(random.SystemRandom().choices(ALPHABET, k=SUFFIX_LENGTH))
    return f"{PREFIX}{base36(int(time.time()))}{host}{base36(os.getpid())}{suffix}"


def collection_name(namespace: str, collection: str) -> str:
    return f"monkey_{namespace}_{collection}"


def topic_name(namespace: str, function: str) -> str:
    return f"monkey-{namespace}-{function}"


def table_name(function: str) -> str:
    return f"{function}_out"
