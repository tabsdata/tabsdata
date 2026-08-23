#
# Copyright 2026 Tabsdata Inc.
#

"""Encryption for descriptor files, keyed by ``$TD_BACKENDS_CRYPTO_KEY``.

Descriptors hold connection settings (hosts, users, passwords), so they're
stored encrypted at rest as PEM-armored AES-256-GCM ciphertext. The key is
derived from the secret with PBKDF2-HMAC-SHA256 (cached per secret+salt); every
encryption draws a fresh 128-bit salt and 96-bit nonce, both stored ahead of the
ciphertext:

    -----BEGIN TDX3 DESCRIPTOR-----
    <base64(salt || nonce || ciphertext+tag)>
    -----END TDX3 DESCRIPTOR-----

A salt is not a secret — it exists to make each derivation unique, so it travels
with the ciphertext exactly as it does in any PHC-style hash string.

``TDX2`` is the previous format: same cipher, but no salt in the payload because
every descriptor derived its key from one hardcoded application salt. A ``TDX2``
descriptor is **rejected** by every ordinary read — only the ``upgrade`` command
passes ``allow_legacy`` to convert it. Failing rather than silently upgrading
keeps the migration something an operator runs once and can see, instead of a
side effect that any command might have on any file.

Each label is distinct so the toolchains never mistake each other's descriptors
(they also use separate store dirs).
"""

from __future__ import annotations

import base64
import hashlib
import os
import re
import textwrap
from contextlib import contextmanager

from tabsdatax.itest.tdksystems2 import constants
from tabsdatax.itest.tdksystems2.errors import (
    DecryptError,
    LegacyDescriptorError,
    TdxError,
)

ENV_VAR = constants.ENV_CRYPTO_KEY
MIN_SECRET_LEN = 12

# The one instruction anything that trips over a TDX2 descriptor should give,
# shared so the CLI, the Python fixtures and the Rust fixtures all say it.
LEGACY_REMEDY = (
    f"descriptor is in the legacy TDX2 format (one shared salt) — run "
    f"'{constants.SYS_TOOL} {constants.V_UPGRADE}' and "
    f"'{constants.DOM_TOOL} {constants.V_UPGRADE}' to convert the store"
)

_PEM_LABEL = "TDX3 DESCRIPTOR"
_LEGACY_PEM_LABEL = "TDX2 DESCRIPTOR"
_BEGIN = f"-----BEGIN {_PEM_LABEL}-----"
_END = f"-----END {_PEM_LABEL}-----"
_ARMOR_RE = re.compile(r"-----BEGIN ([A-Z0-9_]+ DESCRIPTOR)-----")
# The one salt every TDX2 descriptor was derived from. Read-only: nothing new is
# ever encrypted under it.
_LEGACY_SALT = b"tabsdata-tdx2/descriptor/v1"
_ITERATIONS = 200_000
_SALT_LEN = 16
_NONCE_LEN = 12

_key_cache: dict[tuple[str, bytes], bytes] = {}

# Set only inside `using_secret`. Everything else reads the environment.
_secret_override: str | None = None


@contextmanager
def using_secret(secret: str, *, what: str):
    """Read descriptors under `secret` instead of ``$TD_BACKENDS_CRYPTO_KEY`` for
    the duration of the block. ``connect`` is the two-secret case: the remote
    descriptor is encrypted under the server's secret, the local one it writes
    under the client's. `what` names the secret's source in the error a too-short
    value raises."""
    global _secret_override
    if len(secret) < MIN_SECRET_LEN:
        raise TdxError(
            f"{what} must be at least {MIN_SECRET_LEN} characters long "
            f"(it decrypts the remote descriptor)."
        )
    prev = _secret_override
    _secret_override = secret
    try:
        yield
    finally:
        _secret_override = prev


def require_secret() -> str:
    """The personal secret from ``$TD_BACKENDS_CRYPTO_KEY`` (or the one
    :func:`using_secret` pins), or raise if unset / shorter than 12 chars. The
    gate for every descriptor read or write."""
    if _secret_override is not None:
        return _secret_override
    secret = os.environ.get(ENV_VAR, "")
    if len(secret) < MIN_SECRET_LEN:
        raise TdxError(
            f"{ENV_VAR} must be set and at least {MIN_SECRET_LEN} characters long "
            f"to run tdx-system / tdx-domain (it encrypts the descriptors)."
        )
    return secret


def _key(secret: str, salt: bytes) -> bytes:
    cached = _key_cache.get((secret, salt))
    if cached is None:
        cached = hashlib.pbkdf2_hmac(
            "sha256", secret.encode("utf-8"), salt, _ITERATIONS, dklen=32
        )
        _key_cache[(secret, salt)] = cached
    return cached


def descriptor_label(text: str) -> str | None:
    """The PEM descriptor label in `text`, matched with its ``END`` armor, or
    None if `text` carries no descriptor armor."""
    m = _ARMOR_RE.search(text)
    if not m:
        return None
    label = m.group(1)
    return label if f"-----END {label}-----" in text else None


def is_pem(text: str) -> bool:
    """Whether `text` is PEM-armored as one of our descriptors (any label)."""
    return descriptor_label(text) is not None


def is_legacy(text: str) -> bool:
    """Whether `text` is a descriptor in the superseded ``TDX2`` format.

    True means every ordinary read rejects it; ``tdx-system upgrade`` /
    ``tdx-domain upgrade`` convert it.
    """
    return descriptor_label(text) == _LEGACY_PEM_LABEL


def encrypt(plaintext: bytes) -> str:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    salt = os.urandom(_SALT_LEN)
    nonce = os.urandom(_NONCE_LEN)
    ct = AESGCM(_key(require_secret(), salt)).encrypt(nonce, plaintext, None)
    body = "\n".join(textwrap.wrap(base64.b64encode(salt + nonce + ct).decode(), 64))
    return f"{_BEGIN}\n{body}\n{_END}\n"


def decrypt(pem: str, *, allow_legacy: bool = False) -> bytes:
    """Decrypt a PEM string produced by :func:`encrypt`.

    A legacy ``TDX2`` descriptor raises :class:`LegacyDescriptorError` unless
    `allow_legacy` is set — only the ``upgrade`` command sets it.

    Raises TdxError if it is not a descriptor at all, DecryptError if it IS one
    but unreadable (unknown label, wrong secret, or corrupt).
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    if not is_pem(pem):
        raise TdxError("not an encrypted descriptor (no PEM armor)")
    label = descriptor_label(pem)
    if label == _LEGACY_PEM_LABEL and not allow_legacy:
        raise LegacyDescriptorError(LEGACY_REMEDY)
    if label not in (_PEM_LABEL, _LEGACY_PEM_LABEL):
        raise DecryptError(
            f"descriptor PEM label {label!r} is not the expected {_PEM_LABEL!r} — "
            f"recreate it with tdx-system / tdx-domain create"
        )
    b64 = "".join(ln for ln in pem.splitlines() if ln and not ln.startswith("-----"))
    try:
        blob = base64.b64decode(b64)
        if label == _LEGACY_PEM_LABEL:
            salt, rest = _LEGACY_SALT, blob
        else:
            salt, rest = blob[:_SALT_LEN], blob[_SALT_LEN:]
        nonce, ct = rest[:_NONCE_LEN], rest[_NONCE_LEN:]
        return AESGCM(_key(require_secret(), salt)).decrypt(nonce, ct, None)
    except TdxError:
        raise
    except Exception:  # noqa: BLE001 - corrupt blob or wrong key
        raise DecryptError(
            f"could not decrypt descriptor — is {ENV_VAR} the one it was created with?"
        )
