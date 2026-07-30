#
# Copyright 2026 Tabsdata Inc.
#

"""Encryption for descriptor files, keyed by ``$TD_BACKENDS_CRYPTO_KEY``.

Descriptors hold connection settings (hosts, users, passwords), so they're
stored encrypted at rest as PEM-armored AES-256-GCM ciphertext. The key is
derived from the secret with PBKDF2-HMAC-SHA256 over a fixed application salt
(cached per process); every encryption uses a fresh 96-bit nonce stored with the
ciphertext:

    -----BEGIN TDX2 DESCRIPTOR-----
    <base64(nonce || ciphertext+tag)>
    -----END TDX2 DESCRIPTOR-----

The ``TDX2`` label is distinct from the v1 ``TDX`` label so the two toolchains
never mistake each other's descriptors (they also use separate store dirs).
"""

from __future__ import annotations

import base64
import hashlib
import os
import re
import textwrap
from contextlib import contextmanager

from tabsdatax.itest.tdksystems2 import constants
from tabsdatax.itest.tdksystems2.errors import DecryptError, TdxError

ENV_VAR = constants.ENV_CRYPTO_KEY
MIN_SECRET_LEN = 12

_PEM_LABEL = "TDX2 DESCRIPTOR"
_BEGIN = f"-----BEGIN {_PEM_LABEL}-----"
_END = f"-----END {_PEM_LABEL}-----"
_ARMOR_RE = re.compile(r"-----BEGIN ([A-Z0-9_]+ DESCRIPTOR)-----")
_SALT = b"tabsdata-tdx2/descriptor/v1"
_ITERATIONS = 200_000
_NONCE_LEN = 12

_key_cache: dict[str, bytes] = {}

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


def _key(secret: str) -> bytes:
    cached = _key_cache.get(secret)
    if cached is None:
        cached = hashlib.pbkdf2_hmac(
            "sha256", secret.encode("utf-8"), _SALT, _ITERATIONS, dklen=32
        )
        _key_cache[secret] = cached
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


def encrypt(plaintext: bytes) -> str:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    key = _key(require_secret())
    nonce = os.urandom(_NONCE_LEN)
    ct = AESGCM(key).encrypt(nonce, plaintext, None)
    body = "\n".join(textwrap.wrap(base64.b64encode(nonce + ct).decode(), 64))
    return f"{_BEGIN}\n{body}\n{_END}\n"


def decrypt(pem: str) -> bytes:
    """Decrypt a PEM string produced by :func:`encrypt`. Raises TdxError if it is
    not a descriptor at all, DecryptError if it IS one but unreadable (wrong
    label, wrong secret, or corrupt)."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    if not is_pem(pem):
        raise TdxError("not an encrypted descriptor (no PEM armor)")
    label = descriptor_label(pem)
    if label != _PEM_LABEL:
        raise DecryptError(
            f"descriptor PEM label {label!r} is not the expected {_PEM_LABEL!r} — "
            f"recreate it with tdx-system / tdx-domain create"
        )
    b64 = "".join(ln for ln in pem.splitlines() if ln and not ln.startswith("-----"))
    try:
        blob = base64.b64decode(b64)
        nonce, ct = blob[:_NONCE_LEN], blob[_NONCE_LEN:]
        return AESGCM(_key(require_secret())).decrypt(nonce, ct, None)
    except TdxError:
        raise
    except Exception:  # noqa: BLE001 - corrupt blob or wrong key
        raise DecryptError(
            f"could not decrypt descriptor — is {ENV_VAR} the one it was created with?"
        )
