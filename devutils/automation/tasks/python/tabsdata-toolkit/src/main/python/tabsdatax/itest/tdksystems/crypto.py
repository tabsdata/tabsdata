"""Encryption for the descriptor files, keyed by ``$TD_BACKENDS_CRYPTO_KEY``.

Descriptors hold connection settings (hosts, users, sometimes passwords), so
they're stored **encrypted at rest** as PEM-armored AES-256-GCM ciphertext
rather than plaintext JSON. The `tdx-system` / `tdx-domain` commands only run when
``TD_BACKENDS_CRYPTO_KEY`` is set (and at least 12 characters), and the same secret is
used to encrypt on write and decrypt on read.

The key is derived from the secret with PBKDF2-HMAC-SHA256 over a fixed
application salt (a dev secret, not per-file salting — so the derivation is
cheap and cached per process). Every encryption uses a fresh random 96-bit
nonce, stored alongside the ciphertext in the PEM body:

    -----BEGIN TD_GEN DESCRIPTOR-----
    <base64(nonce || ciphertext+tag)>
    -----END TD_GEN DESCRIPTOR-----
"""
from __future__ import annotations

import base64
import contextlib
import hashlib
import os
import textwrap

from .errors import DatagenError

ENV_VAR = "TD_BACKENDS_CRYPTO_KEY"
MIN_SECRET_LEN = 12

# Which env var holds the active personal secret. Defaults to TD_BACKENDS_CRYPTO_KEY;
# `tdx-system connect` points it at the server's secret var (--remote-secret-env)
# while it reads the copied remote descriptor, then resets it to write the local
# one under the client's own secret.
_active_env_var = ENV_VAR


def use_secret_env(name: str | None) -> None:
    """Point crypto at env var `name` for the personal secret; `None` resets to
    the default TD_BACKENDS_CRYPTO_KEY."""
    global _active_env_var
    _active_env_var = name or ENV_VAR


@contextlib.contextmanager
def using_secret_env(name: str | None):
    """Temporarily read the personal secret from env var `name`, restoring the
    previous choice on exit (so a two-secret command like `connect` can read the
    remote descriptor under one secret and write the local one under another)."""
    prev = _active_env_var
    use_secret_env(name)
    try:
        yield
    finally:
        use_secret_env(prev)

_PEM_LABEL = "TD_GEN DESCRIPTOR"
_BEGIN = f"-----BEGIN {_PEM_LABEL}-----"
_END = f"-----END {_PEM_LABEL}-----"
_SALT = b"tabsdata-datagen/descriptor/v1"   # fixed: one dev secret, symmetric
_ITERATIONS = 200_000
_NONCE_LEN = 12

_key_cache: dict[str, bytes] = {}


def require_secret() -> str:
    """Return the active personal secret, or raise if it's unset / shorter than
    12 chars. The active env var is TD_BACKENDS_CRYPTO_KEY unless overridden via
    :func:`use_secret_env` / :func:`using_secret_env`.

    This is the gate for every `tdx-system` / `tdx-domain` action and for reading
    or writing any descriptor."""
    secret = os.environ.get(_active_env_var, "")
    if len(secret) < MIN_SECRET_LEN:
        raise DatagenError(
            f"{_active_env_var} must be set and at least {MIN_SECRET_LEN} "
            f"characters long to run tdx-system / tdx-domain (it encrypts the "
            f"descriptors)."
        )
    return secret


def _key(secret: str) -> bytes:
    cached = _key_cache.get(secret)
    if cached is None:
        cached = hashlib.pbkdf2_hmac("sha256", secret.encode("utf-8"),
                                     _SALT, _ITERATIONS, dklen=32)
        _key_cache[secret] = cached
    return cached


def is_pem(text: str) -> bool:
    """Whether `text` looks like one of our PEM-armored descriptors."""
    return _BEGIN in text and _END in text


def encrypt(plaintext: bytes) -> str:
    """Encrypt `plaintext` under $TD_BACKENDS_CRYPTO_KEY and return a PEM string."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    key = _key(require_secret())
    nonce = os.urandom(_NONCE_LEN)
    ct = AESGCM(key).encrypt(nonce, plaintext, None)
    body = "\n".join(textwrap.wrap(base64.b64encode(nonce + ct).decode(), 64))
    return f"{_BEGIN}\n{body}\n{_END}\n"


def decrypt(pem: str) -> bytes:
    """Decrypt a PEM string produced by :func:`encrypt`. Raises DatagenError
    if it isn't our PEM or the secret is wrong."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    if not is_pem(pem):
        raise DatagenError("not an encrypted descriptor (no PEM armor)")
    b64 = "".join(
        ln for ln in pem.splitlines()
        if ln and not ln.startswith("-----")
    )
    try:
        blob = base64.b64decode(b64)
        nonce, ct = blob[:_NONCE_LEN], blob[_NONCE_LEN:]
        return AESGCM(_key(require_secret())).decrypt(nonce, ct, None)
    except DatagenError:
        raise
    except Exception:  # noqa: BLE001 - corrupt blob or wrong key
        raise DatagenError(
            f"could not decrypt descriptor — is {ENV_VAR} the one it was "
            f"created with?"
        )
