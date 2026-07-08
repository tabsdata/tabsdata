"""Tests for tabsdatax.itest.tdksystems.crypto — the $TD_BACKENDS_CRYPTO_KEY gate and the
PEM-armored encrypt/decrypt used for descriptor files."""
from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems import crypto, descriptor
from tabsdatax.itest.tdksystems.errors import DatagenError


# ---- require_secret --------------------------------------------------------

def test_require_secret_ok(monkeypatch):
    monkeypatch.setenv("TD_BACKENDS_CRYPTO_KEY", "abcdefghijkl")   # exactly 12
    assert crypto.require_secret() == "abcdefghijkl"


def test_require_secret_missing(monkeypatch):
    monkeypatch.delenv("TD_BACKENDS_CRYPTO_KEY", raising=False)
    with pytest.raises(DatagenError, match="TD_BACKENDS_CRYPTO_KEY must be set"):
        crypto.require_secret()


def test_require_secret_too_short(monkeypatch):
    monkeypatch.setenv("TD_BACKENDS_CRYPTO_KEY", "short")          # < 12
    with pytest.raises(DatagenError, match="at least 12 characters"):
        crypto.require_secret()


# ---- encrypt / decrypt -----------------------------------------------------

def test_roundtrip(monkeypatch):
    monkeypatch.setenv("TD_BACKENDS_CRYPTO_KEY", "a-good-dev-secret")
    pem = crypto.encrypt(b'{"hello": "world"}')
    assert crypto.decrypt(pem) == b'{"hello": "world"}'


def test_output_is_pem(monkeypatch):
    monkeypatch.setenv("TD_BACKENDS_CRYPTO_KEY", "a-good-dev-secret")
    pem = crypto.encrypt(b"payload")
    assert pem.startswith("-----BEGIN TDX DESCRIPTOR-----")
    assert pem.strip().endswith("-----END TDX DESCRIPTOR-----")
    assert crypto.is_pem(pem)
    assert not crypto.is_pem("just some text")
    # payload is not present in cleartext
    assert "payload" not in pem


def test_nonce_makes_ciphertext_nondeterministic(monkeypatch):
    monkeypatch.setenv("TD_BACKENDS_CRYPTO_KEY", "a-good-dev-secret")
    assert crypto.encrypt(b"same") != crypto.encrypt(b"same")


def test_wrong_secret_fails_to_decrypt(monkeypatch):
    monkeypatch.setenv("TD_BACKENDS_CRYPTO_KEY", "the-right-secret")
    pem = crypto.encrypt(b"secret data")
    monkeypatch.setenv("TD_BACKENDS_CRYPTO_KEY", "a-different-secret")
    with pytest.raises(DatagenError, match="could not decrypt"):
        crypto.decrypt(pem)


def test_decrypt_rejects_non_pem(monkeypatch):
    monkeypatch.setenv("TD_BACKENDS_CRYPTO_KEY", "a-good-dev-secret")
    with pytest.raises(DatagenError, match="not an encrypted descriptor"):
        crypto.decrypt("plain text")


def test_encrypt_requires_secret(monkeypatch):
    monkeypatch.delenv("TD_BACKENDS_CRYPTO_KEY", raising=False)
    with pytest.raises(DatagenError, match="TD_BACKENDS_CRYPTO_KEY"):
        crypto.encrypt(b"x")


# ---- descriptors are encrypted at rest -------------------------------------

def test_descriptor_file_is_encrypted(tmp_path):
    out = tmp_path / "s.json"
    descriptor.write_cloud_system(
        out=str(out), storage_type="s3", system_kind="file", label="s3",
        sys_path="secret-bucket-prefix", sys_path_env=None,
        forwarded=[("BUCKET", "top-secret-bucket", None)])
    raw = out.read_text()
    # On disk: PEM armor, and none of the cleartext settings are visible.
    assert crypto.is_pem(raw)
    assert "top-secret-bucket" not in raw
    assert "STORAGE_TYPE" not in raw
    # load() decrypts and round-trips.
    assert descriptor.load(out)["system"] == "s3"


def test_load_wrong_secret_reports_it(tmp_path, monkeypatch):
    out = tmp_path / "s.json"
    monkeypatch.setenv("TD_BACKENDS_CRYPTO_KEY", "created-with-this")
    descriptor.write_cloud_system(
        out=str(out), storage_type="fs", system_kind="file", label="fs",
        sys_path="/tmp/x", sys_path_env=None, forwarded=[],
        strip_sys_path_slash=False)
    monkeypatch.setenv("TD_BACKENDS_CRYPTO_KEY", "reading-with-that")
    with pytest.raises(DatagenError, match="could not decrypt"):
        descriptor.load(out)
