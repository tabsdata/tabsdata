#
# Copyright 2026 Tabsdata Inc.
#

"""Descriptor persistence: encryption round-trip, state stamps, info rendering."""

from __future__ import annotations

import shlex

import pytest

from tabsdatax.itest.tdksystems2 import crypto, descriptor
from tabsdatax.itest.tdksystems2.errors import (
    DecryptError,
    LegacyDescriptorError,
    TdxError,
)


def test_system_round_trip(tmp_path):
    p = tmp_path / "s1"
    descriptor.write_system(p, "mysql", "s1", {"HOST": "localhost", "PORT": "3306"})
    assert descriptor.is_descriptor(p)
    info = descriptor.load(p)
    assert isinstance(info, descriptor.SystemInfo)
    assert info.role == "system" and info.system == "mysql"
    assert info.meta == {"HOST": "localhost", "PORT": "3306"}


def test_encrypted_at_rest(tmp_path):
    p = tmp_path / "s1"
    descriptor.write_system(p, "mysql", "s1", {"PASSWORD": "hunter2"})
    text = p.read_text()
    assert "hunter2" not in text
    assert "BEGIN TDX3 DESCRIPTOR" in text


def test_each_descriptor_gets_its_own_salt(tmp_path):
    """Two descriptors with identical content must not share a derivation."""
    a, b = tmp_path / "a", tmp_path / "b"
    for p in (a, b):
        descriptor.write_system(p, "mysql", "s1", {"HOST": "h"})
    assert a.read_text() != b.read_text()
    assert descriptor.load(a).meta == descriptor.load(b).meta


def test_legacy_tdx2_is_rejected_naming_the_upgrade(tmp_path):
    """An ordinary read must refuse a TDX2 descriptor, say which command fixes
    it, and leave the file alone."""
    p = tmp_path / "s1"
    descriptor.write_system(p, "mysql", "s1", {"HOST": "h", "PASSWORD": "hunter2"})
    p.write_text(_as_legacy(p.read_text()), encoding="utf-8")
    before = p.read_text()

    with pytest.raises(LegacyDescriptorError) as excinfo:
        descriptor.load(p)
    assert "tdx-system upgrade" in str(excinfo.value)
    assert "tdx-domain upgrade" in str(excinfo.value)
    assert p.read_text() == before


def test_upgrade_converts_a_legacy_descriptor(tmp_path):
    p = tmp_path / "s1"
    descriptor.write_system(p, "mysql", "s1", {"HOST": "h", "PASSWORD": "hunter2"})
    p.write_text(_as_legacy(p.read_text()), encoding="utf-8")

    assert descriptor.upgrade(p) is True
    upgraded = p.read_text()
    assert not crypto.is_legacy(upgraded)
    assert "BEGIN TDX3 DESCRIPTOR" in upgraded
    assert "hunter2" not in upgraded
    assert descriptor.load(p).meta == {"HOST": "h", "PASSWORD": "hunter2"}
    assert list(tmp_path.iterdir()) == [p]


def test_upgrade_leaves_a_current_descriptor_untouched(tmp_path):
    p = tmp_path / "s1"
    descriptor.write_system(p, "mysql", "s1", {"HOST": "h"})
    before = p.read_text()
    assert descriptor.upgrade(p) is False
    assert p.read_text() == before


def _as_legacy(pem: str) -> str:
    """Re-seal a current descriptor in the TDX2 shape: no salt in the payload,
    key derived from the one hardcoded application salt."""
    import base64
    import hashlib
    import os
    import textwrap

    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    plaintext = crypto.decrypt(pem, allow_legacy=True)
    key = hashlib.pbkdf2_hmac(
        "sha256",
        crypto.require_secret().encode("utf-8"),
        crypto._LEGACY_SALT,
        crypto._ITERATIONS,
        dklen=32,
    )
    nonce = os.urandom(crypto._NONCE_LEN)
    blob = nonce + AESGCM(key).encrypt(nonce, plaintext, None)
    body = "\n".join(textwrap.wrap(base64.b64encode(blob).decode(), 64))
    label = crypto._LEGACY_PEM_LABEL
    return f"-----BEGIN {label}-----\n{body}\n-----END {label}-----\n"


def test_wrong_key_raises_decrypt_error(tmp_path, monkeypatch):
    p = tmp_path / "s1"
    descriptor.write_system(p, "mysql", "s1", {"HOST": "h"})
    monkeypatch.setenv("TD_BACKENDS_CRYPTO_KEY", "a-different-secret")
    crypto._key_cache.clear()
    with pytest.raises(DecryptError):
        descriptor.load(p)


def test_domain_state_lifecycle(tmp_path):
    p = tmp_path / "d1"
    descriptor.write_domain(
        p, "mysql", "d1", sys_name="s1", external_name="d_u_0_d1",
        meta={"SQLALCHEMY_URI": "mysql://x"},
    )
    info = descriptor.load(p)
    assert isinstance(info, descriptor.DomainInfo) and info.model is None

    descriptor.set_model(p, "employees", ["departments", "employees"])
    info = descriptor.load(p)
    assert info.model == "employees" and info.data is None
    assert info.entities == {"departments": 0, "employees": 0}  # zero-initialized

    descriptor.record_data(p, "s", {"employees": 1000})
    info = descriptor.load(p)
    assert info.data == "s" and info.entities["employees"] == 1000 and info.seed == 0

    # an auto modify-data run writes back the new counts and the applied seed
    descriptor.record_changes(p, {"employees": 1002}, seed=3)
    info = descriptor.load(p)
    assert info.entities == {"employees": 1002} and info.seed == 3

    # a fixed-seed run updates counts but leaves the auto cursor alone
    descriptor.record_changes(p, {"employees": 1005})
    info = descriptor.load(p)
    assert info.entities == {"employees": 1005} and info.seed == 3

    # a fresh model install resets data + seed and re-zeroes entities
    descriptor.set_model(p, "employees", ["departments", "employees"])
    assert descriptor.load(p).seed == 0


def test_info_render(tmp_path):
    p = tmp_path / "s1"
    descriptor.write_system(p, "mysql", "s1", {"HOST": "h", "PORT": "1"})
    info = descriptor.load(p)
    assert '"role": "system"' in descriptor.info_render(info, "json")
    exported = descriptor.info_render(info, "export", prefix="MYSQL")
    assert "export MYSQL__HOST=h" in exported
    assert descriptor.info_render(info, "unset") == "unset HOST\nunset PORT"
    with pytest.raises(TdxError):
        descriptor.info_render(info, "bogus")


def test_info_export_is_posix_quoted_so_eval_reproduces_every_value(tmp_path):
    # The lines are eval'd by a shell: repr() would let it expand the $ and the
    # backticks in a password, and flatten a multiline PEM into literal \n.
    p = tmp_path / "s1"
    pem = "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n"
    meta = {"PASSWORD": "a'b$c`d e", "SERVER_CA_CERT": pem}
    descriptor.write_system(p, "mysql", "s1", meta)
    exported = descriptor.info_render(descriptor.load(p), "export", prefix="MYSQL")
    got = dict(tok.split("=", 1) for tok in shlex.split(exported) if tok != "export")
    assert got == {f"MYSQL__{k}": v for k, v in meta.items()}


def test_is_descriptor_false_for_plain_file(tmp_path):
    p = tmp_path / "junk"
    p.write_text("not a descriptor")
    assert not descriptor.is_descriptor(p)
