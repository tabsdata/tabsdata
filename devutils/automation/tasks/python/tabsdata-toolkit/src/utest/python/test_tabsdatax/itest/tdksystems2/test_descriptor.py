#
# Copyright 2026 Tabsdata Inc.
#

"""Descriptor persistence: encryption round-trip, state stamps, info rendering."""

from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems2 import crypto, descriptor
from tabsdatax.itest.tdksystems2.errors import DecryptError, TdxError


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
    assert "BEGIN TDX2 DESCRIPTOR" in text


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
    assert "export MYSQL__HOST='h'" in exported
    assert descriptor.info_render(info, "unset") == "unset HOST\nunset PORT"
    with pytest.raises(TdxError):
        descriptor.info_render(info, "bogus")


def test_is_descriptor_false_for_plain_file(tmp_path):
    p = tmp_path / "junk"
    p.write_text("not a descriptor")
    assert not descriptor.is_descriptor(p)
