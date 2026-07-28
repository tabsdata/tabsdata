#
# Copyright 2026 Tabsdata Inc.
#

"""Per-system verb gating.

A verb belongs to the tool's vocabulary when *some* system exposes it; these
cover the other half — aiming one at a system that does not expose it is
rejected before the shared command runs.
"""

from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems2 import constants, descriptor, store, systems, tdx
from tabsdatax.itest.tdksystems2.errors import TdxError

SYS = constants.SYS_TOOL
DOM = constants.DOM_TOOL


@pytest.mark.parametrize(
    "tool,slug,verb",
    [
        # nothing to restart when there is no container
        (SYS, "s3", constants.V_RESTART),
        (SYS, "openai", constants.V_PURGE),
        # no shared data plane behind these
        (DOM, "mongodb", constants.V_CREATE_DATA),
        (DOM, "bigquery", constants.V_CREATE_MODEL),
        (DOM, "anthropic", constants.V_MODELS),
        # a datasource only materialises on ingestion
        (DOM, "druid", constants.V_CREATE_DATA),
        # append-only / wipe-and-replace data planes have no incremental delta
        (DOM, "clickhouse", constants.V_MODIFY_DATA),
        (DOM, "salesforce", constants.V_MODIFY_DATA),
    ],
)
def test_unsupported_verbs_are_rejected(tool, slug, verb):
    with pytest.raises(TdxError, match=f"system '{slug}' does not support '{verb}'"):
        tdx._require_verb(tool, slug, verb)


@pytest.mark.parametrize(
    "tool,slug,verb",
    [
        (SYS, "mysql", constants.V_RESTART),
        (SYS, "mysql", constants.V_PURGE),
        (SYS, "s3", constants.V_STATUS),  # ungated verbs are open to every system
        (SYS, "openai", constants.V_DESTROY),
        # fetch-bin stays open so a broad selection never fails on a member
        # that happens to run no container
        (SYS, "snowflake", constants.V_FETCH_BIN),
        (SYS, "s3", constants.V_FETCH_BIN),
        (DOM, "mysql", constants.V_MODIFY_DATA),
        (DOM, "redpanda", constants.V_CREATE_DATA),
        (DOM, "salesforce", constants.V_CREATE_DATA),
        (DOM, "druid", constants.V_CREATE_MODEL),
    ],
)
def test_supported_verbs_pass(tool, slug, verb):
    tdx._require_verb(tool, slug, verb)  # does not raise


def test_the_error_names_what_the_system_does_support():
    with pytest.raises(TdxError) as exc:
        tdx._require_verb(DOM, "anthropic", constants.V_CREATE_DATA)
    message = str(exc.value)
    assert "it supports: create, destroy, status" in message
    assert "create-data" not in message.split("it supports:")[1]


def test_every_system_exposes_the_ungated_verbs():
    # create / destroy / status carry no capability flag, so no system may be
    # locked out of its own lifecycle.
    for slug in systems.all_slugs():
        for tool, verbs in (
            (SYS, (constants.V_CREATE, constants.V_DESTROY, constants.V_STATUS)),
            (DOM, (constants.V_CREATE, constants.V_DESTROY, constants.V_STATUS)),
        ):
            for verb in verbs:
                tdx._require_verb(tool, slug, verb)


def test_fetch_bin_skips_a_system_that_runs_no_container(monkeypatch, capsys):
    from tabsdatax.itest.tdksystems2.systems._common.commands import sys_fetch_bin

    pulled: list[str] = []
    monkeypatch.setattr(sys_fetch_bin, "require_docker", lambda: None)
    monkeypatch.setattr(
        sys_fetch_bin, "docker_pull", lambda image, plat: pulled.append(image)
    )

    class _Ctx:
        slug = "mysql"
        args = type("A", (), {"type": "s3,mysql"})()

    assert sys_fetch_bin.run(_Ctx()) == 0
    out = capsys.readouterr().out
    assert "s3 runs no container" in out
    # the container in the selection is still fetched, and s3 is not claimed
    assert pulled and all("mysql" in p for p in pulled)
    assert "fetched images for mysql" in out


def test_fetch_bin_all_selects_only_containers(monkeypatch):
    from tabsdatax.itest.tdksystems2.systems._common.commands import sys_fetch_bin

    class _Ctx:
        slug = "clickhouse"
        args = type("A", (), {"type": "ALL"})()

    slugs = sys_fetch_bin._slugs(_Ctx())
    assert slugs and all(
        systems.get_class(s).is_container for s in slugs
    ), "ALL must not pick up a non-container system"


def test_guard_fires_through_the_real_dispatch(stores, monkeypatch):
    for key in ("URL", "USERNAME", "PASSWORD", "TOKEN"):
        monkeypatch.setenv(f"SALESFORCE__{key}", "x")
    assert tdx._dispatch(SYS, ["create", "--type", "salesforce", "--sys", "org1"]) == 0
    assert tdx._dispatch(DOM, ["create", "--sys", "org1", "--domain", "d1"]) == 0
    assert descriptor.load(store.domain_path("d1")).system == "salesforce"
    with pytest.raises(TdxError, match="does not support 'modify-data'"):
        tdx._dispatch(DOM, ["modify-data", "--domain", "d1", "--sets", "1"])
    with pytest.raises(TdxError, match="does not support 'restart'"):
        tdx._dispatch(SYS, ["restart", "--sys", "org1"])
