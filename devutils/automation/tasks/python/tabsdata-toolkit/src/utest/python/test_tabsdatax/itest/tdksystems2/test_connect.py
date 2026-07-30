#
# Copyright 2026 Tabsdata Inc.
#

"""``tdx-system connect``: the ssh-spec parsing, the two-secret descriptor
exchange, and how a connected system behaves for the verbs that would otherwise
reach for a local container.

The ssh calls themselves are stubbed — these cover everything around them.
"""

from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems2 import (
    constants,
    crypto,
    descriptor,
    ssh,
    store,
    tdx,
)
from tabsdatax.itest.tdksystems2.errors import DecryptError, TdxError

SYS = constants.SYS_TOOL
REMOTE_SECRET = "the-servers-own-secret"


@pytest.mark.parametrize(
    "spec,user,host,path",
    [
        ("host:/p/sys", None, "host", "/p/sys"),
        ("me@host:/p/sys", "me", "host", "/p/sys"),
        ("me@host:rel/path", "me", "host", "rel/path"),
    ],
)
def test_remote_spec_parses(spec, user, host, path):
    ep, remote = ssh.parse_remote_spec(spec)
    assert (ep.user, ep.host, remote) == (user, host, path)


@pytest.mark.parametrize("spec", ["host", "host:", ":/path", "", "@host:/p"])
def test_a_malformed_remote_spec_is_rejected(spec):
    with pytest.raises(TdxError, match=r"\[user@\]HOST:PATH"):
        ssh.parse_remote_spec(spec)


def test_endpoint_carries_port_and_identity():
    ep = ssh.SshEndpoint(host="h", user="u", port=2222, identity="/k")
    assert ep.target == "u@h"
    assert ep.base_args() == ["-p", "2222", "-i", "/k"]
    assert ssh.SshEndpoint(host="h").target == "h"


def test_using_secret_reads_under_another_key_then_restores(monkeypatch):
    monkeypatch.setenv(constants.ENV_CRYPTO_KEY, "the-clients-secret")
    with crypto.using_secret(REMOTE_SECRET, what="$REMOTE"):
        remote_pem = crypto.encrypt(b"payload")
        assert crypto.decrypt(remote_pem) == b"payload"
    # back on the client's secret, the server's ciphertext is unreadable
    assert crypto.require_secret() == "the-clients-secret"
    with pytest.raises(DecryptError):
        crypto.decrypt(remote_pem)


def test_using_secret_rejects_a_short_one():
    with pytest.raises(TdxError, match=r"\$REMOTE must be at least"):
        with crypto.using_secret("tiny", what="$REMOTE"):
            pass


@pytest.fixture
def remote(monkeypatch, stores):
    """A remote mysql descriptor encrypted under the *server's* secret, served by
    a stubbed ssh fetch, with the tunnel stubbed to a fixed pid."""
    monkeypatch.setenv("REMOTE_KEY", REMOTE_SECRET)
    with crypto.using_secret(REMOTE_SECRET, what="$REMOTE_KEY"):
        pem = crypto.encrypt(
            descriptor.SystemInfo(
                system="mysql",
                name="on-the-server",
                started=descriptor.now_stamp(),
                meta={
                    "CONTAINER": "tdx2_remote",
                    "HOST": "localhost",
                    "PORT": "33061",
                    "USER": "root",
                    "PASSWORD": "pw",
                    "IMAGE": "mysql:8.4",
                },
            ).model_dump_json().encode("utf-8")
        )
    opened: list[tuple] = []
    closed: list[int] = []
    monkeypatch.setattr(ssh, "fetch_remote_file", lambda ep, path: pem)
    monkeypatch.setattr(
        ssh,
        "open_tunnel",
        lambda ep, local, target, log: (opened.append((ep.target, local, target)), 4242)[
            1
        ],
    )
    monkeypatch.setattr(ssh, "close_tunnel", lambda pid: closed.append(pid))
    monkeypatch.setattr(ssh, "tunnel_alive", lambda pid: True)
    return {"opened": opened, "closed": closed, "pem": pem}


def _connect(*extra):
    return tdx._dispatch(
        SYS,
        [
            "connect",
            "--remote-sys",
            "deploy@far-host:/srv/sys.mysql",
            "--remote-secret-env",
            "REMOTE_KEY",
            "--sys",
            "remote1",
            *extra,
        ],
    )


def test_connect_writes_a_local_descriptor_pointing_at_the_tunnel(remote):
    assert _connect() == 0
    info = descriptor.load(store.system_path("remote1"))
    assert info.system == "mysql" and info.name == "remote1"
    # the near end of the tunnel, not the port the container publishes remotely
    assert info.meta["HOST"] == "localhost"
    assert info.meta["PORT"] != "33061" and info.meta["PORT"].isdigit()
    assert info.meta[ssh.SSH_TARGET_PORT] == "33061"
    assert info.meta[ssh.TUNNEL_PID] == "4242"
    assert info.meta[ssh.SSH_HOST] == "far-host" and info.meta[ssh.SSH_USER] == "deploy"
    # the credentials come across so a domain can be created over the tunnel
    assert info.meta["USER"] == "root" and info.meta["PASSWORD"] == "pw"
    target, local, remote_port = remote["opened"][0]
    assert target == "deploy@far-host" and remote_port == 33061
    assert str(local) == info.meta["PORT"]


def test_the_local_descriptor_is_readable_with_the_local_secret(remote):
    assert _connect() == 0
    # written under $TD_BACKENDS_CRYPTO_KEY (the conftest's), not the server's
    assert descriptor.load(store.system_path("remote1")).system == "mysql"


def test_connect_refuses_to_clobber_without_force(remote):
    assert _connect() == 0
    with pytest.raises(TdxError, match="already exists"):
        _connect()
    assert _connect("-f") == 0


def test_a_failed_write_closes_the_tunnel(remote, monkeypatch):
    monkeypatch.setattr(
        descriptor, "write_system", lambda *a, **k: (_ for _ in ()).throw(OSError("disk"))
    )
    with pytest.raises(OSError):
        _connect()
    # a tunnel nothing records is a tunnel nothing can ever close
    assert remote["closed"] == [4242]


def test_the_remote_secret_must_be_set(remote, monkeypatch):
    monkeypatch.delenv("REMOTE_KEY", raising=False)
    with pytest.raises(TdxError, match=r"\$REMOTE_KEY is not set"):
        _connect()


def test_destroy_closes_the_tunnel_instead_of_removing_a_container(remote, monkeypatch):
    from tabsdatax.itest.tdksystems2.systems.mysql.backend import Mysql

    monkeypatch.setattr(
        Mysql,
        "destroy_system",
        lambda self, meta: pytest.fail("must not touch a local Docker daemon"),
    )
    assert _connect() == 0
    assert tdx._dispatch(SYS, ["destroy", "--sys", "remote1"]) == 0
    assert remote["closed"] == [4242]
    assert not store.system_path("remote1").exists()


def test_status_probes_the_tunnel_not_a_local_container(remote, monkeypatch, capsys):
    from tabsdatax.itest.tdksystems2.systems._common.commands import sys_status
    from tabsdatax.itest.tdksystems2.systems.mysql.backend import Mysql

    monkeypatch.setattr(
        Mysql,
        "system_status",
        lambda self, meta: pytest.fail("must not look for a local container"),
    )
    assert _connect() == 0
    monkeypatch.setattr(sys_status, "port_open", lambda host, port: True)
    assert tdx._dispatch(SYS, ["status", "--sys", "remote1"]) == 0
    assert "tunnelled to deploy@far-host" in capsys.readouterr().out

    monkeypatch.setattr(ssh, "tunnel_alive", lambda pid: False)
    assert tdx._dispatch(SYS, ["status", "--sys", "remote1"]) == 1
    assert "tunnel to deploy@far-host is gone" in capsys.readouterr().out


def test_restart_is_a_no_op_over_a_tunnel(remote, monkeypatch, capsys):
    from tabsdatax.itest.tdksystems2.systems.mysql.backend import Mysql

    monkeypatch.setattr(
        Mysql,
        "restart_system",
        lambda self, meta: pytest.fail("the far host owns the container"),
    )
    assert _connect() == 0
    assert tdx._dispatch(SYS, ["restart", "--sys", "remote1"]) == 0
    assert "nothing to restart" in capsys.readouterr().out


def test_connect_is_offered_only_by_container_systems():
    tdx._require_verb(SYS, "mysql", constants.V_CONNECT)
    for slug in ("s3", "snowflake", "openai", "fs"):
        with pytest.raises(TdxError, match="does not support 'connect'"):
            tdx._require_verb(SYS, slug, constants.V_CONNECT)
