"""`tdx-system connect`: open an SSH tunnel to a remote container system and
write a LOCAL system descriptor pointing at localhost:<tunnel-port>. The remote
descriptor is decrypted with the server secret (--remote-secret-env); the local
one is written under the client's own secret. Destroying the local handle stops
the tunnel (never touches Docker). SSH is mocked here; the real localhost tunnel
is exercised by the itest."""
from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems import crypto, descriptor, ssh, tdx
from tabsdatax.itest.tdksystems.errors import DatagenError

SERVER_SECRET_ENV = "TD_DEV_PERSONAL_SECRET_SERVER"


@pytest.fixture(autouse=True)
def _reset_secret_env():
    crypto.use_secret_env(None)
    yield
    crypto.use_secret_env(None)


def _write_remote_mysql(path, secret_env: str) -> None:
    """A remote mysql system descriptor, encrypted under the server secret."""
    with crypto.using_secret_env(secret_env):
        descriptor.write_container_system(
            out=str(path), system_type="mysql", system_kind="sql", label="MySQL",
            container="shop_db", host="localhost", port="53306",
            user="root", password="rootpw", domain_desc="a database",
            image="mysql:8.4")
        descriptor.stamp(str(path), name="shop", started="t")


def test_parse_remote_spec_variants():
    ep, path = ssh.parse_remote_spec("me@host.example:/srv/x.tdx")
    assert (ep.user, ep.host, path) == ("me", "host.example", "/srv/x.tdx")
    ep, path = ssh.parse_remote_spec("host:/x.tdx")
    assert (ep.user, ep.host, path) == (None, "host", "/x.tdx")
    for bad in ("nopath", ":/x", "host:", ""):
        with pytest.raises(DatagenError):
            ssh.parse_remote_spec(bad)


def test_using_secret_env_round_trip(monkeypatch):
    monkeypatch.setenv("TD_DEV_PERSONAL_SECRET", "client-secret-123456")
    monkeypatch.setenv(SERVER_SECRET_ENV, "server-secret-123456")
    with crypto.using_secret_env(SERVER_SECRET_ENV):
        blob = crypto.encrypt(b"hi")
    # Decrypts under the server secret, not the (default) client secret.
    with crypto.using_secret_env(SERVER_SECRET_ENV):
        assert crypto.decrypt(blob) == b"hi"
    with pytest.raises(DatagenError):
        crypto.decrypt(blob)   # active var reset to client -> wrong key


def test_connect_writes_local_tunnel_descriptor(tmp_path, monkeypatch):
    monkeypatch.setenv("TD_DEV_PERSONAL_SECRET", "client-secret-123456")
    monkeypatch.setenv(SERVER_SECRET_ENV, "server-secret-123456")
    remote = tmp_path / "remote_mysql.tdx"
    _write_remote_mysql(remote, SERVER_SECRET_ENV)
    remote_text = remote.read_text()

    seen: dict = {}
    monkeypatch.setattr(ssh, "fetch_remote_file",
                        lambda ep, path: seen.update(ep=ep, path=path) or remote_text)
    monkeypatch.setattr(ssh, "open_tunnel",
                        lambda ep, local, target, log, **kw:
                        seen.update(local=local, target=target) or 424242)

    local = tmp_path / "local_mysql.tdx"
    rc = tdx._sys_connect([
        "--remote-sys", "me@server.example:/srv/remote_mysql.tdx",
        "--sys", str(local),
        "--ssh-port", "2222", "--remote-secret-env", SERVER_SECRET_ENV])
    assert rc == 0
    assert seen["path"] == "/srv/remote_mysql.tdx"
    assert (seen["ep"].user, seen["ep"].host, seen["ep"].port) == ("me", "server.example", 2222)
    assert seen["target"] == 53306

    # The local descriptor reads back under the CLIENT secret and points at the tunnel.
    meta = descriptor.resolve(descriptor.load(str(local)))
    assert meta["HOST"] == "localhost"
    assert meta["PORT"] == str(seen["local"])
    assert meta["SSH_TUNNEL_PID"] == "424242"
    assert meta["SSH_HOST"] == "server.example"
    assert meta["SSH_PORT"] == "2222"
    assert meta["SSH_TARGET_PORT"] == "53306"
    assert meta["SYSTEM_LOCATION"] == "ssh-tunnel"
    # Admin identity carried over so domains can be provisioned via the tunnel.
    assert meta["CONTAINER"] == "shop_db"
    assert meta["USER"] == "root" and meta["PASSWORD"] == "rootpw"


def test_connect_rejects_non_container(tmp_path, monkeypatch):
    monkeypatch.setenv("TD_DEV_PERSONAL_SECRET", "client-secret-123456")
    remote = tmp_path / "s3.tdx"
    descriptor.write_cloud_system(
        out=str(remote), storage_type="s3", system_kind="file", label="S3",
        sys_path="data", sys_path_env=None, forwarded=[("BUCKET", "b", None)])
    descriptor.stamp(str(remote), name="store", started="t")
    monkeypatch.setattr(ssh, "fetch_remote_file", lambda ep, p: remote.read_text())
    monkeypatch.setattr(ssh, "open_tunnel", lambda *a, **k: 1)
    with pytest.raises(DatagenError, match="container-only"):
        tdx._sys_connect(["--remote-sys", "h:/s3.tdx",
                          "--sys", str(tmp_path / "l.tdx")])


def test_destroy_connected_stops_tunnel_not_docker(tmp_path, monkeypatch):
    monkeypatch.setenv("TD_DEV_PERSONAL_SECRET", "client-secret-123456")
    local = tmp_path / "local.tdx"
    descriptor.write_container_system(
        out=str(local), system_type="mysql", system_kind="sql", label="MySQL",
        container="shop_db", host="localhost", port="49999", user="root",
        password="rootpw", domain_desc="db", image="mysql:8.4",
        location="ssh-tunnel",
        extra_vars={"SSH_HOST": "server.example", "SSH_USER": "me",
                    "SSH_PORT": "22", "SSH_TARGET_PORT": "53306",
                    "SSH_TUNNEL_PID": "777777"})
    descriptor.stamp(str(local), name="shop", started="t")

    killed: dict = {}
    monkeypatch.setattr(ssh, "close_tunnel", lambda pid: killed.setdefault("pid", pid))
    import tabsdatax.itest.tdksystems.systems._container as cont

    def _boom(_name):
        raise AssertionError("docker must not be touched for a tunnel handle")
    monkeypatch.setattr(cont, "container_state", _boom)

    rc = tdx._sys_destroy(["--sys", str(local)])
    assert rc == 0
    assert killed["pid"] == 777777
    assert not local.exists()
