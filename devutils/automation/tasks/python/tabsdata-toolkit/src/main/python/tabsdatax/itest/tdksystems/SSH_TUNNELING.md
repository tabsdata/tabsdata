# SSH-tunneled access to container systems — design write-up

Status: **proposal** (not implemented). Scope: **container systems only**
(`mysql`, `postgres`, `mssqlserver`, `oracle`, `db2`, `clickhouse`, `mongodb`,
`starrocks`, `redpanda`). Cloud / SaaS systems are already network-reachable and
out of scope.

## 1. The goal

A container system and its domains today only work on the one machine running
the Docker daemon: `tdx-system create` publishes the container on a host port
and bakes `HOST=localhost, PORT=<port>` into the descriptor
(`systems/_container.py:119-133`); domains inherit that
(`systems/_container.py:196-218`); domain admin runs via `docker exec` against
the **local** daemon (`systems/mysql/system.py:67-91` → `shell.docker_exec`).

We want a **server** (has Docker, runs the container) and a separate **client**
(runs `tdk` / the connectors) to share one system, with two hard constraints:

- **The client needs only `ssh` — never Docker.** Anything that must touch
  Docker runs *on the server, over SSH*.
- **The server does nothing SSH-specific.** It runs a plain `tdx-system create`.
  No `--ssh-*` flags, no special `sshd` config beyond ordinary login + the
  default `AllowTcpForwarding`. The SSH endpoint is supplied by the client at
  connect time.

The shape:

```
tdx-system connect user@host:/path/to/srv-mysql.tdx --sys local-mysql.tdx
```

`connect` fetches the (encrypted) system file over SSH, opens a background tunnel
on a random local port, and writes a **local system descriptor** pointing at
`localhost:<local-port>`. Domains are then created with the *ordinary*
`tdx-domain create --sys local-mysql.tdx` and Just Work. `tdx-system destroy
--sys local-mysql.tdx` detects the tunnel handle and shuts the tunnel down.

## 2. What a client needs to get access (onboarding)

Everything the client needs is standard SSH plumbing — the server never learns
about tdx:

1. **An SSH client.** `ssh` (+ `scp`, which ships with OpenSSH). Present on
   macOS, Linux, and Windows 10 (1809+)/11 (OpenSSH client, a default optional
   feature). **No Docker.**
2. **Login to the server.** Generate/pick an SSH keypair and get the *public*
   key onto the server's `~/.ssh/authorized_keys` (the normal `ssh-copy-id` /
   paste-into-authorized_keys step). This is the only server-side touch, and it's
   plain SSH — not tdx-specific.
3. **The server's personal secret**, shared out-of-band, in an env var, e.g.
   `export TD_DEV_PERSONAL_SECRET_SERVER='…'`. The system file is encrypted with
   the server's `TD_DEV_PERSONAL_SECRET` (`crypto.py`); the client needs that
   secret to read it. (`connect` re-encrypts the *local* handle with the client's
   own secret, so nothing after `connect` needs the server secret again.)
4. **The path to the system file on the server**, e.g.
   `user@host:/home/ci/systems/srv-mysql.tdx`.

Then:

```bash
export TD_DEV_PERSONAL_SECRET='…client secret…'          # encrypts local files this client writes
export TD_DEV_PERSONAL_SECRET_SERVER='…server secret…'   # decrypts the fetched system file

tdx-system connect user@host:/home/ci/systems/srv-mysql.tdx \
    --sys local-mysql.tdx --remote-secret-env TD_DEV_PERSONAL_SECRET_SERVER
```

That's it — from here the client uses `tdx-domain` / `tdk` against
`local-mysql.tdx`.

## 3. Why this needs new machinery

Two facts about the current code drive the design:

1. **The client has no Docker daemon.** Domain admin
   (`do_create`/`do_delete`/`do_list`) goes through `shell.docker_exec`, which
   runs `docker exec <container> …` locally (`shell.py:140-159`). The client has
   no local container — so admin must run *on the server over SSH* (§5.2).
2. **Descriptors are encrypted with one env var.** `crypto.require_secret()`
   reads `TD_DEV_PERSONAL_SECRET` and nothing else (`crypto.py:40-51`); both
   `encrypt` and `decrypt` call it. We must read the fetched server file with the
   server secret while writing the local handle with the client secret (§4.1).

## 4. New surface area

### 4.1 `--secret-env` / `--remote-secret-env`

> "an option to specify the env to get the personal secret"

```
--secret-env VAR          # secret for THIS command's descriptors (default: TD_DEV_PERSONAL_SECRET)
--remote-secret-env VAR   # connect only: secret to decrypt the FETCHED system file
                          #               (default: same as --secret-env)
```

`tdx.py::_dispatch` already gates descriptor verbs on `crypto.require_secret()`
(`tdx.py:450-453`). The change: resolve `--secret-env` up front and thread the
chosen secret into `crypto`, instead of `crypto` reading the env var itself.

**Crypto plumbing.** `crypto.require_secret/encrypt/decrypt` must accept an
explicit secret rather than always reading `TD_DEV_PERSONAL_SECRET`. The key
cache is already keyed by secret *value* (`crypto.py:37,54-60`), so several
secrets coexist in one process. `descriptor.load/write/_write_doc/resolve/
read_meta` gain an optional `secret=` they forward to crypto. This is the one
non-trivial refactor; everything else composes on top.

### 4.2 `tdx-system connect user@host:PATH --sys LOCAL`

> "tdx-system connect --remote-sys FILE --sys FILE … start the ssh tunnel …
> create a system file for local access … the server side does not do anything
> special for ssh"

```
tdx-system connect --remote-sys [user@]host:REMOTE_PATH --sys LOCAL_FILE \
    [--ssh-port N] [--identity KEYFILE] \
    [--remote-secret-env VAR] [--secret-env VAR]
```

`--remote-sys` is an **scp-style spec** `[user@]host:REMOTE_PATH`. `user`/`host`
are the SSH endpoint (reused for the tunnel *and* admin, §5); `REMOTE_PATH` is
where the encrypted system file lives on the server.

Steps:

1. **Fetch the file over SSH.** Primary: `ssh [-p N] [-i KEY] user@host -- cat --
   REMOTE_PATH` streamed into memory (single connection, no temp file, no `scp`
   protocol; the file is PEM/ASCII). Fallback: `scp` to a scratch temp. Either
   way the *ciphertext* is all that crosses; decrypt in memory with
   `--remote-secret-env`.
2. From the decrypted doc, read `CONTAINER`, admin creds, and `PORT` (the
   container's published host port on the server → the tunnel target).
3. `shell.find_free_port()` → `<local-port>` (`shell.py:64-70`).
4. Start the **background** SSH tunnel `127.0.0.1:<local-port>` →
   server `127.0.0.1:<PORT>` using the same `user@host` (+ `--ssh-port`,
   `--identity`); probe the local port until it accepts (fail fast if the tunnel
   can't come up — §6).
5. Write `LOCAL_FILE` (encrypted with `--secret-env`) — a *system* descriptor
   with `HOST=localhost, PORT=<local-port>`, the SSH endpoint, and the remote
   back-refs (§7). Same `system=mysql, category=container`, so `tdx-domain
   create --sys LOCAL_FILE` uses `MysqlSystem`'s hooks unchanged.

The server needs nothing but a readable file at `REMOTE_PATH`, `cat` (any POSIX
Docker host has it), and SSH login. `tdx-system create` on the server takes **no
new flags**.

### 4.3 `tdx-system destroy` becomes tunnel-aware

> "tdx-system destroy --sys FILE would detect that is remote system file, and it
> would shutdown the tunnel"

`_sys_destroy` (`tdx.py:200-212`) dispatches to `ContainerBase.destroy_system`
(`_container.py:135-142`, which `docker rm`s). Add a branch keyed on a tunnel
marker in the descriptor (`SYSTEM_LOCATION=ssh-tunnel` / presence of
`SSH_TUNNEL_PID`): a connected descriptor → **tear the tunnel down** (§6) and
remove the local file; do **not** `docker rm` (there's no local container, and
the remote one is not ours to remove here — destroy the *remote* file on the
server for that).

## 5. The SSH endpoint does two jobs (client → server, one endpoint + key)

Both use the `user@host` (+ port/identity) captured at `connect`.

### 5.1 Persistent data tunnel

One long-lived forward per connected system:

```
client 127.0.0.1:<local-port>  ──►  (ssh user@host)  ──►  server 127.0.0.1:<PORT>
```

`127.0.0.1:<PORT>` is resolved **on the server**, so it hits the Docker-published
port. Everything downstream — `tdk`, `info` (`descriptor.info`), and the data
pipeline (`install_schema`/`load_data`, which connect via `SQLALCHEMY_URI`,
`_container.py:233-275`) — talks to `localhost:<local-port>` with **no changes**,
because the local handle bakes that as `HOST/PORT` and domains inherit it.

### 5.2 Domain admin — on the server, over SSH (no client Docker)

`do_create`/`do_delete`/`do_list` shell out to `docker exec` (`shell.py:140-159`).
Keep them **unchanged**, but make `shell.docker_exec`/`docker_exec_check`
SSH-aware: when an SSH context is active, prefix `ssh user@host --`:

```
ssh -p <port> user@host -- docker exec -i <container> mysql …
```

Docker runs **on the server** (which has it); the **client only runs `ssh`**.
This is a short, blocking `ssh` per admin op (no ControlMaster needed → fully
cross-platform, §6), independent of the persistent forwarding tunnel. Every
per-system hook is reused as-is, so no per-system files change.

> **Alternative (hardened, more work): TCP admin over the tunnel.** Run the admin
> DDL over a TCP SQL connection (admin creds) through the tunnel, so *no shell
> command* runs on the server and the SSH key can be locked forward-only (§8).
> This means porting each system's admin from its container CLI (`mysql`,
> `sqlplus`, `db2` CLP …) to a TCP driver — meaningful per-system work. Recommend
> shipping the `ssh -- docker exec` path first.

Either way: **client = SSH only, never Docker.**

## 6. Background + multiplatform tunnel — the mechanics

**Yes — a background tunnel that survives the `connect` command and works on
macOS / Linux / Windows is doable.** The portable approach spawns the OS `ssh`
client as a **detached child process** and tracks its PID — deliberately *not*
relying on features that don't exist everywhere.

### Why the OS `ssh` binary (not paramiko)

- Already the toolkit idiom (shell out to `docker`, `op`), no new Python
  dependency, honors `~/.ssh/config`, keys, and `ssh-agent`.
- Present on all three targets (see §2). `connect` should error clearly if `ssh`
  isn't on `PATH`.
- Pure-Python `sshtunnel`/`paramiko` avoids the binary but, to run in the
  *background*, needs a detached Python daemon hosting the forwarder thread
  (re-implementing the process management below anyway), with more brittle
  key/agent/known_hosts handling. Keep only as a fallback if "no `ssh` binary"
  ever becomes real.

### What does NOT port

- **ControlMaster (`-M -S -O check/exit`)** uses Unix-domain sockets and is
  **not supported by Win32-OpenSSH.** Nice for clean teardown/health on
  macOS/Linux; unavailable on Windows.
- **`ssh -f`** (fork-to-background) is POSIX-only and unreliable on Windows.

So the **portable baseline is a detached `Popen` + PID**, with ControlMaster used
only as a POSIX-side enhancement.

### Launch (detached, backgrounded)

```python
args = [
    "ssh", "-N",                       # no remote command; just forward
    "-o", "ExitOnForwardFailure=yes",  # exit if the -L bind fails → we detect it
    "-o", "BatchMode=yes",             # never prompt; fail fast if auth isn't set up
    "-o", "ServerAliveInterval=30",    # keep-alive; drop a dead tunnel
    "-o", "ServerAliveCountMax=3",
    "-L", f"127.0.0.1:{local}:127.0.0.1:{target}",
    "-p", str(ssh_port), f"{ssh_user}@{ssh_host}",
]
logf = open(log_path, "ab")           # capture ssh stderr for diagnostics
if os.name == "nt":
    flags = subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP
    proc = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=logf,
                            stderr=logf, creationflags=flags)
else:
    proc = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=logf,
                            stderr=logf, start_new_session=True)  # setsid: own group, detached
```

- `start_new_session=True` (POSIX) detaches from the controlling terminal and
  makes the child a process-group leader, so it survives `connect` exiting and
  can be signalled as a group.
- `CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP` (Windows) runs it windowless and
  detached.

### Readiness probe (both platforms)

After spawn, poll `connect_ex(("127.0.0.1", local))` until it succeeds or a short
deadline passes (the `find_free_port` socket idiom, `shell.py:64-70`). With
`ExitOnForwardFailure=yes`, a bad forward makes `ssh` exit and the probe fails →
`connect` reports a clear error and writes **no** dangling local file.

### Teardown (`tdx-system destroy` on a connected file)

Store `SSH_TUNNEL_PID` (and, on POSIX, an optional control socket) in the local
descriptor. Then:

- **POSIX:** `os.killpg(os.getpgid(pid), SIGTERM)` (group kill, thanks to
  `start_new_session`). If a ControlMaster socket was used, prefer
  `ssh -O exit -S <sock>` for a graceful close.
- **Windows:** `taskkill /PID <pid> /T /F` (`/T` kills the child tree). Plain
  `Popen.terminate()` won't take down descendants.

Guard against PID reuse best-effort: before killing, confirm the PID is an `ssh`
to our host (`tasklist /FI "PID eq …"` on Windows, `ps`/`/proc` on POSIX). In a
dev tool a stored-PID + best-effort check is acceptable; note the caveat.

### Status (`tdx-system status` on a connected file)

Tunnel-aware: PID alive **and** the local port accepts (and, where available,
`ssh -O check -S <sock>`). This replaces the container check, which the client
can't run.

### Recommendation

| concern              | macOS / Linux                    | Windows                                |
|----------------------|----------------------------------|----------------------------------------|
| spawn detached       | `start_new_session=True`         | `CREATE_NO_WINDOW`+`NEW_PROCESS_GROUP` |
| background           | `Popen` (no `-f` needed)         | `Popen` (no `-f`)                      |
| clean teardown       | `ssh -O exit` **or** `killpg`    | `taskkill /T /F` (PID)                 |
| health check         | `ssh -O check` / port probe      | port probe / PID alive                 |
| admin over SSH (§5.2)| short `ssh -- docker exec`       | short `ssh -- docker exec`             |
| fetch file (§4.2)    | `ssh -- cat` / `scp`             | `ssh -- cat` / `scp`                   |

Net: **portable via a detached `Popen(ssh -N -L …)` + PID**, with ControlMaster
as a POSIX-only nicety. No `-f`, no ControlMaster dependency, no new Python deps,
no Docker on the client.

## 7. What each descriptor holds

- **Remote system file** (`tdx-system create`, on the server; encrypted with the
  server secret): the *normal* container meta — `CONTAINER/HOST/PORT/USER/
  PASSWORD/IMAGE` (`_container.py:119-133`). **No SSH fields** — the server does
  nothing SSH-specific; the endpoint comes from the client's `connect` argument.
- **Local (connected) system file** (`tdx-system connect`, on the client;
  encrypted with the client secret):
  - `HOST=localhost`, `PORT=<local-port>` — so domains inherit the tunnel.
  - `SYSTEM_LOCATION=ssh-tunnel` marker + `SSH_TUNNEL_PID` (+ optional control
    socket) → `destroy`/`status` tunnel-aware.
  - `SSH_HOST/SSH_USER/SSH_PORT` (from the `connect` spec) and `SSH_TARGET_PORT`
    (= the remote `PORT`) → to reconnect and to run admin over SSH.
  - The remote `CONTAINER` + admin creds (copied from the fetched file) → so
    `ssh -- docker exec` admin (§5.2) works.
- **Domain file** (`tdx-domain create --sys local-mysql.tdx`): **unchanged
  shape** — `HOST=localhost, PORT=<local-port>, SQLALCHEMY_URI=…localhost:<local-port>…`
  built by `do_create` (`systems/mysql/system.py:76-77`, reading the `PORT` env
  that `create_domain` sets, `_container.py:184`). No SSH awareness in the domain
  file — the tunnel is the system handle's job.

## 8. End-to-end flow

### On the server (Docker host) — nothing SSH-specific

```bash
export TD_DEV_PERSONAL_SECRET='…server secret…'
tdx-system create --type mysql --sys /home/ci/systems/srv-mysql.tdx --name shop
# (optional hardening, §9) publish the DB on loopback only, so it is reachable
# ONLY through an SSH tunnel: a --bind-loopback flag on create.
```

### On the client (`tdk`, `ssh` only, no Docker)

```bash
export TD_DEV_PERSONAL_SECRET='…client secret…'
export TD_DEV_PERSONAL_SECRET_SERVER='…server secret…'

# 1. Fetch the system file over SSH, open the tunnel, write a local handle.
tdx-system connect user@host:/home/ci/systems/srv-mysql.tdx \
    --sys local-mysql.tdx --remote-secret-env TD_DEV_PERSONAL_SECRET_SERVER

# 2. Create domains normally, against the local handle (schema/data flow the tunnel).
tdx-domain create --sys local-mysql.tdx --domain shop-dev.tdx --name dev \
    --model sakila --data s
source <(tdx-domain info --domain shop-dev.tdx --format export)   # tdk → localhost:<local-port>

# 3. Tear down.
tdx-domain destroy --domain shop-dev.tdx      # drops the DB (admin: ssh -- docker exec)
tdx-system destroy  --sys   local-mysql.tdx   # detects tunnel handle → stops tunnel, rm local file
```

## 9. Security considerations

- **`--bind-loopback` on create (optional).** `_build_docker_args` emits
  `-p {host_port}:{internal_port}` (`_container.py:299-309`), binding all
  interfaces — the DB is reachable from anywhere the server firewall allows. For
  tunnel-only access, an opt-in `-p 127.0.0.1:{host_port}:{internal_port}` makes
  the DB reachable *only* through SSH. This is a general hardening, decoupled from
  the (now flagless) SSH flow.
- **Forward-only client keys.** `restrict,permitopen="127.0.0.1:<PORT>",no-pty`
  in the server's `authorized_keys` locks a key to *just* forwarding that port —
  the strongest posture, but incompatible with `ssh -- docker exec`/`ssh -- cat`
  (§5.2 v1, §4.2 fetch). Use it only with the TCP-admin variant and a separate
  out-of-band file transfer, or keep an unrestricted key for connect/admin.
- **Descriptors stay encrypted at rest** on both machines (`crypto.py`); the
  server secret lives only in `TD_DEV_PERSONAL_SECRET_SERVER` on the client and
  is never written to a descriptor (the local handle is re-encrypted under the
  client secret).
- **`BatchMode=yes` + host-key policy.** `connect`/admin never prompt; document
  `StrictHostKeyChecking` expectations (first-connect `accept-new`, or a
  pre-seeded `known_hosts`).

## 10. Lifecycle summary and follow-ups

| verb                          | behavior on a connected (tunnel) file                              |
|-------------------------------|--------------------------------------------------------------------|
| `tdx-system connect`          | fetch file over SSH + open background tunnel + write local handle  |
| `tdx-system destroy --sys …`  | detect tunnel handle → stop tunnel (killpg / taskkill / `-O exit`) + rm file; **no** `docker rm` |
| `tdx-system status --sys …`   | PID alive + local port accepts (+ `ssh -O check` where available)  |
| `tdx-system info --sys …`     | unchanged (prints `HOST=localhost`, `PORT=<local-port>`, …)        |
| `tdx-domain create/destroy`   | ordinary; admin via `ssh -- docker exec` (§5.2); data over tunnel  |

**Follow-ups (out of v1 scope):**

- `tdx-system reconnect --sys …` — re-open a dropped tunnel after a client reboot
  (the baked local port may be taken → pick a new one + rewrite the handle).
- `tdx-system pause/resume` on the *remote* file (server side) still work
  (`_container.py:156-172`); a paused container just makes connected clients'
  tunnels dead until resumed (surfaced by tunnel-aware `status`).
- Multiple connected systems / multiple clients → independent tunnels, ports,
  PIDs; nothing shared, so it already composes.

## 11. Implementation touch-points (minimal diff)

1. `crypto.py` — parameterize `require_secret/encrypt/decrypt` by explicit
   secret; keep `TD_DEV_PERSONAL_SECRET` as default.
2. `descriptor.py` — thread optional `secret=` through `load`/`write`/
   `_write_doc`/`resolve`/`read_meta`; carry the SSH/tunnel meta in the container
   writers.
3. `tdx.py` — add `--secret-env` (all verbs) and `--remote-secret-env`; add the
   `connect` verb to `_SYS_VERBS` (`tdx.py:425-428`) parsing `[user@]host:PATH`;
   make `_sys_destroy`/`_sys_status` tunnel-aware. `tdx-system create` gains no
   SSH flags (only optional `--bind-loopback`, §9).
4. New `ssh.py` helper — fetch a remote file (`ssh -- cat` / `scp`), build/spawn
   a detached `ssh -N -L …` (§6), probe the port, check/kill the tunnel across
   platforms; plus SSH-awareness in `shell.docker_exec`/`docker_exec_check` for
   admin (§5.2).
5. `systems/_container.py` — a `connect_system` path (fetch, spawn tunnel, return
   the local handle's settings) and tunnel-aware `destroy_system`/`system_status`;
   per-system `systems/<slug>/system.py` files need **no changes**.
6. `systems/base.py` — a `connect`/`is_remote` capability on the `System`
   contract (default: unsupported), implemented by `ContainerBase`.
```
