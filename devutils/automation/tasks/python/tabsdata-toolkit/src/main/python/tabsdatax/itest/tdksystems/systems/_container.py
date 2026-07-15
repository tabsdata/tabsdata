#
# Copyright 2026 Tabsdata Inc.
#

"""Shared container lifecycle on the unified `System` contract.

A container system's *pluggable* logic is a small hook set (image config +
`_docker_run` / `check_ready` / `do_create` / `do_delete` / `do_list`, and the
optional `_post_start` / `generate_password` / `_readiness_fatal` /
`_system_extra_vars` / `_domain_extra_vars`). This base drives the docker
lifecycle and the SQL data plane around those hooks; each `systems/<slug>/`
subclass supplies only the hooks.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass

from ..errors import DatagenError
from ..shell import (
    container_exists,
    container_id,
    container_state,
    docker_pull,
    docker_rm,
    docker_start,
    docker_stop,
    find_free_port,
    gen_password,
    port_open,
    require_docker,
    validate_name,
)
from .base import (
    CreateDomainSpec,
    CreateSystemSpec,
    DomainContext,
    Health,
    LoadRequest,
    Setting,
    System,
    SystemContext,
)

# Docker container names are prefixed with this so tdx-managed containers are
# recognisable (and don't collide with unrelated containers named after the bare
# system). The descriptor records the prefixed name as CONTAINER, so destroy /
# status / pause / resume address it by the same name.
CONTAINER_NAME_PREFIX = "tdx_"


def container_name(sys_name: str) -> str:
    """The docker container name for a system named `sys_name`: the system name
    prefixed with ``tdx_``."""
    return f"{CONTAINER_NAME_PREFIX}{sys_name}"


@dataclass
class DomainResult:
    """Outcome of `do_create` — the domain's connection info."""

    db: str
    user: str
    password: str
    sqlalchemy_uri: str


@dataclass
class AdminConn:
    """Superuser admin connection to a running container's engine, over TCP on
    the published host port — the replacement for docker-exec-as-superuser.

    `create_domain` / `destroy_domain` build it from the descriptor's HOST /
    PORT and the admin USER / PASSWORD, and hand it to the `do_create` /
    `do_delete` / `do_list` hooks. Because admin now travels over TCP (not a
    local Docker socket), a client with only network reach — e.g. through an
    SSH tunnel — can provision domains without a local Docker daemon."""

    host: str
    port: str
    user: str
    password: str


class ContainerBase(System):
    category = "container"
    kind = "sql"
    supports_pause = True
    domain_flag = "database"  # --domain names a database/schema/topic
    supports_sql = True
    domain_desc = ""
    options = ()  # port is allocated, password generated

    # --- image / docker config (subclass attributes) ---
    image = ""
    image_platform = ""
    internal_port = 0
    system_user = ""
    password_env_name = ""
    extra_env: list[tuple[str, str]] = []
    extra_flags: list[str] = []
    cmd_args: list[str] = []

    # File-staging systems (clickhouse/starrocks) carry a SYS_PATH staging root.
    uses_file_staging = False

    @property
    def staging_root(self) -> bool:  # base adds the SYS_PATH option
        return self.uses_file_staging

    def docker_images(self) -> list[tuple[str, str]]:
        return [(self.image, self.image_platform)]

    def add_create_options(self, parser) -> None:
        super().add_create_options(parser)  # --sys-path/--env-prefix if staging
        parser.add_argument(
            "--port",
            type=int,
            default=None,
            help="optional; host port (default: a free port)",
        )
        parser.add_argument(
            "--ready-timeout",
            type=int,
            default=300,
            help="optional; seconds to wait for readiness (default: 300)",
        )

    # ==================================================================
    # System lifecycle
    # ==================================================================

    def _staging_setting(self, spec: CreateSystemSpec) -> Setting:
        """The SYS_PATH setting to bake for a file-staging system. An explicit
        ``--sys-path`` / a *set* ``$<PREFIX>__SYS_PATH`` is kept as given (a
        literal, or a forwarded env ref that re-resolves at use time). Otherwise
        the staging root defaults to the system name."""
        sp = spec.settings.get("SYS_PATH")
        try:
            resolved = self.resolve_values({"SYS_PATH": sp})["SYS_PATH"] if sp else ""
        except DatagenError:
            resolved = ""  # its env ref points at an unset var
        if sp and resolved:
            return Setting("SYS_PATH", value=sp.value, env_var=sp.env_var, is_path=True)
        return Setting("SYS_PATH", value=spec.name, is_path=True)

    def plan_system(self, spec: CreateSystemSpec) -> dict[str, Setting]:
        require_docker()
        container = container_name(spec.name)
        password = self.generate_password()
        port = spec.host_port or find_free_port()
        # Export PORT / CONTAINER so a system's `_system_extra_vars` (e.g.
        # redpanda's schema-registry endpoint) resolves against the planned
        # values before the container exists.
        os.environ["PORT"] = str(port)
        os.environ["CONTAINER"] = container
        out: dict[str, Setting] = {
            "CONTAINER": Setting("CONTAINER", value=container),
            "IMAGE": Setting("IMAGE", value=self.image),
            "HOST": Setting("HOST", value="localhost"),
            "PORT": Setting("PORT", value=str(port)),
            "USER": Setting("USER", value=self.system_user),
            "PASSWORD": Setting("PASSWORD", value=password),
        }
        if self.uses_file_staging:
            out["SYS_PATH"] = self._staging_setting(spec)
        for k, v in self._system_extra_vars().items():
            out[k] = Setting(k, value=v)
        return out

    def provision_system(
        self, spec: CreateSystemSpec, settings: dict[str, Setting]
    ) -> None:
        require_docker()
        container = settings["CONTAINER"].value or ""
        password = settings["PASSWORD"].value or ""
        port = int(settings["PORT"].value or "0")
        if container_exists(container):
            if spec.force:
                docker_rm(container, force=True)
                print(f">>> --force: removed existing container {container}")
            else:
                raise DatagenError(
                    f"container already exists: {container} (use --force)"
                )
        # flush so our line precedes docker's own pull output when stdout is
        # redirected to a log (block-buffered), not just on a TTY.
        print(f">>> pulling docker image {self.image} …", flush=True)
        docker_pull(self.image, self.image_platform)
        print(f">>> starting container {container} from image {self.image}")
        self._docker_run(container, port, password)
        print(
            f">>> waiting for {self.label} to become ready "
            f"(timeout: {spec.ready_timeout}s)…"
        )
        self._wait_ready(container, password, spec.ready_timeout)
        # Export PORT so hooks (do_create URIs, staging) that read env work.
        os.environ["PORT"] = str(port)
        os.environ["CONTAINER"] = container
        self._post_start(container, password)

    def provisioned_meta(self, settings: dict[str, Setting]) -> dict[str, str]:
        # The container's real docker id, recorded so later commands can detect a
        # replaced/removed container and refuse to act on the wrong one.
        cid = container_id(settings["CONTAINER"].value or "")
        return {"CONTAINER_ID": cid} if cid else {}

    def destroy_system(self, ctx: SystemContext) -> None:
        pid = ctx.meta.get("SSH_TUNNEL_PID")
        if pid:
            # A `tdx-system connect` handle: stop the SSH tunnel, don't touch
            # Docker (the container lives on the server — destroy the remote
            # descriptor there to remove it).
            from .. import ssh

            ssh.close_tunnel(int(pid))
            return
        cname = ctx.meta["CONTAINER"]
        st = container_state(cname)
        if st is None:
            raise DatagenError(f"container does not exist: {cname}")
        if st == "running":
            docker_stop(cname)
        docker_rm(cname)

    def system_status(self, ctx: SystemContext) -> Health:
        pid = ctx.meta.get("SSH_TUNNEL_PID")
        if pid:
            from .. import ssh

            host = ctx.meta.get("HOST", "localhost")
            port = ctx.meta.get("PORT", "")
            up = (
                ssh.tunnel_alive(int(pid))
                and port.isdigit()
                and port_open(host, int(port))
            )
            return Health(
                up, f"ssh tunnel (pid {pid}) -> {host}:{port} {'up' if up else 'down'}"
            )
        cname = ctx.meta["CONTAINER"]
        st = container_state(cname)
        if st is None:
            return Health(False, f"container {cname} does not exist")
        if st != "running":
            return Health(False, f"container {cname} is {st}")
        if self.check_ready(cname, ctx.meta.get("PASSWORD", "")):
            return Health(True, f"{self.label} on localhost")
        return Health(
            False, f"{self.label} container is up but not yet accepting connections"
        )

    def pause(self, ctx: SystemContext) -> str:
        cname = ctx.meta["CONTAINER"]
        if container_state(cname) is None:
            raise DatagenError(f"container does not exist: {cname}")
        if container_state(cname) != "running":
            return "already-stopped"
        docker_stop(cname)
        return "stopped"

    def resume(self, ctx: SystemContext) -> str:
        cname = ctx.meta["CONTAINER"]
        if container_state(cname) is None:
            raise DatagenError(f"container does not exist: {cname}")
        if container_state(cname) == "running":
            return "already-running"
        docker_start(cname)
        return "started"

    # ==================================================================
    # Domain lifecycle
    # ==================================================================

    @staticmethod
    def _admin_conn(ctx: SystemContext) -> AdminConn:
        """The superuser admin connection to the running container's engine,
        built from the SYSTEM descriptor's HOST / PORT / USER / PASSWORD. Domain
        admin runs over TCP on the published host port (no `docker exec`), so a
        network-only client can provision domains."""
        return AdminConn(
            host=ctx.meta.get("HOST", "localhost"),
            port=ctx.meta.get("PORT", os.environ.get("PORT", "")),
            user=ctx.meta.get("USER", ""),
            password=ctx.meta.get("PASSWORD", ""),
        )

    def plan_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec
    ) -> dict[str, Setting]:
        cname = ctx.meta.get("CONTAINER", "")
        entity = spec.entity_name
        validate_name(entity, label=self.domain_flag)
        admin = self._admin_conn(ctx)
        # Some hooks read PORT / CONTAINER from env (e.g. redpanda's broker /
        # schema-registry extra vars); export them for `_domain_extra_vars`.
        # Re-surface system-level extra vars stamped at system-create time (e.g.
        # redpanda's SCHEMA_REGISTRY_URL) so a standalone domain create — whose
        # process never ran system create — can still read them from ctx.meta.
        os.environ["PORT"] = admin.port
        os.environ["CONTAINER"] = cname
        for _k in self.system_var_carryovers:
            os.environ[_k] = ctx.meta.get(_k, "")
        password = self.generate_password()
        result = self.domain_result(admin, entity, password)
        out: dict[str, Setting] = {
            "HOST": Setting("HOST", value="localhost"),
            "PORT": Setting("PORT", value=ctx.meta.get("PORT", "")),
            "USER": Setting("USER", value=result.user),
            "PASSWORD": Setting("PASSWORD", value=result.password),
            "DB": Setting("DB", value=result.db),
            "SQLALCHEMY_URI": Setting("SQLALCHEMY_URI", value=result.sqlalchemy_uri),
            # The image the domain lives on (from the system descriptor).
            "IMAGE": Setting("IMAGE", value=ctx.meta.get("IMAGE", "")),
            # Back-refs so destroy_domain needs only the domain descriptor.
            "SYS_CONTAINER": Setting("SYS_CONTAINER", value=cname),
            # The system container's docker id at domain-create time, so domain
            # commands can detect a replaced/removed system container.
            "SYS_CONTAINER_ID": Setting(
                "SYS_CONTAINER_ID", value=ctx.meta.get("CONTAINER_ID", "")
            ),
            "SYS_ADMIN_USER": Setting("SYS_ADMIN_USER", value=admin.user),
            "SYS_ADMIN_PASSWORD": Setting("SYS_ADMIN_PASSWORD", value=admin.password),
        }
        if self.uses_file_staging:
            root = ctx.meta.get("SYS_PATH", "")
            full = f"{root}/{entity}" if root else entity
            out["SYS_PATH"] = Setting("SYS_PATH", value=root, is_path=True)
            out["BASE_PATH"] = Setting("BASE_PATH", value=full, is_path=True)
        for k, v in self._domain_extra_vars().items():
            out[k] = Setting(k, value=v)
        return out

    def provision_domain(
        self, ctx: SystemContext, spec: CreateDomainSpec, settings: dict[str, Setting]
    ) -> None:
        cname = ctx.meta.get("CONTAINER", "")
        entity = spec.entity_name
        admin = self._admin_conn(ctx)
        os.environ["PORT"] = admin.port
        os.environ["CONTAINER"] = cname
        exists = any(d.lower() == entity.lower() for d in self.do_list(admin))
        if exists and not spec.force:
            raise DatagenError(
                f"{self.domain_flag} '{entity}' already exists (use --force)"
            )
        if spec.force and exists:
            # A leftover entity (typically an orphan whose descriptor was lost, so
            # the tdx-level destroy-first couldn't find it) is torn down over the
            # system admin before recreating. Do NOT swallow a real drop failure —
            # otherwise a half-dropped entity resurfaces as a confusing "already
            # exists" during the later DDL install.
            self.do_delete(admin, entity)
            print(f">>> --force: removed existing {self.domain_flag} '{entity}'")
        elif spec.force:
            # Not present per do_list — best-effort drop of a partial leftover
            # do_list can't see; do_create surfaces any real conflict.
            try:
                self.do_delete(admin, entity)
            except DatagenError:
                pass
        self.do_create(
            admin,
            DomainResult(
                db=settings["DB"].value or "",
                user=settings["USER"].value or "",
                password=settings["PASSWORD"].value or "",
                sqlalchemy_uri=settings["SQLALCHEMY_URI"].value or "",
            ),
        )

    @staticmethod
    def _admin_from_domain_meta(meta: dict[str, str]) -> AdminConn:
        """Rebuild the system admin connection from a DOMAIN's descriptor meta
        (its SYS_* back-refs), for operations that re-touch the entity container
        without loading the system descriptor."""
        return AdminConn(
            host=meta.get("HOST", "localhost"),
            port=meta.get("PORT", ""),
            user=meta.get("SYS_ADMIN_USER", ""),
            password=meta.get("SYS_ADMIN_PASSWORD", ""),
        )

    def reset_domain(self, ctx: DomainContext) -> None:
        """Reset the domain to empty, REUSING its name and credentials: for a
        SQL container drop every table (the entity container), keeping the
        database, its user and password — `recreate` then replays the DDL/data.
        Non-SQL containers (kafka / mongo) fall back to the default (drop +
        re-provision the topic / database)."""
        if not self.supports_sql:
            return super().reset_domain(ctx)
        self._drop_all_tables(self._require_uri(ctx))

    def destroy_domain(self, ctx: DomainContext) -> None:
        admin = AdminConn(
            host=ctx.meta.get("HOST", "localhost"),
            port=ctx.meta.get("PORT", ""),
            user=ctx.meta.get("SYS_ADMIN_USER", ""),
            password=ctx.meta.get("SYS_ADMIN_PASSWORD", ""),
        )
        self.do_delete(admin, ctx.meta["DB"])

    def domain_status(self, ctx: DomainContext) -> Health:
        # TCP reachability of the published port — no Docker daemon needed.
        host = ctx.meta.get("HOST", "localhost")
        port = ctx.meta.get("PORT", "")
        ok = port.isdigit() and port_open(host, int(port))
        return Health(
            ok, f"{self.label} at {host}:{port} {'reachable' if ok else 'unreachable'}"
        )

    # ==================================================================
    # Data plane (SQL) — run the model's DDL / INSERTs against the domain
    # ==================================================================

    def install_schema(
        self, ctx: DomainContext, model: str, *, force: bool = False
    ) -> None:
        if not self.supports_sql:
            raise DatagenError(f"{self.slug} has no SQL schema to install")
        from ..sql_runner import run_sql_text

        uri = self._require_uri(ctx)
        if force:
            self._drop_all_tables(uri)
        print(f">>> creating schema from {self.slug}.ddl ...")
        run_sql_text(uri, self._model_ddl_text(model))
        print(">>> schema ready")

    def load_data(self, ctx: DomainContext, req: LoadRequest) -> int:
        if not self.supports_sql:
            raise DatagenError(
                f"'data load' into {self.slug} domains is not supported "
                f"(no SQL backend)"
            )
        from ..sql_runner import run_sql_file, run_sql_text

        uri = self._require_uri(ctx)
        if req.force:
            self._drop_all_tables(uri)
            print(f">>> creating schema from {self.slug}.ddl ...")
            run_sql_text(uri, self._model_ddl_text(req.model))
        order = req.data_dir / "run-order.txt"
        if not order.is_file():
            raise DatagenError(f"missing run-order.txt in {req.data_dir}")
        print(">>> loading data files in run-order.txt order ...")
        n = 0
        for line in order.read_text().splitlines():
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            if not line.endswith(".sql"):
                raise DatagenError(
                    f"{line} is not a .sql file (load only handles --format sql output)"
                )
            path = req.data_dir / line
            if not path.is_file():
                raise DatagenError(f"missing data file: {path}")
            print(f"    {line}")
            run_sql_file(uri, path)
            n += 1
        print(">>> load complete")
        return n

    @staticmethod
    def _require_uri(ctx: DomainContext) -> str:
        uri = ctx.meta.get("SQLALCHEMY_URI", "")
        if not uri:
            raise DatagenError("domain has no SQLALCHEMY_URI")
        return uri

    @staticmethod
    def _drop_all_tables(uri: str) -> None:
        from sqlalchemy import MetaData, create_engine

        print(">>> --force: dropping existing tables ...")
        engine = create_engine(uri)
        md = MetaData()
        md.reflect(bind=engine)
        if md.tables:
            md.drop_all(bind=engine)
            print(f">>> dropped {len(md.tables)} table(s)")

    # ==================================================================
    # Admin plane (TCP) — provision domains over the published port
    # ==================================================================
    #
    # Domain admin (create / drop / list) runs as the system superuser over TCP
    # on the container's published host port — NOT `docker exec` — so a client
    # with only network access (e.g. an SSH tunnel) can provision domains
    # without a local Docker daemon. SQL systems share the two helpers below and
    # supply `_admin_url`; non-SQL systems (redpanda / mongodb) override the
    # do_* hooks to talk their own admin protocol.

    admin_autocommit = False  # True where CREATE DATABASE can't run inside a
    # transaction (postgres / sql server)

    def _admin_url(self, admin: AdminConn) -> str:
        """SQLAlchemy URL for the superuser admin connection. SQL subclasses
        override; non-SQL systems don't use it."""
        raise NotImplementedError(f"{self.slug} has no SQL admin URL")

    def _run_admin_sql(self, admin: AdminConn, statements: list[str]) -> None:
        """Run admin DDL over TCP as the superuser. `admin_autocommit` systems
        run each statement outside a transaction (CREATE DATABASE can't be
        transactional there); the rest commit once at the end."""
        from sqlalchemy import create_engine

        engine = create_engine(self._admin_url(admin))
        try:
            if self.admin_autocommit:
                conn = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
                with conn:
                    for st in statements:
                        conn.exec_driver_sql(st)
            else:
                with engine.begin() as conn:
                    for st in statements:
                        conn.exec_driver_sql(st)
        except Exception as exc:  # surface the DB's own message, no traceback
            raise DatagenError(f"{self.label} admin SQL failed: {exc}") from exc
        finally:
            engine.dispose()

    def _query_admin_names(self, admin: AdminConn, query: str) -> list[str]:
        """Run a single-column admin query over TCP; return the trimmed
        first-column values (used by `do_list`)."""
        from sqlalchemy import create_engine

        engine = create_engine(self._admin_url(admin))
        try:
            with engine.connect() as conn:
                rows = conn.exec_driver_sql(query).fetchall()
        except Exception as exc:
            raise DatagenError(f"{self.label} admin query failed: {exc}") from exc
        finally:
            engine.dispose()
        return [str(r[0]).strip() for r in rows if r and r[0] is not None]

    # ==================================================================
    # Per-system hooks — subclasses implement
    # ==================================================================

    def _build_docker_args(
        self, container: str, host_port: int, password: str
    ) -> list[str]:
        args = [
            "-d",
            "-p",
            f"{host_port}:{self.internal_port}",
            "--name",
            container,
            "--platform",
            self.image_platform,
        ]
        args.extend(self.extra_flags)
        for k, v in self.extra_env:
            args.extend(["-e", f"{k}={v}"])
        if self.password_env_name:
            args.extend(["-e", f"{self.password_env_name}={password}"])
        args.append(self.image)
        args.extend(self.cmd_args)
        return args

    def generate_password(self) -> str:
        return gen_password()

    # Extra-var keys from `_domain_extra_vars` that are also EXPORTED (surfaced
    # in `info` and the test fixtures), not just kept in `meta`. Empty by
    # default — the back-refs and internal connection vars stay meta-only.
    exported_domain_vars: tuple[str, ...] = ()

    # System-level extra vars (from `_system_extra_vars`, stamped into the system
    # descriptor at system-create) that `plan_domain` must re-export into the env
    # so `_domain_extra_vars` can read them during a standalone domain create
    # (a separate process that never ran system create). Empty by default.
    system_var_carryovers: tuple[str, ...] = ()

    def _system_extra_vars(self) -> dict[str, str]:
        return {}

    def _domain_extra_vars(self) -> dict[str, str]:
        return {}

    def _post_start(self, container: str, password: str) -> None:
        return None

    def _readiness_fatal(self, container: str, password: str) -> str | None:
        return None

    def _wait_ready(self, container: str, password: str, timeout: int) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.check_ready(container, password):
                return
            fatal = self._readiness_fatal(container, password)
            if fatal:
                raise DatagenError(f"{self.label} cannot become ready: {fatal}")
            time.sleep(2)
        raise DatagenError(
            f"{self.label} did not become ready within {timeout}s "
            f"(container {container} is up but not accepting connections)"
        )

    # Abstract-ish hooks (subclasses must provide):
    def _docker_run(self, container: str, host_port: int, password: str) -> str:
        raise NotImplementedError

    def check_ready(self, container: str, password: str) -> bool:
        raise NotImplementedError

    def domain_result(self, admin: AdminConn, name: str, password: str) -> DomainResult:
        """Compute the domain's connection identity (db / user / URI) for the
        pre-generated `password`, WITHOUT touching the system — the pure half of
        domain creation, so the full descriptor can be planned before
        `do_create` runs. Subclasses build their per-driver URI here."""
        raise NotImplementedError

    def do_create(self, admin: AdminConn, result: DomainResult) -> None:
        """Create the entity container (database / schema / topic) described by
        `result` (its `db` / `user` / `password`). Runs the admin DDL; the
        identity was already computed by `domain_result`."""
        raise NotImplementedError

    def do_delete(self, admin: AdminConn, name: str) -> None:
        raise NotImplementedError

    def do_list(self, admin: AdminConn) -> list[str]:
        raise NotImplementedError
