#
# Copyright 2026 Tabsdata Inc.
#

"""``SqlContainerBackend`` — the shared base for docker-container SQL databases.

A concrete SQL system (mysql, postgres, oracle, db2, mssqlserver, clickhouse,
starrocks, …) subclasses this and supplies only its *dialect hooks*: the docker
image and server flags, the readiness probe, the SQLAlchemy driver + identifier
quote, the CREATE/DROP-domain SQL and the schema-listing query, and the set of
the server's own schemas that must never be treated as domains. Everything else
— the container lifecycle, TCP admin over SQLAlchemy, per-domain provisioning,
model/data loading and delta application — lives here once.

The per-model DDL for a system lives beside its backend under
``<slug>/domain/models/<model>/<slug>.ddl`` (a model is available for the system
iff that file exists), so the same generic ``models`` / ``create-model`` /
``create-data`` / ``modify-data`` commands work for every dialect.
"""

from __future__ import annotations

import hashlib
import inspect
import platform
import time
from dataclasses import dataclass
from pathlib import Path

from tabsdatax.itest.tdksystems2 import _venv, datagen, emit, options
from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.shell import (
    container_exists,
    container_id,
    container_state,
    docker_exec_check,
    docker_pull,
    docker_restart,
    docker_rm,
    docker_run,
    docker_stop,
    gen_password,
    require_docker,
    validate_name,
)
from tabsdatax.itest.tdksystems2.sql_runner import run_sql_text, run_sql_transactions
from tabsdatax.itest.tdksystems2.systems._common.ports import PortPoolMixin

_CONTAINER_PREFIX = "tdx2_"


@dataclass
class Admin:
    host: str
    port: str
    user: str
    password: str


class SqlContainerBackend(PortPoolMixin):
    # ---- capability flags (drive verb availability in the dispatcher) ----
    is_container: bool = True
    supports_purge: bool = True
    supports_models: bool = True
    supports_data: bool = True
    supports_changes: bool = True

    # ---- managed venvs (see _venv) ----
    # Starting and probing a container is docker-only (readiness runs as an
    # in-container CLI), so the lifecycle needs no driver; everything that
    # speaks SQL to it does.
    sys_env: str = _venv.ENV_C
    dom_env: str = _venv.ENV_A

    # ---- identity / shape (subclass sets these) ----
    slug: str = ""
    label: str = ""
    kind: str = "sql"
    weight: int = 1
    domain_word: str = "database"
    ready_timeout: int = 300
    max_entity_name_length: int = 63
    login_maxlen: int = 32

    # ---- dialect hooks (subclass overrides) ----
    image: str = ""
    internal_port: int = 0
    root_user: str = ""
    driver: str = ""  # SQLAlchemy driver, e.g. "mysql+mysqlconnector"
    sql_quote: str = '"'
    cmd_args: list[str] = []  # server flags appended after the image
    extra_run_flags: list[
        str
    ] = []  # docker-run flags before the image (db2: --privileged)
    system_schemas: frozenset[str] = frozenset()
    admin_db: str = ""  # maintenance DB the admin connection targets (pg: postgres)
    admin_autocommit: bool = False  # some DBs can't CREATE/DROP DATABASE in a txn
    ddl_from_slug: str = ""  # borrow another system's DDL (CDC reuses the plain one)
    # Rows per INSERT for the initial load; 1 = one INSERT per row (Oracle has no
    # multi-row VALUES). Higher = fewer round-trips where the dialect allows it.
    insert_batch: int = 500

    image_platform = (
        "linux/arm64" if platform.machine() in ("arm64", "aarch64") else "linux/amd64"
    )

    def options(self) -> tuple[options.Option, ...]:
        return (
            options.Option("PORT", "port", options.PORT, "a free host port"),
            options.Option(
                "PASSWORD",
                "password",
                options.RANDOM,
                "a generated password",
                secret=True,
            ),
        )

    # ---- images / naming ------------------------------------------------

    def docker_images(self) -> list[tuple[str, str]]:
        return [(self.image, self.image_platform)]

    def container_name(self, sys_name: str) -> str:
        return f"{_CONTAINER_PREFIX}{sys_name}"

    def _login(self, name: str) -> str:
        # The owner user is named after the domain, capped at the dialect's
        # identifier limit. A plain truncation collides when sibling domains
        # share a prefix longer than the cap (their distinguishing suffix is
        # cut off), so past the cap keep a prefix and append a hash of the full
        # name to stay unique. Deterministic, so create and delete agree.
        if len(name) <= self.login_maxlen:
            return name
        digest = hashlib.sha1(name.encode()).hexdigest()[:8]
        return f"{name[: self.login_maxlen - len(digest) - 1]}_{digest}"

    def _package_dir(self) -> Path:
        return Path(inspect.getfile(type(self))).parent

    # ---- dialect hooks with defaults ------------------------------------

    def generate_password(self) -> str:
        """The system (admin) password. Overridden where the image mandates a
        fixed one (e.g. StarRocks allin1 requires the empty root password)."""
        return gen_password()

    def post_provision(self, container: str, password: str) -> None:
        """Run once the container is ready (e.g. create a shared database, tune a
        server config). Default: nothing."""

    def session_setup(self) -> list[str]:
        """Statements run first on every data-plane connection (DDL install, load,
        modify-data) — e.g. Oracle sets NLS_*_FORMAT so the ISO date/timestamp
        literals the generator emits parse. Default: nothing."""
        return []

    def env_vars(self, password: str) -> dict[str, str]:
        """docker ``-e`` environment for the server container."""
        raise NotImplementedError

    def ready_exec_cmd(self, password: str) -> list[str]:
        """``docker exec`` argv that returns 0 once the server accepts
        connections (used by the default :meth:`check_ready`)."""
        raise NotImplementedError

    def create_domain_sql(self, db: str, user: str, password: str) -> list[str]:
        """Admin statements that create the domain container + owner."""
        raise NotImplementedError

    def delete_domain_sql(self, name: str) -> list[str]:
        raise NotImplementedError

    def list_schemas_sql(self) -> str:
        """Query returning one row per domain-container name (server schemas are
        filtered out by :attr:`system_schemas`)."""
        raise NotImplementedError

    # ---- URLs -----------------------------------------------------------

    def sqlalchemy_url(
        self, host: str, port: str, db: str = "", user: str = "", password: str = ""
    ) -> str:
        creds = f"{user}:{password}@" if user else ""
        tail = f"/{db}" if db else ""
        return f"{self.driver}://{creds}{host}:{port}{tail}"

    # ---- system lifecycle ----------------------------------------------

    def plan_system(
        self, name: str, overrides: dict[str, str] | None = None
    ) -> dict[str, str]:
        o = overrides or {}
        return {
            "CONTAINER": self.container_name(name),
            "IMAGE": self.image,
            "HOST": "localhost",
            "PORT": o.get("PORT") or str(self.next_free_port()),
            "USER": self.root_user,
            "PASSWORD": o.get("PASSWORD") or self.generate_password(),
        }

    def provision_system(
        self, meta: dict[str, str], *, force: bool = False
    ) -> dict[str, str]:
        require_docker()
        container = meta["CONTAINER"]
        password = meta["PASSWORD"]
        port = int(meta["PORT"])
        if container_exists(container):
            if force:
                docker_rm(container, force=True)
                print(f">>> --force: removed existing container {container}")
            else:
                raise TdxError(f"container already exists: {container} (use --force)")
        print(f">>> pulling docker image {self.image} …", flush=True)
        docker_pull(self.image, self.image_platform)
        print(f">>> starting container {container} from image {self.image}")
        docker_run(self._docker_args(container, port, password))
        print(f">>> waiting for {self.label} to become ready …")
        self._wait_ready(container, password, self.ready_timeout)
        self.post_provision(container, password)
        cid = container_id(container)
        return {"CONTAINER_ID": cid} if cid else {}

    def _docker_args(self, container: str, host_port: int, password: str) -> list[str]:
        args = [
            "-d",
            "-p",
            f"{host_port}:{self.internal_port}",
            "--name",
            container,
            "--platform",
            self.image_platform,
        ]
        args.extend(self.extra_run_flags)
        for key, value in self.env_vars(password).items():
            args += ["-e", f"{key}={value}"]
        args.append(self.image)
        args.extend(self.cmd_args)
        return args

    def check_ready(self, container: str, password: str) -> bool:
        return docker_exec_check(container, *self.ready_exec_cmd(password))

    def _wait_ready(self, container: str, password: str, timeout: int) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.check_ready(container, password):
                return
            time.sleep(2)
        raise TdxError(
            f"{self.label} did not become ready within {timeout}s "
            f"(container {container} is up but not accepting connections)"
        )

    def destroy_system(self, meta: dict[str, str]) -> None:
        cname = meta["CONTAINER"]
        st = container_state(cname)
        if st is None:
            raise TdxError(f"container does not exist: {cname}")
        if st == "running":
            docker_stop(cname)
        docker_rm(cname)

    def system_status(self, meta: dict[str, str]) -> tuple[bool, str]:
        cname = meta["CONTAINER"]
        st = container_state(cname)
        if st is None:
            return False, f"container {cname} does not exist"
        if st != "running":
            return False, f"container {cname} is {st}"
        if self.check_ready(cname, meta.get("PASSWORD", "")):
            return True, f"{self.label} on localhost:{meta.get('PORT', '')}"
        return False, f"{self.label} container is up but not yet accepting connections"

    def restart_system(self, meta: dict[str, str]) -> str:
        cname = meta["CONTAINER"]
        if container_state(cname) is None:
            raise TdxError(f"container does not exist: {cname}")
        docker_restart(cname)
        return "restarted"

    # ---- domain lifecycle ----------------------------------------------

    def _admin(self, sys_meta: dict[str, str]) -> Admin:
        return Admin(
            host=sys_meta.get("HOST", "localhost"),
            port=sys_meta.get("PORT", ""),
            user=sys_meta.get("USER", self.root_user),
            password=sys_meta.get("PASSWORD", ""),
        )

    def _admin_url(self, admin: Admin) -> str:
        return self.sqlalchemy_url(
            admin.host,
            admin.port,
            db=self.admin_db,
            user=admin.user,
            password=admin.password,
        )

    def domain_meta(
        self, sys_meta: dict[str, str], entity_name: str, password: str
    ) -> dict[str, str]:
        admin = self._admin(sys_meta)
        login = self._login(entity_name)
        uri = self.sqlalchemy_url(
            admin.host, admin.port, db=entity_name, user=login, password=password
        )
        uri_no_creds = self.sqlalchemy_url(admin.host, admin.port, db=entity_name)
        return {
            "HOST": admin.host,
            "PORT": admin.port,
            "USER": login,
            "PASSWORD": password,
            "DB": entity_name,
            "SQLALCHEMY_URI": uri,
            "URI": uri_no_creds,
            "IMAGE": sys_meta.get("IMAGE", ""),
            "SYS_CONTAINER": sys_meta.get("CONTAINER", ""),
            "SYS_CONTAINER_ID": sys_meta.get("CONTAINER_ID", ""),
            "SYS_ADMIN_USER": admin.user,
            "SYS_ADMIN_PASSWORD": admin.password,
        }

    def new_domain_password(self) -> str:
        return gen_password()

    def provision_domain(
        self,
        sys_meta: dict[str, str],
        entity_name: str,
        domain_meta: dict[str, str],
        *,
        force: bool = False,
    ) -> None:
        validate_name(entity_name, label=self.domain_word)
        admin = self._admin(sys_meta)
        exists = any(d.lower() == entity_name.lower() for d in self._do_list(admin))
        if exists and not force:
            raise TdxError(
                f"{self.domain_word} '{entity_name}' already exists (use --force)"
            )
        if exists:
            self._do_delete(admin, entity_name)
            print(f">>> --force: removed existing {self.domain_word} '{entity_name}'")
        elif force:
            try:
                self._do_delete(admin, entity_name)
            except TdxError:
                pass
        self._do_create(
            admin, domain_meta["DB"], domain_meta["USER"], domain_meta["PASSWORD"]
        )

    def destroy_domain(self, domain_meta: dict[str, str]) -> None:
        admin = Admin(
            host=domain_meta.get("HOST", "localhost"),
            port=domain_meta.get("PORT", ""),
            user=domain_meta.get("SYS_ADMIN_USER", self.root_user),
            password=domain_meta.get("SYS_ADMIN_PASSWORD", ""),
        )
        self._do_delete(admin, domain_meta["DB"])

    def domain_status(self, domain_meta: dict[str, str]) -> tuple[bool, str]:
        from tabsdatax.itest.tdksystems2.shell import port_open

        host = domain_meta.get("HOST", "localhost")
        port = domain_meta.get("PORT", "")
        ok = port.isdigit() and port_open(host, int(port))
        return (
            ok,
            f"{self.label} at {host}:{port} {'reachable' if ok else 'unreachable'}",
        )

    def list_domain_entities(self, sys_meta: dict[str, str]) -> list[str]:
        return self._do_list(self._admin(sys_meta))

    def purge_domain_entity(self, sys_meta: dict[str, str], name: str) -> None:
        if name.lower() in self.system_schemas:
            raise TdxError(f"refusing to purge {self.label} system schema '{name}'")
        self._do_delete(self._admin(sys_meta), name)

    # ---- admin DDL (over TCP) ------------------------------------------

    def _do_create(self, admin: Admin, db: str, user: str, password: str) -> None:
        self._run_admin(admin, self.create_domain_sql(db, user, password))

    def _do_delete(self, admin: Admin, name: str) -> None:
        self._run_admin(admin, self.delete_domain_sql(name))

    def _do_list(self, admin: Admin) -> list[str]:
        names = self._query_admin(admin, self.list_schemas_sql())
        return [n for n in names if n.lower() not in self.system_schemas]

    def _run_admin(self, admin: Admin, statements: list[str]) -> None:
        from sqlalchemy import create_engine

        engine = create_engine(self._admin_url(admin))
        try:
            if self.admin_autocommit:
                with engine.connect() as conn:
                    conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                    for st in statements:
                        conn.exec_driver_sql(st)
            else:
                with engine.begin() as conn:
                    for st in statements:
                        conn.exec_driver_sql(st)
        except Exception as exc:  # noqa: BLE001 - surface the DB's own message
            raise TdxError(f"{self.label} admin SQL failed: {exc}") from exc
        finally:
            engine.dispose()

    def _query_admin(self, admin: Admin, query: str) -> list[str]:
        from sqlalchemy import create_engine

        engine = create_engine(self._admin_url(admin))
        try:
            with engine.connect() as conn:
                rows = conn.exec_driver_sql(query).fetchall()
        except Exception as exc:  # noqa: BLE001
            raise TdxError(f"{self.label} admin query failed: {exc}") from exc
        finally:
            engine.dispose()
        return [str(r[0]).strip() for r in rows if r and r[0] is not None]

    # ---- data plane -----------------------------------------------------

    def data_plane_uri(self, meta: dict[str, str]) -> str:
        return meta["SQLALCHEMY_URI"]

    def _ddl_dir(self, model: str) -> Path:
        root = self._package_dir()
        if self.ddl_from_slug:
            root = root.parent / self.ddl_from_slug
        return root / "domain" / "models" / model

    def after_install_schema(self, meta: dict[str, str], model: str) -> None:
        """Hook run after the model DDL is installed (default: nothing), given the
        domain `meta`. CDC variants use it to prep the tables for capture (e.g.
        REPLICA IDENTITY FULL, or ASN registration inside the container)."""

    def has_model(self, model: str) -> bool:
        d = self._ddl_dir(model)
        return d.is_dir() and any(d.glob("*.ddl"))

    def available_models(self) -> list[str]:
        return [m for m in datagen.list_models() if self.has_model(m)]

    def ddl_text(self, model: str) -> str:
        d = self._ddl_dir(model)
        files = sorted(d.glob("*.ddl"))
        if not files:
            raise TdxError(f"model '{model}' has no {self.label} DDL (looked in {d})")
        return "\n\n".join(f.read_text(encoding="utf-8") for f in files)

    def install_schema(
        self, meta: dict[str, str], model: str, *, force: bool = False
    ) -> None:
        uri = self.data_plane_uri(meta)
        if force:
            self.drop_all_tables(uri)
        print(f">>> creating schema for model '{model}' …")
        setup = "".join(f"{s};\n" for s in self.session_setup())
        run_sql_text(uri, setup + self.ddl_text(model))
        self.after_install_schema(meta, model)
        print(">>> schema ready")

    def load_tables(self, uri: str, tables: list[emit.Table]) -> dict[str, int]:
        statements: list[str] = []
        counts: dict[str, int] = {}
        for tbl in tables:
            counts[tbl.name] = len(tbl.rows)
            statements.extend(
                emit.iter_insert_statements(
                    tbl.name,
                    tbl.columns,
                    tbl.rows,
                    batch=self.insert_batch,
                    quote=self.sql_quote,
                )
            )
        if statements:
            print(
                f">>> loading {sum(counts.values())} rows into {len(tables)} tables …"
            )
            run_sql_transactions(uri, [self.session_setup() + statements], label="load")
        return counts

    def apply_changes(
        self, uri: str, transactions: list, before: dict[str, int]
    ) -> dict[str, int]:
        rendered = [
            self.session_setup()
            + [datagen.render_sql(c, quote=self.sql_quote) for c in txn]
            for txn in transactions
        ]
        run_sql_transactions(uri, rendered, label="modify-data")
        new = dict(before)
        for table, delta in datagen.net_row_deltas(transactions).items():
            new[table] = max(0, new.get(table, 0) + delta)
        return new

    def drop_all_tables(self, uri: str) -> None:
        from sqlalchemy import MetaData, create_engine

        engine = create_engine(uri)
        try:
            md = MetaData()
            md.reflect(bind=engine)
            if md.tables:
                print(f">>> dropping {len(md.tables)} existing table(s) …")
                md.drop_all(bind=engine)
        finally:
            engine.dispose()


class RemoteSqlBackend(SqlContainerBackend):
    """A remote (non-container) SQL warehouse — snowflake, databricks, …

    Reuses the whole :class:`SqlContainerBackend` data plane (install-schema /
    load / modify-data over SQLAlchemy) but has nothing to start: create reads the
    connection from the declared ``options()`` (env-sourced by default), bakes it
    into the descriptor and probes reachability, and a domain is a SCHEMA inside
    the account's namespace (a ``DATABASE`` / ``CATALOG``). Subclasses supply the
    connection ``options()``, the dialect ``_url`` and identifier ``sql_quote``,
    and — where the case differs — ``_schema``; ``_extra_domain_sql`` adds any
    per-domain setup (e.g. a Volume).
    """

    is_container = False
    # Nothing to start, so create probes reachability over SQL — the driver is
    # needed from the outside too.
    sys_env = _venv.ENV_A
    env_prefix: str = ""
    namespace_meta: str = (
        "DATABASE"  # meta key naming the DB/catalog the schema sits in
    )

    def options(self) -> tuple[options.Option, ...]:
        return ()  # subclass declares its connection options

    def docker_images(self) -> list[tuple[str, str]]:
        return []

    # ---- connection ----------------------------------------------------

    def _schema(self, name: str) -> str:
        return name

    def _url(self, meta: dict[str, str], schema: str = "") -> str:
        raise NotImplementedError

    def _location(self, meta: dict[str, str]) -> str:
        return f"{self.slug}://{meta.get(self.namespace_meta, '')}"

    def _qualified(self, ns: str, schema: str) -> str:
        q = self.sql_quote
        return f"{q}{ns}{q}.{q}{schema}{q}"

    def _extra_domain_sql(self, sys_meta: dict[str, str], schema: str) -> list[str]:
        return []

    def _exec(self, url: str, statements: list[str]) -> None:
        from sqlalchemy import create_engine

        engine = create_engine(url)
        try:
            with engine.begin() as conn:
                for stmt in statements:
                    conn.exec_driver_sql(stmt)
        except Exception as exc:  # noqa: BLE001 - surface the warehouse's message
            raise TdxError(f"{self.label} SQL failed: {exc}") from exc
        finally:
            engine.dispose()

    def _query(self, url: str, sql: str) -> list[str]:
        from sqlalchemy import create_engine

        engine = create_engine(url)
        try:
            with engine.connect() as conn:
                rows = conn.exec_driver_sql(sql).fetchall()
        except Exception as exc:  # noqa: BLE001
            raise TdxError(f"{self.label} query failed: {exc}") from exc
        finally:
            engine.dispose()
        return [str(r[0]).strip() for r in rows if r and r[0] is not None]

    # ---- system lifecycle (remote — nothing to run) --------------------

    def plan_system(
        self, name: str, overrides: dict[str, str] | None = None
    ) -> dict[str, str]:
        o = overrides or {}
        meta = {opt.name: o.get(opt.name, "") for opt in self.options()}
        meta["LOCATION"] = self._location(meta)
        return meta

    def provision_system(
        self, meta: dict[str, str], *, force: bool = False
    ) -> dict[str, str]:
        self._exec(self._url(meta), ["SELECT 1"])
        return {}

    def destroy_system(self, meta: dict[str, str]) -> None:
        return None  # the account is externally owned

    def system_status(self, meta: dict[str, str]) -> tuple[bool, str]:
        try:
            self._exec(self._url(meta), ["SELECT 1"])
        except TdxError as exc:
            return False, f"cannot reach {self.label}: {exc}"
        return True, f"{self.label} at {self._location(meta)}"

    # ---- domain lifecycle (a schema in the namespace) ------------------

    def new_domain_password(self) -> str:
        return ""

    def domain_meta(
        self, sys_meta: dict[str, str], entity_name: str, password: str
    ) -> dict[str, str]:
        schema = self._schema(entity_name)
        meta = {opt.name: sys_meta.get(opt.name, "") for opt in self.options()}
        meta["DB"] = schema
        meta["SQLALCHEMY_URI"] = self._url(sys_meta, schema)
        meta["URI"] = f"{self._location(sys_meta)}/{schema}"
        return meta

    def provision_domain(
        self,
        sys_meta: dict[str, str],
        entity_name: str,
        domain_meta: dict[str, str],
        *,
        force: bool = False,
    ) -> None:
        schema = self._schema(entity_name)
        ns = sys_meta[self.namespace_meta]
        url = self._url(sys_meta)
        exists = schema.lower() in {
            s.lower() for s in self.list_domain_entities(sys_meta)
        }
        if exists and not force:
            raise TdxError(f"schema '{schema}' already exists (use --force)")
        if exists or force:
            self._exec(
                url, [f"DROP SCHEMA IF EXISTS {self._qualified(ns, schema)} CASCADE"]
            )
        self._exec(
            url,
            [
                f"CREATE SCHEMA {self._qualified(ns, schema)}",
                *self._extra_domain_sql(sys_meta, schema),
            ],
        )

    def destroy_domain(self, domain_meta: dict[str, str]) -> None:
        ns, schema = domain_meta[self.namespace_meta], domain_meta["DB"]
        self._exec(
            self._url(domain_meta),
            [f"DROP SCHEMA IF EXISTS {self._qualified(ns, schema)} CASCADE"],
        )

    def domain_status(self, domain_meta: dict[str, str]) -> tuple[bool, str]:
        try:
            self._exec(self._url(domain_meta), ["SELECT 1"])
        except TdxError as exc:
            return False, f"cannot reach {self.label}: {exc}"
        return True, f"{self.label} schema {domain_meta.get('DB', '')}"

    def list_domain_entities(self, sys_meta: dict[str, str]) -> list[str]:
        ns = sys_meta[self.namespace_meta]
        q = self.sql_quote
        names = self._query(
            self._url(sys_meta),
            f"SELECT SCHEMA_NAME FROM {q}{ns}{q}.INFORMATION_SCHEMA.SCHEMATA",
        )
        return [n for n in names if n.lower() not in self.system_schemas]

    def purge_domain_entity(self, sys_meta: dict[str, str], name: str) -> None:
        if name.lower() in self.system_schemas:
            raise TdxError(f"refusing to purge {self.label} system schema '{name}'")
        ns = sys_meta[self.namespace_meta]
        self._exec(
            self._url(sys_meta),
            [f"DROP SCHEMA IF EXISTS {self._qualified(ns, name)} CASCADE"],
        )
