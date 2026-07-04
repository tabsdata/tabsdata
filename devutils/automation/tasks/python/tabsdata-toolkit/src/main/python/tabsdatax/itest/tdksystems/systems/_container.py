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

from .base import (
    CreateDomainSpec, CreateSystemSpec, DomainContext, Health, LoadRequest,
    Setting, System, SystemContext,
)
from ..errors import DatagenError
from ..shell import (
    container_exists, container_state, docker_rm, docker_start, docker_stop,
    find_free_port, gen_password, require_docker, validate_name,
)


@dataclass
class DomainResult:
    """Outcome of `do_create` — the domain's connection info."""
    db: str
    user: str
    password: str
    sqlalchemy_uri: str


class ContainerBase(System):
    category = "container"
    kind = "sql"
    supports_pause = True
    domain_flag = "database"        # --name names a database/schema/topic
    supports_sql = True
    domain_desc = ""
    options = ()                    # port is allocated, password generated

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
    def staging_root(self) -> bool:            # base adds the SYS_PATH option
        return self.uses_file_staging

    def add_create_options(self, parser) -> None:
        super().add_create_options(parser)     # --sys-path/--env-prefix if staging
        parser.add_argument("--port", type=int, default=None,
                            help="optional; host port (default: a free port)")
        parser.add_argument("--ready-timeout", type=int, default=300,
                            help="optional; seconds to wait for readiness "
                                 "(default: 300)")

    # ==================================================================
    # System lifecycle
    # ==================================================================

    def create_system(self, spec: CreateSystemSpec) -> dict[str, Setting]:
        if self.uses_file_staging:
            # SYS_PATH is always present as a Setting (the default-prefix
            # fallback fills in an env ref), so a mere membership check is a
            # no-op. Require it to actually *resolve* to a staging root now.
            sp = spec.settings.get("SYS_PATH")
            try:
                root = self.resolve_values({"SYS_PATH": sp})["SYS_PATH"] if sp else ""
            except DatagenError:
                root = ""
            if not root:
                raise DatagenError(
                    f"{self.slug} is a file-staging system: a staging root is "
                    f"required (--sys-path PATH, --env:sys-path VAR, or "
                    f"--env-prefix PREFIX defining $<PREFIX>__SYS_PATH)")
        require_docker()
        container = spec.name
        if container_exists(container):
            if spec.force:
                docker_rm(container, force=True)
                print(f">>> --force: removed existing container {container}")
            else:
                raise DatagenError(
                    f"container already exists: {container} (use --force)")
        password = self.generate_password()
        port = spec.host_port or find_free_port()
        print(f">>> starting container {container} from image {self.image}")
        self._docker_run(container, port, password)
        # Once the container is up, any failure (readiness timeout, a failing
        # _post_start, or a Ctrl-C) must not leave it running.
        try:
            print(f">>> waiting for {self.label} to become ready "
                  f"(timeout: {spec.ready_timeout}s)…")
            self._wait_ready(container, password, spec.ready_timeout)
            # Export PORT so hooks (do_create URIs, staging) that read env work.
            os.environ["PORT"] = str(port)
            os.environ["CONTAINER"] = container
            self._post_start(container, password)
        except BaseException:
            print(f">>> create failed — removing container {container}")
            try:
                docker_rm(container, force=True)
            except Exception:
                pass
            raise
        out: dict[str, Setting] = {
            "CONTAINER": Setting("CONTAINER", value=container),
            "IMAGE": Setting("IMAGE", value=self.image),
            "HOST": Setting("HOST", value="localhost"),
            "PORT": Setting("PORT", value=str(port)),
            "USER": Setting("USER", value=self.system_user),
            "PASSWORD": Setting("PASSWORD", value=password),
        }
        if self.uses_file_staging:
            sp = spec.settings["SYS_PATH"]
            out["SYS_PATH"] = Setting("SYS_PATH", value=sp.value,
                                      env_var=sp.env_var, is_path=True)
        for k, v in self._system_extra_vars().items():
            out[k] = Setting(k, value=v)
        return out

    def destroy_system(self, ctx: SystemContext) -> None:
        cname = ctx.meta["CONTAINER"]
        st = container_state(cname)
        if st is None:
            raise DatagenError(f"container does not exist: {cname}")
        if st == "running":
            docker_stop(cname)
        docker_rm(cname)

    def system_status(self, ctx: SystemContext) -> Health:
        cname = ctx.meta["CONTAINER"]
        st = container_state(cname)
        if st is None:
            return Health(False, f"container {cname} does not exist")
        if st != "running":
            return Health(False, f"container {cname} is {st}")
        if self.check_ready(cname, ctx.meta.get("PASSWORD", "")):
            return Health(True, f"{self.label} on localhost")
        return Health(False, f"{self.label} container is up but not "
                             f"yet accepting connections")

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

    def create_domain(self, ctx: SystemContext, spec: CreateDomainSpec) -> dict[str, Setting]:
        cname = ctx.meta["CONTAINER"]
        admin_pw = ctx.meta.get("PASSWORD", "")
        validate_name(spec.name, label=self.domain_flag)
        # Hooks read the port / staging root from env (as they did on the old
        # base); tdx exports the system meta before calling create_domain.
        os.environ["PORT"] = ctx.meta.get("PORT", os.environ.get("PORT", ""))
        os.environ["CONTAINER"] = cname
        if any(d.lower() == spec.name.lower()
               for d in self.do_list(cname, admin_pw)):
            if spec.force:
                self.do_delete(cname, admin_pw, spec.name)
                print(f">>> --force: removed existing {self.domain_flag} "
                      f"'{spec.name}'")
            else:
                raise DatagenError(
                    f"{self.domain_flag} '{spec.name}' already exists "
                    f"(use --force)")
        result = self.do_create(cname, admin_pw, spec.name)
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
            "SYS_ADMIN_USER": Setting("SYS_ADMIN_USER", value=ctx.meta.get("USER", "")),
            "SYS_ADMIN_PASSWORD": Setting("SYS_ADMIN_PASSWORD", value=admin_pw),
        }
        if self.uses_file_staging:
            root = ctx.meta.get("SYS_PATH", "")
            full = f"{root}/{spec.name}" if root else spec.name
            out["SYS_PATH"] = Setting("SYS_PATH", value=root, is_path=True)
            out["BASE_PATH"] = Setting("BASE_PATH", value=full, is_path=True)
        for k, v in self._domain_extra_vars().items():
            out[k] = Setting(k, value=v)
        return out

    def destroy_domain(self, ctx: DomainContext) -> None:
        self.do_delete(ctx.meta.get("SYS_CONTAINER", ""),
                       ctx.meta.get("SYS_ADMIN_PASSWORD", ""), ctx.meta["DB"])

    def domain_status(self, ctx: DomainContext) -> Health:
        cname = ctx.meta.get("SYS_CONTAINER", "")
        st = container_state(cname)
        return Health(st == "running", f"container {cname} is {st or 'absent'}")

    # ==================================================================
    # Data plane (SQL) — run the model's DDL / INSERTs against the domain
    # ==================================================================

    def install_schema(self, ctx: DomainContext, model: str, *, force: bool = False) -> None:
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
                f"(no SQL backend)")
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
                    f"{line} is not a .sql file (load only handles "
                    f"--format sql output)")
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
    # Per-system hooks — subclasses implement
    # ==================================================================

    def _build_docker_args(self, container: str, host_port: int, password: str) -> list[str]:
        args = ["-d", "-p", f"{host_port}:{self.internal_port}",
                "--name", container, "--platform", self.image_platform]
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
            f"(container {container} is up but not accepting connections)")

    # Abstract-ish hooks (subclasses must provide):
    def _docker_run(self, container: str, host_port: int, password: str) -> str:
        raise NotImplementedError

    def check_ready(self, container: str, password: str) -> bool:
        raise NotImplementedError

    def do_create(self, container: str, password: str, name: str) -> DomainResult:
        raise NotImplementedError

    def do_delete(self, container: str, password: str, name: str) -> None:
        raise NotImplementedError

    def do_list(self, container: str, password: str) -> list[str]:
        raise NotImplementedError
