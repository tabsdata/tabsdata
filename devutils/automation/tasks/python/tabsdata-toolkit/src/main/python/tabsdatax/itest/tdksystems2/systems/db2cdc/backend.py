#
# Copyright 2026 Tabsdata Inc.
#

"""IBM Db2 configured for change-data-capture via IBM SQL Replication (ASN).

A variant of the plain ``db2`` system (same shared ``XP`` database, schema-per-
domain model and DDL, reused via ``ddl_from_slug``) with the ASN capture
apparatus set up at TWO points:

* System create (``post_provision``, once): switch ``XP`` to archive logging +
  take a baseline offline backup (``asncap`` refuses to start otherwise), create
  the ``ASN.IBMSNAP_*`` control tables (``asnclp``), then start ``asncap``.
* Model install (``after_install_schema``, per table): mark each source table
  ``DATA CAPTURE CHANGES``, ``asnclp create registration`` to materialise its CD
  staging table, add the ``IBMSNAP_LOGMARKER`` column + trigger the ingester
  needs, then signal CAPSTART and wait for the registration to go Active.
  First, for all tables at once, the CD table names are claimed
  (:meth:`Db2Cdc._claim_cd_tables`): an earlier install of this domain's own is
  re-registered, a leftover no live registration holds is reclaimed.

A CD table is ``ASN.TD_<TAG>__<TABLE>``, <TAG> 8 hex of the domain's schema, and
the domain records ``CD_TABLE_PREFIX`` (``ASN.TD_<TAG>__``) in its meta for a
function to name its tables by (``${D0__CD_TABLE_PREFIX}EMPLOYEES``). The CD
tables live in the one shared ``ASN`` schema, so a name of the table alone --
``ASN.TD_T__<TABLE>``, as it was -- let only one db2cdc domain per model use a
system: a second domain of the model, or any leftover of one, was refused. A
registration made under that old name is still found, and removed, through
``IBMSNAP_REGISTER``, which records each registration's CD table.

Everything runs as ``db2inst1`` inside the container via the CLP (``db2`` /
``asnclp`` / ``asncap``); the command sequence mirrors the tabsdata-conn-db2 CDC
test fixture.
"""

from __future__ import annotations

import hashlib
import random
import time

from tabsdatax.itest.tdksystems2 import descriptor, store
from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.shell import docker_exec
from tabsdatax.itest.tdksystems2.systems.db2.backend import _XP_DB, Db2

_ASN = "ASN"
_ARCHIVE_DIR = "archive"

# asnccmd reports a stopped capture program with this code, on a zero exit.
_ASNCAP_DOWN = "ASN0506E"
_BACKUP_DIR = "backup"
#: The domain meta key naming the CD tables' qualified prefix.
CD_TABLE_PREFIX_KEY = "CD_TABLE_PREFIX"
_OLD_VALUES_PREFIX = "_"
_WAIT_TIMEOUT_S = 300
_WAIT_POLL_S = 3

# The Db2 CLP returns non-zero for warnings too (e.g. SQL1363W); `db2w` treats
# only rc >= 4 (real errors) as failure so a `set -e`/`&&` chain isn't aborted
# by a perfectly successful step.
_PRELUDE = 'db2w() { db2 "$@"; local r=$?; [ "$r" -ge 4 ] && return "$r"; return 0; }\n'


class Db2Cdc(Db2):
    slug = "db2cdc"
    label = "IBM Db2 Community (CDC)"
    ddl_from_slug = "db2"

    # ---- CLP plumbing --------------------------------------------------

    def _exec_inst(
        self, container: str, script: str, *, ignore_failure: bool = False
    ) -> str:
        cp = docker_exec(container, "bash", "-lc", script, user=self.root_user)
        if cp.returncode != 0 and not ignore_failure:
            raise TdxError(
                f"db2 instance command failed: {cp.stderr.strip() or cp.stdout.strip()}"
            )
        return cp.stdout

    def _run(self, container: str, body: str, *, ignore_failure: bool = False) -> str:
        return self._exec_inst(
            container, _PRELUDE + body, ignore_failure=ignore_failure
        )

    def _clp(self, container: str, sql: str, *, ignore_failure: bool = False) -> str:
        return self._run(
            container,
            f"db2w connect to {_XP_DB} >/dev/null\n"
            f'db2w -v "{sql}"\n'
            f"rc=$?\n"
            f"db2 terminate >/dev/null 2>&1\n"
            f"exit $rc\n",
            ignore_failure=ignore_failure,
        )

    def _clp_scalar(self, container: str, sql: str) -> str:
        out = self._run(
            container,
            f"db2 connect to {_XP_DB} >/dev/null 2>&1\n"
            f'db2 -x "{sql}"\n'
            f"db2 terminate >/dev/null 2>&1\n",
            ignore_failure=True,
        )
        for line in out.splitlines():
            line = line.strip()
            if line and not line.startswith("DB2"):
                return line
        return ""

    def _poll(self, cond, *, what: str) -> None:
        deadline = time.monotonic() + _WAIT_TIMEOUT_S
        last: Exception | None = None
        while time.monotonic() < deadline:
            try:
                if cond():
                    return
            except TdxError as exc:
                last = exc
            time.sleep(_WAIT_POLL_S)
        raise TdxError(
            f"CDC: timed out waiting for {what}" + (f" ({last})" if last else "")
        )

    # ---- system-level CDC apparatus (post_provision, once) -------------

    def post_provision(self, container: str, password: str) -> None:
        super().post_provision(container, password)  # creates the shared XP database
        print(">>> CDC: configuring Db2 SQL Replication (archive log + asncap) …")
        self._setup_archive(container)
        self._setup_asn_control_tables(container)
        self._ensure_asncap(container)
        print(">>> CDC: asncap capture program is running")

    def _archive_configured(self, container: str) -> bool:
        val = self._clp_scalar(
            container,
            "select value from sysibmadm.dbcfg where name = 'logarchmeth1'",
        )
        return val.upper().startswith("DISK:")

    def _setup_archive(self, container: str) -> None:
        if self._archive_configured(container):
            print(">>> CDC: archive logging already configured")
            return
        print(">>> CDC: enabling archive logging + baseline backup (slow) …")
        self._run(
            container,
            f"mkdir -p $HOME/{_ARCHIVE_DIR} $HOME/{_BACKUP_DIR}\n"
            f"db2w connect to {_XP_DB} >/dev/null\n"
            f"db2w update db cfg for {_XP_DB} using logarchmeth1 "
            f"disk:$HOME/{_ARCHIVE_DIR}/\n"
            f"db2 connect reset >/dev/null 2>&1\n"
            f"db2 terminate >/dev/null 2>&1\n"
            f"db2 force applications all >/dev/null 2>&1\n"
            f"sleep 5\n"
            f"db2 deactivate database {_XP_DB} >/dev/null 2>&1\n"
            f"db2w backup db {_XP_DB} to $HOME/{_BACKUP_DIR}/\n",
        )

    def _asn_control_present(self, container: str) -> bool:
        return bool(
            self._clp_scalar(
                container,
                f"select 1 from syscat.tables where tabschema = '{_ASN}' "
                f"and tabname = 'IBMSNAP_CAPPARMS'",
            )
        )

    def _setup_asn_control_tables(self, container: str) -> None:
        if self._asn_control_present(container):
            print(">>> CDC: ASN control tables already present")
            return
        print(">>> CDC: creating ASN control tables …")
        script = "TD_ASN_CONTROL"
        self._run(
            container,
            f'cd "$HOME"\n'
            f"rm -f {script}\n"
            f"printf 'set output capture script {script}\\n"
            f"set server capture to db {_XP_DB}\\n"
            f"set capture schema source {_ASN}\\n"
            f"create control tables for capture server\\n"
            f"quit\\n' | asnclp\n"
            f"db2w connect to {_XP_DB} >/dev/null\n"
            f"db2w -tvf {script}\n"
            f"rc=$?\n"
            f"db2 terminate >/dev/null 2>&1\n"
            f"exit $rc\n",
        )

    def _setup_asncap(self, container: str) -> None:
        home = self._exec_inst(container, "printf '%s' \"$HOME\"").strip()
        self._clp(
            container,
            f"update {_ASN}.ibmsnap_capparms set capture_path = '{home}', "
            f"autostop = 'N', term = 'N'",
        )
        self._exec_inst(
            container,
            f"nohup asncap capture_server={_XP_DB} capture_schema={_ASN} "
            f"> /dev/null 2>&1 &",
        )
        self._poll(
            lambda: self._asncap_responsive(container),
            what="asncap to become responsive",
        )
        self._poll(
            lambda: bool(
                self._clp_scalar(
                    container, f"select max_commitseq from {_ASN}.ibmsnap_restart"
                )
            ),
            what="asncap to populate the restart table",
        )

    def _ensure_asncap(self, container: str) -> None:
        """Start ``asncap`` unless it is already answering.

        ``asncap`` is a process inside the container, not a database object, so
        it does not survive a container restart the way the archive setting and
        the ``ASN.IBMSNAP_*`` tables do -- and it is known to exit on its own
        (the connector's own CDC tests carry a watchdog for exactly that). Both
        ``post_provision`` and ``restart_system`` go through here so bringing it
        back is always safe to attempt.
        """
        if self._asncap_responsive(container):
            print(">>> CDC: asncap already running")
            return
        self._setup_asncap(container)

    def restart_system(self, meta: dict[str, str]) -> str:
        """Restart the container, then put the capture program back.

        The base implementation only restarts the container. That is enough for
        a plain Db2, but here it silently leaves the system without a capture
        program: nothing writes to the CD tables any more, and a CDC reader sees
        a registered source that never produces a change. The archive setting and
        the control tables are durable, so only ``asncap`` needs re-establishing
        -- but both setup steps are guarded, so re-running them costs nothing.
        """
        result = super().restart_system(meta)
        container = meta.get("DOCKER_CONTAINER_NAME") or meta.get("SYS_CONTAINER", "")
        if not container:
            return result
        self._wait_ready(container, meta.get("PASSWORD", ""), self.ready_timeout)
        print(">>> CDC: re-establishing Db2 SQL Replication after restart …")
        self._setup_archive(container)
        self._setup_asn_control_tables(container)
        self._ensure_asncap(container)
        print(">>> CDC: asncap capture program is running")
        return result

    def _asncap_responsive(self, container: str) -> bool:
        """Whether the capture program is actually up.

        ``asnccmd status`` exits 0 whether or not Capture is running: when it is
        down it still succeeds and reports ``ASN0506E ... "Capture" was not
        running``. Treating a clean exit as "up" therefore always says yes, so
        the answer has to come from the output.
        """
        try:
            out = self._exec_inst(
                container,
                f"asnccmd capture_schema={_ASN} capture_server={_XP_DB} status",
            )
        except TdxError:
            return False
        return _ASNCAP_DOWN not in out

    # ---- per-table registration (public: used by after_install_schema and
    # ---- by tests that need a source table of their own) ----------------

    @staticmethod
    def staging_prefix(schema: str) -> str:
        """The CD table name prefix of the domain whose schema is `schema`:
        ``TD_<8 hex of the schema>__``, short enough to leave any table name
        room under Db2's 128, and a domain's own so no two domains share one."""
        tag = hashlib.sha256(schema.upper().encode()).hexdigest()[:8].upper()
        return f"TD_{tag}__"

    def _staging(self, schema: str, table: str) -> str:
        return f"{self.staging_prefix(schema)}{table.upper()}"

    def cd_table(self, schema: str, table: str) -> str:
        """The qualified CD table ASN capture writes `schema.table`'s changes into."""
        return f"{_ASN}.{self._staging(schema, table)}"

    def domain_meta(
        self, sys_meta: dict[str, str], entity_name: str, password: str
    ) -> dict[str, str]:
        meta = super().domain_meta(sys_meta, entity_name, password)
        meta[CD_TABLE_PREFIX_KEY] = (
            f"{_ASN}.{self.staging_prefix(self._schema(meta['DB']))}"
        )
        return meta

    def register_table(
        self, meta: dict[str, str], table: str, *, ddl: str | None = None
    ) -> str:
        """Register `table` in the domain's schema for ASN capture, creating it
        from `ddl` first when one is given, and return its CD table.

        This is the same sequence ``after_install_schema`` runs for the model's
        tables -- ``data capture changes``, ``asnclp create registration``, the
        reserved ``IBMSNAP_LOGMARKER`` column, CAPSTART, wait for Active. A test
        that needs a source table of its own goes through here rather than
        reimplementing it, so registration stays defined in one place.
        """
        container, schema = self._container_and_schema(meta)
        self._ensure_asncap(container)
        if ddl:
            self._clp(container, ddl)
        self._register_table(container, schema, table.upper())
        return self.cd_table(schema, table)

    def deregister_table(
        self, meta: dict[str, str], table: str, *, drop: bool = False
    ) -> None:
        """Undo :meth:`register_table`: stop capturing `table`, remove its
        registration and CD table, and drop the source table when `drop`.

        CAPSTOP comes first and is waited on. asncap holds the registration in
        memory: drop the CD table from under a still-capturing registration and
        its next insert fails with SQLCODE -42704, which takes the whole capture
        program down -- so the table has to be released before it is removed.

        Best-effort otherwise -- a test cleaning up must not fail because some
        part of the registration was never made.
        """
        container, schema = self._container_and_schema(meta)
        table = table.upper()
        self._remove_registration(container, schema, table)
        self._clp(
            container,
            f"alter table {schema}.{table} data capture none",
            ignore_failure=True,
        )
        if drop:
            self._clp(container, f"drop table {schema}.{table}", ignore_failure=True)

    def _remove_registration(self, container: str, schema: str, table: str) -> None:
        """CAPSTOP, then the registration's rows, its trigger and its CD table:
        what a deregistration and a reclaim both take away. The source table's
        own ``DATA CAPTURE`` flag is the deregistration's to clear -- a reclaim
        has no source table left to alter. The CD table is the one the
        registration records, so one made under an older naming goes too."""
        staging = self._registered_staging(container, schema, table)
        self._send_capstop(container, schema, table)
        self._clp(
            container,
            f"delete from {_ASN}.ibmsnap_register where source_owner = "
            f"'{schema}' and source_table = '{table}'",
            ignore_failure=True,
        )
        self._clp(
            container,
            f"delete from {_ASN}.ibmsnap_pruncntl where source_owner = "
            f"'{schema}' and source_table = '{table}'",
            ignore_failure=True,
        )
        self._drop_cd_table(container, staging)

    def _registered_staging(self, container: str, schema: str, table: str) -> str:
        """The CD table the registration of ``schema.table`` writes to, as
        ``IBMSNAP_REGISTER`` records it; this naming's when there is none."""
        recorded = self._clp_scalar(
            container,
            f"select coalesce(min(cd_table), '') from {_ASN}.ibmsnap_register "
            f"where source_owner = '{schema}' and source_table = '{table}'",
        ).strip()
        return recorded or self._staging(schema, table)

    def _drop_cd_table(self, container: str, staging: str) -> None:
        """Drop ``ASN.<staging>`` and its trigger, and make sure it is gone.

        Each drop may fail on its own -- a trigger already removed, a table
        never made -- so neither is fatal by itself; what matters is the
        table. One that survives is the name the next registration of this
        model collides on (ASN1705E), with no registration row left to say who
        holds it, so it is an error here rather than a silent orphan later.
        """
        self._clp(container, f"drop trigger {_ASN}.LM_{staging}", ignore_failure=True)
        self._clp(container, f"drop table {_ASN}.{staging}", ignore_failure=True)
        if self._cd_table_exists(container, staging):
            raise TdxError(
                f"CDC: {_ASN}.{staging} could not be dropped in {container}; "
                f"drop it by hand (db2 drop table {_ASN}.{staging}) or "
                f"recreate the system"
            )

    def _cd_table_exists(self, container: str, staging: str) -> bool:
        return self._clp_scalar(
            container,
            f"select count(*) from syscat.tables where tabschema = '{_ASN}' "
            f"and tabname = '{staging}'",
        ).strip() not in ("", "0")

    def _send_capstop(self, container: str, schema: str, table: str) -> None:
        """Tell asncap to stop capturing ``schema.table`` and wait for it to
        acknowledge, so the CD table can be removed safely."""
        source = f"{schema}.{table}"
        self._clp(
            container,
            f"insert into {_ASN}.ibmsnap_signal (signal_type, signal_subtype, "
            f"signal_input_in, signal_state) values "
            f"('CMD', 'CAPSTOP', '{source}', 'P')",
            ignore_failure=True,
        )
        try:
            self._poll(
                lambda: self._capstop_consumed(container, source),
                what=f"CAPSTOP of {source} to be consumed",
            )
        except TdxError:
            # A capture program that is already down cannot consume it; the
            # registration is going away regardless.
            pass

    def _capstop_consumed(self, container: str, source: str) -> bool:
        state = self._clp_scalar(
            container,
            f"select signal_state from {_ASN}.ibmsnap_signal where "
            f"signal_type = 'CMD' and signal_subtype = 'CAPSTOP' and "
            f"signal_input_in = '{source}' order by signal_time desc "
            f"fetch first 1 row only",
        )
        return state.upper() == "C" or state == ""

    def _container_and_schema(self, meta: dict[str, str]) -> tuple[str, str]:
        container = meta.get("SYS_CONTAINER", "")
        if not container:
            raise TdxError("db2cdc domain has no SYS_CONTAINER back-reference")
        return container, self._schema(meta.get("DB", ""))

    # ---- domain teardown -------------------------------------------------

    def destroy_domain(self, domain_meta: dict[str, str]) -> None:
        """Deregister this domain's tables from ASN capture, then drop its schema.

        The inherited teardown only drops the schema, which is not enough here:
        a registration lives in the SHARED ``ASN`` schema, not in the domain's,
        so dropping the domain leaves its ``ASN.IBMSNAP_REGISTER`` /
        ``IBMSNAP_PRUNCNTL`` rows, ``ASN.LM_TD_<TAG>__<T>`` triggers and
        ``ASN.TD_<TAG>__<T>`` CD tables behind — still ``STATE='A'``, pointing at a
        schema that no longer exists.

        That is not merely untidy: a registration still ``STATE='A'`` holds its
        source table and CD table names, and a later domain given the same
        schema name -- a re-created domain, a forced re-create -- is refused by
        asnclp before it starts (ASN1703E, ASN1705E).

        Deregistering first is what keeps the system reusable. It has to happen
        BEFORE the schema goes: :meth:`deregister_table` sends CAPSTOP for
        ``schema.table`` and clears the source table's ``DATA CAPTURE`` flag,
        neither of which is possible once the table is gone.

        """
        self._deregister_schema(domain_meta)
        super().destroy_domain(domain_meta)

    def provision_domain(
        self,
        sys_meta: dict[str, str],
        entity_name: str,
        domain_meta: dict[str, str],
        *,
        force: bool = False,
    ) -> None:
        """``create --force`` over an existing domain drops its schema and makes
        it again, under the same name. The registrations are in ``ASN``, not in
        the schema, so without this they outlive their tables still held by
        that name, and the next model install onto it is refused table by table
        (ASN1703E, reason code 4: already registered). Deregistered first, for
        the same reason as :meth:`install_schema`."""
        if force:
            self._deregister_schema(domain_meta)
        super().provision_domain(sys_meta, entity_name, domain_meta, force=force)

    def install_schema(
        self, meta: dict[str, str], model: str, *, force: bool = False
    ) -> None:
        """``--force`` drops every table and installs the model again; the
        registrations live in ``ASN``, not in the schema, and survive that.
        Deregistered first, while the tables still exist: CAPSTOP names the
        source table, and one that is already gone is never acknowledged, so
        each stop would wait out the full timeout."""
        if force:
            self._deregister_schema(meta)
        super().install_schema(meta, model, force=force)

    def uninstall_schema(self, meta: dict[str, str], model: str) -> None:
        """Deregistered before the drop, for the reason :meth:`install_schema`
        gives -- and because a registration outliving its table keeps the CD
        table name, so the next model install would be refused."""
        self._deregister_schema(meta)
        super().uninstall_schema(meta, model)

    def _deregister_schema(self, meta: dict[str, str]) -> None:
        """Deregister every table ASN still holds under the domain's schema.
        Best-effort per table -- a teardown must not strand the schema because
        one registration was already half-removed."""
        try:
            container, schema = self._container_and_schema(meta)
        except TdxError:
            # An older descriptor with no back-reference: nothing to deregister,
            # and the schema drop does not need the container.
            return
        tables = self._registered_tables(container, schema)
        if not tables:
            return
        print(f">>> CDC: deregistering {len(tables)} table(s) from ASN capture …")
        for table in tables:
            try:
                self.deregister_table(meta, table)
            except TdxError as exc:  # noqa: PERF203 -- per-table isolation
                print(f"    WARNING: {schema}.{table} not fully deregistered: {exc}")

    def _registered_tables(self, container: str, schema: str) -> list[str]:
        """The tables ASN still holds a registration for under `schema`.

        Driven off ``ASN.IBMSNAP_REGISTER`` rather than off ``syscat.tables``,
        because it is the REGISTRATION that blocks the next create and it can
        outlive its source table — a table dropped by hand leaves a row here,
        and that row still owns the CD table name.
        """
        out = self._run(
            container,
            f"db2 connect to {_XP_DB} >/dev/null 2>&1\n"
            f'db2 -x "select source_table from {_ASN}.ibmsnap_register '
            f"where source_owner = '{schema}'\"\n"
            f"db2 terminate >/dev/null 2>&1\n",
            ignore_failure=True,
        )
        return [
            line.strip()
            for line in out.splitlines()
            if line.strip() and not line.strip().startswith("DB2")
        ]

    # ---- model registration ---------------------------------------------

    def after_install_schema(self, meta: dict[str, str], model: str) -> None:
        container, schema = self._container_and_schema(meta)
        tables = self._list_schema_tables(container, schema)
        if not tables:
            print(">>> CDC: no tables to register")
            return
        self._claim_cd_tables(container, schema, tables)
        print(f">>> CDC: registering {len(tables)} table(s) for ASN capture …")
        for table in tables:
            self._register_table(container, schema, table)
        print(">>> CDC: all tables registered and active")

    # ---- CD table names: this domain's own, left over or not --------------

    def _claim_cd_tables(self, container: str, schema: str, tables: list[str]) -> None:
        """Before the first registration: every CD table this model needs is
        free, or is freed here, or the create is refused with the holder named.

        A CD table's name is the domain's (see :meth:`staging_prefix`), so what
        can hold one is this domain's own earlier install, or a leftover of a
        domain that had this schema before. Left to asnclp, that surfaces one
        table in, after the schema is installed, as ``ASN1705E The change data
        "table" ... already exists``: a table named and no cause. Every table is
        checked first, so a refusal leaves nothing half-registered.

        A registration whose source table is gone -- a schema dropped without
        deregistering, by hand or from a descriptor that predates the
        back-reference -- captures nothing and is reclaimed. So is a CD table
        no registration holds at all: a removal whose drop failed left it. And
        this domain's own registration from an earlier install of the model is
        removed and made again, since asnclp refuses a table already registered. One
        whose source table still exists is a live domain, kept on purpose or
        not, and is not touched: the refusal names its schema and, when this
        store has a descriptor with that schema, the domain and the command
        that frees it.
        """
        blockers: dict[str, list[str]] = {}
        for table in tables:
            owner = self._cd_table_owner(container, schema, table)
            if owner == schema:
                # This domain's own registration from an earlier install: the
                # model installed again over tables that were kept. asnclp
                # refuses a table that is already registered (ASN1703E, reason
                # code 4), so it goes and is made again.
                print(
                    f"    {schema}.{table} is still registered from an earlier "
                    f"install; re-registering …"
                )
                self._remove_registration(container, schema, table)
                continue
            if not owner:
                # No registration holds the name, but the table can still be
                # there: a removal whose drop failed took the rows and left the
                # table, and asnclp refuses on the table, not on the rows.
                if self._cd_table_exists(container, self._staging(schema, table)):
                    print(
                        f"    reclaiming {self.cd_table(schema, table)}, which no "
                        f"registration holds …"
                    )
                    self._drop_cd_table(container, self._staging(schema, table))
                continue
            if self._source_table_exists(container, owner, table):
                blockers.setdefault(owner, []).append(table)
                continue
            print(
                f"    reclaiming {self.cd_table(schema, table)} from {owner}.{table}, "
                f"whose source table is gone …"
            )
            self._remove_registration(container, owner, table)
        if not blockers:
            return
        held = ", ".join(
            self.cd_table(schema, t) for tables_ in blockers.values() for t in tables_
        )
        owners = []
        fixes = []
        for owner in sorted(blockers):
            named = self._domain_with_schema(owner, container)
            if named:
                owners.append(f"schema {owner} -- domain '{named}'")
                fixes.append(f"tdd domain destroy --domain {named}")
            else:
                owners.append(
                    f"schema {owner} -- no domain in this store has that schema "
                    f"(its descriptor is gone, or unreadable here)"
                )
        fix = (
            f"destroy that domain first ({'; '.join(fixes)})"
            if fixes
            else "deregister those tables in the container by hand, or "
            "recreate the system"
        )
        raise TdxError(
            f"CDC: {held} held by the live registrations of {'; '.join(owners)} in "
            f"{container}: {fix}, or create this one on another db2cdc system."
        )

    def _cd_table_owner(self, container: str, schema: str, table: str) -> str:
        """The schema whose registration holds this domain's CD table for
        `table`, or ''."""
        # coalesce over min(): one row always comes back, so an empty result is
        # a blank and not whatever the CLP prints for no rows
        return self._clp_scalar(
            container,
            f"select coalesce(min(source_owner), '') from {_ASN}.ibmsnap_register "
            f"where cd_owner = '{_ASN}' and "
            f"cd_table = '{self._staging(schema, table)}'",
        ).strip()

    def _source_table_exists(self, container: str, schema: str, table: str) -> bool:
        return self._clp_scalar(
            container,
            f"select count(*) from syscat.tables where tabschema = '{schema}' "
            f"and tabname = '{table}'",
        ).strip() not in ("", "0")

    def _domain_with_schema(self, schema: str, container: str) -> str:
        """This store's name for the db2cdc domain whose schema is `schema` in
        `container`, or ''. Best effort: a descriptor that cannot be read here
        (no key in this shell, a legacy format, a stray file) is no reason to
        fail a message that is already explaining a failure."""
        try:
            paths = sorted(store.domain_dir().iterdir())
        except OSError:
            return ""
        for path in paths:
            try:
                info = descriptor.load(path)
            except Exception:  # noqa: BLE001 -- best effort, see above
                continue
            meta = getattr(info, "meta", {}) or {}
            if (
                getattr(info, "system", "") == self.slug
                and self._schema(meta.get("DB", "")) == schema
                and meta.get("SYS_CONTAINER", container) == container
            ):
                return info.name
        return ""

    def _list_schema_tables(self, container: str, schema: str) -> list[str]:
        out = self._run(
            container,
            f"db2 connect to {_XP_DB} >/dev/null 2>&1\n"
            f'db2 -x "select tabname from syscat.tables where '
            f"tabschema = '{schema}' and type = 'T'\"\n"
            f"db2 terminate >/dev/null 2>&1\n",
            ignore_failure=True,
        )
        return [
            line.strip()
            for line in out.splitlines()
            if line.strip() and not line.strip().startswith("DB2")
        ]

    def _register_table(self, container: str, schema: str, table: str) -> None:
        source = f"{schema}.{table}"
        staging = self._staging(schema, table)
        # The domain's own, as the CD table is: two domains of one model may
        # register at the same moment, and share the container's home dir.
        script = f"TD_ASN_REGISTER_{staging}".upper()
        print(f"    registering {source} …")
        self._clp(container, f"alter table {source} data capture changes")
        self._run(
            container,
            f'cd "$HOME"\n'
            f"rm -f {script}\n"
            f"printf 'set output capture script {script}\\n"
            f"set server capture to db {_XP_DB}\\n"
            f"set capture schema source {_ASN}\\n"
            f"create registration ({source}) differential refresh "
            f"stage {_ASN}.{staging} cols all image both "
            f"prefix {_OLD_VALUES_PREFIX} options capture all\\n"
            f"quit\\n' | asnclp\n"
            f"db2w connect to {_XP_DB} >/dev/null\n"
            f"db2w -tvf {script}\n"
            f"rc=$?\n"
            f"db2 terminate >/dev/null 2>&1\n"
            f"exit $rc\n",
        )
        self._add_logmarker(container, staging)
        map_id = self._send_capstart(container, schema, table, staging)
        self._poll(
            lambda: self._capstart_consumed(container, map_id),
            what=f"CAPSTART of {source} to be consumed",
        )
        self._poll(
            lambda: self._registration_active(container, schema, table),
            what=f"registration of {source} to become active",
        )

    def _add_logmarker(self, container: str, staging: str) -> None:
        trigger = f"LM_{staging}"
        self._clp(
            container,
            f"alter table {_ASN}.{staging} add column IBMSNAP_LOGMARKER "
            f"TIMESTAMP NOT NULL WITH DEFAULT CURRENT TIMESTAMP",
        )
        self._clp(
            container,
            f"create trigger {_ASN}.{trigger} no cascade before insert on "
            f"{_ASN}.{staging} referencing new as N for each row "
            f"set N.IBMSNAP_LOGMARKER = coalesce((select IBMSNAP_LOGMARKER from "
            f"{_ASN}.IBMSNAP_UOW where IBMSNAP_COMMITSEQ = N.IBMSNAP_COMMITSEQ), "
            f"CURRENT TIMESTAMP)",
        )

    def _send_capstart(
        self, container: str, schema: str, table: str, staging: str
    ) -> str:
        map_id = str(random.randint(1, 2**31 - 1))  # noqa: S311 - internal signal id
        self._clp(
            container,
            f"insert into {_ASN}.ibmsnap_pruncntl (target_server, target_owner, "
            f"target_table, source_owner, source_table, source_view_qual, "
            f"apply_qual, set_name, cntl_server, target_structure, "
            f"phys_change_owner, phys_change_table, map_id) values "
            f"('{_XP_DB}', '{_ASN}', '{staging}', '{schema}', '{table}', 0, "
            f"'TDQUAL', 'TDSET', '{_XP_DB}', 1, '{_ASN}', '{staging}', '{map_id}')",
        )
        self._clp(
            container,
            f"insert into {_ASN}.ibmsnap_signal (signal_type, signal_subtype, "
            f"signal_input_in, signal_state) values "
            f"('CMD', 'CAPSTART', '{map_id}', 'P')",
        )
        return map_id

    def _capstart_consumed(self, container: str, map_id: str) -> bool:
        state = self._clp_scalar(
            container,
            f"select signal_state from {_ASN}.ibmsnap_signal where "
            f"signal_type = 'CMD' and signal_subtype = 'CAPSTART' and "
            f"signal_input_in = '{map_id}' order by signal_time desc "
            f"fetch first 1 row only",
        )
        return state.upper() == "C" or state == ""

    def _registration_active(self, container: str, schema: str, table: str) -> bool:
        state = self._clp_scalar(
            container,
            f"select state from {_ASN}.ibmsnap_register where "
            f"source_owner = '{schema}' and source_table = '{table}'",
        )
        if not state:
            raise TdxError("registration row not found yet")
        return state.upper() == "A"
