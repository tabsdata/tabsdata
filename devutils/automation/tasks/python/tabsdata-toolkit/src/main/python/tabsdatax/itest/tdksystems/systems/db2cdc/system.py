#
# Copyright 2026 Tabsdata Inc.
#

"""IBM Db2 configured for change-data-capture via IBM SQL Replication (ASN).

Db2 CDC is heavyweight and, unlike MySQL/Postgres, needs work at TWO points:

* System create (`_post_start`, once): switch the shared ``XP`` database to
  archive logging + take a baseline offline backup (``asncap`` refuses to start
  otherwise), create the ``ASN.IBMSNAP_*`` control tables (``asnclp``), then
  start the ``asncap`` capture program.
* Model seed (`configure_cdc`, per table): mark each source table ``DATA
  CAPTURE CHANGES``, ``asnclp create registration`` to materialise its CD
  (change-data) staging table, add the ``IBMSNAP_LOGMARKER`` column + trigger
  the ingester relies on, then signal CAPSTART and wait for the registration to
  go Active. This is exactly the "extra setup once the tables exist" that a
  non-CDC Db2 system never does — and the Tabsdata Db2 connector refuses any
  table missing from ``ASN.IBMSNAP_REGISTER``.

The command sequence mirrors the connector's own CDC test fixture
(``tabsdata-conn-db2`` ``_cdc_docker.py``); everything runs as ``db2inst1``
inside the container via the CLP (``db2`` / ``asnclp`` / ``asncap``), so no
extra driver work is needed. Reuses the base model DDL (``db2.ddl``) via
``ddl_slug``.
"""

from __future__ import annotations

import random
import time

from ...errors import DatagenError
from ..db2.system import XP_DB, Db2System, db2_schema_name

_ASN = "ASN"  # capture schema holding the IBMSNAP_* control + CD tables
_ARCHIVE_DIR = "archive"
_BACKUP_DIR = "backup"
_STAGING_PREFIX = "TD_T__"  # CD staging table name prefix (ASN.TD_T__<TABLE>)
_OLD_VALUES_PREFIX = "_"  # before-image column prefix in the CD table

_WAIT_TIMEOUT_S = 300
_WAIT_POLL_S = 3


class Db2CdcSystem(Db2System):
    slug = "db2cdc"
    label = "IBM Db2 Community (CDC)"

    @property
    def ddl_slug(self) -> str:
        return "db2"

    # ---- system-level CDC apparatus (once, at system create) ----

    def _post_start(self, container: str, password: str) -> None:
        super()._post_start(container, password)  # creates the shared XP database
        print(">>> CDC: configuring Db2 SQL Replication (archive log + asncap) …")
        self._setup_archive(container)
        self._setup_asn_control_tables(container)
        self._setup_asncap(container)
        print(">>> CDC: asncap capture program is running")

    # The Db2 CLP returns a non-zero code for *warnings* too (e.g. SQL1363W on a
    # config change that needs reactivation), so a bare `set -e` / `&&` chain
    # aborts on a perfectly successful step. `db2w` treats only rc >= 4 (real
    # errors) as failure — warnings pass. Every db2cdc script runs through it.
    _PRELUDE = (
        'db2w() { db2 "$@"; local r=$?; [ "$r" -ge 4 ] && return "$r"; return 0; }\n'
    )

    def _run(self, container: str, body: str, *, ignore_failure: bool = False) -> str:
        return self._exec_inst(
            container, self._PRELUDE + body, ignore_failure=ignore_failure
        )

    def _clp(self, container: str, sql: str, *, ignore_failure: bool = False) -> str:
        """Run one SQL statement over the CLP against the XP database (warnings
        tolerated, real errors raise)."""
        return self._run(
            container,
            f"db2w connect to {XP_DB} >/dev/null\n"
            f'db2w -v "{sql}"\n'
            f"rc=$?\n"
            f"db2 terminate >/dev/null 2>&1\n"
            f"exit $rc\n",
            ignore_failure=ignore_failure,
        )

    def _clp_scalar(self, container: str, sql: str) -> str:
        """Return the first column of the first row (``db2 -x``), stripped."""
        out = self._run(
            container,
            f"db2 connect to {XP_DB} >/dev/null 2>&1\n"
            f'db2 -x "{sql}"\n'
            f"db2 terminate >/dev/null 2>&1\n",
            ignore_failure=True,
        )
        for line in out.splitlines():
            line = line.strip()
            if line and not line.startswith("DB2"):
                return line
        return ""

    def _archive_configured(self, container: str) -> bool:
        val = self._clp_scalar(
            container,
            "select value from sysibmadm.dbcfg where name = 'logarchmeth1'",
        )
        return val.upper().startswith("DISK:")

    def _setup_archive(self, container: str) -> None:
        """Archive logging + a baseline offline backup (idempotent)."""
        if self._archive_configured(container):
            print(">>> CDC: archive logging already configured")
            return
        print(">>> CDC: enabling archive logging + baseline backup (slow) …")
        self._run(
            container,
            f"mkdir -p $HOME/{_ARCHIVE_DIR} $HOME/{_BACKUP_DIR}\n"
            f"db2w connect to {XP_DB} >/dev/null\n"
            f"db2w update db cfg for {XP_DB} using logarchmeth1 "
            f"disk:$HOME/{_ARCHIVE_DIR}/\n"
            f"db2 connect reset >/dev/null 2>&1\n"
            f"db2 terminate >/dev/null 2>&1\n"
            f"db2 force applications all >/dev/null 2>&1\n"
            f"sleep 5\n"
            f"db2 deactivate database {XP_DB} >/dev/null 2>&1\n"
            # backup is the step that must succeed; its rc is the script's rc.
            f"db2w backup db {XP_DB} to $HOME/{_BACKUP_DIR}/\n",
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
        """`asnclp create control tables for capture server` (idempotent)."""
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
            f"set server capture to db {XP_DB}\\n"
            f"set capture schema source {_ASN}\\n"
            f"create control tables for capture server\\n"
            f"quit\\n' | asnclp\n"
            f"db2w connect to {XP_DB} >/dev/null\n"
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
            f"nohup asncap capture_server={XP_DB} capture_schema={_ASN} "
            f"> /dev/null 2>&1 &",
        )
        self._poll(
            lambda: self._asncap_responsive(container),
            what="asncap to become responsive",
        )
        self._poll(
            lambda: bool(
                self._clp_scalar(
                    container,
                    f"select max_commitseq from {_ASN}.ibmsnap_restart",
                )
            ),
            what="asncap to populate the restart table",
        )

    def _asncap_responsive(self, container: str) -> bool:
        try:
            self._exec_inst(
                container,
                f"asnccmd capture_schema={_ASN} capture_server={XP_DB} status",
            )
            return True
        except DatagenError:
            return False

    # ---- per-table registration (at model seed) ----

    def configure_cdc(self, ctx, model: str) -> None:
        container = ctx.meta.get("SYS_CONTAINER", "")
        if not container:
            raise DatagenError("db2cdc domain has no SYS_CONTAINER back-reference")
        schema = db2_schema_name(ctx.meta.get("DB", "") or ctx.name)
        tables = self._list_schema_tables(container, schema)
        if not tables:
            print(">>> CDC: no tables to register")
            return
        print(f">>> CDC: registering {len(tables)} table(s) for ASN capture …")
        for table in tables:
            self._register_table(container, schema, table)
        print(">>> CDC: all tables registered and active")

    def _list_schema_tables(self, container: str, schema: str) -> list[str]:
        out = self._run(
            container,
            f"db2 connect to {XP_DB} >/dev/null 2>&1\n"
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
        staging = f"{_STAGING_PREFIX}{table}"
        script = f"TD_ASN_REGISTER_{table}".upper()
        print(f"    registering {source} …")
        self._clp(container, f"alter table {source} data capture changes")
        self._run(
            container,
            f'cd "$HOME"\n'
            f"rm -f {script}\n"
            f"printf 'set output capture script {script}\\n"
            f"set server capture to db {XP_DB}\\n"
            f"set capture schema source {_ASN}\\n"
            f"create registration ({source}) differential refresh "
            f"stage {_ASN}.{staging} cols all image both "
            f"prefix {_OLD_VALUES_PREFIX} options capture all\\n"
            f"quit\\n' | asnclp\n"
            f"db2w connect to {XP_DB} >/dev/null\n"
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
        """Add the IBMSNAP_LOGMARKER column + a BEFORE-INSERT trigger that fills
        it from IBMSNAP_UOW — the connector's ingester reads the per-row commit
        timestamp from it, and Db2 11.5 community asnclp/asncap won't populate
        it on their own."""
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
            f"('{XP_DB}', '{_ASN}', '{staging}', '{schema}', '{table}', 0, "
            f"'TDQUAL', 'TDSET', '{XP_DB}', 1, '{_ASN}', '{staging}', '{map_id}')",
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
            raise DatagenError("registration row not found yet")
        return state.upper() == "A"

    def _poll(self, cond, *, what: str) -> None:
        deadline = time.monotonic() + _WAIT_TIMEOUT_S
        last: Exception | None = None
        while time.monotonic() < deadline:
            try:
                if cond():
                    return
            except DatagenError as exc:
                last = exc
            time.sleep(_WAIT_POLL_S)
        raise DatagenError(
            f"CDC: timed out waiting for {what}" + (f" ({last})" if last else "")
        )
