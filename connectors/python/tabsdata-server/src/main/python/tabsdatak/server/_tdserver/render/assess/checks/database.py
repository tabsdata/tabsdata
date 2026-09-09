#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re
import socket
import ssl
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

import yaml

from tabsdatak.server._tdserver.render.assess import Check, Help, State
from tabsdatak.server._tdserver.render.assess.core.provider import Provider, provider_of
from tabsdatak.server._tdserver.render.assess.core.task import Target
from tabsdatak.server._tdserver.render.assess.core.vantage import Layer, Tier, state_for

CONCERN = "Database"

DATABASE_REACH = "Database reach"
DATABASE_TLS = "Transport security"
DATABASE_VERSION = "Database version"
LOCK_CAPACITY = "Lock capacity"
ROLE_ATTRIBUTES = "Role attributes"
DATABASE_OWNERSHIP = "Database ownership"
PROJECT_MARKER = "Cross-project marker"
DATABASE_CERTIFICATE = "Database certificate"
SCHEMA_STATE = "Schema state"

CODE_UNREACHED = "assess::database::unreached"
CODE_REFUSED = "assess::database::refused"
CODE_VERSION = "assess::database::version"
CODE_LOCKS = "assess::database::locks"
CODE_ROLE = "assess::database::role"
CODE_OWNER = "assess::database::owner"
CODE_MARKER = "assess::database::marker"
CODE_CERTIFICATE = "assess::database::certificate"
CODE_SCHEMA = "assess::database::schema"

CONFIG_FILE = "dbmanager.database.yaml"

SUPPORTED_MAJOR = 18
TESTED_VERSION = 180_004
MINIMUM_LOCKS = 1024
MARKER_ROLE = "td_across_projects"

CONNECT_TIMEOUT = 10
FETCH_TIMEOUT = 15

PLACEHOLDER = re.compile(r"\$\{\{[^}]*\}\}")

CA_URL: dict[Provider, str | None] = {
    Provider.EKS: "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem",
    Provider.AKS: "https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem",
    Provider.GKE: None,
    Provider.KMINUS: None,
}


class Unreached(Exception):
    def __init__(self, layer: Layer, reason: str) -> None:
        super().__init__(reason)
        self.layer = layer


@dataclass(frozen=True)
class Server:
    host: str
    port: int
    database: str
    username: str
    password: str
    certificate: str | None


_bundles: dict[str, bytes | None] = {}
_guard = Lock()


def config_of(instance: str) -> Path:
    return (
        Path.home()
        / ".tabsdata"
        / "instances"
        / instance
        / "init"
        / "input"
        / "configs"
        / CONFIG_FILE
    )


def settled(value) -> str:
    text = "" if value is None else str(value)
    return "" if PLACEHOLDER.search(text) else text


def server_of(instance: str) -> Server | None:
    path = config_of(instance)
    if not path.is_file():
        return None
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return None

    postgres = (document.get("database_server") or {}).get("postgres") or {}
    host = settled(postgres.get("host"))
    port = settled(postgres.get("port"))
    database = settled(postgres.get("database"))
    username = settled(postgres.get("username"))
    password = settled(postgres.get("password"))
    if not (host and port and database and username and password):
        return None
    try:
        number = int(port)
    except ValueError:
        return None
    return Server(
        host=host,
        port=number,
        database=database,
        username=username,
        password=password,
        certificate=settled(postgres.get("ssl_root_cert_pem")) or None,
    )


def bundle_for(provider: Provider | None) -> bytes | None:
    url = CA_URL.get(provider) if provider else None
    if not url:
        return None
    with _guard:
        if url not in _bundles:
            try:
                with urllib.request.urlopen(url, timeout=FETCH_TIMEOUT) as answer:
                    _bundles[url] = answer.read()
            except Exception:
                _bundles[url] = None
        return _bundles[url]


def context_for(server: Server, provider: Provider | None) -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED

    if server.certificate:
        context.load_verify_locations(cadata=server.certificate)
        return context

    bundle = bundle_for(provider)
    if bundle:
        context.load_verify_locations(cadata=bundle.decode("utf-8", "replace"))
    return context


def connect(server: Server, provider: Provider | None):
    import pg8000.dbapi

    try:
        socket.getaddrinfo(server.host, server.port)
    except OSError as error:
        raise Unreached(Layer.RESOLVE, f"the host does not resolve here: {error}")

    try:
        return pg8000.dbapi.connect(
            host=server.host,
            port=server.port,
            database=server.database,
            user=server.username,
            password=server.password,
            ssl_context=context_for(server, provider),
            timeout=CONNECT_TIMEOUT,
        )
    except Exception as error:
        text = spoken(error)
        if "password" in text.lower() or "authentication" in text.lower():
            raise Unreached(Layer.AUTHENTICATE, text)
        if "certificate" in text.lower() or "ssl" in text.lower():
            raise Unreached(
                Layer.CONNECT, f"the transport could not be secured: {text}"
            )
        raise Unreached(Layer.CONNECT, text)


def answered(error: Exception) -> bool:
    return any(isinstance(argument, dict) for argument in getattr(error, "args", ()))


def spoken(error: Exception) -> str:
    for argument in getattr(error, "args", ()):
        if isinstance(argument, dict):
            said = argument.get("M")
            if said:
                return str(said)
    text = str(error).strip()
    return text.splitlines()[0] if text else repr(error)


_asking = Lock()


def asked(connection, statement: str):
    with _asking:
        cursor = connection.cursor()
        cursor.execute(statement)
        rows = cursor.fetchall()
        return rows[0] if rows else None


def outcome(
    check: str,
    started: float,
    layer: Layer,
    satisfied: bool,
    code: str,
    title: str,
    facts: list[str],
    help: list[Help] | None = None,
) -> Check:
    state = state_for(Tier.REMOTE, layer, satisfied)
    if state is State.PASSED:
        return Check(
            concern=CONCERN,
            check=check,
            state=state,
            seconds=time.monotonic() - started,
        )
    return Check(
        concern=CONCERN,
        check=check,
        state=state,
        seconds=time.monotonic() - started,
        code=code,
        title=title,
        facts=facts,
        help=help or [],
    )


def vantage_help() -> list[Help]:
    return [
        Help(
            text="The pods of the instance reach the database from inside the cloud, "
            "so this may be a route your workstation does not have"
        )
    ]


def unknown(check: str, started: float, instance: str) -> Check:
    return Check(
        concern=CONCERN,
        check=check,
        state=State.SKIPPED,
        seconds=time.monotonic() - started,
        reason="the database of the instance is not configured yet",
        facts=[f"Expected file: {config_of(instance)}"],
    )


_opened: dict[tuple, object] = {}
_opening = Lock()


def opened(target: Target):
    server = server_of(target.instance)
    if server is None:
        raise Unreached(Layer.RESOLVE, "the database of the instance is not configured")
    key = (server.host, server.port, server.database, server.username)
    with _opening:
        held = _opened.get(key)
        if held is None:
            try:
                held = connect(server, provider_of(target.provider))
            except Unreached as error:
                held = error
            _opened[key] = held
    if isinstance(held, Unreached):
        raise held
    return server, held


def probe(
    target: Target,
    check: str,
    statement: str,
    verdict,
    code: str,
    title: str,
    guidance: list[Help],
) -> Check:
    started = time.monotonic()

    if server_of(target.instance) is None:
        return unknown(check, started, target.instance)

    try:
        server, connection = opened(target)
    except Unreached as error:
        return outcome(
            check,
            started,
            error.layer,
            False,
            CODE_UNREACHED,
            "The database could not be reached from this workstation",
            [str(error)],
            vantage_help(),
        )

    try:
        row = asked(connection, statement.format(server=server))
    except Exception as error:
        said = spoken(error)
        if answered(error):
            return outcome(
                check,
                started,
                Layer.ASSERT,
                False,
                code,
                "The database refused the question this check asks",
                [said],
            )
        return Check(
            concern=CONCERN,
            check=check,
            state=State.FAILED,
            seconds=time.monotonic() - started,
            title="The check could not be completed",
            facts=[said],
            help=[
                Help(
                    text="This is a defect in assess itself, not in the instance, "
                    "so report it"
                )
            ],
        )

    satisfied, facts = verdict(row, server)
    return outcome(
        check, started, Layer.ASSERT, satisfied, code, title, facts, guidance
    )


def assess_certificate(target: Target) -> Check:
    started = time.monotonic()
    provider = provider_of(target.provider)

    path = config_of(target.instance)
    if not path.is_file():
        return unknown(DATABASE_CERTIFICATE, started, target.instance)

    inline = ""
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        postgres = (document.get("database_server") or {}).get("postgres") or {}
        inline = settled(postgres.get("ssl_root_cert_pem"))
    except yaml.YAMLError:
        pass

    if not inline:
        url = CA_URL.get(provider) if provider else None
        if not url:
            return Check(
                concern=CONCERN,
                check=DATABASE_CERTIFICATE,
                state=State.WAIVED,
                reason="the pods of this provider carry no database certificate",
            )
        if bundle_for(provider) is None:
            return Check(
                concern=CONCERN,
                check=DATABASE_CERTIFICATE,
                state=State.WARNING,
                seconds=time.monotonic() - started,
                code=CODE_CERTIFICATE,
                title="The authority the pods verify the database with is unreachable",
                facts=[url],
                help=[
                    Help(
                        text="Each pod downloads this at startup, so an instance "
                        "cannot start while it is unreachable"
                    )
                ],
            )
        return Check(
            concern=CONCERN,
            check=DATABASE_CERTIFICATE,
            state=State.PASSED,
            seconds=time.monotonic() - started,
            facts=[f"Downloaded by the pods from {url}"],
        )

    try:
        ssl.create_default_context(cadata=inline)
    except Exception as error:
        return Check(
            concern=CONCERN,
            check=DATABASE_CERTIFICATE,
            state=State.FAILED,
            seconds=time.monotonic() - started,
            code=CODE_CERTIFICATE,
            title="The certificate written into the configuration cannot be read",
            facts=[spoken(error)],
            help=[
                Help(
                    text="Google Cloud signs each server with an authority of "
                    "its own, so the certificate travels in the configuration "
                    "rather than being downloaded"
                )
            ],
        )
    return Check(
        concern=CONCERN,
        check=DATABASE_CERTIFICATE,
        state=State.PASSED,
        seconds=time.monotonic() - started,
        facts=[
            "Carried inline, as Google Cloud signs each server with its own authority"
        ],
    )


def assess_database_reach(target: Target) -> Check:
    started = time.monotonic()
    server = server_of(target.instance)
    if server is None:
        return unknown(DATABASE_REACH, started, target.instance)

    try:
        opened(target)
    except Unreached as error:
        return outcome(
            DATABASE_REACH,
            started,
            error.layer,
            False,
            CODE_REFUSED if error.layer is Layer.AUTHENTICATE else CODE_UNREACHED,
            (
                "The database refused these credentials"
                if error.layer is Layer.AUTHENTICATE
                else "The database could not be reached from this workstation"
            ),
            [str(error)],
            [] if error.layer is Layer.AUTHENTICATE else vantage_help(),
        )
    return Check(
        concern=CONCERN,
        check=DATABASE_REACH,
        state=State.PASSED,
        seconds=time.monotonic() - started,
    )


def assess_transport_security(target: Target) -> Check:
    def verdict(row, server):
        if not row or not row[0]:
            return False, ["The connection carries no transport security"]
        return True, []

    return probe(
        target,
        DATABASE_TLS,
        "select ssl, version from pg_stat_ssl where pid = pg_backend_pid()",
        verdict,
        CODE_REFUSED,
        "The connection to the database is not encrypted",
        [
            Help(
                text="The server is open to every address, so the transport is the "
                "only thing keeping its traffic private"
            )
        ],
    )


def spelled(number: int) -> str:
    return f"{number // 10000}.{number % 10000}"


def judged(found: int) -> tuple[State, str, list[Help]]:
    if found < SUPPORTED_MAJOR * 10000:
        return (
            State.FAILED,
            f"PostgreSQL {spelled(found)} is older than tabsdata supports",
            [
                Help(
                    text=f"An instance needs PostgreSQL {SUPPORTED_MAJOR}, and its "
                    "schema uses features earlier majors do not carry"
                )
            ],
        )
    if found >= (SUPPORTED_MAJOR + 1) * 10000:
        return (
            State.WARNING,
            f"PostgreSQL {spelled(found)} is newer than tabsdata supports",
            [
                Help(
                    text=f"Tabsdata is supported on PostgreSQL {SUPPORTED_MAJOR}, and "
                    "a later major is neither tested nor supported"
                )
            ],
        )
    if found != TESTED_VERSION:
        return (
            State.WARNING,
            f"PostgreSQL {spelled(found)} is supported but not tested",
            [
                Help(
                    text=f"Tabsdata is tested against PostgreSQL "
                    f"{spelled(TESTED_VERSION)}, and a different minor of the same "
                    "major is expected to work"
                )
            ],
        )
    return State.PASSED, "", []


def assess_database_version(target: Target) -> Check:
    started = time.monotonic()

    if server_of(target.instance) is None:
        return unknown(DATABASE_VERSION, started, target.instance)

    try:
        _, connection = opened(target)
    except Unreached as error:
        return outcome(
            DATABASE_VERSION,
            started,
            error.layer,
            False,
            CODE_UNREACHED,
            "The database could not be reached from this workstation",
            [str(error)],
            vantage_help(),
        )

    try:
        row = asked(connection, "select current_setting('server_version_num')::int")
    except Exception as error:
        return Check(
            concern=CONCERN,
            check=DATABASE_VERSION,
            state=State.WARNING,
            seconds=time.monotonic() - started,
            code=CODE_VERSION,
            title="The version of the database could not be read",
            facts=[spoken(error)],
        )

    found = int(row[0]) if row else 0
    state, title, guidance = judged(found)
    if state is State.PASSED:
        return Check(
            concern=CONCERN,
            check=DATABASE_VERSION,
            state=state,
            seconds=time.monotonic() - started,
            facts=[f"PostgreSQL {spelled(found)}"],
        )
    return Check(
        concern=CONCERN,
        check=DATABASE_VERSION,
        state=state,
        seconds=time.monotonic() - started,
        code=CODE_VERSION,
        title=title,
        facts=[f"Tested against PostgreSQL {spelled(TESTED_VERSION)}"],
        help=guidance,
    )


def assess_lock_capacity(target: Target) -> Check:
    def verdict(row, server):
        found = int(row[0]) if row else 0
        if found >= MINIMUM_LOCKS:
            return True, []
        return False, [
            f"max_locks_per_transaction is {found}, and tabsdata needs "
            f"{MINIMUM_LOCKS} or more",
        ]

    return probe(
        target,
        LOCK_CAPACITY,
        "select current_setting('max_locks_per_transaction')::int",
        verdict,
        CODE_LOCKS,
        "The lock table of the database is too small for tabsdata",
        [
            Help(
                text="A versioned table takes one lock per row it closes, and a "
                "register writes thousands in one transaction, so the instance "
                "refuses to start below this floor"
            ),
            Help(
                text="The setting is static on every managed provider, so the server "
                "has to be restarted once it is raised"
            ),
        ],
    )


def assess_role_attributes(target: Target) -> Check:
    def verdict(row, server):
        if not row:
            return False, [f"The role {server.username} was not found"]
        creates_roles, creates_databases, bypasses = row
        missing = []
        if not creates_roles:
            missing.append("CREATEROLE")
        if not creates_databases:
            missing.append("CREATEDB")
        if bypasses:
            missing.append("NOBYPASSRLS (the role bypasses row-level security)")
        return not missing, missing

    return probe(
        target,
        ROLE_ATTRIBUTES,
        "select rolcreaterole, rolcreatedb, rolbypassrls from pg_roles "
        "where rolname = current_user",
        verdict,
        CODE_ROLE,
        "The role of the instance lacks something it needs on the server",
        [
            Help(
                text="An instance provisions a role and a schema for each of its "
                "components, and a role that bypasses row-level security would see "
                "across every project"
            )
        ],
    )


def assess_database_ownership(target: Target) -> Check:
    def verdict(row, server):
        owner = row[0] if row else ""
        if owner == server.username:
            return True, []
        return False, [
            f"The database {server.database} is owned by {owner or 'nobody readable'}",
            f"The instance connects as {server.username}",
        ]

    return probe(
        target,
        DATABASE_OWNERSHIP,
        "select pg_get_userbyid(datdba) from pg_database "
        "where datname = current_database()",
        verdict,
        CODE_OWNER,
        "The role of the instance does not own its database",
        [
            Help(
                text="Connecting is not owning, and the difference surfaces at the "
                "first migration rather than here"
            )
        ],
    )


def assess_schema_state(target: Target) -> Check:
    def verdict(row, server):
        held = (row[0] if row else "") or ""
        if not held:
            return True, []
        return False, [f"Already holds {held}"]

    return probe(
        target,
        SCHEMA_STATE,
        "select string_agg(nspname, ', ' order by nspname) from pg_namespace "
        "where nspname not like 'pg\\_%' and nspname <> 'information_schema' "
        "and nspname <> 'public'",
        verdict,
        CODE_SCHEMA,
        "The database already holds the schemas of an instance",
        [
            Help(
                text="An instance provisions a schema for each of its components, and "
                "creating over schemas left by another one fails partway through"
            ),
            Help(text="Use an empty database, or drop what the previous instance left"),
        ],
    )


def assess_project_marker(target: Target) -> Check:
    def verdict(row, server):
        if not row:
            return False, [f"The role {MARKER_ROLE} does not exist on this server"]
        member, admin = row
        if member and admin:
            return True, []
        if not member:
            return False, [f"{server.username} does not hold {MARKER_ROLE}"]
        return False, [
            f"{server.username} holds {MARKER_ROLE} but cannot grant it onward",
        ]

    return probe(
        target,
        PROJECT_MARKER,
        "select "
        f"  pg_has_role(current_user, '{MARKER_ROLE}', 'MEMBER'), "
        f"  pg_has_role(current_user, '{MARKER_ROLE}', 'USAGE') "
        f"where exists (select 1 from pg_roles where rolname = '{MARKER_ROLE}')",
        verdict,
        CODE_MARKER,
        "The cross-project marker is not in place for this role",
        [
            Help(
                text="An instance grants this marker to each component role it "
                "creates, and holding a role is not enough to grant it onward"
            ),
            Help(
                text="Whoever administers the server creates it once, then grants it "
                "with the option to pass it on"
            ),
        ],
    )
