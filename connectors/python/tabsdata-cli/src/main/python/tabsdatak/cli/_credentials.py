#
# Copyright 2026 Tabsdata Inc.
#

"""Credentials passed through the environment. With ``TDK_SERVER`` and either
``TDK_BEARER`` (a session) or ``TDK_TOKEN`` (a service token) set, tdk runs
statelessly: it never reads or writes the profile files under the Tabsdata
directory and never refreshes the credential. ``TDK_TOKEN`` without
``TDK_SERVER`` only feeds ``tdk login --token``."""

import base64
import binascii
import json
import os
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace

import rich_click as click

from tabsdatak.cli._capability import SESSION
from tabsdatak.cli._errors import TdkError, TdkException

BEARER_ENV = "TDK_BEARER"
TOKEN_ENV = "TDK_TOKEN"
SERVER_ENV = "TDK_SERVER"
PROJECT_ENV = "TDK_PROJECT"
SERVICE_TOKEN_PREFIX = "tdst_"
EXPIRY_MARGIN_SECONDS = 5
SERVICE_TOKEN = "service_token"

_SERVICE_TOKEN_SHAPE = re.compile(r"tdst_v1_[0-9a-f]{64}")
_TOKEN_REFUSED_HINT = (
    "The service token was refused: it may have expired or been revoked; check "
    "'tdk auth service-token list' from a password session."
)
_LOGIN_EXPIRED_HINT = "The 'tdk login' session has expired; run 'tdk login' again."
_SESSION_ONLY_HINT = (
    "This command needs a user session; the service token in {env} carries only "
    "view_project and develop_project on the routes that accept tokens."
)


@dataclass(frozen=True)
class EnvCredential:
    kind: str
    bearer: str
    server: str
    expires_at: float | None
    env: str


def credential_kind(bearer: str) -> str:
    return SERVICE_TOKEN if bearer.startswith(SERVICE_TOKEN_PREFIX) else SESSION


def _jwt_expiry(token: str) -> float:
    try:
        payload = token.split(".")[1]
        claims = json.loads(
            base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
        )
        return float(claims["exp"])
    except (IndexError, KeyError, TypeError, ValueError, binascii.Error):
        raise TdkException(
            "usage", f"{BEARER_ENV} does not hold a session token (a JWT with 'exp')."
        ) from None


def env_credential(env: Mapping[str, str] | None = None) -> EnvCredential | None:
    env = os.environ if env is None else env
    bearer = env.get(BEARER_ENV)
    token = env.get(TOKEN_ENV)
    server = env.get(SERVER_ENV)
    if bearer and token:
        raise TdkException(
            "ambiguous_credentials",
            f"Both {BEARER_ENV} and {TOKEN_ENV} are set; set only one of them.",
        )
    if bearer:
        if credential_kind(bearer) == SERVICE_TOKEN:
            raise TdkException(
                "usage", f"{BEARER_ENV} holds a service token; pass it in {TOKEN_ENV}."
            )
        if not server:
            raise TdkException("usage", f"{BEARER_ENV} is set but {SERVER_ENV} is not.")
        return EnvCredential(
            kind=SESSION,
            bearer=bearer,
            server=server,
            expires_at=_jwt_expiry(bearer),
            env=BEARER_ENV,
        )
    if token and server:
        if not _SERVICE_TOKEN_SHAPE.fullmatch(token):
            raise TdkException(
                "usage", f"{TOKEN_ENV} does not hold a service token (tdst_v1_...)."
            )
        return EnvCredential(
            kind=SERVICE_TOKEN,
            bearer=token,
            server=server,
            expires_at=None,
            env=TOKEN_ENV,
        )
    return None


def env_server(env: Mapping[str, str] | None = None) -> str | None:
    env = os.environ if env is None else env
    return env.get(SERVER_ENV) or None


def env_project(env: Mapping[str, str] | None = None) -> str | None:
    env = os.environ if env is None else env
    return env.get(PROJECT_ENV) or None


def ensure_unexpired(credential: EnvCredential, now: float | None = None) -> None:
    if credential.expires_at is None:
        return
    now = time.time() if now is None else now
    if credential.expires_at <= now + EXPIRY_MARGIN_SECONDS:
        raise TdkException(
            "auth_expired",
            f"The session in {BEARER_ENV} has expired.",
            hint="Sign in again to get a fresh session.",
        )


def stored_expiry(connection_file: str | None) -> float | None:
    try:
        with open(connection_file, encoding="utf-8") as stored:
            expiry = json.load(stored).get("expiration_time")
    except (OSError, TypeError, ValueError, AttributeError):
        return None
    return float(expiry) if isinstance(expiry, (int, float)) else None


def refine_auth_error(
    error: TdkError,
    credential: EnvCredential | None,
    now: float | None = None,
    login_expiry: float | None = None,
) -> TdkError:
    now = time.time() if now is None else now
    if credential is None:
        if (
            error.type != "auth_required"
            or login_expiry is None
            or now < login_expiry - EXPIRY_MARGIN_SECONDS
        ):
            return error
        return replace(error, type="auth_expired", hint=_LOGIN_EXPIRED_HINT)
    if credential.kind == SERVICE_TOKEN:
        if error.type == "auth_required":
            return replace(error, hint=_TOKEN_REFUSED_HINT)
        if error.type == "service_token_unsupported":
            return replace(error, hint=_SESSION_ONLY_HINT.format(env=credential.env))
        return error
    if error.type != "auth_required":
        return error
    if now < credential.expires_at - EXPIRY_MARGIN_SECONDS:
        return error
    return replace(error, type="auth_expired")


def ensure_stateful(ctx: click.Context, command: str) -> None:
    credential = ctx.obj.get("env_credential")
    if credential is not None:
        raise TdkException(
            "usage",
            f"'{command}' changes stored credentials or settings, and the credential "
            f"comes from {credential.env}; there is nothing stored to change.",
        )
