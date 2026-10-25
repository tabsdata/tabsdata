#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

import yaml

from tabsdatak.common._storage.transfer import STORE_KIND_BY_SCHEME, StoreKind

STORAGE_KEY = "storage"
MOUNTS_KEY = "mounts"
ID_KEY = "id"
URI_KEY = "uri"
OPTIONS_KEY = "options"

SECRET_PREFIX = "SECRETS__"
SECRET_SEPARATOR = "__"
SEGMENT_REPLACEMENT = "_"
PATH_SEPARATOR = "."

PLACEHOLDER_PATTERN = re.compile(r"^\$\{\{secret(Auto)?(?::([A-Za-z0-9._-]*))?(\?)?}}$")
OPTIONAL_GROUP = 3

TRUE = "true"
FALSE = "false"

AWS_ACCESS_KEY_ID_OPTION = "aws_access_key_id"
AWS_SECRET_ACCESS_KEY_OPTION = "aws_secret_access_key"
AWS_SESSION_TOKEN_OPTION = "aws_session_token"
AWS_REGION_OPTION = "aws_region"
AWS_ENDPOINT_OPTION = "aws_endpoint"
AWS_VIRTUAL_HOSTED_STYLE_REQUEST_OPTION = "aws_virtual_hosted_style_request"
AWS_ALLOW_HTTP_OPTION = "aws_allow_http"
AWS_SKIP_SIGNATURE_OPTION = "aws_skip_signature"

AZURE_STORAGE_ACCOUNT_NAME_OPTION = "azure_storage_account_name"
AZURE_STORAGE_ACCOUNT_KEY_OPTION = "azure_storage_account_key"
AZURE_STORAGE_SAS_TOKEN_OPTION = "azure_storage_sas_token"
AZURE_STORAGE_ENDPOINT_OPTION = "azure_storage_endpoint"
AZURE_ALLOW_HTTP_OPTION = "azure_allow_http"

GOOGLE_SERVICE_ACCOUNT_KEY_OPTION = "google_service_account_key"
GOOGLE_SERVICE_ACCOUNT_OPTION = "google_service_account"
GOOGLE_ALLOW_HTTP_OPTION = "google_allow_http"

S3_SIGNATURE_REQUIREMENT = "access_key_id and secret_access_key, or anonymous"
S3_ADDRESS_REQUIREMENT = "region or endpoint"
AZURE_ACCOUNT_REQUIREMENT = "account_name"
AZURE_SECRET_REQUIREMENT = "account_key or sas_token"
GCS_ACCOUNT_REQUIREMENT = "service_account_key or service_account"

FRIENDLY_KEYS = {
    StoreKind.S3: {
        "access_key_id": AWS_ACCESS_KEY_ID_OPTION,
        "secret_access_key": AWS_SECRET_ACCESS_KEY_OPTION,
        "session_token": AWS_SESSION_TOKEN_OPTION,
        "region": AWS_REGION_OPTION,
        "endpoint": AWS_ENDPOINT_OPTION,
        "virtual_hosted_style_request": AWS_VIRTUAL_HOSTED_STYLE_REQUEST_OPTION,
        "allow_http": AWS_ALLOW_HTTP_OPTION,
        "anonymous": AWS_SKIP_SIGNATURE_OPTION,
        "skip_signature": AWS_SKIP_SIGNATURE_OPTION,
    },
    StoreKind.AZURE: {
        "account_name": AZURE_STORAGE_ACCOUNT_NAME_OPTION,
        "account_key": AZURE_STORAGE_ACCOUNT_KEY_OPTION,
        "sas_token": AZURE_STORAGE_SAS_TOKEN_OPTION,
        "endpoint": AZURE_STORAGE_ENDPOINT_OPTION,
        "allow_http": AZURE_ALLOW_HTTP_OPTION,
    },
    StoreKind.GCS: {
        "service_account_key": GOOGLE_SERVICE_ACCOUNT_KEY_OPTION,
        "service_account": GOOGLE_SERVICE_ACCOUNT_OPTION,
        "allow_http": GOOGLE_ALLOW_HTTP_OPTION,
    },
    StoreKind.LOCAL: {},
}

CANONICAL_PREFIXES = {
    StoreKind.S3: "aws_",
    StoreKind.AZURE: "azure_",
    StoreKind.GCS: "google_",
}


class MountError(ValueError):
    pass


@dataclass(frozen=True)
class MountDeclaration:
    id: str
    uri: str
    options: dict[str, str] = field(default_factory=dict)


def text(value: object) -> str:
    if isinstance(value, bool):
        return TRUE if value else FALSE
    return str(value)


def encode_segment(segment: str) -> str:
    encoded = "".join(
        character
        if character.isascii()
        and (character.isalnum() or character == SEGMENT_REPLACEMENT)
        else SEGMENT_REPLACEMENT
        for character in segment.upper()
    )
    if encoded[:1].isdigit():
        encoded = f"{SEGMENT_REPLACEMENT}{encoded}"
    return encoded


def secret_variable(*path: str) -> str:
    encoded = SECRET_SEPARATOR.join(encode_segment(segment) for segment in path)
    return f"{SECRET_PREFIX}{encoded}"


def resolve(text: str, *path: str) -> str | None:
    match = PLACEHOLDER_PATTERN.match(text)
    if match is None:
        return text
    variable = secret_variable(*path)
    resolved = os.environ.get(variable)
    if resolved is not None:
        return resolved.strip()
    if match.group(OPTIONAL_GROUP):
        return None
    raise MountError(
        f"The value at {PATH_SEPARATOR.join(path)} is a secret placeholder, and "
        f"the environment variable {variable} is unset"
    )


def read(storage_config: Path, mount: str) -> MountDeclaration:
    try:
        document = yaml.safe_load(storage_config.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise MountError(
            f"Cannot read the storage configuration {storage_config}: {error}"
        ) from error
    for entry in ((document or {}).get(STORAGE_KEY) or {}).get(MOUNTS_KEY) or []:
        if str(entry.get(ID_KEY)) == mount:
            return MountDeclaration(
                id=mount,
                uri=text(entry.get(URI_KEY) or ""),
                options={
                    str(key): text(value)
                    for key, value in (entry.get(OPTIONS_KEY) or {}).items()
                },
            )
    raise MountError(f"Mount {mount} is not declared in {storage_config}")


def uri(declaration: MountDeclaration) -> str:
    resolved = resolve(
        declaration.uri,
        STORAGE_KEY,
        MOUNTS_KEY,
        declaration.id,
        URI_KEY,
    )
    if not resolved:
        raise MountError(f"Mount {declaration.id} declares no uri")
    return resolved


def store_kind(location: str) -> StoreKind:
    scheme = urlparse(location).scheme
    if scheme not in STORE_KIND_BY_SCHEME:
        raise MountError(f"Location {location} uses an unsupported scheme")
    return STORE_KIND_BY_SCHEME[scheme]


def canonical(kind: StoreKind, key: str) -> str:
    if key in FRIENDLY_KEYS[kind]:
        return FRIENDLY_KEYS[kind][key]
    prefix = CANONICAL_PREFIXES.get(kind)
    if prefix and key.startswith(prefix):
        return key
    raise MountError(f"Option {key} is not a known {kind} option")


def options(declaration: MountDeclaration, location: str) -> dict[str, str]:
    kind = store_kind(location)
    resolved = {}
    for key, declared in declaration.options.items():
        value = resolve(
            declared,
            STORAGE_KEY,
            MOUNTS_KEY,
            declaration.id,
            OPTIONS_KEY,
            key,
        )
        if value is not None:
            resolved[canonical(kind, key)] = value
    missing = missing_options(kind, resolved)
    if missing:
        raise MountError(f"Mount {declaration.id} lacks the {kind} options {missing}")
    return resolved


def missing_options(kind: StoreKind, resolved: dict[str, str]) -> str | None:
    if kind == StoreKind.S3:
        anonymous = resolved.get(AWS_SKIP_SIGNATURE_OPTION) == TRUE
        signed = (
            AWS_ACCESS_KEY_ID_OPTION in resolved
            and AWS_SECRET_ACCESS_KEY_OPTION in resolved
        )
        if not (anonymous or signed):
            return S3_SIGNATURE_REQUIREMENT
        if AWS_REGION_OPTION not in resolved and AWS_ENDPOINT_OPTION not in resolved:
            return S3_ADDRESS_REQUIREMENT
    elif kind == StoreKind.AZURE:
        if AZURE_STORAGE_ACCOUNT_NAME_OPTION not in resolved:
            return AZURE_ACCOUNT_REQUIREMENT
        if (
            AZURE_STORAGE_ACCOUNT_KEY_OPTION not in resolved
            and AZURE_STORAGE_SAS_TOKEN_OPTION not in resolved
        ):
            return AZURE_SECRET_REQUIREMENT
    elif kind == StoreKind.GCS:
        if (
            GOOGLE_SERVICE_ACCOUNT_KEY_OPTION not in resolved
            and GOOGLE_SERVICE_ACCOUNT_OPTION not in resolved
        ):
            return GCS_ACCOUNT_REQUIREMENT
    return None
