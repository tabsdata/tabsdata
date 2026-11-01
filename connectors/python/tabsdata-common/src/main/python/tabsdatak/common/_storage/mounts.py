#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import re
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from tabsdatak.common._storage.transfer import (
    FALSE,
    TRUE,
    StoreKind,
    TransferError,
)

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


class MountError(ValueError):
    pass


class MountDeclaration(ABC):
    @abstractmethod
    def storage_options(self, location: str) -> dict[str, str]: ...


@dataclass(frozen=True)
class AnonymousMountDeclaration(MountDeclaration):
    def storage_options(self, location: str) -> dict[str, str]:
        return {}


@dataclass(frozen=True)
class NamedMountDeclaration(MountDeclaration):
    id: str
    uri: str
    options: dict[str, str] = field(default_factory=dict)

    @classmethod
    def read_all(cls, path: Path) -> list[NamedMountDeclaration]:
        try:
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as error:
            raise MountError(
                f"Cannot read the storage configuration {path}: {error}"
            ) from error
        entries = ((document or {}).get(STORAGE_KEY) or {}).get(MOUNTS_KEY) or []
        return [
            cls(
                id=str(entry.get(ID_KEY)),
                uri=text(entry.get(URI_KEY) or ""),
                options={
                    str(key): text(value)
                    for key, value in (entry.get(OPTIONS_KEY) or {}).items()
                },
            )
            for entry in entries
            if entry.get(ID_KEY) is not None
        ]

    @classmethod
    def read(cls, path: Path, mount: str) -> NamedMountDeclaration:
        for declaration in cls.read_all(path):
            if declaration.id == mount:
                return declaration
        raise MountError(f"Mount {mount} is not declared in {path}")

    def resolved_uri(self) -> str:
        resolved = resolve(
            self.uri,
            STORAGE_KEY,
            MOUNTS_KEY,
            self.id,
            URI_KEY,
        )
        if not resolved:
            raise MountError(f"Mount {self.id} declares no uri")
        return resolved

    def storage_options(self, location: str) -> dict[str, str]:
        resolved = {}
        for key, declared in self.options.items():
            value = resolve(
                declared,
                STORAGE_KEY,
                MOUNTS_KEY,
                self.id,
                OPTIONS_KEY,
                key,
            )
            if value is not None:
                resolved[key] = value
        return complete_options(self.id, location, resolved)


@dataclass(frozen=True)
class EnvironmentMountDeclaration(MountDeclaration):
    id: str

    def storage_options(self, location: str) -> dict[str, str]:
        resolved = {
            key: os.environ[variable].strip()
            for key, variable in mount_option_variables(self.id).items()
        }
        return complete_options(self.id, location, resolved)


def canonical_keys(
    location: str, values: Mapping[str, str]
) -> tuple[StoreKind, dict[str, str]]:
    try:
        kind = StoreKind.of(location)
        return kind, {kind.canonical(key): value for key, value in values.items()}
    except TransferError as error:
        raise MountError(str(error)) from error


def complete_options(
    mount: str, location: str, values: Mapping[str, str]
) -> dict[str, str]:
    kind, resolved = canonical_keys(location, values)
    missing = kind.missing(resolved)
    if missing is not None:
        raise MountError(f"Mount {mount} lacks the {kind} options {missing}")
    return resolved


def mount_option_variables(
    mount: str, environment: Mapping[str, str] | None = None
) -> dict[str, str]:
    variables = os.environ if environment is None else environment
    prefix = secret_variable(STORAGE_KEY, MOUNTS_KEY, mount, OPTIONS_KEY)
    prefix = f"{prefix}{SECRET_SEPARATOR}"
    return {
        name[len(prefix) :].lower(): name
        for name in sorted(variables)
        if name.startswith(prefix) and len(name) > len(prefix)
    }


def canonical_option_variables(mount: str, location: str) -> dict[str, str]:
    return canonical_keys(location, mount_option_variables(mount))[1]


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
