#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import yaml

from tabsdatak.server._tdserver.render.assess import Check, Help, State
from tabsdatak.server._tdserver.render.assess.core.task import Target
from tabsdatak.server._tdserver.render.assess.core.vantage import Layer, Tier, state_for

CONCERN = "Storage"

STORAGE_SHAPE = "Storage shape"
STORAGE_CREDENTIALS = "Storage credentials"
STORAGE_REACH = "Storage reach"

CODE_SHAPE = "assess::storage::shape"
CODE_CREDENTIALS = "assess::storage::credentials"
CODE_REACH = "assess::storage::reach"

CONFIG_FILE = "apiserver.storage.yaml"

WANTED_MOUNTS = ("ROOT", "BUNDLES", "IMAGES")

PLACEHOLDER = re.compile(r"\$\{\{[^}]*\}\}")

SCHEME_OPTIONS: dict[str, tuple[str, ...]] = {
    "s3": ("aws_region", "aws_access_key_id", "aws_secret_access_key"),
    "az": ("azure_storage_account_name", "azure_storage_account_key"),
    "gs": ("google_service_account_key",),
}

SCHEME_CAPTION = {
    "s3": "AWS S3",
    "az": "Azure Blob Storage",
    "gs": "Google Cloud Storage",
}


@dataclass(frozen=True)
class Mount:
    identifier: str
    uri: str
    options: dict

    @property
    def scheme(self) -> str:
        return urlparse(self.uri).scheme

    @property
    def container(self) -> str:
        return urlparse(self.uri).netloc

    @property
    def prefix(self) -> str:
        return urlparse(self.uri).path.strip("/")


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


def mounts_of(instance: str) -> list[Mount] | None:
    path = config_of(instance)
    if not path.is_file():
        return None
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return None

    declared = (document.get("storage") or {}).get("mounts") or []
    mounts = []
    for entry in declared:
        if not isinstance(entry, dict):
            continue
        options = {
            key: settled(value) for key, value in (entry.get("options") or {}).items()
        }
        mounts.append(
            Mount(
                identifier=str(entry.get("id", "")),
                uri=settled(entry.get("uri")),
                options=options,
            )
        )
    return mounts


def unknown(check: str, started: float, instance: str) -> Check:
    return Check(
        concern=CONCERN,
        check=check,
        state=State.SKIPPED,
        seconds=time.monotonic() - started,
        reason="the storage of the instance is not configured yet",
        facts=[f"Expected file: {config_of(instance)}"],
    )


def passed(check: str, started: float) -> Check:
    return Check(
        concern=CONCERN,
        check=check,
        state=State.PASSED,
        seconds=time.monotonic() - started,
    )


def overlapping(mounts: list[Mount]) -> list[str]:
    clashes = []
    for one in range(len(mounts)):
        for other in range(one + 1, len(mounts)):
            first, second = mounts[one], mounts[other]
            if not (first.uri and second.uri):
                continue
            if first.container != second.container:
                continue
            if first.prefix == second.prefix:
                clashes.append(
                    f"{first.identifier} and {second.identifier} share {first.uri}"
                )
            elif first.prefix.startswith(
                second.prefix + "/"
            ) or second.prefix.startswith(first.prefix + "/"):
                clashes.append(
                    f"{first.identifier} and {second.identifier} nest under one another"
                )
    return clashes


def assess_storage_shape(target: Target) -> Check:
    started = time.monotonic()
    mounts = mounts_of(target.instance)
    if mounts is None:
        return unknown(STORAGE_SHAPE, started, target.instance)

    named = {mount.identifier for mount in mounts}
    faults = []

    for wanted in WANTED_MOUNTS:
        if wanted not in named:
            faults.append(f"The mount {wanted} is not declared")
    for surplus in sorted(named - set(WANTED_MOUNTS)):
        faults.append(f"The mount {surplus} is not one tabsdata uses")

    for mount in mounts:
        if not mount.uri:
            faults.append(f"{mount.identifier} has no uri yet")
            continue
        if mount.scheme not in SCHEME_OPTIONS and mount.scheme != "file":
            faults.append(
                f"{mount.identifier} uses the unsupported scheme {mount.scheme}"
            )

    faults.extend(overlapping(mounts))

    if not faults:
        return passed(STORAGE_SHAPE, started)

    return Check(
        concern=CONCERN,
        check=STORAGE_SHAPE,
        state=State.FAILED,
        seconds=time.monotonic() - started,
        code=CODE_SHAPE,
        title="The storage the instance declares is not usable as it stands",
        facts=faults,
        help=[
            Help(
                text="An instance keeps its tables, the code of its functions and the "
                "environments it builds in three mounts, which may share a bucket "
                "under different prefixes but must not overlap"
            )
        ],
    )


def assess_storage_credentials(target: Target) -> Check:
    started = time.monotonic()
    mounts = mounts_of(target.instance)
    if mounts is None:
        return unknown(STORAGE_CREDENTIALS, started, target.instance)

    faults = []
    for mount in mounts:
        if not mount.uri:
            continue
        wanted = SCHEME_OPTIONS.get(mount.scheme)
        if wanted is None:
            continue
        for option in wanted:
            if not mount.options.get(option):
                faults.append(f"{mount.identifier} has no {option}")
        for surplus in sorted(set(mount.options) - set(wanted)):
            faults.append(
                f"{mount.identifier} carries {surplus}, which {mount.scheme}:// ignores"
            )

    if not faults:
        return passed(STORAGE_CREDENTIALS, started)

    return Check(
        concern=CONCERN,
        check=STORAGE_CREDENTIALS,
        state=State.FAILED,
        seconds=time.monotonic() - started,
        code=CODE_CREDENTIALS,
        title="The credentials of a mount do not match the storage it names",
        facts=faults,
        help=[
            Help(
                text="Each scheme takes its own options, and one left over from "
                "another provider is ignored rather than reported"
            )
        ],
    )


def reached(mount: Mount) -> tuple[bool, str]:
    if mount.scheme == "s3":
        import boto3

        client = boto3.client(
            "s3",
            region_name=mount.options.get("aws_region") or None,
            aws_access_key_id=mount.options.get("aws_access_key_id"),
            aws_secret_access_key=mount.options.get("aws_secret_access_key"),
        )
        client.head_bucket(Bucket=mount.container)
        return True, ""

    if mount.scheme == "az":
        from azure.storage.blob import BlobServiceClient

        account = mount.options.get("azure_storage_account_name")
        service = BlobServiceClient(
            account_url=f"https://{account}.blob.core.windows.net",
            credential=mount.options.get("azure_storage_account_key"),
        )
        service.get_container_client(mount.container).get_container_properties()
        return True, ""

    if mount.scheme == "gs":
        import json

        from google.cloud import storage
        from google.oauth2 import service_account

        key = json.loads(mount.options.get("google_service_account_key") or "{}")
        credentials = service_account.Credentials.from_service_account_info(key)
        client = storage.Client(credentials=credentials, project=key.get("project_id"))
        client.get_bucket(mount.container)
        return True, ""

    return True, ""


def assess_storage_reach(target: Target) -> Check:
    started = time.monotonic()
    mounts = mounts_of(target.instance)
    if mounts is None:
        return unknown(STORAGE_REACH, started, target.instance)

    ready = [
        mount
        for mount in mounts
        if mount.uri and mount.scheme in SCHEME_OPTIONS and all(mount.options.values())
    ]
    if not ready:
        return Check(
            concern=CONCERN,
            check=STORAGE_REACH,
            state=State.SKIPPED,
            seconds=time.monotonic() - started,
            reason="no mount carries the credentials this check would use",
        )

    seen: set[tuple[str, str]] = set()
    refused: list[str] = []
    unreachable: list[str] = []
    for mount in ready:
        key = (mount.scheme, mount.container)
        if key in seen:
            continue
        seen.add(key)
        try:
            reached(mount)
        except Exception as error:
            said = str(error).strip().splitlines()[0]
            if any(
                word in said.lower()
                for word in ("forbidden", "denied", "authoriz", "credential", "403")
            ):
                refused.append(f"{mount.container}: {said}")
            else:
                unreachable.append(f"{mount.container}: {said}")

    if not refused and not unreachable:
        return passed(STORAGE_REACH, started)

    layer = Layer.AUTHENTICATE if refused else Layer.CONNECT
    return Check(
        concern=CONCERN,
        check=STORAGE_REACH,
        state=state_for(Tier.REMOTE, layer, False),
        seconds=time.monotonic() - started,
        code=CODE_REACH,
        title=(
            "The storage refused these credentials"
            if refused
            else "The storage could not be reached from this workstation"
        ),
        facts=refused + unreachable,
        help=(
            []
            if refused
            else [
                Help(
                    text="The pods of the instance reach the storage from inside the "
                    "cloud, so this may be a route your workstation does not have"
                )
            ]
        ),
    )
