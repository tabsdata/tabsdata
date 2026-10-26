#
# Copyright 2026 Tabsdata Inc.
#

import json
import os
import shlex
import subprocess
from pathlib import Path

import rich_click as click

from tabsdatax.itest.tdkinstance.commands.stubs import check_variables

FILE_STORAGE = "file"
STORAGE_SYSTEMS = {
    "awsc": "s3",
    "gcpc": "gcs",
    "azrc": "azureblob",
    "awse": "s3local",
    "gcpe": "gcslocal",
    "azre": "azurebloblocal",
}
STORAGE_IDS = (FILE_STORAGE, *STORAGE_SYSTEMS)
DOMAIN_PREFIX = "instances_"
STUBS_FOLDER = Path("stubs") / "storages"
STUB_SUFFIX = ".yaml"

ENV_PREFIX = "TD_STORAGE"
ENV_SEPARATOR = "__"
ENV_URI = f"{ENV_PREFIX}{ENV_SEPARATOR}URI"
ENV_OPTIONS = f"{ENV_PREFIX}{ENV_SEPARATOR}STORAGE_OPTIONS"
ENV_OPTION_PREFIX = f"{ENV_PREFIX}{ENV_SEPARATOR}OPTIONS{ENV_SEPARATOR}"
ENV_INSTANCE = f"{ENV_PREFIX}{ENV_SEPARATOR}INSTANCE"
EXPORT_PREFIX = "export "

STORAGE_OPTION = click.option(
    "--storage",
    type=click.Choice(STORAGE_IDS),
    default=FILE_STORAGE,
    show_default=True,
    help="Storage the instance keeps its data in.",
)


def is_cloud(storage: str) -> bool:
    return storage != FILE_STORAGE


def domain_of(storage: str) -> str:
    return f"{DOMAIN_PREFIX}{storage}"


def storage_stub(config: Path, storage: str) -> Path | None:
    if is_cloud(storage):
        stub = (
            config.resolve().parent / STUBS_FOLDER / f"{storage}{STUB_SUFFIX}"
        ).resolve()
    else:
        stub = None
    return stub


def parse_exports(text: str) -> dict[str, str]:
    wanted = {ENV_URI, ENV_OPTIONS}
    found = {}
    for line in text.splitlines():
        line = line.strip()
        if line.startswith(EXPORT_PREFIX):
            name, separator, quoted = line[len(EXPORT_PREFIX) :].partition("=")
            if separator and name in wanted:
                parts = shlex.split(quoted)
                found[name] = parts[0] if parts else ""
    return found


def storage_environment(exports: dict[str, str], instance: str) -> dict[str, str]:
    missing = sorted({ENV_URI, ENV_OPTIONS} - exports.keys())
    if missing:
        raise click.ClickException(
            f"The storage domain does not report {', '.join(missing)}."
        )
    try:
        options = json.loads(exports[ENV_OPTIONS])
    except json.JSONDecodeError as error:
        raise click.ClickException(
            f"The storage domain reports {ENV_OPTIONS} that is not JSON: {error.msg}."
        )
    environment = {
        ENV_URI: exports[ENV_URI].rstrip("/"),
        ENV_INSTANCE: instance,
    }
    for key, value in options.items():
        environment[f"{ENV_OPTION_PREFIX}{key.upper()}"] = str(value)
    return environment


def run_tdd(tdd: Path, arguments: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(tdd), *arguments], capture_output=True, text=True, check=False
    )


@click.group(name="storage")
def storage():
    """Keep test instances' storage in a cloud store or an emulator."""


@storage.command(name="create")
@STORAGE_OPTION
@click.option("--tdd", type=click.Path(path_type=Path), required=True)
def create(storage: str, tdd: Path):
    """Create, afresh, the tdd domain holding the instances of a storage."""
    if is_cloud(storage):
        domain = domain_of(storage)
        completed = run_tdd(
            tdd,
            [
                "domain",
                "create",
                "--sys",
                STORAGE_SYSTEMS[storage],
                "--domain",
                domain,
                "--force",
            ],
        )
        if completed.returncode != 0:
            raise click.ClickException(
                f"Could not create the storage domain '{domain}' "
                f"(exit code {completed.returncode}).\n{completed.stderr}"
            )


@storage.command(name="exec", context_settings={"ignore_unknown_options": True})
@click.option("--config", type=click.Path(path_type=Path), required=True)
@STORAGE_OPTION
@click.option("--instance", required=True)
@click.option("--tdd", type=click.Path(path_type=Path), required=True)
@click.argument("command", nargs=-1, required=True, type=click.UNPROCESSED)
def execute(config: Path, storage: str, instance: str, tdd: Path, command):
    """Run a command with the environment the storage stub reads."""
    environment = dict(os.environ)
    if is_cloud(storage):
        domain = domain_of(storage)
        completed = run_tdd(
            tdd,
            [
                "domain",
                "info",
                "--domain",
                domain,
                "--format",
                "export",
                "--prefix",
                ENV_PREFIX,
            ],
        )
        if completed.returncode != 0:
            raise click.ClickException(
                f"Could not read the storage domain '{domain}' "
                f"(exit code {completed.returncode})."
            )
        environment.update(
            storage_environment(parse_exports(completed.stdout), instance)
        )
        check_variables(storage_stub(config, storage), storage, environment)
    raise SystemExit(subprocess.run(list(command), env=environment).returncode)
