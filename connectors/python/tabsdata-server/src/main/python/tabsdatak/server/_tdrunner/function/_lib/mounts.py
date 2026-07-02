#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from pathlib import Path

import yaml

from tabsdatak.server._tdrunner.function._lib.errors import CommandErrorCode

STORAGE_KEY = "storage"
MOUNTS_KEY = "mounts"
MOUNT_ID_KEY = "id"
MOUNT_OPTIONS_KEY = "options"

SECRET_ENV_PREFIX = "SECRETS__STORAGE__MOUNTS__"
SECRET_ENV_OPTIONS_INFIX = "__OPTIONS__"


def load_mount_option_keys(storage_config_file: Path) -> dict[str, list[str]]:
    """Read the staged storage config and return ``{mount_id: [option_key, ...]}``."""
    try:
        document = yaml.safe_load(storage_config_file.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise CommandErrorCode.CMD_3.exception(
            cause=exc, path=storage_config_file, detail=str(exc)
        )
    mounts = (document or {}).get(STORAGE_KEY, {}).get(MOUNTS_KEY, []) or []
    option_keys: dict[str, list[str]] = {}
    for mount in mounts:
        mount_id = mount.get(MOUNT_ID_KEY)
        if mount_id is None:
            continue
        options = mount.get(MOUNT_OPTIONS_KEY) or {}
        option_keys[mount_id] = list(options.keys())
    return option_keys


def secret_env_name(mount_id: str, option_key: str) -> str:
    return (
        f"{SECRET_ENV_PREFIX}{mount_id}{SECRET_ENV_OPTIONS_INFIX}{option_key.upper()}"
    )
