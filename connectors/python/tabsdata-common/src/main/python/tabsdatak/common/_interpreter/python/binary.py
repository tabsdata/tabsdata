#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sysconfig
import tomllib
from dataclasses import dataclass
from pathlib import Path

SETUP_TOML_FILE = "setup.toml"
SETUP_TOML_KEY_BINARY_GROUPS = "binary_groups"
SETUP_TOML_KEY_NAMES = "names"
SETUP_TOML_KEY_INSTALL_DEST = "install_dest"

SYSCONFIG_VAR_EXE = "EXE"
SYSCONFIG_PATH_SCRIPTS = "scripts"
SYSCONFIG_PATH_DATA = "data"

UTF_8 = "utf-8"


@dataclass(frozen=True)
class Binary:
    name: str
    prefix: str

    def link(self, source_root: Path, target_root: Path) -> None:
        source = source_root / self.prefix / self.exe()
        target = target_root / self.prefix / self.exe()
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() or target.is_symlink():
            target.unlink()
        target.symlink_to(source)

    def exe(self) -> str:
        # noinspection PyDeprecation
        return self.name + sysconfig.get_config_var(SYSCONFIG_VAR_EXE)


def binaries_from_setup_toml(project_root: Path) -> tuple[Binary, ...]:
    setup_toml = project_root / SETUP_TOML_FILE
    if not setup_toml.is_file():
        return tuple()
    data = tomllib.loads(setup_toml.read_text(encoding=UTF_8))
    default_prefix = default_scripts_prefix()
    binaries: list[Binary] = []
    for group in data.get(SETUP_TOML_KEY_BINARY_GROUPS, []):
        prefix = group.get(SETUP_TOML_KEY_INSTALL_DEST, default_prefix)
        for name in group.get(SETUP_TOML_KEY_NAMES, []):
            binaries.append(Binary(name=name, prefix=prefix))
    return tuple(binaries)


def default_scripts_prefix() -> str:
    scripts = Path(sysconfig.get_path(SYSCONFIG_PATH_SCRIPTS))
    data = Path(sysconfig.get_path(SYSCONFIG_PATH_DATA))
    return str(scripts.relative_to(data))


def current_scripts_dir() -> Path:
    return Path(sysconfig.get_path(SYSCONFIG_PATH_SCRIPTS))
