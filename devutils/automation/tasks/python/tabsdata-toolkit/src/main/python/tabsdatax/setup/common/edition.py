#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
from pathlib import Path

import yaml

ENTERPRISE_FEATURE = "enterprise"

OS_CONNECTORS_FOLDER = os.path.join("connectors", "python")
EE_CONNECTORS_FOLDER = os.path.join("connectors.ee", "python")


def is_enterprise(root: str | os.PathLike[str]) -> bool:
    feature_yaml_path = Path(root) / ".manifest" / "feature.yaml"
    if not feature_yaml_path.exists():
        return False
    # noinspection PyBroadException
    try:
        with open(feature_yaml_path, "r", encoding="utf-8") as f:
            features = yaml.safe_load(f)
    except Exception:
        return False
    return isinstance(features, list) and ENTERPRISE_FEATURE in features


def os_connectors_folder(root: str | os.PathLike[str]) -> str:
    return os.path.join(root, OS_CONNECTORS_FOLDER)


def ee_connectors_folder(root: str | os.PathLike[str]) -> str:
    return os.path.join(root, EE_CONNECTORS_FOLDER)


def edition_connectors_folders(root: str | os.PathLike[str]) -> list[str]:
    if is_enterprise(root):
        return [os_connectors_folder(root), ee_connectors_folder(root)]
    return [os_connectors_folder(root)]
