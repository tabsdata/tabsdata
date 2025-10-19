#
# Copyright 2025 Tabs Data Inc.
#

import copy
import logging
import shutil
from pathlib import Path
from typing import Any, Optional

from packaging.version import Version
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

from tabsdata._tabsserver.server.upgrader.entity import Upgrade
from tabsdata._utils.internal._resources import td_resource

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _create_yaml() -> YAML:
    yaml = YAML(typ="rt")
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.width = 4096
    return yaml


def _load_yaml(path: Path) -> Any:
    yaml = _create_yaml()
    with path.open("r", encoding="utf-8") as stream:
        data = yaml.load(stream)
    if data is None:
        return CommentedMap()
    return data


def _dump_yaml(path: Path, data: Any) -> None:
    yaml = _create_yaml()
    with path.open("w", encoding="utf-8") as stream:
        yaml.dump(data, stream)


def _normalize_comment_value(value: str) -> tuple[Optional[str], Optional[int]]:
    lines = value.splitlines()
    normalized_lines: list[str] = []
    indent: Optional[int] = None

    for line in lines:
        if not line.strip():
            normalized_lines.append("")
            continue

        hash_index = line.find("#")
        if hash_index == -1:
            continue

        if indent is None or hash_index < indent:
            indent = hash_index

        content = line[hash_index + 1 :]
        if content.startswith(" "):
            content = content[1:]
        normalized_lines.append(content)

    if not normalized_lines:
        return None, indent

    comment_text = "\n".join(normalized_lines)
    return comment_text, indent


def _extract_trailing_sequence_comment(value: str) -> Optional[str]:
    if not value:
        return None

    if value.startswith("\n"):
        trimmed = value.lstrip("\n")
        return trimmed if trimmed.strip() else None

    newline_index = value.find("\n#")
    if newline_index == -1:
        return None

    trailing = value[newline_index + 1 :]
    return trailing if trailing.strip() else None


# noinspection DuplicatedCode
def _extract_sequence_comment(
    sequence: CommentedSeq, index: int
) -> tuple[Optional[str], Optional[int]]:
    if hasattr(sequence, "ca"):
        item_meta = sequence.ca.items.get(index)
        if item_meta and item_meta[0] is not None:
            comment_text, indent = _normalize_comment_value(item_meta[0].value)
            if comment_text:
                return comment_text, indent

        if index > 0:
            prev_meta = sequence.ca.items.get(index - 1)
            if prev_meta and prev_meta[0] is not None:
                trailing = _extract_trailing_sequence_comment(prev_meta[0].value)
                if trailing:
                    comment_text, indent = _normalize_comment_value(trailing)
                    if comment_text:
                        return comment_text, indent

        if index == 0 and getattr(sequence.ca, "comment", None):
            comment_tokens = sequence.ca.comment[1] or []
            combined = "".join(token.value for token in comment_tokens)
            comment_text, indent = _normalize_comment_value(combined)
            if comment_text:
                return comment_text, indent

    return None, None


# noinspection DuplicatedCode
def _extract_map_comment(
    mapping: CommentedMap, key: Any
) -> tuple[Optional[str], Optional[int]]:
    if not hasattr(mapping, "ca"):
        return None, None

    key_meta = mapping.ca.items.get(key)
    if key_meta and key_meta[0] is not None:
        comment_text, indent = _normalize_comment_value(key_meta[0].value)
        if comment_text:
            return comment_text, indent

    keys = list(mapping.keys())
    try:
        key_index = keys.index(key)
    except ValueError:
        key_index = None

    if key_index == 0 and getattr(mapping.ca, "comment", None):
        comment_tokens = mapping.ca.comment[1] or []
        combined = "".join(token.value for token in comment_tokens)
        comment_text, indent = _normalize_comment_value(combined)
        if comment_text:
            return comment_text, indent

    if key_index and key_index > 0:
        prev_key = keys[key_index - 1]
        prev_meta = mapping.ca.items.get(prev_key)
        if prev_meta and prev_meta[2] is not None:
            comment_text, indent = _normalize_comment_value(prev_meta[2].value)
            if comment_text:
                return comment_text, indent

    return None, None


# noinspection PyPep8Naming
class Upgrade_1_3_0_to_1_4_0(Upgrade):
    source_version = Version("1.3.0")
    target_version = Version("1.4.0")

    # noinspection DuplicatedCode
    def upgrade(
        self,
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        messages: list[str] = []

        messages.extend(self.upgrade_instance(instance, dry_run))
        messages.extend(self.upgrade_instance_apiserver(instance, dry_run))

        return messages

    @staticmethod
    def upgrade_instance(  # noqa: C901
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        def find_worker_index(workers: list[Any], name: str) -> Optional[int]:
            for f_worker_index, f_worker in enumerate(workers):
                if isinstance(f_worker, dict) and f_worker.get("name") == name:
                    return f_worker_index
            return None

        messages: list[str] = []

        instance_config_path = instance / "workspace" / "config" / "config.yaml"
        if not instance_config_path.exists():
            logger.warning(
                f"Instance config file '{instance_config_path}' not found; "
                "skipping upgrade."
            )
            return messages

        instance_config_loaded = _load_yaml(instance_config_path)
        if not isinstance(instance_config_loaded, dict):
            logger.warning(
                "Instance config has no 'controllers' section; skipping upgrade."
            )
            return messages

        instance_config = (
            copy.deepcopy(instance_config_loaded) if dry_run else instance_config_loaded
        )

        instance_controllers_section = instance_config.get("controllers")
        if not isinstance(instance_controllers_section, dict):
            logger.warning(
                "Instance config has no 'controllers' section; skipping upgrade."
            )
            return messages

        instance_regular_section = instance_controllers_section.get("regular")
        if not isinstance(instance_regular_section, dict):
            logger.warning(
                "Instance config has no "
                "'controllers/regular' "
                "section; skipping upgrade."
            )
            return messages

        instance_workers_section = instance_regular_section.get("workers")
        if not isinstance(instance_workers_section, list):
            logger.warning(
                "Instance config has no "
                "'controllers/regular/workers' "
                "section; skipping upgrade."
            )
            return messages

        resources_config_path = td_resource(
            "resources/profile/workspace/config/config.yaml"
        )

        resources_config = _load_yaml(resources_config_path)

        resources_controllers_section = resources_config.get("controllers")
        if not isinstance(resources_controllers_section, dict):
            logger.warning(
                "Resources config has no 'controllers' section; skipping upgrade."
            )
            return messages

        resources_regular = resources_controllers_section.get("regular")
        if not isinstance(resources_regular, dict):
            logger.warning(
                "Resources config has no "
                "'controllers/regular' "
                "section; skipping upgrade."
            )
            return messages

        resources_workers = resources_regular.get("workers")
        if not isinstance(resources_workers, list):
            logger.warning(
                "Resources config has no "
                "'controllers/regular/workers' "
                "section; skipping upgrade."
            )
            return messages

        new_resources_workers: list[str] = []

        for resource_index, resource_worker in enumerate(resources_workers):
            if not isinstance(resource_worker, dict):
                continue
            worker_name = resource_worker.get("name")
            if not isinstance(worker_name, str):
                continue
            if find_worker_index(instance_workers_section, worker_name) is not None:
                continue
            insert_position = len(instance_workers_section)
            for next_resource_worker in resources_workers[resource_index + 1 :]:
                if not isinstance(next_resource_worker, dict):
                    continue
                next_name = next_resource_worker.get("name")
                if not isinstance(next_name, str):
                    continue
                existing_index = find_worker_index(instance_workers_section, next_name)
                if existing_index is not None:
                    insert_position = existing_index
                    break

            worker_copy = copy.deepcopy(resource_worker)
            if insert_position == len(instance_workers_section):
                instance_workers_section.append(worker_copy)
            else:
                instance_workers_section.insert(insert_position, worker_copy)

            comment_text, indent = _extract_sequence_comment(
                resources_workers, resource_index
            )
            if comment_text and hasattr(
                instance_workers_section, "yaml_set_comment_before_after_key"
            ):
                if indent is not None:
                    instance_workers_section.yaml_set_comment_before_after_key(
                        insert_position,
                        before=comment_text,
                        indent=indent,
                    )
                else:
                    instance_workers_section.yaml_set_comment_before_after_key(
                        insert_position,
                        before=comment_text,
                    )

            new_resources_workers.append(worker_name)

        if not new_resources_workers:
            logger.info(
                "No new workers to add to the "
                "'controllers.regular' "
                "section; skipping upgrade."
            )
            return messages

        message = (
            "Added missing worker(s) "
            f"{', '.join(new_resources_workers)} to "
            "controllers.regular.workers "
            f"section in '{instance_config_path}'."
        )

        if dry_run:
            messages.append(f"[dry-run] {message}")
            return messages

        _dump_yaml(instance_config_path, instance_config)

        for worker in new_resources_workers:
            source_worker_config_folder = td_resource(
                f"resources/profile/workspace/config/proc/regular/{worker}/"
            )
            target_worker_config_folder = (
                instance / "workspace" / "config" / "proc" / "regular" / worker
            )
            shutil.copytree(
                source_worker_config_folder,
                target_worker_config_folder,
                dirs_exist_ok=True,
            )

            source_worker_work_folder = td_resource(
                f"resources/profile/workspace/work/proc/regular/{worker}/"
            )
            target_worker_work_folder = (
                instance / "workspace" / "work" / "proc" / "regular" / worker
            )
            shutil.copytree(
                source_worker_work_folder,
                target_worker_work_folder,
                dirs_exist_ok=True,
            )

        return messages

    @staticmethod
    def upgrade_instance_apiserver(
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        messages: list[str] = []

        instance_apiserver_config_path = (
            instance
            / "workspace"
            / "config"
            / "proc"
            / "regular"
            / "apiserver"
            / "config"
            / "config.yaml"
        )
        if not instance_apiserver_config_path.exists():
            logger.warning(
                "Instance apiserver config file "
                f"'{instance_apiserver_config_path}' not found; "
                "skipping apiserver upgrade."
            )
            return messages

        resources_apiserver_config_path = td_resource(
            "resources/"
            "profile/"
            "workspace/"
            "config/"
            "proc/"
            "regular/"
            "apiserver/"
            "config/"
            "config.yaml"
        )

        instance_apiserver_loaded = _load_yaml(instance_apiserver_config_path)
        if not isinstance(instance_apiserver_loaded, dict):
            logger.warning(
                "Instance apiserver config has unexpected structure; "
                "skipping apiserver upgrade."
            )
            return messages

        instance_apiserver_config = (
            copy.deepcopy(instance_apiserver_loaded)
            if dry_run
            else instance_apiserver_loaded
        )
        if not isinstance(instance_apiserver_config, CommentedMap):
            instance_apiserver_config = CommentedMap(instance_apiserver_config)

        resources_apiserver_config = _load_yaml(resources_apiserver_config_path)
        if not isinstance(resources_apiserver_config, dict):
            logger.warning(
                "Resources apiserver config has unexpected structure; "
                "skipping apiserver upgrade."
            )
            return messages
        if not isinstance(resources_apiserver_config, CommentedMap):
            resources_apiserver_config = CommentedMap(resources_apiserver_config)

        new_apiserver_config: list[str] = []
        resource_items = list(resources_apiserver_config.items())
        instance_keys = list(instance_apiserver_config.keys())

        for resource_index, (key, value) in enumerate(resource_items):
            if key in instance_apiserver_config:
                continue

            next_existing_key: Optional[str] = None
            for next_key, _ in resource_items[resource_index + 1 :]:
                if next_key in instance_apiserver_config:
                    next_existing_key = next_key
                    break

            value_copy = copy.deepcopy(value)
            if next_existing_key is not None and hasattr(
                instance_apiserver_config, "insert"
            ):
                insert_index = instance_keys.index(next_existing_key)
                instance_apiserver_config.insert(insert_index, key, value_copy)
                instance_keys.insert(insert_index, key)
            else:
                instance_apiserver_config[key] = value_copy
                instance_keys.append(key)

            comment_text, indent = _extract_map_comment(resources_apiserver_config, key)
            if comment_text and hasattr(
                instance_apiserver_config, "yaml_set_comment_before_after_key"
            ):
                if indent is not None:
                    instance_apiserver_config.yaml_set_comment_before_after_key(
                        key,
                        before=comment_text,
                        indent=indent,
                    )
                else:
                    instance_apiserver_config.yaml_set_comment_before_after_key(
                        key,
                        before=comment_text,
                    )

            new_apiserver_config.append(key)

        if not new_apiserver_config:
            logger.info(
                "No missing apiserver config nodes detected; skipping apiserver"
                " upgrade."
            )
            return messages
        apiserver_message = (
            "Added missing apiserver config node(s) "
            f"{', '.join(new_apiserver_config)} "
            f"in '{instance_apiserver_config_path}'."
        )

        if dry_run:
            messages.append(f"[dry-run] {apiserver_message}")
            return messages

        _dump_yaml(instance_apiserver_config_path, instance_apiserver_config)

        return messages
