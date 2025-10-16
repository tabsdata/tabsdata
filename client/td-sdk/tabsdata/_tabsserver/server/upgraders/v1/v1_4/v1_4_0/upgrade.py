#
# Copyright 2025 Tabs Data Inc.
#

import copy
import logging
from pathlib import Path
from typing import Any, Optional

import yaml
from packaging.version import Version

from tabsdata._tabsserver.server.entity import Upgrade
from tabsdata._utils.internal._resources import td_resource

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# noinspection PyPep8Naming
class Upgrade_1_3_0_to_1_4_0(Upgrade):
    source_version = Version("1.3.0")
    target_version = Version("1.4.0")

    # noinspection DuplicatedCode
    def upgrade(  # noqa: C901
        self,
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

        with instance_config_path.open("r", encoding="utf-8") as instance_config_file:
            instance_config = yaml.safe_load(instance_config_file) or {}

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

        instance_workers = copy.deepcopy(instance_workers_section)
        instance_workers_indexes: dict[str, int] = {}
        for worker_index, worker in enumerate(instance_workers):
            if isinstance(worker, dict):
                worker_name = worker.get("name")
                if (
                    isinstance(worker_name, str)
                    and worker_name not in instance_workers_indexes
                ):
                    instance_workers_indexes[worker_name] = worker_index

        resources_config_path = td_resource(
            "resources/profile/workspace/config/config.yaml"
        )

        with resources_config_path.open("r", encoding="utf-8") as resources_config_file:
            resources_config = yaml.safe_load(resources_config_file) or {}

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

        all_resources_workers: list[str] = []
        new_resources_workers: list[str] = []

        for resource_worker in resources_workers:
            if not isinstance(resource_worker, dict):
                continue
            worker_name = resource_worker.get("name")
            if not isinstance(worker_name, str):
                continue
            all_resources_workers.append(worker_name)
            if find_worker_index(instance_workers_section, worker_name) is not None:
                continue
            instance_workers_section.append(copy.deepcopy(resource_worker))
            new_resources_workers.append(worker_name)

        if not new_resources_workers:
            logger.info(
                "No new workers to add to the "
                "'controllers.regular' "
                "section; skipping upgrade."
            )
            return messages

        resources_workers_indexes = {
            name: index for index, name in enumerate(all_resources_workers)
        }

        def sort_workers(worker_config: Any) -> tuple[int, int]:
            if isinstance(worker_config, dict):
                w_worker_name = worker_config.get("name")
                if isinstance(w_worker_name, str):
                    if w_worker_name in resources_workers_indexes:
                        return resources_workers_indexes[w_worker_name], 0
                    return (
                        len(resources_workers_indexes),
                        instance_workers_indexes.get(
                            w_worker_name, len(instance_workers)
                        ),
                    )
            return len(resources_workers_indexes), len(instance_workers)

        instance_workers_section.sort(key=sort_workers)

        message = (
            "Added missing worker(s) "
            f"{', '.join(new_resources_workers)} to "
            "controllers.regular.workers "
            f"section in '{instance_config_path}'."
        )

        if dry_run:
            instance_workers_section.clear()
            instance_workers_section.extend(instance_workers)
            messages.append(f"[dry-run] {message}")
            return messages

        with instance_config_path.open("w", encoding="utf-8") as instance_config_file:
            yaml.safe_dump(
                instance_config,
                instance_config_file,
                sort_keys=False,
                default_flow_style=False,
            )

        messages.append(message)
        return messages
