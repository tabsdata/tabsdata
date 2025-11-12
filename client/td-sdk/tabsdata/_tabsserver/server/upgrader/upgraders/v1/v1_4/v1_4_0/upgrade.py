#
# Copyright 2025 Tabs Data Inc.
#

import logging
import shutil
from pathlib import Path

from packaging.version import Version
from ruamel.yaml import YAML

from tabsdata._tabsserver.server.upgrader.editors.yaml.yaml_e import YamlEditor
from tabsdata._tabsserver.server.upgrader.entity import Upgrade
from tabsdata._utils.internal._resources import td_resource

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# noinspection PyPep8Naming,DuplicatedCode
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
        messages.extend(self.upgrade_apiserver(instance, dry_run))
        messages.extend(self.upgrade_janitor(instance, dry_run))

        return messages

    @staticmethod
    def upgrade_instance(  # noqa: C901
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        messages: list[str] = []

        instance_config_path = instance / "workspace" / "config" / "config.yaml"
        if not instance_config_path.exists():
            logger.warning(
                f"Instance config file '{instance_config_path}' not found; "
                "skipping upgrade."
            )
            return messages

        instance_config_upgrade_spec_path = td_resource(
            "resources/upgrade/v1/v1_4/v1_4_0/upgrade.yaml"
        )

        instance_config_upgrade_spec_yaml = YAML()
        instance_config_upgrade_spec_yaml.preserve_quotes = True
        with open(instance_config_upgrade_spec_path, "r", encoding="utf-8") as f:
            upgrade_spec = instance_config_upgrade_spec_yaml.load(f)
        instance_config_upgrade_spec = (
            upgrade_spec.get("instance", []) if isinstance(upgrade_spec, dict) else []
        )

        instance_config_upgrade_editor = YamlEditor(
            spec=instance_config_upgrade_spec,
            source=instance_config_path,
        )

        if dry_run:
            instance_config_content_upgraded = instance_config_upgrade_editor.apply(
                dry_run=True
            )
            with open(instance_config_path, "r", encoding="utf-8") as f:
                instance_config_content = f.read()
            if (
                instance_config_content_upgraded
                and instance_config_content_upgraded.strip()
                != instance_config_content.strip()
            ):
                messages.append(
                    f"[dry-run] Would upgrade '{instance_config_path}' "
                    "using upgrade spec."
                )
        else:
            instance_config_upgrade_editor.apply(dry_run=False)
            messages.append(f"Upgraded '{instance_config_path}' using upgrade spec.")

            workers = []
            for spec_item in instance_config_upgrade_spec:
                if isinstance(spec_item, dict):
                    data = spec_item.get("data", {})
                    if isinstance(data, dict) and "name" in data:
                        worker_name = data.get("name")
                        if worker_name:
                            workers.append(worker_name)

            for worker in workers:
                logger.debug(f"Provisioning config folder for worker '{worker}'.")
                target_worker_config_folder = (
                    instance / "workspace" / "config" / "proc" / "regular" / worker
                )
                if not target_worker_config_folder.exists():
                    source_worker_config_folder = None
                    # noinspection PyBroadException
                    try:
                        source_worker_config_folder = td_resource(
                            f"resources/profile/workspace/config/proc/regular/{worker}/"
                        )
                    except Exception:
                        logger.debug(
                            "No workspace config folder in resources for worker "
                            f"'{worker}'; "
                            "skipping folder provisioning."
                        )
                    if source_worker_config_folder is not None:
                        logger.debug(
                            "Copying from "
                            f"'{source_worker_config_folder}' "
                            "to "
                            f"'{target_worker_config_folder}'."
                        )
                        shutil.copytree(
                            source_worker_config_folder,
                            target_worker_config_folder,
                            dirs_exist_ok=True,
                            ignore=shutil.ignore_patterns(".gitkeep"),
                        )

                logger.debug(f"Provisioning work folder for worker '{worker}'.")
                target_worker_work_folder = (
                    instance / "workspace" / "work" / "proc" / "regular" / worker
                )
                if not target_worker_work_folder.exists():
                    source_worker_work_folder = None
                    # noinspection PyBroadException
                    try:
                        source_worker_work_folder = td_resource(
                            f"resources/profile/workspace/work/proc/regular/{worker}/"
                        )
                    except Exception:
                        logger.debug(
                            "No workspace work folder in resources for worker "
                            f"'{worker}'; "
                            "skipping folder provisioning."
                        )
                    if source_worker_work_folder is not None:
                        logger.debug(
                            "Copying from "
                            f"'{source_worker_work_folder}' "
                            "to "
                            f"'{target_worker_work_folder}'..."
                        )
                        shutil.copytree(
                            source_worker_work_folder,
                            target_worker_work_folder,
                            dirs_exist_ok=True,
                            ignore=shutil.ignore_patterns(".gitkeep"),
                        )

        return messages

    @staticmethod
    def upgrade_apiserver(
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        messages: list[str] = []

        apiserver_config_path = (
            instance
            / "workspace"
            / "config"
            / "proc"
            / "regular"
            / "apiserver"
            / "config"
            / "config.yaml"
        )
        if not apiserver_config_path.exists():
            logger.warning(
                "Instance apiserver config file "
                f"'{apiserver_config_path}' not found; "
                "skipping apiserver upgrade."
            )
            return messages

        apiserver_config_spec_path = td_resource(
            "resources/upgrade/v1/v1_4/v1_4_0/upgrade.yaml"
        )

        apiserver_config_upgrade_spec_yaml = YAML()
        apiserver_config_upgrade_spec_yaml.preserve_quotes = True
        with open(apiserver_config_spec_path, "r", encoding="utf-8") as f:
            upgrade_spec = apiserver_config_upgrade_spec_yaml.load(f)
        apiserver_config_upgrade_spec = (
            upgrade_spec.get("apiserver", []) if isinstance(upgrade_spec, dict) else []
        )

        apiserver_config_upgrade_editor = YamlEditor(
            spec=apiserver_config_upgrade_spec,
            source=apiserver_config_path,
        )

        if dry_run:
            apiserver_config_content_upgraded = apiserver_config_upgrade_editor.apply(
                dry_run=True
            )
            with open(apiserver_config_path, "r", encoding="utf-8") as f:
                apiserver_config_content = f.read()
            if (
                apiserver_config_content_upgraded
                and apiserver_config_content_upgraded.strip()
                != apiserver_config_content.strip()
            ):
                messages.append(
                    f"[dry-run] Would upgrade '{apiserver_config_path}' "
                    "using upgrade spec."
                )
        else:
            apiserver_config_upgrade_editor.apply(dry_run=False)
            messages.append(f"Upgraded '{apiserver_config_path}' using upgrade spec.")

        return messages

    @staticmethod
    def upgrade_janitor(  # noqa: C901
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        messages: list[str] = []

        if dry_run:
            messages.append("[dry-run] Would upgrade 'janitor' log.yaml configuration.")
        else:
            source_janitor_config_log_file = None
            # noinspection PyBroadException
            try:
                source_janitor_config_log_file = td_resource(
                    "resources/profile/workspace/config/proc/regular/"
                    "janitor/config/log.yaml"
                )
            except Exception:
                logger.debug(
                    "No workspace config file log.yaml for worker 'janitor'"
                    "skipping folder provisioning."
                )
            target_janitor_config_log_file = (
                instance
                / "workspace"
                / "config"
                / "proc"
                / "regular"
                / "janitor"
                / "config"
                / "log.yaml"
            )
            if source_janitor_config_log_file is not None:
                target_janitor_config_log_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(
                    source_janitor_config_log_file,
                    target_janitor_config_log_file,
                )

        return messages


if __name__ == "__main__":
    print(
        Upgrade_1_3_0_to_1_4_0().upgrade(
            Path("~/.tabsdata/instances/tabsdata").expanduser(),
            dry_run=True,
        )
    )
