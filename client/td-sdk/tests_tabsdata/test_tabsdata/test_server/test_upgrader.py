import unittest
from pathlib import Path

from packaging.version import Version

from tabsdata.tabsserver.server.entity import Upgrade
from tabsdata.tabsserver.server.upgrader import upgrade


# noinspection PyPep8Naming
class Upgrade_0_9_0_to_0_9_1(Upgrade):
    source_version = Version("0.9.0")
    target_version = Version("0.9.1")

    def upgrade(
        self,
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        return ["action1"]


# noinspection PyPep8Naming
class Upgrade_0_9_1_to_0_9_2(Upgrade):
    source_version = Version("0.9.1")
    target_version = Version("0.9.2")

    def upgrade(
        self,
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        return ["action2"]


# noinspection PyPep8Naming
class Upgrade_0_9_2_to_0_9_3(Upgrade):
    source_version = Version("0.9.2")
    target_version = Version("0.9.3")

    def upgrade(
        self,
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        return ["action3"]


class TestUpgradeFunction(unittest.TestCase):

    def setUp(self):
        self.instance = Path("/fake/path")
        self.upgrade_plan = {
            Version("0.9.0"): Upgrade_0_9_0_to_0_9_1,
            Version("0.9.1"): Upgrade_0_9_1_to_0_9_2,
            Version("0.9.2"): Upgrade_0_9_2_to_0_9_3,
        }

    def test_upgrade_no_upgrade_needed(self):
        source_version = Version("0.9.3")
        target_version = Version("0.9.3")
        actions = upgrade(
            self.instance,
            source_version,
            target_version,
            self.upgrade_plan,
            True,
        )
        self.assertEqual(actions, {})

    def test_upgrade_successful(self):
        source_version = Version("0.9.0")
        target_version = Version("0.9.3")
        actions = upgrade(
            self.instance,
            source_version,
            target_version,
            self.upgrade_plan,
            True,
        )
        expected_actions = {
            Version("0.9.1"): ["action1"],
            Version("0.9.2"): ["action2"],
            Version("0.9.3"): ["action3"],
        }
        self.assertEqual(actions, expected_actions)

    def test_upgrade_no_upgrade_class_found(self):
        source_version = Version("0.9.0")
        target_version = Version("0.9.3")
        incomplete_upgrade_plan = {
            Version("0.9.0"): Upgrade_0_9_0_to_0_9_1,
        }
        with self.assertRaises(RuntimeError) as context:
            upgrade(
                self.instance,
                source_version,
                target_version,
                incomplete_upgrade_plan,
                True,
            )
        self.assertIn("No upgrade class found for", str(context.exception))

    def test_upgrade_loop_detected(self):
        # noinspection PyPep8Naming
        class Upgrade_0_9_2_to_0_9_1(Upgrade):
            source_version = Version("0.9.2")
            target_version = Version("0.9.1")

            def upgrade(
                self,
                instance: Path,
                dry_run: bool,
            ) -> list[str]:
                return ["action_loop"]

        source_version = Version("0.9.0")
        target_version = Version("0.9.3")
        loop_upgrade_plan = {
            Version("0.9.0"): Upgrade_0_9_0_to_0_9_1,
            Version("0.9.1"): Upgrade_0_9_1_to_0_9_2,
            Version("0.9.2"): Upgrade_0_9_2_to_0_9_1,
        }
        with self.assertRaises(RuntimeError) as context:
            upgrade(
                self.instance,
                source_version,
                target_version,
                loop_upgrade_plan,
                True,
            )
        self.assertIn("Loop detected in upgrade plan", str(context.exception))

    def test_upgrade_missed_versions(self):
        source_version = Version("0.9.0")
        target_version = Version("0.9.3")
        missed_upgrade_plan = {
            Version("0.9.0"): Upgrade_0_9_0_to_0_9_1,
            Version("0.9.2"): Upgrade_0_9_2_to_0_9_3,
        }
        with self.assertRaises(RuntimeError) as context:
            upgrade(
                self.instance,
                source_version,
                target_version,
                missed_upgrade_plan,
                True,
            )
        self.assertIn("Some versions cannot be reached", str(context.exception))
