#
#  Copyright 2025 Tabsdata Inc.
#

import unittest
from pathlib import Path

from packaging.version import Version

from tabsdata._tabsserver.server.upgrader.entity import Upgrade
from tabsdata._tabsserver.server.upgrader.upgrader import upgrade


# noinspection PyPep8Naming
class Upgrade_0_9_to_1_0(Upgrade):
    source_version = Version("0.9")
    target_version = Version("1.0")

    def upgrade(
        self,
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        return ["action1"]


# noinspection PyPep8Naming
class Upgrade_1_0_to_1_1(Upgrade):
    source_version = Version("1.0")
    target_version = Version("1.1")

    def upgrade(
        self,
        instance: Path,
        dry_run: bool,
    ) -> list[str]:
        return ["action2"]


# noinspection PyPep8Naming
class Upgrade_1_1_to_1_2(Upgrade):
    source_version = Version("1.1")
    target_version = Version("1.2")

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
            Version("0.9"): Upgrade_0_9_to_1_0,
            Version("1.0"): Upgrade_1_0_to_1_1,
            Version("1.1"): Upgrade_1_1_to_1_2,
        }

    def test_upgrade_no_upgrade_needed(self):
        source_version = Version("1.2.0")
        target_version = Version("1.2.0")
        actions = upgrade(
            self.instance,
            source_version,
            target_version,
            self.upgrade_plan,
            True,
        )
        self.assertEqual(actions, {})

    def test_upgrade_patch_only_no_upgrade(self):
        source_version = Version("1.0.0")
        target_version = Version("1.0.5")
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
        target_version = Version("1.2.0")
        actions = upgrade(
            self.instance,
            source_version,
            target_version,
            self.upgrade_plan,
            True,
        )
        expected_actions = {
            Version("1.0"): ["action1"],
            Version("1.1"): ["action2"],
            Version("1.2"): ["action3"],
        }
        self.assertEqual(actions, expected_actions)

    def test_upgrade_with_patch_versions(self):
        source_version = Version("0.9.3")
        target_version = Version("1.2.7")
        actions = upgrade(
            self.instance,
            source_version,
            target_version,
            self.upgrade_plan,
            True,
        )
        expected_actions = {
            Version("1.0"): ["action1"],
            Version("1.1"): ["action2"],
            Version("1.2"): ["action3"],
        }
        self.assertEqual(actions, expected_actions)

    def test_upgrade_partial_chain(self):
        source_version = Version("1.0.2")
        target_version = Version("1.2.0")
        actions = upgrade(
            self.instance,
            source_version,
            target_version,
            self.upgrade_plan,
            True,
        )
        expected_actions = {
            Version("1.1"): ["action2"],
            Version("1.2"): ["action3"],
        }
        self.assertEqual(actions, expected_actions)

    def test_upgrade_no_upgrade_class_found(self):
        source_version = Version("0.9.0")
        target_version = Version("1.2.0")
        incomplete_upgrade_plan = {
            Version("0.9"): Upgrade_0_9_to_1_0,
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
        class Upgrade_1_1_to_1_0(Upgrade):
            source_version = Version("1.1")
            target_version = Version("1.0")

            def upgrade(
                self,
                instance: Path,
                dry_run: bool,
            ) -> list[str]:
                return ["action_loop"]

        source_version = Version("0.9.0")
        target_version = Version("1.2.0")
        loop_upgrade_plan = {
            Version("0.9"): Upgrade_0_9_to_1_0,
            Version("1.0"): Upgrade_1_0_to_1_1,
            Version("1.1"): Upgrade_1_1_to_1_0,
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
        target_version = Version("1.2.0")
        missed_upgrade_plan = {
            Version("0.9"): Upgrade_0_9_to_1_0,
            Version("1.1"): Upgrade_1_1_to_1_2,
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
