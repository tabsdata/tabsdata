#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os

from ruamel.yaml import YAML

from tabsdatak.cli.cli_utils import td_resource

logger = logging.getLogger(__name__)

ACTIONS_DIRECTORY = "resources/cli/actions"


class Action:
    """
    Base class for all action implementations.
    """

    def __init__(self, name: str, definition: dict):
        self.name = name
        self.definition = definition

    @property
    def enabled(self) -> bool:
        return self.definition.get("enabled", True)

    def execute(self, **kwargs):
        raise NotImplementedError("Subclasses must implement the execute method.")

    def __str__(self):
        return f"Action(name={self.name}, definition={self.definition})"

    def __repr__(self):
        return self.__str__()


def variant_action_classes() -> dict[str, type[Action]]:
    try:
        from tabsdatak.extensions.variant.cli.config_utils import ACTION_CLASSES
    except ImportError:
        return {}
    return ACTION_CLASSES


def create_action(action_name: str, action_definition: dict) -> Action | None:
    action_cls = variant_action_classes().get(action_name)
    if not action_cls:
        logger.warning(
            f"Skipping action {action_name!r}: no implementation available in "
            "the current variant."
        )
        return None
    return action_cls(action_name, action_definition)


def load_actions_from_config(config: dict) -> list[Action]:
    actions = []
    raw_actions = config.get("actions", []) or []
    for action_configuration in raw_actions:
        action_name, action_definition = next(iter(action_configuration.items()))
        action = create_action(action_name, action_definition)
        if action is not None:
            actions.append(action)
    return actions


def load_actions_config_for_command(command_name: str) -> dict:
    config_path = os.path.join(td_resource(ACTIONS_DIRECTORY), f"{command_name}.yaml")
    try:
        with open(config_path, "r") as f:
            # ruamel, not PyYAML: PyYAML is NOT a declared tabsdata-cli
            # dependency (only ruamel is, transitively via tabsdata-api), and
            # this module is imported eagerly by the root CLI -- an undeclared
            # top-level ``import yaml`` would fail the whole CLI on a minimal
            # install. ``or {}`` keeps the empty-file case a dict (safe_load
            # returned None), matching the FileNotFoundError default.
            config = YAML(typ="safe").load(f) or {}
    except FileNotFoundError:
        config = {}
    return config


def execute_actions_for_command(command_name: str, **kwargs):
    config = load_actions_config_for_command(command_name)
    actions = load_actions_from_config(config)
    for action in actions:
        if action.enabled:
            action.execute(**kwargs)
