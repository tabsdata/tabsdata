#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import os

import yaml

from tabsdata._utils.internal._resources import td_resource

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


def create_action(action_name: str, action_definition: dict) -> Action:
    from tabsdata.extensions.variant.cli.config_utils import ACTION_CLASSES

    action_cls = ACTION_CLASSES.get(action_name)
    if not action_cls:
        raise ValueError(f"Unknown action: {action_name}")
    return action_cls(action_name, action_definition)


def load_actions_from_config(config: dict) -> list[Action]:
    actions = []
    raw_actions = config.get("actions", []) or []
    for action_configuration in raw_actions:
        action_name, action_definition = next(iter(action_configuration.items()))
        action = create_action(action_name, action_definition)
        actions.append(action)
    return actions


def load_actions_config_for_command(command_name: str) -> dict:
    config_path = os.path.join(td_resource(ACTIONS_DIRECTORY), f"{command_name}.yaml")
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        config = {}
    return config


def execute_actions_for_command(command_name: str, **kwargs):
    config = load_actions_config_for_command(command_name)
    actions = load_actions_from_config(config)
    for action in actions:
        if action.enabled:
            action.execute(**kwargs)
