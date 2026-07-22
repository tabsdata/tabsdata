#
# Copyright 2026 Tabsdata Inc.
#

import pytest

from tabsdata._table_actions.actions import OnTablesAction, _build_actions_list_or_raise
from tabsdata.exceptions import (
    DecoratorConfigurationError,
    ErrorCode,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


class DummyAction(OnTablesAction):
    """Concrete subclass for testing."""
    pass


class NotAnAction:
    """Not an OnTablesAction subclass."""
    pass


class TestOnTablesAction:
    def test_is_base_class(self):
        action = OnTablesAction()
        assert isinstance(action, OnTablesAction)

    def test_subclass_isinstance(self):
        action = DummyAction()
        assert isinstance(action, OnTablesAction)


class TestBuildActionsListOrRaise:
    def test_single_action_becomes_list(self):
        action = DummyAction()
        result = _build_actions_list_or_raise(
            action, ErrorCode.DCE1, DecoratorConfigurationError
        )
        assert result == [action]

    def test_list_of_actions_passthrough(self):
        actions = [DummyAction(), DummyAction()]
        result = _build_actions_list_or_raise(
            actions, ErrorCode.DCE1, DecoratorConfigurationError
        )
        assert result is actions

    def test_none_returns_empty_list(self):
        result = _build_actions_list_or_raise(
            None, ErrorCode.DCE1, DecoratorConfigurationError
        )
        assert result == []

    def test_invalid_type_raises(self):
        with pytest.raises(DecoratorConfigurationError):
            _build_actions_list_or_raise(
                "not_an_action", ErrorCode.DCE1, DecoratorConfigurationError
            )

    def test_list_with_non_action_raises(self):
        with pytest.raises(DecoratorConfigurationError):
            _build_actions_list_or_raise(
                [DummyAction(), NotAnAction()],
                ErrorCode.DCE1,
                DecoratorConfigurationError,
            )

    def test_integer_raises(self):
        with pytest.raises(DecoratorConfigurationError):
            _build_actions_list_or_raise(
                42, ErrorCode.DCE1, DecoratorConfigurationError
            )

    def test_empty_list_passthrough(self):
        result = _build_actions_list_or_raise(
            [], ErrorCode.DCE1, DecoratorConfigurationError
        )
        assert result == []
