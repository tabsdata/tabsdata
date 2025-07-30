#
# Copyright 2024 Tabs Data Inc.
#

import pytest

from tabsdata import LocalFileDestination, LocalFileSource, TableInput, TableOutput
from tabsdata.decorators import ALL_DEPS, publisher, subscriber, transformer
from tabsdata.exceptions import DecoratorConfigurationError, ErrorCode


def test_transformer_wrong_data_type_raises_exception():
    with pytest.raises(DecoratorConfigurationError) as e:
        transformer(LocalFileSource("input.csv"), TableOutput("output"))
    assert e.value.error_code == ErrorCode.DCE1


def test_transformer_wrong_destination_type_raises_exception():
    with pytest.raises(DecoratorConfigurationError) as e:
        transformer(TableInput("input"), LocalFileDestination("output.csv"))
    assert e.value.error_code == ErrorCode.DCE2


def test_transformer_wrong_data_type_in_list_raises_exception():
    with pytest.raises(DecoratorConfigurationError) as e:
        transformer(["input", 42], TableOutput("output"))
    assert e.value.error_code == ErrorCode.DCE1


def test_transformer_wrong_destination_type_in_list_raises_exception():
    with pytest.raises(DecoratorConfigurationError) as e:
        transformer(TableInput("input"), ["output", 42])
    assert e.value.error_code == ErrorCode.DCE2


def test_transformer_with_optional_parameters():
    @transformer(
        TableInput("input"),
        TableOutput("output"),
        name="func_name",
        trigger_by="trigger_table",
    )
    def func():
        pass

    assert func.name == "func_name"
    assert func.input == TableInput("input")
    assert func.output == TableOutput("output")
    assert func.trigger_by == ["trigger_table"]


def test_transformer_all_correct():
    @transformer(TableInput("input"), TableOutput("output"))
    def func():
        pass

    assert func.name == "func"
    assert func.input == TableInput("input")
    assert func.output == TableOutput("output")
    assert func.trigger_by is None


def test_transformer_all_deps():
    @transformer(TableInput("input"), TableOutput("output"), trigger_by=ALL_DEPS)
    def func():
        pass

    assert func.name == "func"
    assert func.input == TableInput("input")
    assert func.output == TableOutput("output")
    assert func.trigger_by is None


def test_transformer_trigger_by_none():
    @transformer(TableInput("input"), TableOutput("output"), trigger_by=None)
    def func():
        pass

    assert func.name == "func"
    assert func.input == TableInput("input")
    assert func.output == TableOutput("output")
    assert func.trigger_by == []


def test_transformer_all_string():
    @transformer("input", "output")
    def func():
        pass

    assert func.name == "func"
    assert func.input == TableInput("input")
    assert func.output == TableOutput("output")
    assert func.trigger_by is None


def test_transformer_all_correct_string_list():
    @transformer(["input", "input2"], ["output", "output2"])
    def func():
        pass

    assert func.name == "func"
    assert func.input == TableInput(["input", "input2"])
    assert func.output == TableOutput(["output", "output2"])
    assert func.trigger_by is None


def test_publisher_wrong_data_type_raises_exception():
    with pytest.raises(DecoratorConfigurationError) as e:
        publisher(TableInput("input"), TableOutput("output"))
    assert e.value.error_code == ErrorCode.DCE3


def test_publisher_wrong_destination_type_raises_exception():
    with pytest.raises(DecoratorConfigurationError) as e:
        publisher(LocalFileSource("input.csv"), LocalFileDestination("output.csv"))
    assert e.value.error_code == ErrorCode.DCE4


def test_publisher_with_optional_parameters():
    @publisher(
        LocalFileSource("input.csv"),
        TableOutput("output"),
        name="func_name",
        trigger_by="trigger_table",
    )
    def func():
        pass

    assert func.name == "func_name"
    assert isinstance(func.input, LocalFileSource)
    assert func.output == TableOutput("output")
    assert func.trigger_by == ["trigger_table"]


def test_publisher_all_correct():
    @publisher(LocalFileSource("input.csv"), TableOutput("output"))
    def func():
        pass

    assert func.name == "func"
    assert isinstance(func.input, LocalFileSource)
    assert func.output == TableOutput("output")
    assert func.trigger_by is None


def test_publisher_all_correct_with_tables_string():
    @publisher(LocalFileSource("input.csv"), tables="output")
    def func():
        pass

    assert func.name == "func"
    assert isinstance(func.input, LocalFileSource)
    assert func.output == TableOutput("output")
    assert func.trigger_by is None


def test_publisher_all_correct_with_tables_string_list():
    @publisher(LocalFileSource("input.csv"), tables=["output", "output2"])
    def func():
        pass

    assert func.name == "func"
    assert isinstance(func.input, LocalFileSource)
    assert func.output == TableOutput(["output", "output2"])
    assert func.trigger_by is None


def test_publisher_wrong_destination_type_list_raises_exception():
    with pytest.raises(DecoratorConfigurationError) as e:
        publisher(LocalFileSource("input.csv"), ["output", 42])
    assert e.value.error_code == ErrorCode.DCE4


def test_subscriber_wrong_data_type_raises_exception():
    with pytest.raises(DecoratorConfigurationError) as e:
        subscriber(LocalFileSource("input.csv"), LocalFileDestination("output.csv"))
    assert e.value.error_code == ErrorCode.DCE5


def test_subscriber_wrong_destination_type_raises_exception():
    with pytest.raises(DecoratorConfigurationError) as e:
        subscriber(TableInput("input"), TableOutput("output"))
    assert e.value.error_code == ErrorCode.DCE6


def test_subscriber_all_correct():
    @subscriber(TableInput("input"), LocalFileDestination("output.csv"))
    def func():
        pass

    assert func.name == "func"
    assert func.input == TableInput("input")
    assert func.output == LocalFileDestination("output.csv")
    assert func.trigger_by is None


def test_subscriber_all_correct_with_tables_string():
    @subscriber("input", LocalFileDestination("output.csv"))
    def func():
        pass

    assert func.name == "func"
    assert func.input == TableInput("input")
    assert func.output == LocalFileDestination("output.csv")
    assert func.trigger_by is None


def test_subscriber_all_correct_with_tables_string_list():
    @subscriber(["input", "input2"], LocalFileDestination("output.csv"))
    def func():
        pass

    assert func.name == "func"
    assert func.input == TableInput(["input", "input2"])
    assert func.output == LocalFileDestination("output.csv")
    assert func.trigger_by is None


def test_subscriber_trigger_by_all_deps():
    @subscriber(
        ["input", "input2"], LocalFileDestination("output.csv"), trigger_by=ALL_DEPS
    )
    def func():
        pass

    assert func.name == "func"
    assert func.input == TableInput(["input", "input2"])
    assert func.output == LocalFileDestination("output.csv")
    assert func.trigger_by is None


def test_subscriber_trigger_by_none():
    @subscriber(
        ["input", "input2"], LocalFileDestination("output.csv"), trigger_by=None
    )
    def func():
        pass

    assert func.name == "func"
    assert func.input == TableInput(["input", "input2"])
    assert func.output == LocalFileDestination("output.csv")
    assert func.trigger_by == []


def test_subscriber_wrong_destination_type_list_raises_exception():
    with pytest.raises(DecoratorConfigurationError) as e:
        subscriber(["output", 42], LocalFileDestination("output.csv"))
    assert e.value.error_code == ErrorCode.DCE5


def test_subscriber_with_optional_parameters():
    @subscriber(
        TableInput("input"),
        LocalFileDestination("output.csv"),
        name="func_name",
        trigger_by="trigger_table",
    )
    def func():
        pass

    assert func.name == "func_name"
    assert func.input == TableInput("input")
    assert func.output == LocalFileDestination("output.csv")
    assert func.trigger_by == ["trigger_table"]
