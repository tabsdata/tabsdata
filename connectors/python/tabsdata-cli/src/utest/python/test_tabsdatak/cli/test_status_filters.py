#
# Copyright 2026 Tabsdata Inc.
#

"""``--status`` is checked as it is parsed, and ``--health`` filters by every
status of a health group, on each execution-family listing."""

from __future__ import annotations

import pytest
import rich_click as click
from click.testing import CliRunner

from tabsdatak.cli._errors import classify
from tabsdatak.cli.td import execution_utils
from tabsdatak.sdk.status_utils.groups import STATUS_GROUPS, Entity, Health

LISTINGS = (Entity.EXECUTION, Entity.TRANSACTION, Entity.FUNCTION_RUN, Entity.WORKER)


def _command(entity: Entity) -> click.Command:
    @click.command()
    @execution_utils.status_filter_options(entity)
    def listing(status, healths):
        click.echo(",".join(execution_utils.status_codes(entity, status, healths)))

    return listing


@pytest.mark.parametrize("entity", LISTINGS)
def test_health_filters_by_every_status_of_its_groups(entity):
    groups = STATUS_GROUPS[entity]
    healths = [h for h in (Health.ERROR, Health.WARNING) if groups[h]]
    argv = [arg for h in healths for arg in ("--health", h.value.upper())]
    result = CliRunner().invoke(_command(entity), argv)
    assert result.exit_code == 0, result.output
    assert result.output.strip().split(",") == sorted(
        code for h in healths for code in groups[h]
    )


def test_a_health_with_no_status_is_refused_rather_than_matching_everything():
    result = CliRunner().invoke(_command(Entity.EXECUTION), ["--health", "warning"])
    assert result.exit_code == 2
    assert "'warning' is not one of 'error', 'running', 'ok'" in result.output


def test_status_takes_a_name_or_a_code_and_sends_the_code():
    result = CliRunner().invoke(
        _command(Entity.EXECUTION), ["--status", "stalled", "--status", "U"]
    )
    assert result.output.strip() == "L,U"


def test_an_unknown_status_is_a_usage_error_that_points_to_health():
    assert (
        CliRunner().invoke(_command(Entity.EXECUTION), ["--status", "failed"]).exit_code
        == 2
    )
    option = next(
        param for param in _command(Entity.EXECUTION).params if param.name == "status"
    )
    with pytest.raises(click.BadParameter) as raised:
        option.type.convert("failed", option, None)
    message = raised.value.format_message()
    assert "'failed' is not one of" in message
    assert "'--health' (error: Stalled, Unexpected; running:" in message
    assert classify(raised.value).type == "usage"


def test_status_and_health_together_are_refused():
    result = CliRunner().invoke(
        _command(Entity.FUNCTION_RUN), ["--status", "F", "--health", "error"]
    )
    assert result.exit_code == 2
    assert "Pass '--status' or '--health', not both." in result.output
