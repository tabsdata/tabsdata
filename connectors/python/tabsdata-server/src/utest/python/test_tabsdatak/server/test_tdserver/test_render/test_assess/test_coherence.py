#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.server._tdserver.render.assess import State
from tabsdatak.server._tdserver.render.assess.checks.coherence import (
    AGENT_ENVIRONMENT,
    assess_agent_keys,
    assess_package_index,
    assess_secret_shape,
    configs_of,
)
from tabsdatak.server._tdserver.render.assess.core.task import Target

pytestmark = pytest.mark.unit

INDEXED = """
pods:
  global:
    envs:
      UV_INDEX: https://index.example/simple/
"""

AGENT = """
ai_providers:
  anthropic:
    api_key: ${{secret:anthropic_key}}
"""


def target(name: str = "probe") -> Target:
    return Target(instance=name)


def test_the_configs_of_an_instance_sit_under_its_init_folder(instances):
    found = configs_of("probe")
    assert found.parts[-4:] == ("probe", "init", "input", "configs")


def test_an_instance_without_a_config_folder_cannot_be_judged(instances):
    found = assess_package_index(target())
    assert found.state is State.SKIPPED


def test_an_instance_without_the_scheduling_file_cannot_be_judged(prepared):
    prepared("probe")
    assert assess_package_index(target()).state is State.SKIPPED


def test_an_unreadable_scheduling_file_cannot_be_judged(prepared):
    prepared("probe", configs={"tabsdata.yaml": "pods: [unclosed"})
    assert assess_package_index(target()).state is State.SKIPPED


def test_an_instance_naming_no_index_is_waived(prepared):
    prepared("probe", configs={"tabsdata.yaml": "pods:\n  global:\n    envs: {}\n"})
    found = assess_package_index(target())
    assert found.state is State.WAIVED


def test_an_index_left_as_a_placeholder_is_waived(prepared):
    prepared(
        "probe",
        configs={
            "tabsdata.yaml": (
                "pods:\n  global:\n    envs:\n      UV_INDEX: ${{literal:index}}\n"
            )
        },
    )
    assert assess_package_index(target()).state is State.WAIVED


def test_the_index_is_read_from_the_envs_of_the_global_pod(prepared, monkeypatch):
    prepared("probe", configs={"tabsdata.yaml": INDEXED})
    asked = []

    def answer(request, timeout):
        asked.append(request.full_url)
        raise OSError("no route to host")

    monkeypatch.setattr(
        "tabsdatak.server._tdserver.render.assess.checks.coherence"
        ".urllib.request.urlopen",
        answer,
    )
    assess_package_index(target())
    assert asked == ["https://index.example/simple/"]


def test_an_index_that_cannot_be_reached_only_warns(prepared, monkeypatch):
    prepared("probe", configs={"tabsdata.yaml": INDEXED})

    def answer(request, timeout):
        raise OSError("no route to host")

    monkeypatch.setattr(
        "tabsdatak.server._tdserver.render.assess.checks.coherence"
        ".urllib.request.urlopen",
        answer,
    )
    assert assess_package_index(target()).state is State.WARNING


def test_an_instance_without_an_agent_file_runs_no_agent(prepared):
    prepared("probe")
    found = assess_agent_keys(target())
    assert found.state is State.WAIVED


def test_an_agent_declaring_no_provider_is_waived(prepared):
    prepared("probe", configs={"aiagent.yaml": "ai_providers: {}\n"})
    assert assess_agent_keys(target()).state is State.WAIVED


def test_an_unreadable_agent_file_is_reported(prepared):
    prepared("probe", configs={"aiagent.yaml": "ai_providers: [unclosed"})
    assert assess_agent_keys(target()).state is State.FAILED


def test_a_provider_with_a_supplied_key_passes(prepared, monkeypatch):
    monkeypatch.delenv(AGENT_ENVIRONMENT["anthropic"], raising=False)
    prepared(
        "probe",
        configs={"aiagent.yaml": "ai_providers:\n  anthropic:\n    api_key: sk-real\n"},
    )
    assert assess_agent_keys(target()).state is State.PASSED


def test_a_placeholder_key_is_answered_by_the_environment(prepared, monkeypatch):
    monkeypatch.setenv(AGENT_ENVIRONMENT["anthropic"], "sk-from-environment")
    prepared("probe", configs={"aiagent.yaml": AGENT})
    assert assess_agent_keys(target()).state is State.PASSED


def test_a_placeholder_key_with_nothing_behind_it_warns(prepared, monkeypatch):
    monkeypatch.delenv(AGENT_ENVIRONMENT["anthropic"], raising=False)
    prepared("probe", configs={"aiagent.yaml": AGENT})
    found = assess_agent_keys(target())
    assert found.state is State.WARNING
    assert AGENT_ENVIRONMENT["anthropic"] in found.facts[0]


def test_one_provider_answering_is_enough(prepared, monkeypatch):
    monkeypatch.delenv(AGENT_ENVIRONMENT["anthropic"], raising=False)
    monkeypatch.delenv(AGENT_ENVIRONMENT["openai"], raising=False)
    prepared(
        "probe",
        configs={
            "aiagent.yaml": (
                "ai_providers:\n"
                "  anthropic:\n    api_key: ${{secret:anthropic_key}}\n"
                "  openai:\n    api_key: sk-real\n"
            )
        },
    )
    assert assess_agent_keys(target()).state is State.PASSED


def test_an_optional_placeholder_counts_as_no_key(prepared, monkeypatch):
    monkeypatch.delenv(AGENT_ENVIRONMENT["anthropic"], raising=False)
    prepared(
        "probe",
        configs={
            "aiagent.yaml": (
                "ai_providers:\n  anthropic:\n    api_key: ${{secret:key?}}\n"
            )
        },
    )
    assert assess_agent_keys(target()).state is State.WARNING


def test_a_name_asked_for_in_one_file_and_generated_in_another_clashes(prepared):
    prepared(
        "probe",
        configs={
            "one.yaml": "password: ${{secret:shared}}\n",
            "two.yaml": "password: ${{secretAuto:shared}}\n",
        },
    )
    found = assess_secret_shape(target())
    assert found.state is State.FAILED
    assert "shared" in found.facts[0]
    assert "one.yaml" in found.facts[0] and "two.yaml" in found.facts[0]


def test_a_name_asked_for_and_generated_in_one_file_clashes(prepared):
    prepared(
        "probe",
        configs={
            "one.yaml": "asked: ${{secret:shared}}\nmade: ${{secretAuto:shared}}\n"
        },
    )
    assert assess_secret_shape(target()).state is State.FAILED


def test_separate_names_do_not_clash(prepared):
    prepared(
        "probe",
        configs={
            "one.yaml": "password: ${{secret:asked}}\n",
            "two.yaml": "password: ${{secretAuto:made}}\n",
        },
    )
    assert assess_secret_shape(target()).state is State.PASSED


def test_an_optional_placeholder_never_clashes_with_a_generated_one(prepared):
    prepared(
        "probe",
        configs={
            "one.yaml": "password: ${{secret:shared?}}\n",
            "two.yaml": "password: ${{secretAuto:shared}}\n",
        },
    )
    assert assess_secret_shape(target()).state is State.PASSED


def test_an_instance_without_a_config_folder_yields_no_secret_shape(instances):
    assert assess_secret_shape(target()).state is State.SKIPPED
