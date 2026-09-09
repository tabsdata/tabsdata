#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.server._tdserver.render.assess.checks.scheduling import (
    contract_of,
    largest_request,
    tolerations_of,
)

pytestmark = pytest.mark.unit

CONTRACT = """
pods:
  global:
    envs:
      UV_INDEX: https://index.example/simple/
  service:
    priority: tabsdata-service
    resources:
      cpu: "2"
    placement:
      selectors:
        role: tabsdata-services
      tolerations: []
    aiagent:
      resources:
        cpu: "0.5"
      placement:
        selectors:
          role: tabsdata-services
  function:
    priority: tabsdata-function
    resources:
      cpu: "1"
    placement:
      selectors:
        role: tabsdata-functions
      tolerations:
        - key: workload
          value: batch
          effect: NoSchedule
"""


def test_the_contract_reads_both_priority_classes(prepared):
    prepared("probe", configs={"tabsdata.yaml": CONTRACT})
    found = contract_of("probe")
    assert found.service_priority == "tabsdata-service"
    assert found.function_priority == "tabsdata-function"


def test_the_contract_reads_the_selectors_of_each_tier(prepared):
    prepared("probe", configs={"tabsdata.yaml": CONTRACT})
    found = contract_of("probe")
    assert found.service_selectors == {"role": "tabsdata-services"}
    assert found.function_selectors == {"role": "tabsdata-functions"}


def test_the_contract_reads_the_toleration_of_the_function_pods(prepared):
    prepared("probe", configs={"tabsdata.yaml": CONTRACT})
    found = contract_of("probe")
    assert [entry.caption for entry in found.function_tolerations] == [
        "workload=batch:NoSchedule"
    ]


def test_the_largest_request_is_the_largest_single_pod(prepared):
    prepared("probe", configs={"tabsdata.yaml": CONTRACT})
    assert contract_of("probe").largest_cores == 2.0


def test_an_instance_without_the_file_has_no_contract(prepared):
    prepared("probe")
    assert contract_of("probe") is None


def test_an_unreadable_file_has_no_contract(prepared):
    prepared("probe", configs={"tabsdata.yaml": "pods: [unclosed"})
    assert contract_of("probe") is None


def test_a_request_that_is_not_a_number_is_ignored():
    assert largest_request({"service": {"resources": {"cpu": "plenty"}}}) == 0.0


def test_a_placement_without_tolerations_yields_none():
    assert tolerations_of({}) == []
    assert tolerations_of({"tolerations": None}) == []


def test_a_toleration_reads_its_three_binding_parts():
    found = tolerations_of(
        {"tolerations": [{"key": "workload", "value": "batch", "effect": "NoSchedule"}]}
    )
    assert found[0].key == "workload"
    assert found[0].value == "batch"
    assert found[0].effect == "NoSchedule"
