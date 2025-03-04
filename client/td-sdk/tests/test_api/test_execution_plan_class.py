#
# Copyright 2025 Tabs Data Inc.
#

import time

import pytest

from tabsdata.api.tabsdata_server import Collection, ExecutionPlan, Worker


@pytest.mark.integration
def test_execution_plan_class(tabsserver_connection):
    time_triggered = int(time.time())
    execution_plan = ExecutionPlan(
        connection=tabsserver_connection.connection,
        id="execution_plan_id",
        name="execution_plan_name",
        triggered_by="triggered_by",
        triggered_on=time_triggered,
        status="F",
        random_kwarg="kwarg",
        started_on=time_triggered,
        ended_on=time_triggered,
    )
    assert execution_plan.id == "execution_plan_id"
    assert execution_plan.name == "execution_plan_name"
    assert execution_plan.triggered_by == "triggered_by"
    assert execution_plan.triggered_on == time_triggered
    assert isinstance(execution_plan.triggered_on_str, str)
    assert execution_plan.started_on == time_triggered
    assert isinstance(execution_plan.started_on_str, str)
    assert execution_plan.ended_on == time_triggered
    assert isinstance(execution_plan.ended_on_str, str)
    assert execution_plan.status == "Failed"
    assert execution_plan.kwargs
    assert execution_plan.__repr__()
    assert execution_plan.__str__()


@pytest.mark.integration
@pytest.mark.slow
def test_execution_plan_class_lazy_properties(
    tabsserver_connection, testing_collection_with_table
):
    api_plan = tabsserver_connection.execution_plans[0]
    assert api_plan.id
    execution_plan = ExecutionPlan(tabsserver_connection.connection, api_plan.id)
    assert api_plan.id == execution_plan.id
    assert api_plan.name == execution_plan.name
    assert isinstance(execution_plan.collection, Collection)
    assert api_plan.collection == execution_plan.collection
    assert api_plan.function == execution_plan.function
    execution_plan.refresh()
    assert api_plan.triggered_by == execution_plan.triggered_by
    assert execution_plan.dot
    assert execution_plan.__repr__()
    assert execution_plan.__str__()
    assert isinstance(execution_plan.workers, list)
    assert all(isinstance(worker, Worker) for worker in execution_plan.workers)
