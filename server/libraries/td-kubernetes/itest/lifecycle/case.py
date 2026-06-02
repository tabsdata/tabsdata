#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class State:
    cluster: bool
    instance: bool

    def label(self) -> str:
        cluster = "on" if self.cluster else "off"
        instance = "on" if self.instance else "off"
        return f"cluster={cluster}__and__instance={instance}"


STATE_BOTH_STOPPED = State(cluster=False, instance=False)
STATE_CLUSTER_ONLY = State(cluster=True, instance=False)
STATE_BOTH_STARTED = State(cluster=True, instance=True)
ALL_PRE_STATES = [STATE_BOTH_STOPPED, STATE_CLUSTER_ONLY, STATE_BOTH_STARTED]


@dataclass(frozen=True)
class Case:
    noun: str
    verb: str
    pre_state: State
    post_state: State
    expect_substrings: tuple[str, ...]
    expect_exit_zero: bool = True

    def id(self) -> str:
        return f"from_{self.pre_state.label()}__{self.noun}={self.verb}"


def build_cases() -> list[Case]:
    cases: list[Case] = []
    for pre in ALL_PRE_STATES:
        if pre.cluster:
            cases.append(
                Case(
                    noun="cluster",
                    verb="start",
                    pre_state=pre,
                    post_state=pre,
                    expect_substrings=("already started",),
                )
            )
        else:
            cases.append(
                Case(
                    noun="cluster",
                    verb="start",
                    pre_state=pre,
                    post_state=State(cluster=True, instance=pre.instance),
                    expect_substrings=("Started",),
                )
            )

        if pre.cluster:
            cases.append(
                Case(
                    noun="cluster",
                    verb="stop",
                    pre_state=pre,
                    post_state=State(cluster=False, instance=False),
                    expect_substrings=("Stopped",),
                )
            )
        else:
            cases.append(
                Case(
                    noun="cluster",
                    verb="stop",
                    pre_state=pre,
                    post_state=pre,
                    expect_substrings=("already stopped",),
                )
            )

        cases.append(
            Case(
                noun="cluster",
                verb="restart",
                pre_state=pre,
                post_state=State(cluster=True, instance=pre.instance),
                expect_substrings=("Restarted",),
            )
        )

        if pre.instance:
            cases.append(
                Case(
                    noun="instance",
                    verb="start",
                    pre_state=pre,
                    post_state=pre,
                    expect_substrings=("already started",),
                )
            )
        elif pre.cluster:
            cases.append(
                Case(
                    noun="instance",
                    verb="start",
                    pre_state=pre,
                    post_state=State(cluster=True, instance=True),
                    expect_substrings=("Started",),
                )
            )
        else:
            cases.append(
                Case(
                    noun="instance",
                    verb="start",
                    pre_state=pre,
                    post_state=pre,
                    expect_substrings=("cluster",),
                    expect_exit_zero=False,
                )
            )

        if pre.instance:
            cases.append(
                Case(
                    noun="instance",
                    verb="stop",
                    pre_state=pre,
                    post_state=State(cluster=pre.cluster, instance=False),
                    expect_substrings=("Stopped",),
                )
            )
        else:
            cases.append(
                Case(
                    noun="instance",
                    verb="stop",
                    pre_state=pre,
                    post_state=pre,
                    expect_substrings=("already stopped",),
                )
            )

        if pre.cluster:
            cases.append(
                Case(
                    noun="instance",
                    verb="restart",
                    pre_state=pre,
                    post_state=State(cluster=True, instance=True),
                    expect_substrings=("Restarted",),
                )
            )
        else:
            cases.append(
                Case(
                    noun="instance",
                    verb="restart",
                    pre_state=pre,
                    post_state=pre,
                    expect_substrings=("cluster",),
                    expect_exit_zero=False,
                )
            )

        if pre.cluster and pre.instance:
            cases.append(
                Case(
                    noun="server",
                    verb="start",
                    pre_state=pre,
                    post_state=pre,
                    expect_substrings=("already started",),
                )
            )
        else:
            cases.append(
                Case(
                    noun="server",
                    verb="start",
                    pre_state=pre,
                    post_state=State(cluster=True, instance=True),
                    expect_substrings=("Started",),
                )
            )

        if pre.cluster or pre.instance:
            cases.append(
                Case(
                    noun="server",
                    verb="stop",
                    pre_state=pre,
                    post_state=State(cluster=False, instance=False),
                    expect_substrings=("Stopped",),
                )
            )
        else:
            cases.append(
                Case(
                    noun="server",
                    verb="stop",
                    pre_state=pre,
                    post_state=pre,
                    expect_substrings=("Server is already stopped",),
                )
            )

        cases.append(
            Case(
                noun="server",
                verb="restart",
                pre_state=pre,
                post_state=State(cluster=True, instance=True),
                expect_substrings=("Restarted",),
            )
        )

    return cases


CASES = build_cases()
