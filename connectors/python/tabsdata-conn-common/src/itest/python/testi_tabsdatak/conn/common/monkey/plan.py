#
# Copyright 2026 Tabsdata Inc.
#

"""What a monkey run does: a fleet of functions of every workload kind and an
ordered list of operations on them, drawn from a seed. Each function belongs
to one lane; a lane runs its operations in plan order while lanes run
concurrently, so every function sees its operations in a fixed order however
the lanes interleave. The plan is the reproducible part of a run and is what a
descriptor stores."""

from __future__ import annotations

import random
from dataclasses import asdict, dataclass

GENERATOR = 4

PUBLISHER = "publisher"
CRON_PUBLISHER = "cron_publisher"
STREAM_PUBLISHER = "stream_publisher"
TRANSFORMER = "transformer"
SUBSCRIBER = "subscriber"

TRIGGER_KINDS = (CRON_PUBLISHER, STREAM_PUBLISHER)

REGISTER = "register"
DELETE = "delete"
ENABLE = "enable"
DISABLE = "disable"
UPDATE = "update"
TRIGGER = "trigger"
CANCEL_EXECUTION = "cancel_execution"
CANCEL_TRANSACTION = "cancel_transaction"
RECOVER_EXECUTION = "recover_execution"
RECOVER_TRANSACTION = "recover_transaction"
LOSE_JOB = "lose_job"
LOSE_STREAMER = "lose_streamer"
ORPHAN = "orphan"
PAUSE = "pause"

FAULTS = (LOSE_JOB, LOSE_STREAMER, ORPHAN)

SOURCES = "sources"
DESTINATIONS = "destinations"
DEFAULT = "default"

DATAGEN = "datagen"
TRASH = "trash"
KAFKA = "kafka"

FLEET_SIZES = {
    PUBLISHER: 10,
    TRANSFORMER: 6,
    SUBSCRIBER: 4,
    CRON_PUBLISHER: 6,
    STREAM_PUBLISHER: 6,
}
PUBLISHER_SECONDS = (0, 0, 10, 30, 90, 240)
FAILING_SHARE = 0.2

PRESENT_WEIGHTS = {
    PUBLISHER: {
        TRIGGER: 12,
        CANCEL_EXECUTION: 4,
        CANCEL_TRANSACTION: 2,
        RECOVER_EXECUTION: 2,
        RECOVER_TRANSACTION: 1,
        UPDATE: 3,
        ENABLE: 2,
        DISABLE: 2,
        DELETE: 1,
        LOSE_JOB: 2,
    },
    TRANSFORMER: {
        TRIGGER: 4,
        CANCEL_EXECUTION: 2,
        UPDATE: 3,
        ENABLE: 2,
        DISABLE: 2,
        DELETE: 1,
    },
    SUBSCRIBER: {
        TRIGGER: 4,
        CANCEL_EXECUTION: 2,
        UPDATE: 3,
        ENABLE: 2,
        DISABLE: 2,
        DELETE: 1,
    },
    CRON_PUBLISHER: {TRIGGER: 2, UPDATE: 4, ENABLE: 4, DISABLE: 4, DELETE: 1},
    STREAM_PUBLISHER: {UPDATE: 4, ENABLE: 4, DISABLE: 4, DELETE: 1, LOSE_STREAMER: 2},
}
ABSENT_WEIGHTS = {REGISTER: 8, ENABLE: 1, TRIGGER: 1, DELETE: 1}
GLOBAL_WEIGHTS = {PAUSE: 2, ORPHAN: 1}
GLOBAL_SHARE = 0.03

DELAY_WEIGHTS = ((0.0, 0.0, 4), (0.1, 1.0, 4), (1.0, 5.0, 2))
PAUSE_SECONDS = (5.0, 20.0)


@dataclass(frozen=True)
class CollectionSpec:
    name: str
    group: str
    connection: str | None


@dataclass(frozen=True)
class FunctionSpec:
    name: str
    kind: str
    collection: str
    seconds: int = 0
    upstream: str | None = None
    fails: bool = False


@dataclass(frozen=True)
class Fleet:
    collections: tuple[CollectionSpec, ...]
    functions: tuple[FunctionSpec, ...]

    def function(self, name: str) -> FunctionSpec:
        return next(spec for spec in self.functions if spec.name == name)

    def collection(self, name: str) -> CollectionSpec:
        return next(spec for spec in self.collections if spec.name == name)

    def of_kind(self, *kinds: str) -> list[FunctionSpec]:
        return [spec for spec in self.functions if spec.kind in kinds]

    def registration_order(self) -> list[FunctionSpec]:
        order = (PUBLISHER, CRON_PUBLISHER, STREAM_PUBLISHER, TRANSFORMER, SUBSCRIBER)
        return sorted(self.functions, key=lambda spec: order.index(spec.kind))


@dataclass(frozen=True)
class Op:
    index: int
    lane: int
    delay: float
    action: str
    function: str | None = None


@dataclass(frozen=True)
class Plan:
    seed: int
    lanes: int
    faults: bool
    fleet: Fleet
    ops: tuple[Op, ...]

    def lane_ops(self, lane: int) -> list[Op]:
        return [op for op in self.ops if op.lane == lane]

    def to_dict(self) -> dict:
        return {
            "generator": GENERATOR,
            "seed": self.seed,
            "lanes": self.lanes,
            "faults": self.faults,
            "fleet": {
                "collections": [asdict(spec) for spec in self.fleet.collections],
                "functions": [
                    {key: value for key, value in asdict(spec).items() if value}
                    for spec in self.fleet.functions
                ],
            },
            "ops": [
                {key: value for key, value in asdict(op).items() if value is not None}
                for op in self.ops
            ],
        }

    @staticmethod
    def from_dict(data: dict) -> Plan:
        fleet = data["fleet"]
        return Plan(
            seed=data["seed"],
            lanes=data["lanes"],
            faults=data["faults"],
            fleet=Fleet(
                collections=tuple(
                    CollectionSpec(**spec) for spec in fleet["collections"]
                ),
                functions=tuple(FunctionSpec(**spec) for spec in fleet["functions"]),
            ),
            ops=tuple(Op(**op) for op in data["ops"]),
        )


def build_fleet(rng: random.Random, scale: float, max_seconds: int) -> Fleet:
    seconds = [value for value in PUBLISHER_SECONDS if value <= max_seconds] or [0]
    collections = (
        CollectionSpec("publishers_a", SOURCES, DATAGEN),
        CollectionSpec("publishers_b", SOURCES, DATAGEN),
        CollectionSpec("cron_publishers", SOURCES, DATAGEN),
        CollectionSpec("stream_publishers", SOURCES, KAFKA),
        CollectionSpec("transformers", DEFAULT, None),
        CollectionSpec("subscribers", DESTINATIONS, TRASH),
    )
    sizes = {kind: max(1, round(size * scale)) for kind, size in FLEET_SIZES.items()}
    functions: list[FunctionSpec] = []
    for index in range(sizes[PUBLISHER]):
        functions.append(
            FunctionSpec(
                name=f"pub{index:02d}",
                kind=PUBLISHER,
                collection="publishers_a" if index % 2 == 0 else "publishers_b",
                seconds=rng.choice(seconds),
            )
        )
    publishers = [spec.name for spec in functions]
    for index in range(sizes[CRON_PUBLISHER]):
        functions.append(
            FunctionSpec(f"crn{index:02d}", CRON_PUBLISHER, "cron_publishers")
        )
    for index in range(sizes[STREAM_PUBLISHER]):
        functions.append(
            FunctionSpec(f"stm{index:02d}", STREAM_PUBLISHER, "stream_publishers")
        )
    transformers = []
    for index in range(sizes[TRANSFORMER]):
        name = f"tfr{index:02d}"
        transformers.append(name)
        functions.append(
            FunctionSpec(
                name,
                TRANSFORMER,
                "transformers",
                upstream=rng.choice(publishers),
                fails=rng.random() < FAILING_SHARE,
            )
        )
    for index in range(sizes[SUBSCRIBER]):
        functions.append(
            FunctionSpec(
                f"sub{index:02d}",
                SUBSCRIBER,
                "subscribers",
                upstream=rng.choice(transformers),
            )
        )
    return Fleet(collections=collections, functions=tuple(functions))


def draw(rng: random.Random, weights: dict[str, int], faults: bool) -> str:
    choices = {
        action: weight
        for action, weight in weights.items()
        if faults or action not in FAULTS
    }
    return rng.choices(list(choices), weights=list(choices.values()))[0]


def delay(rng: random.Random) -> float:
    low, high, _ = rng.choices(DELAY_WEIGHTS, weights=[w for *_, w in DELAY_WEIGHTS])[0]
    return round(rng.uniform(low, high), 1)


def generate(
    seed: int,
    operations: int,
    lanes: int,
    faults: bool,
    scale: float = 1.0,
    max_seconds: int = max(PUBLISHER_SECONDS),
) -> Plan:
    rng = random.Random(seed)
    fleet = build_fleet(rng, scale, max_seconds)
    lane_of = {spec.name: index % lanes for index, spec in enumerate(fleet.functions)}
    present = {spec.name: True for spec in fleet.functions}
    ops: list[Op] = []
    for index in range(operations):
        if rng.random() < GLOBAL_SHARE:
            action = draw(rng, GLOBAL_WEIGHTS, faults)
            wait = (
                round(rng.uniform(*PAUSE_SECONDS), 1) if action == PAUSE else delay(rng)
            )
            ops.append(Op(index, rng.randrange(lanes), wait, action))
            continue
        spec = rng.choice(fleet.functions)
        if present[spec.name]:
            action = draw(rng, PRESENT_WEIGHTS[spec.kind], faults)
        else:
            action = draw(rng, ABSENT_WEIGHTS, faults)
        if action == DELETE:
            present[spec.name] = False
        elif action == REGISTER:
            present[spec.name] = True
        ops.append(Op(index, lane_of[spec.name], delay(rng), action, spec.name))
    return Plan(seed=seed, lanes=lanes, faults=faults, fleet=fleet, ops=tuple(ops))
