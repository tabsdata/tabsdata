#
# Copyright 2026 Tabsdata Inc.
#

"""Apply deterministic changes to a domain's data (Insert / Update / Delete / Mix).

``--entity ALL`` applies each model entity's per-set row count across ``--sets``
transactions; ``--entity NAME`` applies ``--sets`` rows to that one entity.
``--seed N`` is reproducible; ``--seed AUTO`` continues from the last seed (last
+ 1). Insert ordinals derive from the seed, so the same op/set/seed sequence is
repeatable.
"""

from __future__ import annotations

import argparse
import json

from tabsdatax.itest.tdksystems2 import cli, console, constants, datagen, descriptor
from tabsdatax.itest.tdksystems2.datagen import ChangeSpec, OpKind, OpSelector
from tabsdatax.itest.tdksystems2.errors import TdxError

_OPS = {
    "insert": OpSelector.INSERT,
    "update": OpSelector.UPDATE,
    "delete": OpSelector.DELETE,
    "mix": OpSelector.MIX,
}


def add_arguments(parser: argparse.ArgumentParser) -> None:
    cli.add_domain(parser)
    cli.add_domain_dir(parser)
    parser.add_argument(
        constants.OPT_ENTITY,
        default=constants.ENTITY_ALL,
        metavar="NAME|ALL",
        help="entity to change, or ALL (default ALL)",
    )
    parser.add_argument(
        constants.OPT_SETS,
        type=int,
        default=1,
        metavar="COUNT",
        help="number of change sets/transactions; for a single --entity, the row "
        "count (default 1)",
    )
    parser.add_argument(
        constants.OPT_OPS,
        default="Mix",
        metavar="OP",
        help="Insert | Update | Delete | Mix (default Mix)",
    )
    parser.add_argument(
        constants.OPT_SEED,
        default=constants.SEED_AUTO,
        metavar="N|AUTO",
        help="seed number, or AUTO to continue from the last (default AUTO)",
    )
    cli.add_out(parser)
    cli.add_latch(parser)


def run(ctx: cli.Context) -> int:
    assert isinstance(ctx.info, descriptor.DomainInfo) and ctx.path is not None
    a = ctx.args
    info = ctx.info
    sysobj = ctx.system
    json_mode = a.out == constants.OUT_JSON
    model = info.model
    if not model:
        raise TdxError(
            f"domain '{ctx.name}' has no model — create-model + create-data first"
        )
    if not info.data:
        raise TdxError(f"domain '{ctx.name}' has no data — run create-data first")

    ops = _OPS.get(a.ops.strip().lower())
    if ops is None:
        raise TdxError(
            f"{constants.OPT_OPS} must be Insert|Update|Delete|Mix (got: {a.ops})"
        )

    model_obj = datagen.load_model(model)
    by = {e.table: e for e in model_obj.entities}
    counts = info.entities

    if a.entity.upper() == constants.ENTITY_ALL:
        entities = [e.table for e in model_obj.entities]
        rows_per_entity = {e.table: e.rows_per_set for e in model_obj.entities}
        if not entities:
            raise TdxError(
                f"model '{model}' has no entities that modify-data can change "
                f"(all are fixed — see 'tdx-domain models')"
            )
        fixed = [t for t in datagen.entities_of(model) if t not in by]
        if fixed and not json_mode:
            console.out(f"no modify-data support (skipped): {', '.join(fixed)}")
    else:
        ent = a.entity
        if ent not in by:
            raise TdxError(
                f"unknown entity '{ent}' "
                f"(this model changes: {', '.join(by) or 'none'})"
            )
        if ops is not OpSelector.MIX and OpKind(ops.value) not in by[ent].supports:
            supported = ", ".join(k.value for k in by[ent].supports)
            raise TdxError(
                f"entity '{ent}' does not support {ops.value} (supports: {supported})"
            )
        entities = [ent]
        rows_per_entity = {ent: 1}  # single entity: --sets is the row count

    auto = str(a.seed).strip().upper() == constants.SEED_AUTO
    if auto:
        seed = info.seed + 1
    else:
        try:
            seed = int(a.seed)
        except ValueError:
            raise TdxError(
                f"{constants.OPT_SEED} must be a number or AUTO (got: {a.seed})"
            )
    # Inserts derive from the seed's own block, so auto and a fixed seed mint
    # identical rows and the seed sequence alone makes a run repeatable.
    insert_base = seed * datagen.FIXED_SEED_INSERT_STRIDE

    spec = ChangeSpec(
        entities=entities,
        ops=ops,
        sets=a.sets,
        rows_per_entity=rows_per_entity,
        seed=seed,
        insert_base=insert_base,
    )
    txns = datagen.plan(model_obj, spec, counts)
    uri = sysobj.data_plane_uri(info.meta)
    new_entities = sysobj.apply_changes(uri, txns, counts)
    descriptor.record_changes(ctx.path, new_entities, seed=seed if auto else None)
    applied = len(txns)
    summary = datagen.op_counts(txns)
    if json_mode:
        report = {
            table: datagen.iud(
                insert=per[OpKind.INSERT],
                update=per[OpKind.UPDATE],
                delete=per[OpKind.DELETE],
            )
            for table, per in summary.items()
        }
        console.result(json.dumps(report, indent=2))
    else:
        console.good(
            f"applied {applied} change set(s) [{ops.value}] seed={seed} "
            f"to domain '{ctx.name}'"
        )
        if summary:
            width = max(len(t) for t in summary)
            for table, per in summary.items():
                rows = (
                    f"  (rows={new_entities[table]})" if table in new_entities else ""
                )
                console.out(
                    f"  {table:<{width}}  I={per[OpKind.INSERT]}  "
                    f"U={per[OpKind.UPDATE]}  D={per[OpKind.DELETE]}{rows}"
                )
    return 0
