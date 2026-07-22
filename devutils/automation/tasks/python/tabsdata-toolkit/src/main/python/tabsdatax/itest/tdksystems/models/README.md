# Bundled models

Each subdirectory of this folder is a *model*: a self-contained sample
domain shipped as part of the `tabsdata-toolkit` package. A model has a
Python data generator, a `sizing.json` describing what kinds of system
it can target (and how its size buckets scale), and one DDL file per
supported SQL system.

`tdx-domain create --model <name> [--data SIZE]` resolves the model name
(e.g. `--model courierlogistics`) against this folder and runs the generator
at the chosen size, materialising data into `sql`, `csv`, `json`, or `parquet`
format.

## Where models live

```
src/main/python/tabsdatax/itest/tdksystems/models/
  <name>/
    generator.py          # generator (must expose main())
    sizing.json         # kinds + how size buckets scale the generator's flags
    mysql.ddl           # one DDL per supported SQL system
    postgres.ddl
    mssqlserver.ddl
    oracle.ddl
    db2.ddl
    data.json           # optional — bundled real reference data
    *.csv               # optional — bundled fixed seed data (e.g. accounts.csv)
    *.yml               # optional — non-DDL recipes (salesforce uses cci/snowfakery)
    README.md           # optional per-model docs
```

The DDL filename's stem matches the system type (`tdx-system create --type
<slug>`). A model can support a subset of systems — only those with a matching
`<slug>.ddl` file.

Models are bundled with the package via `pyproject.toml`'s
`[tool.setuptools.package-data]`, so they ship inside the wheel and are
resolvable through `importlib.resources` after `pip install`.

## Discovering models

Each subdirectory of `models/` is a model; pass its name to
`tdx-domain create --model <name>`. Each model's `sizing.json` declares the
system `kinds` it can target (any subset of `sql`, `file`, `salesforce`, …) and
whether it is *sizeable* (`--data SIZE` scales it) or fixed-data.

### The bundled set

| model              | kinds        | sizeable | what it is                                                                |
|--------------------|-------------|----------|---------------------------------------------------------------------------|
| `airportdb`        | sql, file   | yes      | flights / bookings / airports                                             |
| `courierlogistics` | sql, file   | yes      | shipments / hubs / couriers                                               |
| `employees`        | sql, file   | yes      | classic HR (employees / departments / salaries)                           |
| `finance`          | sql         | no       | subscription billing keyed by Salesforce Account Id (pairs with `salesforce`) |
| `hospital`         | sql, file   | yes      | patients / visits / billing                                               |
| `restaurant`       | sql, file   | yes      | menu / orders / inventory                                                 |
| `sakila`           | sql, file   | yes      | canonical MySQL DVD-rental sample                                         |
| `salesforce`       | salesforce  | no       | Accounts + Opportunities, loaded via Snowfakery + CumulusCI (manual)      |
| `schoolgrades`     | sql, file   | yes      | students / classes / grades                                               |
| `supermarket`      | sql, file   | yes      | retail POS                                                                |
| `world`            | sql, file   | yes      | canonical MySQL world sample (countries / cities / languages)             |

The `finance` ↔ `salesforce` pair is tied together via
`finance.customers.salesforce_account_id` (the 18-char SF Id) — load the
salesforce model first if you want real SF Ids on both sides; the
`finance` model also ships with a frozen 351-account snapshot
(`accounts.csv`) so it is fully self-contained.

The `salesforce` model is **not** loaded through the normal data pipeline.
Loading data into a Salesforce org needs cci / snowfakery + an interactive
OAuth dance, documented step-by-step in
[`salesforce/SALESFORCE.md`](salesforce/SALESFORCE.md). Running
`tdx-domain create --model salesforce --data ...` prints those instructions.

## How `tdx-domain create` uses a model

`tdx-domain create --sys SYS.json --domain DOM.json --name NAME [--model M
[--data SIZE]]`:

| Step | What it does |
|---|---|
| create the domain | make the database / schema / topic / prefix and write `DOM.json`. |
| `--model M` (schema) | run `<model>/<system>.ddl` against a SQL domain (no-op for cloud / fs; prints the README for `salesforce`). |
| `--data SIZE` (load) | generate the model's data + run the INSERTs (SQL) or upload the files (cloud / fs). Sizeable models take the `SIZE`; fixed-data models ignore it. |

`data load` writes its generated files into a temp dir and passes them
straight to the domain (it doesn't expose them to you). For ad-hoc
inspection, point at an `fs` domain with a known root path and load
into it.

## `sizing.json`

```json
{
  "kinds":         ["sql", "file"],
  "prolific":      "--shipments",
  "prolific_base": 1000,
  "defaults": {
    "--customers": 200,
    "--couriers":   50,
    "--hubs":       10
  },
  "mins": { "--hubs": 2 },
  "caps": { "--hubs": 50 }
}
```

| key | meaning |
|---|---|
| `kinds` | system kinds this model is compatible with — checked against the domain's `SYSTEM_KIND` at `data create` / `data load` time. **Required.** |
| `prolific` | the dominant entity flag — receives the bucket value directly. **Required for sizeable models.** Its presence is what makes `is_sizeable(model)` return `True`. |
| `prolific_base` | the prolific value that the `m` bucket should produce (so all defaults are calibrated against it). |
| `defaults` | per-flag default counts; each scales linearly with the bucket. |
| `mins` | optional floor per flag (after scaling). |
| `caps` | optional ceiling per flag (after scaling). |

Size buckets (value passed to the prolific flag):

| size | n         |
|------|----------:|
| xs   |       100 |
| s    |     1,000 |
| m    |    10,000 |
| l    |   100,000 |
| xl   | 1,000,000 |

For every other flag in `defaults`:
`value = max(default * n / prolific_base, mins[flag] or 1)`,
then clamped by `caps[flag]` if present.

For example, `courierlogistics` at `--size xs` (n=100, base=1000) ends
up running:

```
--shipments 100 --customers 20 --couriers 5 --hubs 2
```

(`--hubs` default is `10*100/1000 = 1`, lifted to its `mins` floor of 2.)

## Fixed-data models

A model is **fixed** if its `sizing.json` is missing the `prolific` key
(typically because the generator emits a fixed dataset, e.g. real sample
data shipped alongside the model — `finance` and `salesforce` are both
fixed). In that case:

- the data load calls the generator with no scaling args.
- passing `--data <SIZE>` for a fixed model just loads it (the size is ignored).

## Generator contract — `generator.py`

The generator is loaded via `importlib.util.spec_from_file_location`. It
must expose `main()` and read its options from `sys.argv`:

- `--format {sql,csv,json,parquet}` — required.
- `--out OUTPUT_DIR` — required (must not already exist).
- `--chunks N` — optional; if given, the writer splits each entity's
  rows into N-row files named `<entity>-<NNNNN>.<ext>` (5-digit
  zero-padded). `run-order.txt` lists every chunk file in load order.
- The model's `prolific` flag (e.g. `--shipments`) and any flag named
  in `defaults` (sizeable models only).

Build the data and call `tabsdatax.itest.tdksystems.emit.write_tables`:

```python
from tabsdatax.itest.tdksystems.emit import Column, Table, write_tables

def main():
    p = argparse.ArgumentParser(...)
    ...  # add --format, --out, --chunks, the prolific flag, defaults
    args = p.parse_args()

    tables = [
        Table(name="cou_hubs", columns=[
            Column("hub_id", "int", auto=True),       # IDENTITY → skipped in INSERT
            Column("code",   "varchar(10)"),
            Column("city",   "varchar(100)"),
        ], rows=[...]),
        ...
    ]
    write_tables(
        tables, args.out, args.format,
        source_dir=Path(__file__).parent,
        chunks=args.chunks,
    )
```

Notes:

- `write_tables(..., source_dir=…)` copies every `<sys>.ddl` next to the
  generated data when `--format sql`, so the schema step can find the right
  DDL for the target SQL system.
- Mark IDENTITY columns with `auto=True` so SQL `INSERT`s skip them and
  let the engine assign ids.
- Be reentrant — the loader calls `main()` after rewriting `sys.argv`,
  so don't rely on import-time state.
- The generated rows must be valid against every system whose DDL you
  ship. Watch out for dialect quirks: Oracle treats `''` as `NULL`,
  Postgres `BOOLEAN` is strict (`1/0` won't auto-cast), Db2 caps
  database names at 8 chars (we map at the system layer; just ensure
  table names stay within Db2's 128-byte identifier limit).

## Output directory layout

```
OUTPUT_DIR/
  <table_1>.<ext>            # one file per table (or per chunk if --chunks)
  <table_2>.<ext>
  ...
  run-order.txt              # data file names in load order
  <slug>.ddl                 # for --format sql only: every DDL copied from the model
```

`run-order.txt` is what `data load` walks. The DDL files are what
`data create` reads (matched by `<system_type>.ddl`).

## Change recipes — `changes.py` (optional)

A model may also ship a `changes.py` next to its `generator.py`. It powers
`tdx-domain changes --domain NAME --shape SHAPE --ops OPS --count N`, which
applies a deterministic set of INSERT/UPDATE/DELETE operations to an
*already-seeded* domain — the point being to drive change-data-capture (the
`mysqlcdc` / `postgrescdc` / `db2cdc` systems) or just to mutate the data.
It works on any seeded SQL domain; on a file domain only inserts apply (each
insert is a new file).

`--shape` groups the changes into transactions — `single-row`,
`multi-row-same` (one table), `multi-row-diff` (across tables), or `random`;
`--ops` selects the verbs (`insert` / `update` / `delete` / `mix`); `--count`
is the number of transactions, `--rows` the rows per transaction for the
multi-row shapes.

`--seed auto` (the default) walks a per-domain seed 1,2,3,… and a compact
insert cursor, both persisted in the domain descriptor, so every mode keeps
working across repeated runs — inserts never collide and each run touches fresh
rows. `--seed N` applies that exact seed and leaves the descriptor untouched
(reproducible; re-running an insert with the same N re-mints the same key and
collides). Salesforce is intentionally not supported.

To make `--seed auto` "always work", a recipe must (a) mint inserts whose key is
unique for *any* ordinal — the planner passes an ever-growing `i`, so key
spaces must be large (integer keys, or wide CHAR keys via `base36`; don't insert
into a tiny CHAR key), and (b) key UPDATE/DELETE targets off the seeded row
*position* (`target(i, existing)`), which the planner keeps in range.

The generic shape/ops/count planning lives in
`tabsdatax.itest.tdksystems.changes`; a recipe only supplies model knowledge:

```python
from tabsdatax.itest.tdksystems.changes import ChangeModel, Entity, OpKind
from tabsdatax.itest.tdksystems.emit import Column

class _Widgets(Entity):
    table = "widgets"
    columns = [Column("widget_id", "int", auto=True), Column("name", "varchar(40)")]
    supports = (OpKind.INSERT, OpKind.UPDATE, OpKind.DELETE)

    def new_row(self, rng, i):        # a brand-new, non-colliding row for INSERT #i
        return {"name": f"New Widget {i}"}
    def target(self, i, existing):    # PK predicate of the i-th seeded row (in PK order)
        return {"widget_id": i + 1}   # auto ids load 1..N, so row i is id i+1
    def mutate(self, rng, i, existing):  # column -> new value for an UPDATE
        return {"name": f"Renamed {i}"}

def recipe() -> ChangeModel:
    return ChangeModel(entities=[_Widgets()])   # list entities parents-first
```

Notes:

- List entities **parents-first**: a `multi-row-diff` transaction (and any
  cross-table insert) applies them in that order so foreign keys hold.
- Mark auto-increment / IDENTITY primary keys `Column(..., auto=True)` — inserts
  omit them and `target` reconstructs the seeded row's key from its 1-based load
  order. For an explicit key, give inserts a value in a **high, non-colliding
  range** (well above the largest `--data` size).
- Restrict `supports` to what's safe: a table other tables reference is unsafe
  to delete from; a leaf table is the natural delete target. Deletes walk the
  tail (so a re-run targets rows already gone), updates walk the front.
- Every INSERT must set all NOT NULL columns explicitly (don't rely on DB
  DEFAULTs) so the same statement runs on MySQL / Postgres / Db2 alike.

`employees`, `finance`, and `airportdb` ship recipes; see them for templates.

## Adding a new model

1. **Pick a name** that's a valid Python identifier (used as the model
   slug and in env-var prefixes).
2. **Decide the kinds** it supports — any subset of `sql`, `file`,
   `salesforce`. Put them in `sizing.json`'s `"kinds"` field.
3. **Drop `<name>/<sys>.ddl`** for each SQL system you want to support.
   Use a short table-name prefix (`cou_*`, `hp_*`, `rst_*`, …) so several
   models can coexist in one database.
4. **Write `<name>/generator.py`** exposing `main()` — see the contract
   above. (For non-`sql/file` kinds, the data path may be entirely
   manual — see `salesforce/generator.py` for a stub example.)
5. **Write `<name>/sizing.json`** with `kinds`, `prolific`,
   `prolific_base`, `defaults` (and optional `mins` / `caps`). Omit
   `prolific` for a fixed-data model.
6. **Smoke-test against an `fs` domain** (no Docker required):
   ```bash
   tdx-system create --type fs --sys target/local-fs.json --name lfs \
       --sys-path target/store -f
   tdx-domain create --sys target/local-fs.json --domain target/m.json \
       --name m --model <name> --data xs -f
   ls target/store/m
   ```
7. **Add a `changes.py`** (optional) if the model should support
   `tdx-domain changes` — see the "Change recipes" section above.
8. **Add to package_data** if you ship a new file kind — the existing
   `pyproject.toml` glob picks up `generator.py`, `changes.py`, `sizing.json`,
   `*.ddl`, `*.csv`, `*.json`, `*.yml`, `*.md`. Then `pip install -e .` to
   refresh.

The existing models are good templates — pick the one whose shape
matches what you're building.
