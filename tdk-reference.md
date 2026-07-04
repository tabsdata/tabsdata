# `tdk` CLI — proposed reference

Proposed command surface after the intuitiveness pass:

- **Identifiers are named options** — every command takes its target via `--name NAME` (entities) or `--plan`/`--trx`/`--run`/`--worker`/`--user` (execution & access); nothing is positional (except `hints {on|off}`).
- **Full canonical names + short aliases**: `collection`/`col`, `function`/`fn`, `transaction`/`trx`.
- **Execution split into entity subgroups** (`plan`, `transaction`, `run`, `worker`).
- **Consistent flags**: `--enabled/--disabled`, `-y/--yes` for destructive ops, `-o/--output {table,json,yaml}`.
- **Pinned-aware defaults**: `--proj` → pinned project, `--col` → pinned collection.
- `role`, `cron` and `stage` groups removed.

---

## `tdk`

```
tdk — Tabsdata client

Usage: tdk [GLOBAL OPTIONS] COMMAND [ARGS]...

Session
  login                  Log in to a Tabsdata server
  logout                 Log out and clear cached credentials
  status                 Show server status
  auth                   Manage certificates and change password

Work
  project                Manage projects
  collection  (col)      Manage collections
  group                  Manage user-groups within a project
  function    (fn)       Register, update and trigger functions
  table                  Inspect and download function output tables

Execution
  plan                   Execution plans (a triggered DAG run)
  transaction (trx)      Transactions within a plan
  run                    Function runs within a transaction
  worker                 Worker processes and their logs

Ops & access
  metrics                Operational metrics
  permission             Grant/revoke user permissions
  pin                    Pin a default project/collection locally
  connection             Inspect connector types & build connection docs (offline)

Help & misc
  shell                  Interactive shell
  examples               Generate example projects
  info / about / version
  hints {on|off}

Global options
  -h, --help             Show help
      --version          Show version
      --no-prompt        Never prompt; fail if input is required
```

## Common options

Applied where shown in each command. Every value option takes an argument.

```
  -p, --proj   PROJECT            Target project. Default: pinned project.
  -c, --col    COLLECTION         Target collection. Default: pinned collection.
  -o, --output {table|json|yaml}  Output format. Default: table.
  -y, --yes                       Skip the confirmation prompt (destructive ops).
  -h, --help                      Show help.
```

Required identifiers (`--name`, `--plan`, ...) are shown without brackets; `[...]` marks optional options.

`--password` (`login` / `auth password-change` / `user`) is long-only — it has no short flag, since `-p` is `--proj`.

---

## `tdk project` *(system-scoped — no `--proj`)*

```
project create --name NAME [--description D] [--groups g1,g2,...]
project update --name NAME [--new-name N] [--description D]
project delete --name NAME [-y]
project info   --name NAME
project list
```

## `tdk collection` (alias `col`)

```
collection create --name NAME [--proj PROJECT] [--description D] [--group G]
                              [--base-path PATH] [--conn-file FILE]
collection update --name NAME [--proj PROJECT] [--new-name N] [--description D]
                              [--group G] [--base-path PATH|-] [--conn-file FILE]
                              # '-' = inherit project base-path; --conn-file updates the connection
collection delete --name NAME [--proj PROJECT] [-y]
collection info   --name NAME [--proj PROJECT] [-o]
collection list               [--proj PROJECT] [-o]
```

## `tdk group`

```
group create --name NAME [--proj PROJECT] [--description D]
group update --name NAME [--proj PROJECT] [--new-name N] [--description D]
group delete --name NAME [--proj PROJECT] [-y]
group info   --name NAME [--proj PROJECT] [-o]
group list               [--proj PROJECT] [-o]
```

## `tdk function` (alias `fn`)

```
function register --path PATH [--proj PROJECT] [--col COLLECTION] [--description D] [--dir-to-bundle DIR]
                              [--requirements FILE] [--local-pkg PKG ...]
                              [--reuse-tables] [--enabled|--disabled] [--update] [--base-path PATH]
                              #   name is taken from the decorated function in the bundle
function update   --name NAME [--proj PROJECT] [--col COLLECTION] [--description D] [--path PATH]
                              [--dir-to-bundle DIR] [--requirements FILE] [--local-pkg PKG ...] [--reuse-tables]
                              [--enabled|--disabled] [--base-path PATH] [--reset-state] [--reset-stage]
function delete   --name NAME [--proj PROJECT] [--col COLLECTION] [-y]
function info     --name NAME [--proj PROJECT] [--col COLLECTION] [--version V] [--history] [-o]
                              #   --version: details of a specific version (default: current)
function bundle   --name NAME [--proj PROJECT] [--col COLLECTION] [--version V] --file FILE
                              #   downloads a function version's bundle (default: current version)
function list                 [--proj PROJECT] [--col COLLECTION] [--cron] [--stream] [-o]
function trigger  --name NAME [--proj PROJECT] [--col COLLECTION] [--plan-name N] [--detach]
function enable   --name NAME [--proj PROJECT] [--col COLLECTION]
function disable  --name NAME [--proj PROJECT] [--col COLLECTION]
function patch    --name NAME [--proj PROJECT] [--col COLLECTION] --patch-of REF [--path PATH] [--description D]
                              [--dir-to-bundle DIR] [--requirements FILE] [--local-pkg PKG ...]
function patches  --name NAME [--proj PROJECT] [--col COLLECTION] [-o]      # was: list-patches
```

## `tdk table`

```
table list                 [--proj PROJECT] [--col COLLECTION] [--name PATTERN] [--at TS] [--at-trx TRX] [-o]
table schema   --name NAME  [--proj PROJECT] [--col COLLECTION] [--at TS|--at-trx TRX|--version V] [-o]
table versions --name NAME  [--proj PROJECT] [--col COLLECTION] [--details] [-o]
table sample   --name NAME  [--proj PROJECT] [--col COLLECTION] [--sql SQL] [--len N] [--offset N]
                            [--at TS|--at-trx TRX|--version V] [--file FILE] [-o]
                            #   --sql: server-side SQL query over the sampled table
table download --name NAME  [--proj PROJECT] [--col COLLECTION] --file FILE [--at TS|--at-trx TRX|--version V]
table delete   --name NAME  [--proj PROJECT] [--col COLLECTION] [-y]
```

---

## Execution

### `tdk plan`

```
plan list                 [--proj PROJECT] [--col COLLECTION] [--status S] [--function F] [--last] [--at TS] [--monitor] [-o]
plan info    --plan PLAN   [--proj PROJECT] [-o]
plan graph   --plan PLAN   [--proj PROJECT] [-o]      # full execution graph
plan cancel  --plan PLAN   [--proj PROJECT] [-y]
plan recover --plan PLAN   [--proj PROJECT] [-y]
plan monitor --plan PLAN   [--proj PROJECT]
```

### `tdk transaction` (alias `trx`)

```
transaction list                [--proj PROJECT] [--col COLLECTION] [--status S] [--plan PLAN] [--last] [--at TS] [--monitor] [-o]
transaction info    --trx TRX   [--proj PROJECT] [-o]
transaction cancel  --trx TRX   [--proj PROJECT] [-y]
transaction recover --trx TRX   [--proj PROJECT] [-y]
transaction monitor --trx TRX   [--proj PROJECT]
```

### `tdk run` *(function runs)*

```
run list              [--proj PROJECT] [--col COLLECTION] [--status S] [--plan PLAN] [--function F] [--trx TRX] [-o]
run info    --run RUN  [--proj PROJECT] [-o]
run logs    --run RUN  [--proj PROJECT] [--file FILE]
run replay  --run RUN  [--proj PROJECT] [--col COLLECTION]      # was: exe replay --lfr
```

### `tdk worker`

```
worker list                  [--proj PROJECT] [--col COLLECTION] [--status S] [--plan PLAN] [--function F] [--run RUN] [--trx TRX] [-o]
worker logs --worker WORKER  [--proj PROJECT] [--file FILE]
```

---

## `tdk metrics`

```
metrics summary  [--proj PROJECT] [--dim collection|group] [--unit U] [--align A] [--from T] [--span S]
                 [--collection C ...] [--collection-exclude C ...]
                 [--group G ...] [--group-exclude G ...] [--status S ...] [-o]   # was: show, --json
metrics inflight [--proj PROJECT] [--collection C ...] [--collection-exclude C ...]
                 [--group G ...] [--group-exclude G ...] [--status S ...] [-o]
```

## `tdk connection` *(offline — no server, no `--proj`)*

```
connection types                            [-o]      # list available connector types
connection template --type TYPE [--file FILE]         # print/write a fill-in template
connection validate --file FILE                       # validate a connection document
```

## `tdk permission`

```
permission grant  --user USER --permission PERM [--proj PROJECT]   # --proj here = the grant's target project
permission revoke --user USER --permission PERM [--proj PROJECT]
permission list   [--user USER] [--proj PROJECT] [-o]
```

## `tdk pin`

```
pin set   [--proj PROJECT] [--col COLLECTION]
pin unset [--proj] [--col]
pin show
```

## `tdk auth`

```
auth info
auth add-cert        --server S --pem FILE
auth delete-cert     --server S
auth password-change --server S --old-password OLD --new-password NEW
```

## Root commands

```
login    [--server S] [--user U] [--password P]
logout
status
info     [--license] [--third-party] [--release-notes]
about
version
shell
examples [--dir D] [--guide]
hints    {on|off}
```
