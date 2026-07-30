# `tdk` CLI — proposed reference

Proposed command surface after the intuitiveness pass:

- **Identifiers are named options** — every command takes its target via `--name NAME` (entities) or `--plan`/`--trx`/`--fnrun`/`--worker`/`--user` (execution & access); nothing is positional (except `hints {on|off}`).
- **Full canonical names + short aliases**: `collection`/`col`, `function`/`fn`, `transaction`/`trx`.
- **Execution split into entity subgroups** (`plan`, `transaction`, `fnrun`, `worker`).
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
  fnrun                  Function runs within a transaction
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
      --full             With --help: print the full help for every command and
                         subcommand. Pair with --output text|json (default: text),
                         e.g. `tdk --help --full --output json`.

Note: tdk refuses to run inside AI agents — use the Tabsdata MCP server instead.
Set TDK_IN_AGENT=1 to override (see Environment).
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
project get    --name NAME
project list
```

## `tdk collection` (alias `col`)

```
collection create --name NAME [--proj PROJECT] [--description D] [--group G]
                              [--storage-path PATH] [--conn-file FILE]
collection update --name NAME [--proj PROJECT] [--new-name N] [--description D]
                              [--group G] [--storage-path PATH|-] [--conn-file FILE]
                              # '-' = inherit project storage path; --conn-file updates the connection
collection delete --name NAME [--proj PROJECT] [-y]
collection get    --name NAME [--proj PROJECT] [-o]
collection list               [--proj PROJECT] [-o]
```

## `tdk group`

```
group create --name NAME [--proj PROJECT] [--description D]
group update --name NAME [--proj PROJECT] [--new-name N] [--description D]
group delete --name NAME [--proj PROJECT] [-y]
group get    --name NAME [--proj PROJECT] [-o]
group list               [--proj PROJECT] [-o]
```

## `tdk function` (alias `fn`)

```
function register --path PATH [--proj PROJECT] [--col COLLECTION] [--description D] [--dir-to-bundle DIR]
                              [--reuse-tables] [--enabled|--disabled] [--update] [--storage-path PATH]
                              #   name is taken from the decorated function in the bundle
                              #   (--requirements / --local-pkg are temporarily disabled)
function update   --name NAME [--proj PROJECT] [--col COLLECTION] [--description D] [--path PATH]
                              [--dir-to-bundle DIR] [--reuse-tables]
                              [--enabled|--disabled] [--storage-path PATH] [--reset-state] [--reset-stage]
function delete   --name NAME [--proj PROJECT] [--col COLLECTION] [-y]
function get      --name NAME [--proj PROJECT] [--col COLLECTION] [--version V] [--history] [-o]
                              #   --version: details of a specific version (default: current)
function bundle   --fn-version FVID [--proj PROJECT] --file FILE
                              #   downloads a function version's bundle (a .tar.zst archive)
function list                 [--proj PROJECT] [--col COLLECTION] [--cron] [--stream] [-o]
function trigger  --name NAME [--proj PROJECT] [--col COLLECTION] [--plan-name N] [--detach]
function enable   --name NAME [--proj PROJECT] [--col COLLECTION]
function disable  --name NAME [--proj PROJECT] [--col COLLECTION]
function patch    --name NAME [--proj PROJECT] [--col COLLECTION] --fn-version REF [--path PATH] [--description D]
                              [--dir-to-bundle DIR]
                              #   (--requirements / --local-pkg are temporarily disabled)
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

All ids (plan, transaction, run, worker) are globally unique: the id-keyed
commands never take a project or collection. The four `list` commands always
span every project you can view (the pinned project does not narrow them);
every option is a filter, `--proj` included.

### `tdk plan`

```
plan list                 [--proj PROJECT] [--coll COLLECTION] [--fn FUNCTION] [--name PATTERN] [--status S ...]
                          [--last] [--at TS] [--monitor] [-o]
plan get     --plan PLAN   [-o]      # plan details + its function runs and workers
plan cancel  --plan PLAN   [--confirm cancel]
plan recover --plan PLAN
plan monitor --plan PLAN
plan logs    --plan PLAN   [--file FILE]   # pick a worker when several ran
```

### `tdk trx`

```
trx list                [--proj PROJECT] [--coll COLLECTION] [--status S ...] [--plan PLAN] [--plan-name PATTERN]
                        [--last] [--at TS] [--monitor] [-o]
trx get     --trx TRX   [-o]      # transaction details + its function runs and workers
trx cancel  --trx TRX   [--confirm cancel]
trx recover --trx TRX
trx monitor --trx TRX
trx logs    --trx TRX   [--file FILE]   # pick a worker when several ran
```

### `tdk fnrun` *(function runs)*

```
fnrun list                  [--proj PROJECT] [--coll COLLECTION] [--status S ...] [--plan PLAN] [--plan-name PATTERN]
                            [--fn FUNCTION] [--trx TRX] [-o]
fnrun get     --fnrun RUN  [-o]      # run details + its workers (retries)
fnrun logs    --fnrun RUN  [--file FILE]   # pick a worker when several ran
fnrun replay  --fnrun RUN
fnrun recover --fnrun RUN      # recovers the run's owning transaction
```

### `tdk worker`

```
worker list                  [--proj PROJECT] [--coll COLLECTION] [--status S ...] [--plan PLAN] [--plan-name PATTERN]
                             [--fn FUNCTION] [--fnrun RUN] [--trx TRX] [-o]
worker logs    --worker WORKER  [--file FILE] [-o]
worker recover --worker WORKER      # recovers the worker's owning transaction
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

## Environment

### AI-agent guard

tdk is a human-facing CLI and refuses to run inside an AI agent; agents should
drive Tabsdata through the **Tabsdata MCP server** instead. On startup — before
any argument is parsed, so even `tdk --help` / `tdk --version` are covered — tdk
checks for the marker environment variables that supported agents always set:

| Variable          | Set by      |
|-------------------|-------------|
| `CLAUDECODE`      | Claude Code |
| `GEMINI_CLI`      | Gemini CLI  |
| `CODEX_THREAD_ID` | Codex       |

If any is present (and non-empty), tdk prints

```
Tabsdata tdk command does not support running within an AI agent, use Tabsdata MCP instead
```

to stderr and exits with status `1`.

### `TDK_IN_AGENT`

Escape hatch for the guard above. When set to a truthy value (`1`, `true`, or
`yes`, case-insensitive) tdk prints a warning to stderr and runs anyway. Use it
only when you knowingly want to run tdk from within an agent shell — agents
should not set this override themselves.

```
TDK_IN_AGENT=1 tdk status
```
