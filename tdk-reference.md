<!--
Copyright 2026 Tabsdata Inc.
-->

# `tdk` CLI — proposed reference

Proposed command surface after the intuitiveness pass:

- **Identifiers are named options** — every command takes its target via `--name NAME` (entities) or `--plan`/`--trx`/`--fnrun`/`--worker`/`--user` (execution & access); nothing is positional.
- **Full canonical names + short aliases**: `collection`/`col`, `function`/`fn`, `transaction`/`trx`.
- **Execution split into entity subgroups** (`plan`, `transaction`, `fnrun`, `worker`).
- **Consistent flags**: `--enabled/--disabled`, `-y/--yes` for destructive ops, `-o/--format {table,json,yaml}`.
- **Pinned-aware defaults**: `--proj` → pinned project, `--col` → pinned collection.
- `role` and `stage` groups removed; `cron` is back alongside a matching `stream`.

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
  cron                   Cron-triggered functions
  stream                 Stream-publisher functions
  table                  Inspect and download function output tables

Execution
  plan                   Execution plans (a triggered DAG run)
  transaction (trx)      Transactions within a plan
  fnrun                  Function runs within a transaction
  worker                 Worker processes and their logs

Ops & access
  metrics                Operational metrics
  permission             Grant/revoke user permissions
  connection             Inspect connector types & build connection docs (offline)
  set / unset            Persistent CLI settings (pins, server, hints, prompting,
                         default profile)
  settings               Show the persistent CLI settings

Help & misc
  shell                  Interactive shell
  examples               Generate example projects
  info / about / version

Global options
  -h, --help             Show help
      --version          Show version
      --no-prompt        Never prompt; fail if input is required. `tdk set
                         --prompt false` does the same for every invocation.
      --full             With --help: print the full help for every command and
                         subcommand. Pair with --format text|json (default: text),
                         e.g. `tdk --help --full --format json`.
```

tdk refuses to run inside AI agents — use the Tabsdata MCP server instead. Set
`TDK_IN_AGENT=1` to override (see Environment).

## Common options

Applied where shown in each command. Every value option takes an argument.

```
  -p, --proj   PROJECT            Target project. Default: pinned project; a
                                  command that needs one and has neither prompts
                                  for it before any other value.
  -c, --col    COLLECTION         Target collection. Default: pinned collection.
  -o, --format {table|json|yaml}  Output format. Default: table.
      --profile NAME              Profile whose connection and settings the
                                  command uses (`<NAME>-connection.json`,
                                  `<NAME>-options.json`). Default: the profile
                                  from `tdk set --profile`, else the bare files.
                                  On every command; see Profiles.
      --max-result N              Maximum rows a `list` command shows. Default: 100.
                                  0 lists everything. When rows are left over,
                                  the command says so (on stderr under
                                  --format json|yaml, so stdout stays parseable).
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
function list                 [--proj PROJECT] [--col COLLECTION] [-o]
                              #   cron-triggered / stream-publisher functions: see `tdk cron|stream list`
function trigger  --name NAME [--proj PROJECT] [--col COLLECTION] [--plan-name N] [--detach]
function enable   --name NAME [--proj PROJECT] [--col COLLECTION]
function disable  --name NAME [--proj PROJECT] [--col COLLECTION]
                              #   already in that state: reports it, changes nothing
function patch    --name NAME [--proj PROJECT] [--col COLLECTION] --fn-version REF [--path PATH] [--description D]
                              [--dir-to-bundle DIR]
                              #   (--requirements / --local-pkg are temporarily disabled)
function patches  --name NAME [--proj PROJECT] [--col COLLECTION] [-o]      # was: list-patches
```

## `tdk cron` / `tdk stream`

The two kinds of orchestrator-managed trigger: a function registered with a
`CronTrigger` gets a cron trigger, a `@stream_publisher` gets a stream one. Each
`list` is the worker listing narrowed to that trigger type and `plans` the plan
listing narrowed to it; `enable` / `disable` are `tdk fn enable` / `tdk fn
disable` restricted to functions that have the matching trigger — a function
without one is refused, naming `tdk fn` instead — and, like them, do nothing when
the function is already in that state.

```
cron   list                 [--proj PROJECT] [--coll COLLECTION] [--name FUNCTION] [--all-projects] [-o]
cron   plans                [--proj PROJECT] [--coll COLLECTION] [--name FUNCTION] [--all-projects] [-o]
cron   enable  --name NAME  [--proj PROJECT] [--coll COLLECTION]
cron   disable --name NAME  [--proj PROJECT] [--coll COLLECTION]
stream list                 [--proj PROJECT] [--coll COLLECTION] [--name FUNCTION] [--all-projects] [-o]
stream plans                [--proj PROJECT] [--coll COLLECTION] [--name FUNCTION] [--all-projects] [-o]
stream enable  --name NAME  [--proj PROJECT] [--coll COLLECTION]
stream disable --name NAME  [--proj PROJECT] [--coll COLLECTION]
```

`plans` is the other side of `list`: the execution plans the triggers actually
started, newest first, capped by `--max-result` like every listing. It is the
plan listing narrowed to the matching trigger type (`CT`/`ST`), so unlike the
`tdk plan` family it honors the pinned project rather than always spanning.

On both listings, `--coll` and `--name` are filters; `--coll` defaults to the
pinned collection and with none the listing spans the project. Like
`collection list` they can also span projects — with `--all-projects`, or when no
project is selected at all — and `list` then carries a Project column (`plans`
always shows one).

## `tdk table`

```
table list                 [--proj PROJECT] [--col COLLECTION] [--name PATTERN] [--at TS] [--at-trx TRX] [-o]
table schema   --name NAME  [--proj PROJECT] [--col COLLECTION] [--at TS|--at-trx TRX|--version V]
                            [--extra-cols none|meta|sys|all] [-o]
table versions --name NAME  [--proj PROJECT] [--col COLLECTION] [--details] [-o]
table data     --name NAME  [--proj PROJECT] [--col COLLECTION] [--sql SQL] [--len N] [--offset N]
                            [--at TS|--at-trx TRX|--version V] [--extra-cols none|meta|sys|all]
                            [--file FILE] [-o]
                            #   --sql: server-side SQL query over the table, which is the
                            #          only queryable relation and is named after itself
                            #   --len: defaults to 100 rows, cannot exceed 1000
                            #   --extra-cols: non-user columns to add: 'meta' the '@td.'
                            #          connector metadata, 'sys' the '$td.' system ones,
                            #          'all' both; defaults to 'none'
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

## `tdk set` / `tdk unset` / `tdk settings`

Persistent CLI settings, stored in `options.json` under the Tabsdata directory
(`~/.tabsdata`) and applied to every later invocation. `set` gives a setting a
value, `unset` drops what was stored so the default applies again, and `settings`
shows them all. Each takes any combination of its settings in one command.

```
set   [--hints true|false] [--prompt true|false] [--server SERVER]
      [--proj PROJECT] [--coll COLLECTION] [--profile NAME]
unset [--hints] [--prompt] [--server] [--proj] [--coll] [--profile] | --all
settings [-o]                                       # current settings
```

`unset --all` is shorthand for every flag but `--profile` at once, leaving every
setting of the profile at its shipped default.

| Setting | What it does |
|---|---|
| `--hints true\|false` | Show the hints the CLI adds to errors. Default: true |
| `--prompt true\|false` | Prompt for a missing value, or fail instead. Default: true |
| `--server SERVER` | Server `tdk login` uses without `--server` |
| `--proj PROJECT` | Default project of project-scoped commands |
| `--coll COLLECTION` | Default collection (requires a pinned project) |
| `--profile NAME` | Profile commands run under when given no `--profile` (see Profiles) |

`set --prompt false` is the standing form of the root `--no-prompt` flag: with
prompting off no command prompts, and one missing a required value fails, naming
the setting to flip back. The root flag does the same for a single invocation.

A collection pin only resolves under a project, so `unset --proj` unpins the
collection too, and switching to a different project drops a collection pinned
under the old one.

A server URL with no protocol defaults to `http://`, and one with no port to
port `2457` — so `set --server 127.0.0.1` means `http://127.0.0.1:2457`.

### Profiles

A profile is a separate login and settings store: `<NAME>-connection.json` and
`<NAME>-options.json` next to the bare `connection.json` / `options.json`, which
are the default profile. Every command takes `--profile NAME` to run under a
profile for that invocation, and `login --profile NAME` is how a profile's
connection comes to exist. `set --profile NAME` records NAME in `profile.json`
as the profile commands use when given no `--profile`, and stores the other
settings of the same command in it; `unset --profile` removes `profile.json`,
so the bare files apply again. `settings` names the profile it reports on.

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
```

`login` takes the server from `tdk set --server` when `--server` is
omitted, and prompts only when neither is set. The URL defaults to the `http://`
protocol and port `2457` when it carries neither. `login --profile NAME` stores
the session in the profile's own connection file (see Profiles).

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
