<!--
Copyright 2026 Tabsdata Inc.
-->

# `tdk` CLI reference

The command surface of `tdk`, the Tabsdata client. `tdk --help --full` prints
the same from the installed CLI.

- **Identifiers are named options** — a command takes its target via `--name NAME` (entities) or `--plan`/`--trx`/`--fnrun`/`--worker`/`--user` (execution & access). Positional arguments are kept for free text and local inputs: a search query, a symbol, an example name or path, and the server of `auth password-change`.
- **Execution split into entity subgroups** (`plan`, `trx`, `fnrun`, `worker`).
- **Consistent flags**: `--enabled/--disabled`, `--confirm WORD` for destructive ops, `-o/--format {table,json,jsonb,yaml}`.
- **Pinned-aware defaults**: `--proj` → pinned project, `--coll` → pinned collection. The
  notice naming a pinned value goes to stderr, so `-o json|jsonb|yaml` stdout stays parseable.

---

## `tdk`

```
tdk — Tabsdata client

Usage: tdk [GLOBAL OPTIONS] COMMAND [ARGS]...

Session
  login                  Log in to a Tabsdata server
  logout                 Log out and clear cached credentials
  status                 Show server status
  auth                   Manage certificates, password and service tokens

Work
  project                Manage projects
  collection             Manage collections
  group                  Manage user-groups within a project
  fn                     Register, update and trigger functions
  cron                   Cron-triggered functions
  stream                 Stream-publisher functions
  table                  Inspect and download function output tables

Execution
  plan                   Execution plans (a triggered DAG run)
  trx                    Transactions within a plan
  fnrun                  Function runs within a transaction
  worker                 Worker processes and their logs

Ops & access
  metrics                Operational metrics
  user                   Manage users
  permission             Grant/revoke user permissions
  connection             Connector types, connection docs, the connection catalog
                         and probes
  set / unset            Persistent CLI settings (pins, server, hints, prompting,
                         default profile)
  settings               Show the persistent CLI settings
  ai                     Tabsdata AI (present when the AI agent package is installed)

Help & misc
  shell                  Interactive shell
  examples               Curated function examples and example projects
  docs                   SDK reference for the installed packages (offline)
  doctor                 Local development preflight
  info / about           License, third-party dependencies, release notes, build
                         metadata

Global options
  -h, --help             Show help
      --version          Show version
      --no-prompt        Never prompt; fail if input is required. `tdk set
                         --prompt false` does the same for every invocation.
                         Aborting a prompt (Ctrl+C, Esc, end of input) exits
                         with status 1.
      --full             With --help: print the full help for every command and
                         subcommand. Pair with --format text|json (default: text),
                         e.g. `tdk --help --full --format json`.
```

## Common options

Applied where shown in each command. Every value option takes an argument.

```
  -P, --proj   PROJECT            Target project. Default: pinned project; a
                                  command that needs one and has neither prompts
                                  for it before any other value.
  -c, --coll   COLLECTION         Target collection. Default: pinned collection.
  -o, --format {table|json|jsonb|yaml}
                                  Output format. Default: table. See Machine
                                  output for json and yaml against jsonb.
      --browse|--print            Open the table in a viewer that scrolls both
                                  ways, or print it and return. Default: --print
                                  (--browse for `table data`). Off a terminal the
                                  table is always printed.
      --wrap                      Show every value whole, wrapping long and
                                  multi-line values inside their column.
      --max-width N               Characters per column before a value is cut
                                  ('…'). Default: 48 (128 for `table data`); 0
                                  means no limit.
                                  These three come with every command that
                                  shows a table and apply only to `-o table`.
      --profile [NAME]            Profile whose login and pinned project and
                                  collection the command uses; with no NAME,
                                  the default profile. Default: the profile
                                  from `tdk set --profile`, else the default
                                  profile. On every command; see Profiles.
      --max-result N              Maximum rows a `list` command shows. Default: 100.
                                  0 lists everything. When rows are left over,
                                  the command says so (on stderr under
                                  --format json|jsonb|yaml, so stdout stays
                                  parseable).
      --filter FIELD:OP:VALUE     Server-side filter on a paged `list` (every one
                                  with --max-result), OP one of eq ne gt ge lt le
                                  lk btw is. Repeatable: conditions on different
                                  fields all hold, on the same field any one
                                  does. Combines with the command's own filters.
      --order-by FIELD[+|-]       Sort a paged `list`, ascending by default.
      --cursor CURSOR             Continue a paged `list` from the `next` cursor
                                  it returned; pass the same filters and order.
      --confirm WORD              Confirm a destructive op without the prompt by
                                  giving its word: `delete`, or `cancel` for
                                  `plan|trx cancel`; another word is a usage
                                  error (exit 2).
  -h, --help                      Show help.
```

Required identifiers (`--name`, `--plan`, ...) are shown without brackets; `[...]` marks optional options.

A listing the server refuses over a field it does not support (a `ListError`,
exit 1, `invalid_request`) carries a hint to check the fields in `--filter`
and `--order-by` only when the command was given one of them.

The only short flag a secret option takes is `-p`, for `--password` (`login` / `user`) and `--old-password` (`auth password-change`); `--proj` is `-P`. `--new-password` and `--token` are long-only (`-n` for `--new-password` is a deprecated alias).

---

## `tdk project`

```
project create  --name NAME [--description D] [--secrets-file FILE]
project update  --name NAME [--new-name N] [--description D] [--secrets-file FILE]
                # --secrets-file replaces all the project's secrets
project delete  --name NAME [--confirm delete]
project get     --name NAME [-o]
project list    [-o]
project secrets --name NAME [-o]   # secret names and keys, never values
project graph   [--proj PROJECT] [--topology-version V|--at TS] [-o]
project topology list [--proj PROJECT] [-o]
```

`create`, `update`, `delete`, `get`, `list` and `secrets` are system-scoped and
take no `--proj`; `graph` and `topology list` read one project.

- `graph` reads the project's whole graph at a topology version, or as it
  was at `--at`, the latest by default. The table view lists the functions and
  tables, each with its topology tags (`_parent`, ...), and the edges between
  them as `table flow` shows them; system tables are left out of it. The
  machine formats give the server's document: `version` (the topology version
  read), `at` (epoch ms), `all_functions` and `all_tables` keyed by version id
  (`collection`, `name`, `tags`, ...), `system_tables`, `user_tables`,
  `relations` (`function_version_id`, `table_version_id` and an `edge` that is
  `Output`, `Dependency` or `Trigger`) and the current `collections` and
  `groups` labels.
- `topology list` is a paged listing of the project's topology versions: `id`,
  `valid_from` and `valid_to` (epoch ms; `valid_to` null for the open
  version), oldest first. `--filter` takes `id` and `valid_from`; `--order-by`
  takes `valid_from`.

`--secrets-file` takes a YAML file (`-` reads stdin) mapping each secret to its
keys and each key to its value:

    aws:
      key1: mykey1               # the value itself ("" stores an empty value)
      key2: $secret:AWS_KEY2     # read from the AWS_KEY2 environment variable
      key3:                      # no value: keep the stored one (empty if none)

A secret or key left out of the file is removed; an empty file removes them
all. Functions read them through `ctx.secret("aws", "key1")` or
`ctx.secret("aws.key1")`.

## `tdk collection`

```
collection create --name NAME [--proj PROJECT] [--description D] [--group G]
                              [--storage-path PATH] [--conn-file FILE]
                              # a source/destination collection is created with its connection
collection update --name NAME [--proj PROJECT] [--new-name N] [--description D]
                              [--storage-path PATH|-] [--conn-file FILE]
                              # '-' = inherit project storage path; --conn-file updates the connection
collection delete --name NAME [--proj PROJECT] [--confirm delete]
collection get    --name NAME [--proj PROJECT] [-o]
                              # with its `tags`
collection list               [--proj PROJECT] [--all-projects] [-o]
```

`--conn-file` takes a `connectionDef` YAML document (`-` reads stdin); `tdk
connection template` writes one to fill in. A collection's connection and its
catalog are under `tdk connection`.

## `tdk group`

```
group create --name NAME [--proj PROJECT] [--description D]
group update --name NAME [--proj PROJECT] [--new-name N] [--description D]
group delete --name NAME [--proj PROJECT] [--confirm delete]
group get    --name NAME [--proj PROJECT] [-o]   # with its `tags`
group list               [--proj PROJECT] [-o]
```

## `tdk user` *(system-scoped — no `--proj`)*

```
user create --name NAME [--full-name F] [--email E] [--password P] [--disabled]
user update --name NAME [--full-name F] [--email E] [--password P] [--enabled true|false]
user delete --name NAME [--confirm delete]
user get    --name NAME [-o]
user list               [-o]
```

## `tdk fn`

```
fn register --path FILE::FUNCTION [--proj PROJECT] [--coll COLLECTION] [--description D] [--dir-to-bundle DIR]
                        [--requirements FILE] [--local-pkg DIR]... [--reuse-tables]
                        [--enabled|--disabled] [--update] [--storage-path PATH] [--dry-run]
                        #   name is taken from the decorated function in the bundle
fn update   --name NAME [--proj PROJECT] [--coll COLLECTION] [--description D] [--path PATH]
                        [--dir-to-bundle DIR] [--requirements FILE] [--local-pkg DIR]... [--reuse-tables]
                        [--enabled|--disabled] [--storage-path PATH] [--reset-state] [--reset-stage]
                        [--dry-run]
                        #   --dry-run: `fn check --coll` on the same flags plus what would be done;
                        #   nothing is uploaded
fn check    --path PATH [--dir-to-bundle DIR] [--requirements FILE] [--local-pkg DIR]...
                        [--proj PROJECT] [--coll COLLECTION] [--name NAME] [-o]
                        #   imports the function as register does, builds its bundle and flags
                        #   eval/exec/subprocess-style calls; with --coll, also checks the
                        #   collection, the input tables, output clashes and the Python version
fn delete   --name NAME [--proj PROJECT] [--coll COLLECTION] [--confirm delete]
fn get      --name NAME [--proj PROJECT] [--coll COLLECTION] [--show-history] [-o]
                        #   with its `tags` and, for a child, its `parent` (function_id, collection, name)
fn bundle   --fn-version FVID | --name NAME [--coll COLLECTION] [--proj PROJECT]
                        --file FILE | --extract DIR [--force] | --print [-o]
                        #   downloads a function version's bundle (a .tar.zst archive), unpacks
                        #   it (into a new or empty DIR unless --force), or prints the module
                        #   defining the function; --name reads the function's current version
fn list                 [--proj PROJECT] [--coll COLLECTION] [-o]
                        #   --coll, else the pinned collection, else every collection of the project
                        #   cron-triggered / stream-publisher functions: see `tdk cron|stream list`
fn trigger  --name NAME [--proj PROJECT] [--coll COLLECTION] [--plan-name N]
            [--wait [--timeout S] | --no-wait]
                        #   --wait waits for the plan (see Waiting), --no-wait returns it at once;
                        #   with neither, `tdk set --trigger-wait` decides (default: true, waits)
fn enable   --name NAME [--proj PROJECT] [--coll COLLECTION]
fn disable  --name NAME [--proj PROJECT] [--coll COLLECTION]
                        #   already in that state: reports it, changes nothing
fn patch    --name NAME [--proj PROJECT] [--coll COLLECTION] --fn-version REF [--path PATH] [--description D]
                        [--dir-to-bundle DIR] [--requirements FILE] [--local-pkg DIR]... [--dry-run]
fn patch    --name NAME [--proj PROJECT] [--coll COLLECTION] --fn-version REF --source FILE
                        [--add-package PKG... | --requirements FILE] [--local-pkg DIR]...
                        [--description D] [--dry-run]
                        #   --source replaces only the module defining the function in the bundle
                        #   of --fn-version, keeping its sibling modules, requirements.yaml and
                        #   local packages; --add-package replaces the pin of the same package
fn patches  --name NAME [--proj PROJECT] [--coll COLLECTION] [-o]      # was: list-patches
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
table list                 [--proj PROJECT] [--coll COLLECTION] [--name PATTERN]
                            [--at TS|--at-trx TRX] [-o]
table schema   --name NAME  [--proj PROJECT] [--coll COLLECTION] [--at TS|--at-trx TRX|--version V]
                            [--extra-cols none|meta|sys|all] [-o]
table versions --name NAME  [--proj PROJECT] [--coll COLLECTION] [-o]
table data     --name NAME  [--proj PROJECT] [--coll COLLECTION] [--sql SQL] [--len N] [--offset N]
                            [--at TS|--at-trx TRX|--version V] [--extra-cols none|meta|sys|all]
                            [--file FILE] [--browse|--print] [--wrap] [--max-width N] [-o]
                            #   --sql: server-side SQL query over the table, which is the
                            #          only queryable relation and is named after itself
                            #   --len: defaults to 100 rows, cannot exceed 1000
                            #   --extra-cols: non-user columns to add: 'meta' the '@td.'
                            #          connector metadata, 'sys' the '$td.' system ones,
                            #          'all' both; defaults to 'none'
                            #   --browse: the default; on a terminal the table opens in a
                            #          viewer that scrolls both ways ('q' or 'esc'
                            #          quits), each cell showing its first line and
                            #          '…' opening the whole value
                            #   --print: print the table and return instead, each line
                            #          cut at the terminal width ('…')
                            #   --wrap: show every value whole, wrapping long and
                            #          multi-line values inside their column
                            #   --max-width: characters per column before a value is
                            #          cut, 128 by default; 0 means no limit
                            #   the view options apply only to '-o table'
table download --name NAME  [--proj PROJECT] [--coll COLLECTION] --file FILE [--at TS|--at-trx TRX|--version V]
                            #   fails (exit 1, partial file removed) once the table outgrows the
                            #   room in FILE's directory: its free space less 16 MiB, at most the
                            #   process's file size limit (RLIMIT_FSIZE)
                            # schema, data, download and profile fail with not_found (exit 5)
                            # when no committed version holds data at the point asked for (none
                            # yet, its run still going or failed, or only runs without data); an
                            # empty table is a version with data and gives zero rows
table delete   --name NAME  [--proj PROJECT] [--coll COLLECTION] [--confirm delete]
table stats    --name NAME  [--proj PROJECT] [--coll COLLECTION] [--at TS|--at-trx TRX|--version V] [-o]
table profile  --name NAME  [--proj PROJECT] [--coll COLLECTION] [--at TS|--at-trx TRX|--version V]
                            [--columns A,B] [--approx [--sample-rows N]] [-o]
table flow     --name NAME  [--proj PROJECT] [--coll COLLECTION] [--topology-version V|--at TS]
                            [--direction up|down|both] [--depth N] [--include-system] [-o]
table diff     --name NAME  [--proj PROJECT] [--coll COLLECTION] [--max-events N] [-o]
table diff     --name NAME  [--proj PROJECT] [--coll COLLECTION] --from V [--to V] [-o]
table diff     --name NAME  [--proj PROJECT] [--coll COLLECTION] --against [COLLECTION/]TABLE [--version V] [-o]
```

`--version` takes a data version id or `HEAD`, `HEAD^`, `HEAD~N`, counted back
over the committed versions.

- `list` lists the tables of `--coll`, else of the pinned collection, else the
  current tables of every collection in the project, with a Collection column
  (never prompted for; a listing a pinned collection narrows says so on stderr).
  `list --at`/`--at-trx` lists the tables of one collection as they were at
  that instant, so it needs one.
- `stats` gives the pinned committed version (the newest by default), its
  `row_count`, `data_bytes`, `column_count` and `schema`.
- `profile` gives each column's `non_null`, `null_rate` and `distinct`, and
  `min`/`max` (numbers, strings, temporals) and `mean` (numbers); nested and
  binary columns get `non_null` only. It is exact by default: the server
  aggregates the whole version, 32 columns per query. With `--approx`, or when
  the server refuses the scan (`reason`: `scan_cap`, `timeout` or `busy`), it
  profiles `--sample-rows` rows (default 1000, at most 10000) read in windows
  of up to 1000 at random offsets, one per equal slice of the table, and says
  `approximate` with the `sample_size` unless the sample held every row.
- `flow` walks the table's flow graph: `nodes` (tables and functions, ids
  `t:`/`f:`), `edges` (`output` from a function to the table it writes,
  `dependency` or `trigger` from a table to a function that reads it, with
  `trigger` true when a dependency also triggers), and the `upstream` and
  `downstream` tables with their `distance` in edges. `--depth` counts edges;
  system tables are left out unless `--include-system`. The graph is the
  latest unless `--topology-version` (see `tdk project topology list`) or
  `--at` pins it.
- `diff` with neither `--from` nor `--against` lists the schema changes over
  the committed versions, newest first: `events` with the `version_id` that
  introduced each schema and its `added`, `removed` and `retyped` columns;
  `capped` is true when older changes exist than `--max-events`. `--from`
  compares two versions, `--against` another table (its newest version).

## Tags (`tag` under `project`, `collection`, `group`, `fn`, `table`)

```
project tag list                     [--proj PROJECT] [-o]
project tag set                      [--proj PROJECT] --key KEY [--value VALUE]
project tag delete                   [--proj PROJECT] --key KEY
project tag keys                     [--proj PROJECT] [-o]
project tag values                   [--proj PROJECT] --key KEY [-o]
collection|group tag list   --name NAME [--proj PROJECT] [-o]
collection|group tag set    --name NAME [--proj PROJECT] --key KEY [--value VALUE]
collection|group tag delete --name NAME [--proj PROJECT] --key KEY
fn|table tag list           --name NAME [--proj PROJECT] [--coll COLLECTION] [-o]
fn|table tag set            --name NAME [--proj PROJECT] [--coll COLLECTION] --key KEY [--value VALUE]
fn|table tag delete         --name NAME [--proj PROJECT] [--coll COLLECTION] --key KEY
                            #   `list` shows the `_`-prefixed tags the system sets (kind
                            #   Topology or System) and user tags; `project tag list` lists the
                            #   tags of every entity in the project, with the entity type
                            #   (P/G/C/F/T) and id of each. `set` / `delete` act on user tags only.
                            #   `project tag keys` lists the keys open on the project's entities
                            #   and `project tag values` the values one key is open with, sorted,
                            #   at most 1000 of each, in the list envelope.
```

Listings also take tag filters beside their column filters,
`<entity>.<key>:eq:<value>`, `:ne:<value>`, `:lk:<pattern>` and
`:is:null` / `:is:not_null`, where `<entity>` is `project`, `group`,
`collection`, `function` or `table`: on `fn list`, `function.team:eq:a` keeps
the functions tagged `team=a`; on the run lists it keeps the runs of those
functions. A list accepts the entities whose ids its rows carry (its API
schema marks those fields `tags`); the same entity and key repeated is OR,
otherwise AND. `:ne:` and `:lk:` match only rows whose entity carries the
key, so an untagged row matches neither. Tag filters test current tags,
except on `table list --at` / `--at-trx`, which lists the tables and tests
the tags of that instant. System keys start with `_`, like `_parent`.
`project tag keys` and `project tag values` (the SDK's `list_tag_keys()` and
`list_tag_values(key)`) list the keys and values in use.

---

## Execution

All ids (plan, transaction, run, worker) are globally unique: the id-keyed
commands never take a project or collection. The four `list` commands always
span every project you can view (the pinned project does not narrow them);
every option is a filter, `--proj` included.

`--plan`, `--trx`, `--fnrun`, `--worker` and `--attempt`, as the key of a
command or as a listing's filter, and the `--at-trx` of `table`,
take an id: the 26-character form or the
logical one (`ep20260930-1` for a plan, `tx…` a transaction, `fr…` a function
run, `pfr…` an attempt, `wkr…` a worker). Anything else, a plan name or a list
index, is a usage error (exit 2) before any request, `'--plan daily' is not an
id.`, with a hint naming where the id comes from (`tdk plan list`, `tdk trx
list`, `tdk fnrun list`, `tdk worker list`; for `--attempt`, a replay's
`new_function_run_id`).

### `tdk plan`

```
plan list                 [--proj PROJECT] [--coll COLLECTION] [--fn FUNCTION] [--name PATTERN]
                          [--status S ... | --health H ...] [--last] [--at TS] [--monitor [--timeout S]] [-o]
plan get     --plan PLAN   [--wait [--timeout S]] [-o]      # plan details + its function runs and workers
plan cancel  --plan PLAN   [--confirm cancel]
plan recover --plan PLAN   [--wait [--timeout S]]
plan monitor --plan PLAN   [--timeout S]
plan logs    --plan PLAN   [--all [--max-result N]] [--tail N] [--failed-only] [--file FILE] [-o]
plan diagnose --plan PLAN  [--max-runs N] [--log-lines N] [--no-logs] [-o]
```

### `tdk trx`

```
trx list                [--proj PROJECT] [--coll COLLECTION] [--status S ... | --health H ...] [--plan PLAN]
                        [--plan-name PATTERN] [--last] [--at TS] [--monitor [--timeout S]] [-o]
trx get     --trx TRX   [--wait [--timeout S]] [-o]      # transaction details + its function runs and workers
trx cancel  --trx TRX   [--confirm cancel]
trx recover --trx TRX   [--wait [--timeout S]]
trx monitor --trx TRX   [--timeout S]
trx logs    --trx TRX   [--all [--max-result N]] [--tail N] [--failed-only] [--file FILE] [-o]
```

### `tdk fnrun` *(function runs)*

```
fnrun list                  [--proj PROJECT] [--coll COLLECTION] [--status S ... | --health H ...] [--plan PLAN]
                            [--plan-name PATTERN] [--fn FUNCTION] [--trx TRX] [-o]
fnrun get     --fnrun RUN  [--wait [--attempt ATTEMPT] [--timeout S]] [-o]      # run details + its workers (retries)
fnrun logs    --fnrun RUN  [--all [--max-result N]] [--tail N] [--file FILE] [-o]
fnrun diagnose --fnrun RUN [--log-lines N] [--no-logs] [-o]
fnrun replay  --fnrun RUN  [--wait [--timeout S]]      # --wait waits for the new attempt
fnrun recover --fnrun RUN  [--wait [--timeout S]]      # recovers the run's owning transaction
```

### `tdk worker`

```
worker list                  [--proj PROJECT] [--coll COLLECTION] [--status S ... | --health H ...] [--plan PLAN]
                             [--plan-name PATTERN] [--fn FUNCTION] [--fnrun RUN] [--trx TRX] [-o]
worker logs    --worker WORKER  [--tail N] [--file FILE] [-o]
worker recover --worker WORKER [--wait [--timeout S]]      # recovers the worker's owning transaction
```

The `plan|trx|fnrun|worker list` listings span every project you can view; `--proj` narrows them. `--status` takes
the entity's own statuses and refuses any other value as a usage error that names `--health`. `--health` (`error`,
`warning`, `running`, `ok`, repeatable, not together with `--status`) matches every status of that group, the
`health` each listed row carries: what failed is `fnrun list --health error`, a plan that needs action
`plan list --health warning` (Stalled) or `--health error` (Unexpected).

`cancel` and `recover` report `{"done": {..., "changed": bool}}`: `changed` is false when no run was in a state the
action applies to (a plan that already finished, nothing recoverable), and the message says so instead of
claiming success, in the table view and as the envelope's `message` (`{"done": {..., "changed": false,
"message": "Nothing to cancel: every run of plan 'P1' had already ended."}}`); with `--wait`, nothing is awaited
then.

The `plan|trx|fnrun logs` commands show one worker's log, picked from the
listing when several ran (with prompting off, they fail naming `--all`). `--all`
shows every worker's, newest first, at most `--max-result` (default 20, 0 for
all); `--failed-only` keeps the workers whose health is `warning` or `error`;
`--tail N` keeps the last N lines of each log. A worker that did not succeed
carries the error signature of its log (category, code, message and the
user-code frame); a log you may not read (it takes `DevelopProject`) is
reported in `log_unavailable` instead of failing the command.

`plan diagnose` and `fnrun diagnose` say why a plan or a run did not succeed:
the plan or run with its annotated status, a `summary` of its runs by health,
the `failed_runs` (failed and retrying, first failure first, at most
`--max-runs`, default 5) and the `held_runs`, the `root_cause` and `truncated`
(`{failed_runs_shown, failed_runs_total}`). A failed run carries its repair
handles (`function_version_id`, `ran_patch_id`, `physical_function_run_id`,
`transaction_id`, `can_replay`, `can_recover`, and the `argv` to replay, recover
and patch it; a retrying run gets none), its latest worker, `attempts`, the
`signature` of that worker's log and a `log_excerpt` of its last `--log-lines`
lines, headed by the traceback when it lies earlier. A held run names the failed
runs upstream that block it in `blocked_by`, and `blocked_by_outside_plan` when
the chain leaves the plan. The root cause is the earliest failure's signature,
its `category` (`missing_dependency`, `schema_mismatch`, `auth`, `connectivity`,
`resource`, `user_code`, `platform` or `unknown`, else `on_hold`) and a `hint`.
`--no-logs` diagnoses from statuses alone. They exit 0 whenever the diagnosis is
produced, whatever the verdict; a plan or run that is not found exits 5.

### Waiting

`fn trigger --wait` (or with neither `--wait` nor `--no-wait` while `tdk set
--trigger-wait` is true, its default),
`plan|trx monitor`, `plan|trx get --wait`, the `recover`
commands with `--wait`, `fnrun replay --wait`, `fnrun get --wait` and `plan|trx
list --monitor` poll until what they wait for is final, backing off from one
second to five. The wait lasts `--timeout` seconds, without limit when it is
not given. The expiry of a `TDK_BEARER` session caps every wait (5 seconds
before it); a cap that applies is reported on stderr. No request reads for
longer than the time left. `--timeout` (or `--attempt`) on a command that does
not wait, without `--wait` or with `--no-wait`, is a usage
error: `'--timeout' applies only while waiting.`; on `plan|trx list` it needs
`--monitor`.

- Out of time: exit 7 (`wait_timeout`). The hint names the command that
  continues the wait, e.g. `tdk plan get --plan <id> --wait`, with the
  `--profile` the wait ran under (a bare `--profile` when that was the default
  profile while `set --profile` records another); it never triggers or
  recovers again.
- Finished with a function run that did not succeed (a status whose health is
  `warning` or `error`): exit 8 (`run_failed`), with a hint naming `tdk fnrun
  diagnose --fnrun <id>`. A status tdk
  does not know is not a failure; a run waited on in such a status goes on
  until the wait runs out of time.

A waited plan or transaction is rendered annotated, with its `runs` and the
`waited_ms` it took; `get --wait` adds `waited_ms` to the `get` document.
`fnrun replay --wait` and `fnrun get --wait --attempt ATTEMPT` wait for an
attempt (a physical function run id, the replay's `new_function_run_id`) to
end: Done, Error and Failed all end it, and Error or Failed exit 8. When a
newer attempt replaces it, the wait ends with `conflict` (exit 6), naming the
current attempt. `ATTEMPT` is a `pfr<YYYYMMDD>-<n>` id (or its 26-character
form); anything else is a usage error (exit 2) with a hint on where attempts
come from.
`fnrun replay --wait` gives the run with the replay result under `replay`.

`connection check|refresh|sample` wait unless `--no-wait`, and `connection probe
--wait` waits; these waits last `--timeout` seconds, at most 300, 60 when it is
not given, capped the same way. Each request holds for up to 55 seconds on the
server until the probe ends. Out of time exits 7, with a hint naming `tdk
connection probe --probe <id> --wait --proj <project>`; a probe that ended
`failed`, `timeout` or `cancelled` exits 8.

---

## `tdk metrics`

```
metrics show     [--proj PROJECT | --all-projects] [--dim collection|group] [--unit U] [--align A] [--from T] [--span S]
                 [--collection C ...] [--collection-exclude C ...]
                 [--group G ...] [--group-exclude G ...] [--status S ...] [-o]
metrics inflight [--proj PROJECT | --all-projects] [--collection C ...] [--collection-exclude C ...]
                 [--group G ...] [--group-exclude G ...] [--status S ...] [-o]
```

Collections and groups are given by id.

With no project selected (or with `--all-projects`), `metrics show` and `metrics inflight`
cover every project the caller can view, from the server's cross-project routes, and the
tables gain a Project column.

The `metrics show` table has one row per node and function run status: `Runs`, `Tables out`,
`Rows out`, `Bytes out`, `Tables in` and `Rows in`; tables count table versions. External
sources and destinations are labelled as such. `-o json|yaml` emits the server
response verbatim, edges included. It is one total for the window: for a per-day
series run one `--align rolling --span 1 --unit day` window per day, each with `--from`
at the day's end (a calendar window starts at the boundary holding `--from`, so a
`--from` on a boundary is empty, and the error says so).

## `tdk connection`

```
connection types                            [-o]      # list available connector types (offline)
connection template --type TYPE [--file FILE]         # print/write a fill-in template (offline)
connection validate --file FILE                       # validate a connection document (offline)
connection list     [--proj PROJECT | --all-projects] [--type TYPE] [--mode src|dest]
                    [--status STATUS] [--max-result N] [--filter F] [--order-by F] [--cursor C] [-o]
connection get      --name COLLECTION [--proj PROJECT] [--definition] [-o]
connection entities --name COLLECTION [--proj PROJECT] [--fields]
                    [--max-result N] [--filter F] [--order-by F] [--cursor C] [-o]
connection check    --name COLLECTION [--proj PROJECT] [--no-wait] [--timeout S] [-o]
connection check    --conn-file FILE [--metadata] [--sample FQN [--len N]] [-o]   # offline
connection refresh  --name COLLECTION [--proj PROJECT] [--no-wait] [--timeout S] [-o]
connection sample   --name COLLECTION --entity FQN [--proj PROJECT] [--len N]
                    [--no-wait] [--timeout S] [-o]
connection probe    --probe PROBE [--proj PROJECT] [--wait [--timeout S]] [-o]
```

A connection belongs to one source or destination collection and is named by
it; `--name` defaults to the pinned collection. The server keeps a catalog per
connection: the last check, the entities (tables, files, topics) the last
refresh found and their schema fingerprint.

- `list` gives the list envelope of `{"connection_id", "project", "collection",
  "kind", "collection_type", "check_status", "checked_on",
  "metadata_status", "metadata_on", "entity_count", "schema_hash", "truncated",
  "compatible"}`. `kind` is the connection's Conn class (`module:qualname`);
  `--type` takes a connector type and filters on its class. It narrows to the
  pinned or `--proj` project like the other listings.
- `get` gives that document plus `check_error`, `metadata_error` and
  `last_probe_id`. `--definition` adds the dehydrated
  connection document under `definition` (no secret values or names); it needs
  a user session with `manage_project`.
- `compatible` is computed by tdk from the installed connectors, never by the
  server: `{"types", "publishers", "subscribers"}` that take the connection's
  class.
- `entities` gives the list envelope of `{"id", "fqn", "entity_kind",
  "field_count", "doc"}`; `doc`, the explorer's entity with its fields, only
  with `--fields`. A file's fields are its columns, read from the file's head
  at refresh (CSV header and first lines, NDJSON first lines, the Parquet
  footer, the Avro header); files of a folder whose newest files agree share
  that schema, a folder whose newest files differ has each file read for its
  own, and `doc.schema_from` names the file it was read from. Match
  names with `--filter fqn:lk:GLOB` (`*` is any text).
  Listing entities, checking, refreshing and sampling need `develop_project`.
- `check`, `refresh` and `sample` run a probe as a server-side job: a `check`,
  a `metadata` probe that replaces the entity catalog, or a `sample` of an
  entity already in the catalog (404 `entity_not_in_catalog` otherwise; at
  most 100 rows and 1 MiB, kept ten minutes, audit-logged). A submission equal
  to one still queued or running joins it. Too many submissions are refused
  with `rate_limited` (exit 1). They wait for the probe (see
  [Waiting](#waiting)) and give it as `{"probe_id", "connection_id",
  "collection", "kind", "status", "requested_by_id", "requested_on",
  "started_on", "ended_on", "error", "result", "expires_on", "waited_ms"}`;
  `--no-wait` gives it as submitted. A check that ran and could not connect is
  a success with `result: {"ok": false, "error"}`. A waited `sample` gives
  `{"probe_id", "expires_on", "waited_ms", "entity", "columns": [{"name"}],
  "rows", "row_count", "truncated"}`, or `{"probe_id", "expires_on",
  "waited_ms", "supported": false}` for a connector that cannot read rows.
- `check --conn-file` runs the installed connector's explorer here against a
  connection document, `$secret:` references resolved from the environment,
  with no server: `{"ok", "error"}`, plus `entity_count` and the first 20
  `entities` with `--metadata` and a `sample` (as above, without the probe
  fields) with `--sample`. A connector that is not installed exits 2.
- `probe` reads a probe, and with `--wait` waits for it; it is the command a
  timed-out probe wait resumes with. `--probe` takes the id `check|refresh|sample`
  print; a collection name there is a usage error (exit 2) before any request,
  with a hint that the name goes to `--name` of those commands.

## `tdk permission`

```
permission grant  --user USER --permission PERM [--proj PROJECT]   # --proj here = the grant's target project
permission revoke --user USER --permission PERM [--proj PROJECT]
permission list   [--user USER] [--proj PROJECT] [-o]
```

## `tdk set` / `tdk unset` / `tdk settings`

Persistent CLI settings, applied to every later invocation. The pinned project
and collection are per profile: they belong to the profile the command runs
under, stored in its options file. Every other setting is global, one value for
all profiles, stored in `tdk-settings.json`. Both live under the Tabsdata
directory (`~/.tabsdata`; credentials and client certificates live there too).
`set` gives a setting a value, `unset` drops what was stored so the default
applies again, and `settings` shows the global ones and the pins of the
profile. Each takes any combination of its settings in one command.

```
set   [--hints true|false] [--prompt true|false] [--trigger-wait true|false]
      [--server SERVER] [--proj PROJECT] [--coll COLLECTION] [--profile NAME]
unset [--hints] [--prompt] [--trigger-wait] [--server] [--proj] [--coll] [--profile] | --all
settings [-o]                                       # current settings
```

`unset --all` is shorthand for every flag but `--profile` at once, leaving every
global setting and the pins of the profile at their shipped default.

| Setting | Scope | What it does |
|---|---|---|
| `--hints true\|false` | Global | Show the hints the CLI adds to errors. Default: true |
| `--prompt true\|false` | Global | Prompt for a missing value, or fail instead. Default: true |
| `--trigger-wait true\|false` | Global | Make `fn trigger` wait for its plan when given neither `--wait` nor `--no-wait`. Default: true. A machine run (agent mode) never reads it |
| `--server SERVER` | Global | Server `tdk login` uses without `--server` |
| `--proj PROJECT` | Per profile | Default project of project-scoped commands |
| `--coll COLLECTION` | Per profile | Default collection (requires a pinned project) |
| `--profile NAME` | Global | Profile commands run under when given no `--profile` (see Profiles) |

`set --prompt false` is the standing form of the root `--no-prompt` flag: with
prompting off no command prompts, and one missing a required value fails, naming
the setting to flip back. The root flag does the same for a single invocation,
and so does an unattended session (see `TDK_AGENT` under Environment).

A collection pin only resolves under a project, so `unset --proj` unpins the
collection too, and switching to a different project drops a collection pinned
under the old one.

A server URL with no protocol defaults to `http://`, and one with no port to
port `2457` — so `set --server 127.0.0.1` means `http://127.0.0.1:2457`.

### Profiles

A profile is a separate login and set of pins: `<NAME>-connection.json` and
`<NAME>-options.json` next to the bare `connection.json` / `options.json`, which
are the default profile. The other settings are global and apply under every
profile. Every command takes `--profile NAME` to run under a
profile for that invocation, or a bare `--profile` (or `--profile=`) to run
under the default profile whatever `set --profile` recorded; a bare `--profile`
must not be followed by a positional argument, which it would take as NAME.
`login --profile NAME` is how a profile's connection comes to exist. `set
--profile NAME` records NAME in `tdk-settings.json` as the profile commands use
when given no `--profile`, and a `--proj` or `--coll` of the same command is
pinned in it; `unset --profile` drops it, so the default profile applies again.
`settings` names the profile it reports on.

2.2 kept every setting in each profile's options file and the recorded profile
in `profile.json`. The first command run without `tdk-settings.json` carries
over the recorded profile and the settings of that profile's options file;
`profile.json` and the old keys of the options files are then ignored.

## `tdk auth`

```
auth info
auth add-cert        --server S --pem FILE
auth delete-cert     --server S
auth password-change SERVER [--user U] --old-password OLD --new-password NEW
auth service-token create    --slot 1|2 --expires-in-days N|-1 [--comment C]
auth service-token list
auth service-token revoke    --slot 1|2
```

### Service tokens

A service token authenticates automation without a password. An account
holds at most two, in slots `1` and `2`, so a rotation always has a free slot
to fill while the other stays in production.

`auth service-token create` prints the token once and never again — the server keeps
only a SHA-256 digest of it. `--expires-in-days` has no default: pass a positive
number of days, or `-1` for a token that never expires, which only works where
the deployment permits it. Creating into an occupied slot is refused; revoke it
first. The whole lifecycle needs a password session: a token can neither create,
list nor revoke a token, so it cannot mint its own successor.

What a token may do is its owner's live permissions narrowed to project
development work: reading and changing collections, functions and tables, and
triggering and recovering runs, in the projects its owner can already reach.
Everything else answers `403` with `This operation cannot be performed with a
service token`, the same answer whether or not the owner holds that
permission. Removing a project grant from the account removes it from the
account's tokens.

A deployment may also hold server-managed system tokens, seeded at boot from its
configuration. They never expire, and revoking one is refused: replacing its
configured value and restarting is the only way to retire it.

Rotating a token, with both slots valid throughout so no request is ever
refused:

1. Create a token in the free slot and move consumers to it one at a time.
2. Wait for `auth service-token list` to show the old slot's `Last used` going quiet.
   That column has a resolution of a few minutes.
3. Revoke the old slot, freeing it for the next rotation.

## `tdk docs` *(offline)*

```
docs show   --name SYMBOL [--category C] [-o]
docs list   [--category C] [--max-result N] [-o]
docs search --query WORDS [--category C] [--max-result N] [-o]
```

Reference pages introspected from the installed `tabsdatak` packages, so they
always match the SDK in the current environment. Categories: `api` (decorators,
`Src`/`Dest`/`StreamSrc`, `TrxCtx`, the `*Spec` type aliases), `triggers`,
`connectors` (source, stream-source and destination classes, their `Conn`
classes and per-connector `*Spec` aliases), `tableframe` (`TableFrame`, its
methods, `col`/`lit`/`concat`), `types`, `errors` (every `*ErrorCode`, such as
`COMMON_11`, and its exception class), `statuses` (per entity, with health and
finality) and `credentials`.

`SYMBOL` resolves, in order: a qualified name (`tabsdatak.api.publisher`), a
bare name or alias (`PostgresSrc`, `fnrun`), a connection type id
(`postgres-sql-in` → `PostgresSrc`), then `Class.member`
(`TableFrame.with_columns`). A bare name that several symbols share fails with
exit code 2 and lists the qualified names; an unknown one exits 5 with close
matches. `search` matches names, qualified names and summaries, best match
first; `list` and `search` show at most `--max-result` symbols (100 by default).

The table view of `docs show` is a Markdown page: signature, docstring, a
parameters table, members, and see-also commands such as
`tdk connection template -t TYPE`. `-o json` is
`{"symbol", "qualified_name", "category", "kind", "signature", "summary",
"description", "parameters": [{"name", "type", "required", "default",
"description"}], "members": [{"name", "kind", "signature", "summary"}],
"connection_types": [...], "see_also": [...]}`, plus `role`, `system`,
`connection_class` and `collection_group` for connectors, `enum`, `exception`
and `message` for errors, and `statuses` for statuses. `list` and `search`
use the list envelope with `{"symbol", "qualified_name", "category", "kind",
"summary"}` items.

## `tdk examples`

```
examples list    [--kind K] [--connector TYPE] [--tag T] [-o]
examples show    --name NAME [-o]
examples path    [--name NAME] [-o]
examples extract --name NAME --dir DIR [-o]
examples scaffold [--kind publisher|stream_publisher|transformer|subscriber] [--connector TYPE]
                 [--name N] [--input TABLE]... [--output TABLE]... [--file FILE] [-o]
examples scaffold --list-connectors [-o]
                 #   offline; the connector's required values are '<value>' placeholders
```

The examples bundled with tdk. A **snippet** is one file, `NAME.py`, holding
one decorated function (or several, for kind `flow`) and YAML metadata in its
module docstring; CI imports every snippet, so the decorators validate each
signature, and runs every transformer that carries a `SAMPLE` on it. A
**project** is a folder with input data, connection files and functions
(`quickstart-ai`, `quickstart-dev`).

`list` filters by `--kind` (`publisher`, `stream_publisher`, `transformer`,
`subscriber`, `flow`), `--connector` (a connection type id such as
`postgres-sql-in`) and `--tag`; filters combine. `-o jsonb` uses the list
envelope with `{"name", "form": "snippet"|"project", "title", "kind",
"connectors", "tags", "summary", "source"}` items (`kind` is null for
projects; `source` is `bundled`). `show` prints a
snippet's source, or a project's README and function files; `-o json` adds
`"files": [{"path", "content"}]` to the list item. `path` gives `{"path"}`: the examples
directory, or one example's file or folder. `extract` writes a snippet as
`DIR/NAME.py` (`DIR` may exist; an existing file is a conflict, exit 6) and
copies a project into `DIR`, which must not exist, then runs its setup step.
`scaffold` writes the skeleton of a function bound to an installed connector
(`--list-connectors` lists their types), with `<value>` placeholders for the
values the connector needs, and names the examples of the same kind and
connector; the function never names a connection.

## `tdk doctor`

```
doctor [--check NAME ...] [--offline] [-o]
```

Local development preflight. Every check reports `pass`, `warn`, `fail` or
`skip` with a detail and, when there is one, the fix. Exits 0 when no check
fails (warnings allowed) and 1 otherwise. A check that crashes is reported as
failed and never stops the others.

| Check | Fails / warns when |
|---|---|
| `python` | fails when the local `X.Y.Z` is not one the server accepts (exact match, the rule behind `COMMON_11`); warns when the server cannot be consulted |
| `server` | fails when the server is unreachable or refuses the client's tabsdata version; warns when an older server, which does not refuse it, runs another version |
| `login` | fails without valid credentials; warns when a session that cannot be renewed expires within 10 minutes |
| `venv` | fails outside a virtual environment; warns for installed packages from no registry (editable, local, VCS), which are not bundled with functions, and for more than 25 extra registry packages |
| `uv` | fails when `uv` is not on `PATH` |
| `connectors` | warns for each connection type used by the pinned project's source and destination collections with no local connector package |

`--offline` skips `server`, `login` and `connectors`, and compares Python with
the supported range only. `-o json` is `{"ok": bool, "checks": [{"name",
"status", "detail", "fix"}]}` when no check fails. A failed run exits 1 with
`checks_failed`: with `-o json|yaml` nothing is rendered and the error goes
to stderr; the table is rendered before the error.

## Root commands

```
login    [--server S] [--user U] [--password P] [--token[=T]]
logout                   # always drops the stored login; says so when the server could not end the session
status
info     license
info     release-notes
info     third-party [--wide]
about
shell
```

`login` takes the server from `tdk set --server` when `--server` is
omitted, and prompts only when neither is set. The URL defaults to the `http://`
protocol and port `2457` when it carries neither. `login --profile NAME` stores
the session in the profile's own connection file (see Profiles).

`--token` logs in with a service token instead of a username and password,
and cannot be combined with either. Given no value it reads `TDK_TOKEN`, so the
token need not appear in a command line or shell history. A token session is
never renewed: it works until the token expires or is revoked, and then `login`
runs again.

`info third-party --wide` shows the license column at its full width instead
of wrapping it. `tdk --version` prints the client version.

## Machine output

`-o jsonb` carries the full objects, and so does every document this section and
the command sections describe. `-o json` and `-o yaml` give the same document but
for the shapes tdk 2.2 printed, which scripts parse: a listing is the bare array
of its items, `table data` an array of row objects, worker logs `{"logs": "Worker
<id>\n<log>"}`, one block per worker, and `examples list` items also carry
`description`. A machine run, such as the agent mode of the AI package, always
gives the `jsonb` document, `-o json` included.

In `jsonb` a listing is always an object, never a bare array:
`{"items": [...], "truncated": bool, "next": cursor}`, where
`truncated` says `--max-result` cut it short and `next`, on a paged listing, is
the `--cursor` that continues it (null otherwise). A cursor continues only the
command, options, filters and order that returned it; anything else is a usage
error.
`table data` gives
`{"columns": [{"name", "dtype"}], "rows": [[...]], "row_count", "truncated"}`,
with dates as ISO-8601 strings, decimals as strings and binary as base64. Logs
use the list envelope, one item per worker: `worker_id`, `function_run_id`,
`collection`, `function`, the annotated `status`, `attempt`, `lines_total`,
`log`, `log_truncated`, `log_unavailable` and `signature`
(`{category, code, message, user_frame}` or null).

Plans, transactions, function runs, workers (including `cron|stream list`) and
table data versions carry their raw one-letter `status` code plus three derived
fields, in lists, `get`, `monitor` and `fn trigger` alike:

- `status_name`: the display name, e.g. `Finished`.
- `health`: one of `ok`, `running`, `warning` or `error`.
- `final`: `true` when the server will not change the status on its own.

The same code means different things on different entities: `F` is Finished
(`ok`) on a plan and Failed (`error`) on a function run. On function runs and
data versions, `E` (Error, retrying automatically) is `warning` and not final;
`F` (retries used up, needs `recover`) is `error`; `H` (On Hold) and `Y`
(Yanked) are `warning` and final. On plans, `L` (Stalled) is `warning` and `U`
is `error`. On transactions, `Y` is `warning`. On workers, `D`, `SP` and `X`
are `ok` and final, and `E` is `warning`. The table view is unchanged.

Every command but `shell` and `auth password-change` takes `-o`, including the ones that
change something:

- A create, update, register, patch, trigger or replay gives the object it
  produced, the same object `get` shows. `fn trigger` gives the plan it
  started; `plan monitor` / `trx monitor` give the plan or transaction once it
  finishes (see [Waiting](#waiting)).
- A delete gives `{"deleted": {"kind", "name", "collection", "project"}}`, with
  `kind` one of `collection`, `function`, `table`, `group`, `project`, `user`.
- Any other action gives `{"done": {"action", "kind", "id", ...}}`, e.g.
  `{"done": {"action": "cancel", "kind": "plan", "id": "…"}}`. Extra keys name
  what the id alone does not: `collection`, `file`, `user`, `project`,
  `function_run` / `worker` (for `fnrun|worker recover`, which recover the
  owning transaction), and `changed: false` when `fn|cron|stream
  enable|disable` or `auth delete-cert` found nothing to change. The actions
  are `enable`, `disable`, `cancel`, `recover`, `grant`, `revoke`, `download`
  (`table download`, `fn bundle --file`), `add` / `delete` (certificates),
  `extract` (`examples extract`, `fn bundle --extract`, with `dir`) and
  `login` / `logout` (`kind: session`).
- `status` gives `{"status", "latency_as_nanos", ...}`; `set` / `unset` give
  the resulting settings, as `settings` does; `connection template` gives
  `{"type", "file", "template"}` (the YAML text); `connection validate` gives
  `{"valid", "type", "secret_env_vars"}`; `info license|release-notes|third-party`
  give `{"title", "text"}` and `about` gives `{"version", "text"}`.
- `fn check` gives `{"valid", "function": {"name", "kind", "inputs", "outputs",
  "triggers", "connector"}, "bundle": {"files", "bytes", "python_version"},
  "findings": [{"severity", "code", "message", "line"}], "server_checks"}`;
  `valid` is false when a finding is an `error`, and `server_checks` (with
  `--coll`) is `{"collection", "inputs_exist", "output_conflicts",
  "python_version"}`, else null. `fn register|update|patch --dry-run` give the
  same plus `"would": {"action", "collection", "name"}` (`fn_version` for a
  patch). A function that is not valid exits 1 with `checks_failed`, as `tdk
  doctor` does. `examples scaffold` gives `{"path", "source", "connector": {"type",
  "system"}, "examples"}` and `fn bundle --print` gives `{"function_version_id",
  "defining_module", "files", "skipped", "truncated"}`, the bundle's text files
  within a size budget.

Under `-o json|jsonb|yaml`, stdout holds only the rendered document: hints, notices,
progress lines and the live monitor table go to stderr.

`--no-prompt` is accepted in any position before `--`.

## Deprecated aliases

These still work, print a warning on stderr naming what replaces them, and are
left out of `--help` and `--help --full`. The agent mode of the AI package refuses them.

| Alias | Use instead |
|---|---|
| `collection connection --name C [-o json\|yaml]` (prints the dehydrated connection document, JSON by default) | `connection get --name C --definition` |
| `table data --wide` | `table data --max-width 0` |
| `auth password-change -n NEW` | `--new-password NEW` |
| `fn trigger --detach` (`-d`) | `fn trigger --no-wait` |

## Exit codes

Every command exits with one of these codes, whatever the output format.

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Server error, server unavailable or rate limited, server unreachable, a server running another tabsdata version, invalid request, aborted prompt, unexpected failure |
| 2 | Usage error: bad or missing option, no project selected |
| 3 | Not logged in, or the credential was refused |
| 4 | Forbidden, including operations a service token or a session cannot perform |
| 5 | Not found |
| 6 | Conflict: the object already exists or its state does not allow the operation |
| 7 | A wait ran out of time (see [Waiting](#waiting)) |
| 8 | A waited plan, transaction or function run finished with runs that did not succeed, or a waited connection probe did not succeed |

## Environment

### `TDK_BEARER`, `TDK_SERVER`, `TDK_PROJECT`

With `TDK_SERVER` and either `TDK_BEARER` (a session token) or `TDK_TOKEN` (a
service token) set, tdk runs statelessly: it uses that credential against that
server, never reads or writes the files under the Tabsdata directory, and never
renews the credential. Setting both `TDK_BEARER` and `TDK_TOKEN` is a usage
error. A session fails with exit code 3 when it is within 5 seconds of expiring;
a refused service token also exits 3, and commands that need a user session exit
4. `login`, `logout`, `set`, `unset`, `auth add-cert`, `auth delete-cert` and
`--profile` are refused, since there is nothing stored to change. `auth info`
reports which kind of credential is in use. `TDK_PROJECT` is the default
project, in every mode: `--proj` overrides it, and it overrides the pinned
project.

### `TDK_AGENT`, `CLAUDECODE`, `GEMINI_CLI`, `CODEX_THREAD_ID`, `CURSOR_AGENT`, `VSCODE_AGENT`, `OPENCODE`

An unattended session never prompts: `TDK_AGENT=1`, or any of the variables
coding agents set (`CLAUDECODE`, `GEMINI_CLI`, `CODEX_THREAD_ID`,
`CURSOR_AGENT`, `VSCODE_AGENT`, `OPENCODE`; set to `0` or `false`, one counts
as unset), turns prompting off as `--no-prompt` does, and a missing value fails
naming the variable. An installed extension may read the same variables for
more. Without any of them, a prompt whose standard input is not a terminal and
ends without an answer fails the same way (`missing_argument`, exit 2, naming
the option to pass) instead of aborting.

### `TDK_TOKEN`

The service token `tdk login --token` reads when the option is given no
value. With `TDK_SERVER` set too, tdk uses the token directly and statelessly,
without logging in (see below). See [Service tokens](#service-tokens).

