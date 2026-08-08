<!--
Copyright 2026 Tabsdata Inc.
-->

# Command Basics

All Tabsdata commands used directly from pod YAMLs must conform to the following 
specification.

## Common Options

Every command must support the following options:

* `--help|-h`: Brief description of the command and its options.
* `--man|-m`: Detailed user guide.
* `--version|-v`: Command build version.
* `--info|-i`: Command build info.
* `--liveness|-l PATH`: A file that the command must touch at least once a
  minute to signal liveness to the framework.

It is the command's responsibility to refresh the liveness file from a point
in the code that meaningfully reflects processing progress.

## Command Options

Command-specific options always take the form `--<OPTION> <ARG>`. The flag
name is the same regardless of how the value is supplied; only the argument
shape changes:

* `--<OPTION> <VALUE>` — the argument is the literal value.
* `--<OPTION> env:<ENV_VAR>` — the argument is read from the named
  environment variable. If the environment variable is not defined, the
  command must fail.
* `--<OPTION> val:<VALUE>` — explicit literal-value form, for cases where
  the value would otherwise be ambiguous (e.g. a literal that starts with
  `env:` or `val:`). Equivalent to `--<OPTION> <VALUE>`.

An option may support either form or both.

For flag options, the `env:` form must accept `true` or `false`
(case insensitive) in the environment variable.

## Command Error Codes

* `1`: Invalid or missing options.
* `2`: Undefined environment variable used in an `env:` option.
* `3..9`: Reserved.
* `10+`: Command-specific.

## Command StdIn/StdOut/StdErr

*STDIN* is not used.

*STDOUT* and *STDERR* are used for logging. `INFO`, `DEBUG` and `TRACE` go to
*STDOUT*; `WARN` and `ERROR` go to *STDERR*.
