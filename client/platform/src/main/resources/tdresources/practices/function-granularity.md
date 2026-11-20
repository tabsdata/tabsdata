<!--
Copyright 2026 Tabsdata Inc.
-->
---
title: How much one function should do
summary: >-
  One publisher per logical set of ingested tables, one transformer per output
  table unless its tables are used together, one subscriber per destination
  model.
applies_to: [publisher, stream_publisher, transformer, subscriber]
examples:
  - pub-mysql-multi-query
  - tfr-join-two-tables
  - tfr-multi-output-split
  - medallion-cdc-bronze-scd2-silver
  - sub-postgres-model
---
A function runs as one unit: a run writes all its outputs, they commit in the
same transaction or not at all, and `tdk table flow` shows every input of a
function upstream of each of its outputs. That is what you want where data
enters and leaves Tabsdata, where a set of tables must stay consistent, and
what you do not want in between, where unrelated tables would claim each
other's inputs. So the edges group tables and the middle splits them.

## Publishers: one per logical set of ingested data

A logical set is the tables whose rows refer to each other and that a reader
expects to match: an order and its lines, a customer and its subscriptions.

- Read a set in one publisher, one output per table. Its tables then get their
  new versions in the same transaction.
- Keep the source's `transactional=True` where it has one: every query then
  runs in one session. Whether that session is one snapshot depends on the
  database's isolation level: MySQL InnoDB's default, REPEATABLE READ, is one
  snapshot; PostgreSQL's, READ COMMITTED, is one per query.
- For CDC, put the set's tables in one stream publisher: their changes arrive
  in the same batches, in the source's commit order. A stream per table loses
  that order and opens another reader on the same log.
- Files and object stores have no transaction; one publisher still reads a
  set's files in one run, so land them together, such as one directory per drop.
- Separate the sets that are unrelated or arrive on different schedules, even
  from the same source.

## Transformers: one per output table, unless the tables belong together

A function's tables commit together and are read together, so a transformer
lists several only when they must be:

- inputs that one output needs: a join, a lookup, a union, an aggregate
  across them, a filter of one by another;
- outputs that one computation writes: an SCD2 change-detect, whose one fold
  writes the open and the closed versions, or the split of one table into
  several;
- outputs that a reader needs at the same version: a version leaves an SCD2's
  valid table as it enters the expired one, so the two stay together in every
  step, the cleanse included.

A table's own previous version (`<table>@HEAD`) is the same table, not a
second one.

Tables that only share a step do not belong together. If the function turns
table A into A' and table B into B' side by side, with no row of A affecting
B' and no reader needing both at once, write one transformer per table, even
when they share a helper. The sign is a body that applies the same step to
each parameter: a loop or a `zip` over them, or a tuple of the same helper's
results. Name them `tfr_<table>_<step>`: `tfr_customers_bronze`,
`tfr_plans_scd1`.

Each table then has its own lineage and a one-slot signature that is simple
to test.

## Subscribers: one per destination model

Tables that go to the same system as one related set, such as a model's
dimensions and facts, go in one subscriber, one destination slot per table,
named `publish_<model>`.

- A reader gets them all from the same versions. A destination with a
  `transactional` option writes them in one session, which is one commit
  where the database can roll back a `replace`: PostgreSQL, SQL Server and
  Db2. A `replace` truncates, and MySQL and Oracle commit at each truncate.
  The other destinations (Snowflake, BigQuery, Databricks, the object stores)
  write one table after another, so a reader can catch the set halfway.
- The subscriber runs when any input changes and reads the latest version of
  every input, so write the whole set each run: `if_table_exists="replace"`
  (or `"merge"` on a destination that has it), not `"append"`.
- Return a frame for every slot, or `None` for all of them when any input is
  missing: `None` leaves a slot's table as it is, so `None` in only some
  slots writes a set mixed with an older one.
- Split only what is unrelated: tables no reader joins, tables bound for
  another system or schema, or large tables on very different schedules.

## Costs

- More functions to register, each a smaller run.
- Tables that one publisher commits together trigger all their transformers on
  every commit: splitting a transformer means more, smaller workers per run,
  not fewer runs.
- Only triggers order the functions of a run. A function that reads tables
  written by different functions gets this run's version only of the tables
  that trigger it, and the previous version of the others: leave `trigger_by`
  out, or list every table it must read fresh.
- In the enterprise edition a transaction spans the collection by default, so
  one function's failure still holds back the rest of its collection's commit.
