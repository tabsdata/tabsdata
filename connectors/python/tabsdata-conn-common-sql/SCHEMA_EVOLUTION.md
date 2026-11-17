<!-- Copyright 2026 Tabsdata Inc. -->

# Schema evolution by destination

Generated from `tabsdatak/conn/common/sql/_schema/matrix.py`, which the
destinations validate against, the plugins resolve from and registration
reports; `test_matrix.py` fails when this file and the module disagree.
Regenerate it with:

```
python -c 'from tabsdatak.conn.common.sql._schema import matrix; print(matrix.markdown(), end="")' > SCHEMA_EVOLUTION.md
```

## Rules

- Writes are data only. A replace empties the table (TRUNCATE; DELETE on
  Db2, which takes TRUNCATE only as the first statement of a unit of
  work) and inserts the batch: under strict it just inserts, under update
  it expands the schema first. A batch the policy refuses leaves the rows
  in place. Schemas only grow.
- One engine per destination and disposition: native where the backend
  supports that operation, otherwise iceberg. The engine is never written,
  except iceberg with update on a BigQuery or Snowflake merge.
- A policy left out resolves to strict, except on Druid, where update is
  the only policy.
- Each destination's `validate()` resolves what the user wrote through
  `matrix.resolve()`, so registration rejects every INVALID and PLANNED
  combination (`SQL_COMMON_11`), and `tdk fn register` reports the
  resolved disposition, policy and engine. The plugins write with the
  same resolution.
- Implementation paths the matrix does not offer (iceberg on a BigQuery or
  Snowflake append, for one) may stay in the code, but registration never
  lets a write reach them.

## Matrix

How to read the table:

- Columns are what the user writes. *any other engine* covers native
  written explicitly, and iceberg written either without update or where
  native already applies.
- *left out →* is what registration applies when the policy is left out.
- PLANNED means not offered yet and pending definition; registration
  rejects it until then.

| Destination | Disposition | Engine | Status | left out → | strict | update | update + iceberg | any other engine |
|---|---|---|---|---|---|---|---|---|
| BigQuery | append | native | offered | strict | strict | update · native ¹ | INVALID | INVALID |
| BigQuery | replace | native | offered | strict | strict | update · native ¹ | INVALID | INVALID |
| BigQuery | merge | iceberg (explicit) | offered | strict | strict | INVALID ² | update · iceberg ² | INVALID |
| Snowflake | append | native | offered | strict | strict | update · native ¹ | INVALID | INVALID |
| Snowflake | replace | native | offered | strict | strict | update · native ¹ | INVALID | INVALID |
| Snowflake | merge | iceberg (explicit) | offered | strict | strict | INVALID ² | update · iceberg ² | INVALID |
| Databricks | append | native | offered | strict | strict | update · native | INVALID | INVALID |
| Databricks | replace | native | offered | strict | strict | update · native | INVALID | INVALID |
| Databricks | merge | native | offered | strict | strict | update · native | INVALID | INVALID |
| AWS Glue | append | native | offered | strict | strict | update · native | INVALID | INVALID |
| AWS Glue | replace | native | offered | strict | strict | update · native | INVALID | INVALID |
| ClickHouse | append | iceberg | offered | strict | strict | update · iceberg | INVALID | INVALID |
| ClickHouse | replace | iceberg | offered | strict | strict | update · iceberg | INVALID | INVALID |
| ClickHouse | merge | PLANNED | planned · semantics to define ⁵ | PLANNED | PLANNED | PLANNED | PLANNED | PLANNED |
| StarRocks | append | iceberg | offered | strict | strict | update · iceberg | INVALID | INVALID |
| StarRocks | replace | iceberg | offered | strict | strict | update · iceberg | INVALID | INVALID |
| StarRocks | merge | PLANNED | planned · backend can (Primary Key upsert) ⁶ | PLANNED | PLANNED | PLANNED | PLANNED | PLANNED |
| Postgres | append | iceberg | offered | strict ³ | strict ³ | update · iceberg | INVALID | INVALID |
| Postgres | replace | iceberg | offered | strict ³ | strict ³ | update · iceberg | INVALID | INVALID |
| Postgres | merge | PLANNED | planned · backend can (MERGE 15+, ON CONFLICT) | PLANNED | PLANNED | PLANNED | PLANNED | PLANNED |
| MySQL | append | iceberg | offered | strict ³ | strict ³ | update · iceberg | INVALID | INVALID |
| MySQL | replace | iceberg | offered | strict ³ | strict ³ | update · iceberg | INVALID | INVALID |
| MySQL | merge | PLANNED | planned · backend can (ON DUPLICATE KEY) | PLANNED | PLANNED | PLANNED | PLANNED | PLANNED |
| SQL Server | append | iceberg | offered | strict ³ | strict ³ | update · iceberg | INVALID | INVALID |
| SQL Server | replace | iceberg | offered | strict ³ | strict ³ | update · iceberg | INVALID | INVALID |
| SQL Server | merge | PLANNED | planned · backend can (MERGE) | PLANNED | PLANNED | PLANNED | PLANNED | PLANNED |
| Oracle | append | iceberg | offered | strict ³ | strict ³ | update · iceberg | INVALID | INVALID |
| Oracle | replace | iceberg | offered | strict ³ | strict ³ | update · iceberg | INVALID | INVALID |
| Oracle | merge | PLANNED | planned · backend can (MERGE) | PLANNED | PLANNED | PLANNED | PLANNED | PLANNED |
| Db2 | append | iceberg | offered | strict ³ | strict ³ | update · iceberg | INVALID | INVALID |
| Db2 | replace | iceberg | offered | strict ³ | strict ³ | update · iceberg | INVALID | INVALID |
| Db2 | merge | PLANNED | planned · backend can (MERGE) | PLANNED | PLANNED | PLANNED | PLANNED | PLANNED |
| Druid | append | native | offered | update · native ⁴ | INVALID | update · native | INVALID | INVALID |
| Druid | replace | native | offered (time ranges) | update · native ⁴ | INVALID | update · native | INVALID | INVALID |
| Druid | merge | — | impossible · no keyed upsert | INVALID | INVALID | INVALID | INVALID | INVALID |

## Notes

1. Native evolution on BigQuery and Snowflake adds columns and makes columns
   nullable, but never widens a type.
2. Their native evolution doesn't apply to MERGE. To evolve a merge, write the
   engine iceberg explicitly; update alone is rejected with a message saying so.
3. Strict refuses a wider type unless its values fit, as on every other
   destination.
4. Druid can't enforce a schema, so update is its only policy and its default.
5. ClickHouse has no MERGE. There are two workable designs: a ReplacingMergeTree
   table keyed on the merge columns, cheap but deduplicated only eventually, so
   readers need FINAL; or rebuilding the touched partitions and swapping them in
   with REPLACE PARTITION, exact and atomic, but each touched partition is
   rewritten. Which one to use is still to be defined.
6. StarRocks upserts by key when it loads into a Primary Key table, and deletes
   through __op. The target must be a Primary Key table whose key is the merge
   columns.
