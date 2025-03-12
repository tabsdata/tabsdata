<!--
Copyright 2025 Tabs Data Inc.
-->

# Tabsdata SQL Schema

## Requirements

Cargo `sqlx-cli` package. 

Install it using `cargo install sqlx-cli`.

# Create a SQL Schema Snippet File

Run the following command from `tabsdata` repo root directory where `<name>` is the entity group
for the DDLs (i.e. `users`, `collections`, `roles`, `functions`, etc.):


```bash
sqlx migrate add -r <name> --source server/libraries/td-schema/resources/schemas/tabsdata/live
```

