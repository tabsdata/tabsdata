<!--
Copyright 2026 Tabs Data Inc.
-->

# Historic SQLite Migrations

This directory contains the **original sqlx migration files** from the legacy
migration system (versions 0–5). They are embedded at compile time by
`mod.rs` and applied when upgrading a database created by an older Tabsdata
release.

## DO NOT MODIFY THESE FILES

These SQL files are a frozen record of the exact migrations that were shipped
in previous releases. Changing them would break upgrades from those releases.

If the upgrade path needs adjustment, add logic in `mod.rs` or create a new
programmatic delta in `../deltas/` — never edit the `.sql` files.

## How they are used

When `TdPool::upgrade_db_version()` detects a database with `db_version <= 5`
(the version at which we switched to programmatic sea-query migrations), it
runs the following steps inside a **single transaction**:

1. Verifies the database is **SQLite** (the only backend supported before v5).
2. Applies the historic `.up.sql` files in order to bring the database to v5.
3. **Normalizes** the schema (`normalize()` in `mod.rs`): rebuilds every
   table, view, and index using the v5 archive's `build()` output while
   preserving all data. This ensures type spellings (`TIMESTAMP` vs
   `timestamp_text`), index names, and column constraints exactly match a
   fresh programmatic install.
4. Drops the legacy `_sqlx_migrations` table.
5. Continues with the normal programmatic delta path (v5 → v6 → … → CURRENT).

If any step fails, the entire transaction rolls back and the database is left
unchanged.

For **new installations**, the current schema is created directly at
`CURRENT_VERSION` — these historic files are never used.

## File layout

The `live/` subdirectory contains timestamped sqlx migration pairs
(`*.up.sql` / `*.down.sql`). Only the `.up.sql` files are used. They group
into version steps as follows:

| Version bump | Migration files applied |
|---|---|
| v0 (initial) | `20240726070228_system` through `20250331150456_sessions` |
| v0 → v1 | `20250531100253_v1` |
| v1 → v2 | `20250922095747_functions`, `20250922095756_v2` |
| v2 → v3 | `20251106170725_functions`, `20251106170859_v3` |
| v3 → v4 | `20251201104113_functions`, `20251201104119_v4` |
| v4 → v5 | `20260112120013_indices`, `20260113102005_functions`, `20260113102019_v5` |
