-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

UPDATE "users" SET "email" = NULL WHERE "email" = '-'

UPDATE tabsdata_system SET value = '2' WHERE name = 'db_version'
