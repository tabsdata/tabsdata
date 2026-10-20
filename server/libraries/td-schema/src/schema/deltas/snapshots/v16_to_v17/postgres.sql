-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

ALTER TABLE executions DROP COLUMN recursion_options

UPDATE tabsdata_system SET value = '17' WHERE name = 'db_version'
