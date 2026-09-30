-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

ALTER TABLE "functions" ADD COLUMN "function_type" text NULL

UPDATE "functions" SET "function_type" = 'R'

ALTER TABLE "functions" ALTER COLUMN "function_type" TYPE text, ALTER COLUMN "function_type" SET NOT NULL

ALTER TABLE "functions" ADD COLUMN "parent_function_id" text NULL COLLATE "C"

UPDATE tabsdata_system SET value = '11' WHERE name = 'db_version'
