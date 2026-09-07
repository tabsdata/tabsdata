-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

ALTER TABLE "functions" RENAME COLUMN "defined_on" TO "valid_from"

ALTER TABLE "functions" RENAME CONSTRAINT "functions_defined_on_not_null" TO "functions_valid_from_not_null"

ALTER TABLE "function_patches" RENAME COLUMN "defined_on" TO "valid_from"

ALTER TABLE "function_patches" RENAME CONSTRAINT "function_patches_defined_on_not_null" TO "function_patches_valid_from_not_null"

ALTER TABLE "tables" RENAME COLUMN "defined_on" TO "valid_from"

ALTER TABLE "tables" RENAME CONSTRAINT "tables_defined_on_not_null" TO "tables_valid_from_not_null"

ALTER TABLE "dependencies" RENAME COLUMN "defined_on" TO "valid_from"

ALTER TABLE "dependencies" RENAME CONSTRAINT "dependencies_defined_on_not_null" TO "dependencies_valid_from_not_null"

ALTER TABLE "triggers" RENAME COLUMN "defined_on" TO "valid_from"

ALTER TABLE "triggers" RENAME CONSTRAINT "triggers_defined_on_not_null" TO "triggers_valid_from_not_null"

ALTER TABLE "physical_function_runs" RENAME COLUMN "triggered_on" TO "valid_from"

ALTER TABLE "physical_function_runs" RENAME CONSTRAINT "physical_function_runs_triggered_on_not_null" TO "physical_function_runs_valid_from_not_null"

ALTER TABLE "physical_table_data_versions" RENAME COLUMN "triggered_on" TO "valid_from"

ALTER TABLE "physical_table_data_versions" RENAME CONSTRAINT "physical_table_data_versions_triggered_on_not_null" TO "physical_table_data_versions_valid_from_not_null"

UPDATE tabsdata_system SET value = '6' WHERE name = 'db_version'
