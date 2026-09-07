-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

ALTER TABLE "physical_function_runs" ADD COLUMN "base_path" text NULL

UPDATE "physical_function_runs" SET "base_path" = COALESCE(
                        (SELECT "functions"."base_path" FROM "functions"
                          WHERE "functions"."id" = "physical_function_runs"."function_version_id"),
                        (SELECT "collections"."base_path" FROM "collections"
                          WHERE "collections"."id" = "physical_function_runs"."collection_id"),
                        (SELECT "projects"."base_path" FROM "projects"
                          WHERE "projects"."id" = "physical_function_runs"."project_id"),
                        ''
                    )

ALTER TABLE "physical_function_runs" ALTER COLUMN "base_path" TYPE text, ALTER COLUMN "base_path" SET NOT NULL

UPDATE tabsdata_system SET value = '3' WHERE name = 'db_version'
