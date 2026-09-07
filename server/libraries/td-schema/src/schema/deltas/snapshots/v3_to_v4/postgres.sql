-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

ALTER TABLE "physical_function_runs" ADD COLUMN "logical_function_run_id" text NULL

ALTER TABLE "physical_function_runs" ADD COLUMN "valid_to" timestamptz NULL

UPDATE "physical_function_runs" SET "logical_function_run_id" = e."logical_function_run_id" FROM "logical_function_run_pointers" AS e WHERE e."physical_function_run_id" = "physical_function_runs"."id"

ALTER TABLE "physical_table_data_versions" ADD COLUMN "logical_table_data_version_id" text NULL

ALTER TABLE "physical_table_data_versions" ADD COLUMN "valid_to" timestamptz NULL

UPDATE "physical_table_data_versions" SET "logical_table_data_version_id" = e."logical_table_data_version_id" FROM "logical_table_data_version_pointers" AS e WHERE e."physical_table_data_version_id" = "physical_table_data_versions"."id"

UPDATE "physical_function_runs" SET "logical_function_run_id" = l."id" FROM "logical_function_runs" AS l WHERE "physical_function_runs"."logical_function_run_id" IS NULL AND l."execution_id" = "physical_function_runs"."execution_id" AND l."function_id" = "physical_function_runs"."function_id"

UPDATE "physical_table_data_versions" SET "logical_table_data_version_id" = l."id" FROM "logical_table_data_versions" AS l, "physical_function_runs" AS p WHERE "physical_table_data_versions"."logical_table_data_version_id" IS NULL AND p."id" = "physical_table_data_versions"."physical_function_run_id" AND l."producer_logical_function_run_id" = p."logical_function_run_id" AND l."table_id" = "physical_table_data_versions"."table_id"

ALTER TABLE "physical_function_runs" ALTER COLUMN "logical_function_run_id" SET NOT NULL

ALTER TABLE "physical_function_runs" ADD FOREIGN KEY ("logical_function_run_id") REFERENCES "logical_function_runs" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "physical_table_data_versions" ALTER COLUMN "logical_table_data_version_id" SET NOT NULL

ALTER TABLE "physical_table_data_versions" ADD FOREIGN KEY ("logical_table_data_version_id") REFERENCES "logical_table_data_versions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "functions" ADD COLUMN "valid_to" timestamptz NULL

ALTER TABLE "function_patches" ADD COLUMN "valid_to" timestamptz NULL

ALTER TABLE "tables" ADD COLUMN "valid_to" timestamptz NULL

ALTER TABLE "dependencies" ADD COLUMN "valid_to" timestamptz NULL

ALTER TABLE "triggers" ADD COLUMN "valid_to" timestamptz NULL

UPDATE "functions" SET "valid_to" = COALESCE(n."next_valid_from", 'infinity') FROM (SELECT "id", LEAD("defined_on") OVER (PARTITION BY "function_id" ORDER BY "defined_on", "id") AS "next_valid_from" FROM "functions") AS n WHERE "functions"."id" = n."id"

ALTER TABLE "functions" ALTER COLUMN "valid_to" SET NOT NULL

UPDATE "function_patches" SET "valid_to" = COALESCE(n."next_valid_from", 'infinity') FROM (SELECT "id", LEAD("defined_on") OVER (PARTITION BY "parent_function_version_id" ORDER BY "defined_on", "id") AS "next_valid_from" FROM "function_patches") AS n WHERE "function_patches"."id" = n."id"

ALTER TABLE "function_patches" ALTER COLUMN "valid_to" SET NOT NULL

UPDATE "tables" SET "valid_to" = COALESCE(n."next_valid_from", 'infinity') FROM (SELECT "id", LEAD("defined_on") OVER (PARTITION BY "table_id" ORDER BY "defined_on", "id") AS "next_valid_from" FROM "tables") AS n WHERE "tables"."id" = n."id"

ALTER TABLE "tables" ALTER COLUMN "valid_to" SET NOT NULL

UPDATE "dependencies" SET "valid_to" = COALESCE(n."next_valid_from", 'infinity') FROM (SELECT "id", LEAD("defined_on") OVER (PARTITION BY "dependency_id" ORDER BY "defined_on", "id") AS "next_valid_from" FROM "dependencies") AS n WHERE "dependencies"."id" = n."id"

ALTER TABLE "dependencies" ALTER COLUMN "valid_to" SET NOT NULL

UPDATE "triggers" SET "valid_to" = COALESCE(n."next_valid_from", 'infinity') FROM (SELECT "id", LEAD("defined_on") OVER (PARTITION BY "trigger_id" ORDER BY "defined_on", "id") AS "next_valid_from" FROM "triggers") AS n WHERE "triggers"."id" = n."id"

ALTER TABLE "triggers" ALTER COLUMN "valid_to" SET NOT NULL

UPDATE "physical_function_runs" SET "valid_to" = COALESCE(n."next_valid_from", 'infinity') FROM (SELECT "id", LEAD("triggered_on") OVER (PARTITION BY "logical_function_run_id" ORDER BY "triggered_on", "id") AS "next_valid_from" FROM "physical_function_runs") AS n WHERE "physical_function_runs"."id" = n."id"

ALTER TABLE "physical_function_runs" ALTER COLUMN "valid_to" SET NOT NULL

UPDATE "physical_table_data_versions" SET "valid_to" = COALESCE(n."next_valid_from", 'infinity') FROM (SELECT "id", LEAD("triggered_on") OVER (PARTITION BY "logical_table_data_version_id" ORDER BY "triggered_on", "id") AS "next_valid_from" FROM "physical_table_data_versions") AS n WHERE "physical_table_data_versions"."id" = n."id"

ALTER TABLE "physical_table_data_versions" ALTER COLUMN "valid_to" SET NOT NULL

UPDATE "tables" SET "function_version_id" = (SELECT f."id" FROM "functions" AS f WHERE f."function_id" = "tables"."function_id" AND f."status" <> 'D' ORDER BY f."defined_on" DESC, f."id" DESC LIMIT 1) WHERE "function_version_id" IN (SELECT "id" FROM "functions" WHERE "status" = 'D')

DELETE FROM "dependencies" WHERE "status" = 'D'

DELETE FROM "triggers" WHERE "status" = 'D'

DELETE FROM "tables" WHERE "status" = 'D'

DELETE FROM "functions" WHERE "status" = 'D'

ALTER TABLE "dependencies" DROP COLUMN "status"

UPDATE "tables" SET "valid_to" = f."closed_on" FROM (SELECT "function_id", MAX("valid_to") AS "closed_on" FROM "functions" GROUP BY "function_id" HAVING MAX("valid_to") < 'infinity') AS f WHERE "tables"."function_id" = f."function_id" AND "tables"."valid_to" = 'infinity' AND "tables"."status" = 'A' AND "tables"."name" LIKE 'td.fn\_state\_\_%'

UPDATE "dependencies" SET "valid_to" = f."closed_on" FROM (SELECT "function_id", MAX("valid_to") AS "closed_on" FROM "functions" GROUP BY "function_id" HAVING MAX("valid_to") < 'infinity') AS f WHERE "dependencies"."function_id" = f."function_id" AND "dependencies"."valid_to" = 'infinity'

UPDATE "triggers" SET "valid_to" = f."closed_on" FROM (SELECT "function_id", MAX("valid_to") AS "closed_on" FROM "functions" GROUP BY "function_id" HAVING MAX("valid_to") < 'infinity') AS f WHERE "triggers"."function_id" = f."function_id" AND "triggers"."valid_to" = 'infinity'

DROP TABLE "functions_current"

DROP TABLE "function_patches_current"

DROP TABLE "tables_current"

DROP TABLE "dependencies_current"

DROP TABLE "triggers_current"

DROP TABLE "logical_function_run_pointers_current"

DROP TABLE "logical_table_data_version_pointers_current"

DROP TABLE "logical_function_run_pointers"

DROP TABLE "logical_table_data_version_pointers"

DROP POLICY project_isolation ON groups

CREATE POLICY project_isolation ON groups USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON groups TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON connections

CREATE POLICY project_isolation ON connections USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON connections TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON collections

CREATE POLICY project_isolation ON collections USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON collections TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON bundles

CREATE POLICY project_isolation ON bundles USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON bundles TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON functions

CREATE POLICY project_isolation ON functions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON functions TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON function_patches

CREATE POLICY project_isolation ON function_patches USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON function_patches TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON tables

CREATE POLICY project_isolation ON tables USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON tables TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON dependencies

CREATE POLICY project_isolation ON dependencies USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON dependencies TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON triggers

CREATE POLICY project_isolation ON triggers USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON triggers TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON executions

CREATE POLICY project_isolation ON executions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON executions TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON transactions

CREATE POLICY project_isolation ON transactions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON transactions TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON physical_function_runs

CREATE POLICY project_isolation ON physical_function_runs USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON physical_function_runs TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON physical_table_data_versions

CREATE POLICY project_isolation ON physical_table_data_versions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON physical_table_data_versions TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON table_partitions

CREATE POLICY project_isolation ON table_partitions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON table_partitions TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON workers

CREATE POLICY project_isolation ON workers USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON workers TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON logical_function_runs

CREATE POLICY project_isolation ON logical_function_runs USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON logical_function_runs TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON logical_table_data_versions

CREATE POLICY project_isolation ON logical_table_data_versions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON logical_table_data_versions TO td_across_projects USING (true) WITH CHECK (true)

DROP POLICY project_isolation ON logical_function_run_inputs

CREATE POLICY project_isolation ON logical_function_run_inputs USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON logical_function_run_inputs TO td_across_projects USING (true) WITH CHECK (true)

UPDATE tabsdata_system SET value = '4' WHERE name = 'db_version'
