-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

DO $$
DECLARE
  duplicates text;
BEGIN
  SELECT string_agg(format('%s %s in collection %s', kind, name, collection_id), '; ' ORDER BY kind, collection_id, name)
    INTO duplicates
    FROM (
      SELECT 'function' AS kind, collection_id, name FROM functions
       WHERE valid_to = 'infinity' GROUP BY collection_id, name HAVING count(*) > 1
      UNION ALL
      SELECT 'table' AS kind, collection_id, name FROM tables
       WHERE valid_to = 'infinity' GROUP BY collection_id, name HAVING count(*) > 1
    ) d;
  IF duplicates IS NOT NULL THEN
    RAISE EXCEPTION 'Live names must be unique within a collection; rename or delete one of each, then upgrade again: %', duplicates;
  END IF;
END $$

ALTER TABLE projects DROP CONSTRAINT IF EXISTS projects_name_key

ALTER TABLE roles DROP CONSTRAINT IF EXISTS roles_name_key

DELETE FROM "users_roles" WHERE "user_id" NOT IN (SELECT "id" FROM "users")

ALTER TABLE "users_roles" ADD FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "logical_function_run_inputs" ADD COLUMN "requirement_table_version_id" text NULL COLLATE "C"

UPDATE "logical_function_run_inputs" AS i SET "requirement_table_version_id" = t."id"
FROM "logical_function_runs" AS r, "functions" AS f, "dependencies" AS d, "tables" AS t
WHERE i."requirement_dependency_pos" IS NOT NULL
  AND r."id" = i."logical_function_run_id"
  AND f."id" = r."function_version_id"
  AND d."function_id" = r."function_id" AND d."dep_pos" = i."requirement_dependency_pos"
  AND d."valid_from" <= f."valid_from" AND f."valid_from" < d."valid_to"
  AND t."table_id" = d."table_id"
  AND t."valid_from" <= r."triggered_on" AND r."triggered_on" < t."valid_to"

ALTER TABLE "workers" ADD COLUMN "failures" integer NOT NULL DEFAULT 0, ADD COLUMN "retry_at" timestamp with time zone NULL, ADD COLUMN "destination_write_on" timestamp with time zone NULL, DROP COLUMN "message_status"

UPDATE workers
SET status = 'SP', ended_on = transaction_timestamp()
WHERE status = 'SPS'
  AND NOT worker_enabled
  AND worker_type IN ('CT', 'ST')

ALTER TABLE "logical_function_runs" ADD COLUMN "recoveries" integer NOT NULL DEFAULT 0

UPDATE physical_table_data_versions t
SET valid_to = t.valid_from
FROM physical_function_runs r
WHERE r.id = t.physical_function_run_id
  AND t.has_data IS NULL
  AND r.status <> 'D'
  AND t.valid_to IS DISTINCT FROM t.valid_from

UPDATE physical_table_data_versions t
SET valid_to = n.next_valid_from
FROM (
    SELECT id, COALESCE(
        LEAD(valid_from) OVER (PARTITION BY logical_table_data_version_id ORDER BY valid_from, id),
        'infinity'
    ) AS next_valid_from
    FROM physical_table_data_versions
    WHERE valid_to IS DISTINCT FROM valid_from
      AND logical_table_data_version_id IN (
          SELECT logical_table_data_version_id FROM physical_table_data_versions WHERE valid_to = valid_from
      )
) AS n
WHERE t.id = n.id
  AND t.valid_to IS DISTINCT FROM n.next_valid_from

SET LOCAL work_mem = '64MB'

UPDATE "transactions" AS "target" SET "status" = "summary"."status", "started_on" = "summary"."started_on", "ended_on" = (CASE WHEN ("summary"."finished") THEN "summary"."ended_on" END) FROM (SELECT "parents"."id" AS "id", (CASE WHEN ((count(*) FILTER (WHERE "children"."status" IN ('S'))) = COUNT("children"."status")) THEN 'S' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('S', 'RR', 'RS', 'R', 'E'))) > 0) THEN 'R' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('F', 'H'))) > 0) THEN 'L' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('Y'))) > 0) THEN 'Y' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('D'))) > 0) THEN 'R' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('C'))) = COUNT("children"."status")) THEN 'C' ELSE 'X' END) AS "status", (count(*)) = (count(*) FILTER (WHERE "children"."status" IN ('C', 'X', 'Y'))) AS "finished", MIN("instants"."started_on") AS "started_on", MAX("instants"."ended_on") AS "ended_on" FROM "transactions" AS "parents" LEFT JOIN "logical_function_runs" AS "children" ON "children"."transaction_id" = "parents"."id" LEFT JOIN (SELECT "physical_runs"."transaction_id" AS "parent_id", MIN("physical_runs"."started_on") AS "started_on", MAX("physical_runs"."ended_on") AS "ended_on" FROM "physical_function_runs" AS "physical_runs" GROUP BY "physical_runs"."transaction_id") AS "instants" ON "instants"."parent_id" = "parents"."id" GROUP BY "parents"."id") AS "summary" WHERE "target"."id" = "summary"."id" AND (("target"."status" IS DISTINCT FROM "summary"."status") OR ("target"."started_on" IS DISTINCT FROM "summary"."started_on") OR ("target"."ended_on" IS DISTINCT FROM (CASE WHEN ("summary"."finished") THEN "summary"."ended_on" END)))

UPDATE "executions" AS "target" SET "status" = "summary"."status", "started_on" = "summary"."started_on", "ended_on" = (CASE WHEN ("summary"."finished") THEN "summary"."ended_on" END) FROM (SELECT "parents"."id" AS "id", (CASE WHEN ((count(*) FILTER (WHERE "children"."status" IN ('S'))) = COUNT("children"."status")) THEN 'S' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('S', 'R'))) > 0) THEN 'R' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('L'))) > 0) THEN 'L' ELSE 'F' END) AS "status", (count(*)) = (count(*) FILTER (WHERE "children"."status" IN ('C', 'X', 'Y'))) AS "finished", MIN("instants"."started_on") AS "started_on", MAX("instants"."ended_on") AS "ended_on" FROM "executions" AS "parents" LEFT JOIN "transactions" AS "children" ON "children"."execution_id" = "parents"."id" LEFT JOIN (SELECT "physical_runs"."execution_id" AS "parent_id", MIN("physical_runs"."started_on") AS "started_on", MAX("physical_runs"."ended_on") AS "ended_on" FROM "physical_function_runs" AS "physical_runs" GROUP BY "physical_runs"."execution_id") AS "instants" ON "instants"."parent_id" = "parents"."id" GROUP BY "parents"."id") AS "summary" WHERE "target"."id" = "summary"."id" AND (("target"."status" IS DISTINCT FROM "summary"."status") OR ("target"."started_on" IS DISTINCT FROM "summary"."started_on") OR ("target"."ended_on" IS DISTINCT FROM (CASE WHEN ("summary"."finished") THEN "summary"."ended_on" END)))

RESET work_mem

UPDATE tabsdata_system SET value = '13' WHERE name = 'db_version'
