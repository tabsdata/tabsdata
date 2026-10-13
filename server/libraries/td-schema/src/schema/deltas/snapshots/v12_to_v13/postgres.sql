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

SET LOCAL work_mem = '64MB'

UPDATE "transactions" AS "target" SET "status" = "summary"."status", "started_on" = "summary"."started_on", "ended_on" = (CASE WHEN ("summary"."finished") THEN "summary"."ended_on" END) FROM (SELECT "parents"."id" AS "id", (CASE WHEN ((count(*) FILTER (WHERE "children"."status" IN ('S'))) = COUNT("children"."status")) THEN 'S' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('S', 'RR', 'RS', 'R', 'E'))) > 0) THEN 'R' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('F', 'H'))) > 0) THEN 'L' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('Y'))) > 0) THEN 'Y' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('D'))) > 0) THEN 'R' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('C'))) = COUNT("children"."status")) THEN 'C' ELSE 'X' END) AS "status", (count(*)) = (count(*) FILTER (WHERE "children"."status" IN ('C', 'X', 'Y'))) AS "finished", MIN("instants"."started_on") AS "started_on", MAX("instants"."ended_on") AS "ended_on" FROM "transactions" AS "parents" LEFT JOIN "logical_function_runs" AS "children" ON "children"."transaction_id" = "parents"."id" LEFT JOIN (SELECT "physical_runs"."transaction_id" AS "parent_id", MIN("physical_runs"."started_on") AS "started_on", MAX("physical_runs"."ended_on") AS "ended_on" FROM "physical_function_runs" AS "physical_runs" GROUP BY "physical_runs"."transaction_id") AS "instants" ON "instants"."parent_id" = "parents"."id" GROUP BY "parents"."id") AS "summary" WHERE "target"."id" = "summary"."id" AND (("target"."status" IS DISTINCT FROM "summary"."status") OR ("target"."started_on" IS DISTINCT FROM "summary"."started_on") OR ("target"."ended_on" IS DISTINCT FROM (CASE WHEN ("summary"."finished") THEN "summary"."ended_on" END)))

UPDATE "executions" AS "target" SET "status" = "summary"."status", "started_on" = "summary"."started_on", "ended_on" = (CASE WHEN ("summary"."finished") THEN "summary"."ended_on" END) FROM (SELECT "parents"."id" AS "id", (CASE WHEN ((count(*) FILTER (WHERE "children"."status" IN ('S'))) = COUNT("children"."status")) THEN 'S' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('S', 'R'))) > 0) THEN 'R' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('L'))) > 0) THEN 'L' ELSE 'F' END) AS "status", (count(*)) = (count(*) FILTER (WHERE "children"."status" IN ('C', 'X', 'Y'))) AS "finished", MIN("instants"."started_on") AS "started_on", MAX("instants"."ended_on") AS "ended_on" FROM "executions" AS "parents" LEFT JOIN "transactions" AS "children" ON "children"."execution_id" = "parents"."id" LEFT JOIN (SELECT "physical_runs"."execution_id" AS "parent_id", MIN("physical_runs"."started_on") AS "started_on", MAX("physical_runs"."ended_on") AS "ended_on" FROM "physical_function_runs" AS "physical_runs" GROUP BY "physical_runs"."execution_id") AS "instants" ON "instants"."parent_id" = "parents"."id" GROUP BY "parents"."id") AS "summary" WHERE "target"."id" = "summary"."id" AND (("target"."status" IS DISTINCT FROM "summary"."status") OR ("target"."started_on" IS DISTINCT FROM "summary"."started_on") OR ("target"."ended_on" IS DISTINCT FROM (CASE WHEN ("summary"."finished") THEN "summary"."ended_on" END)))

RESET work_mem

UPDATE tabsdata_system SET value = '13' WHERE name = 'db_version'
