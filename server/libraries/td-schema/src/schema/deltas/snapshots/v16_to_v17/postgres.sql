-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

ALTER TABLE executions DROP COLUMN recursion_options

DROP TABLE IF EXISTS pg_temp.v17_ended_attempts

CREATE TEMPORARY TABLE pg_temp.v17_ended_attempts (id bigint PRIMARY KEY) ON COMMIT DROP

WITH repaired AS (
  SELECT p.id,
         COALESCE(GREATEST(t.ended_on, p.started_on, p.dispatched_on), now()) AS ended_on
    FROM physical_function_runs p
    JOIN logical_function_runs l ON l.id = p.logical_function_run_id
    JOIN transactions t ON t.id = p.transaction_id
   WHERE p.status IN ('RR', 'DP', 'R')
     AND l.status = 'X'
), failed_workers AS (
  UPDATE workers w SET status = 'F', ended_on = r.ended_on
    FROM repaired r
   WHERE w.physical_function_run_id = r.id
     AND w.worker_type = 'F' AND w.status IN ('RR', 'R')
), ended AS (
  UPDATE physical_function_runs p SET status = 'F', ended_on = r.ended_on
    FROM repaired r
   WHERE p.id = r.id
  RETURNING p.id
)
INSERT INTO pg_temp.v17_ended_attempts SELECT id FROM ended

UPDATE "transactions" AS "target" SET "status" = "summary"."status", "started_on" = "summary"."started_on", "ended_on" = (CASE WHEN ("summary"."finished") THEN COALESCE("summary"."ended_on", transaction_timestamp()) END) FROM (SELECT "parents"."id" AS "id", (CASE WHEN ((count(*) FILTER (WHERE "children"."status" IN ('S'))) = COUNT("children"."status")) THEN 'S' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('S', 'RR', 'RS', 'R', 'E'))) > 0) THEN 'R' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('F', 'H'))) > 0) THEN 'L' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('Y'))) > 0) THEN 'Y' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('D'))) > 0) THEN 'R' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('C'))) = COUNT("children"."status")) THEN 'C' ELSE 'X' END) AS "status", (count(*)) = (count(*) FILTER (WHERE "children"."status" IN ('C', 'X', 'Y'))) AS "finished", MIN("instants"."started_on") AS "started_on", MAX("instants"."ended_on") AS "ended_on" FROM "transactions" AS "parents" LEFT JOIN "logical_function_runs" AS "children" ON "children"."transaction_id" = "parents"."id" LEFT JOIN (SELECT "physical_runs"."transaction_id" AS "parent_id", MIN("physical_runs"."started_on") AS "started_on", MAX("physical_runs"."ended_on") AS "ended_on" FROM "physical_function_runs" AS "physical_runs" WHERE "physical_runs"."transaction_id" IN (SELECT "transaction_id" FROM "physical_function_runs" WHERE "id" IN (SELECT id FROM pg_temp.v17_ended_attempts)) GROUP BY "physical_runs"."transaction_id") AS "instants" ON "instants"."parent_id" = "parents"."id" WHERE "parents"."id" IN (SELECT "transaction_id" FROM "physical_function_runs" WHERE "id" IN (SELECT id FROM pg_temp.v17_ended_attempts)) GROUP BY "parents"."id") AS "summary" WHERE "target"."id" = "summary"."id" AND (("target"."status" IS DISTINCT FROM "summary"."status") OR ("target"."started_on" IS DISTINCT FROM "summary"."started_on") OR ("target"."ended_on" IS DISTINCT FROM (CASE WHEN ("summary"."finished") THEN COALESCE("summary"."ended_on", transaction_timestamp()) END)))

UPDATE "executions" AS "target" SET "status" = "summary"."status", "started_on" = "summary"."started_on", "ended_on" = (CASE WHEN ("summary"."finished") THEN COALESCE("summary"."ended_on", transaction_timestamp()) END) FROM (SELECT "parents"."id" AS "id", (CASE WHEN ((count(*) FILTER (WHERE "children"."status" IN ('S'))) = COUNT("children"."status")) THEN 'S' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('S', 'R'))) > 0) THEN 'R' WHEN ((count(*) FILTER (WHERE "children"."status" IN ('L'))) > 0) THEN 'L' ELSE 'F' END) AS "status", (count(*)) = (count(*) FILTER (WHERE "children"."status" IN ('C', 'X', 'Y'))) AS "finished", MIN("instants"."started_on") AS "started_on", MAX("instants"."ended_on") AS "ended_on" FROM "executions" AS "parents" LEFT JOIN "transactions" AS "children" ON "children"."execution_id" = "parents"."id" LEFT JOIN (SELECT "physical_runs"."execution_id" AS "parent_id", MIN("physical_runs"."started_on") AS "started_on", MAX("physical_runs"."ended_on") AS "ended_on" FROM "physical_function_runs" AS "physical_runs" WHERE "physical_runs"."execution_id" IN (SELECT "execution_id" FROM "physical_function_runs" WHERE "id" IN (SELECT id FROM pg_temp.v17_ended_attempts)) GROUP BY "physical_runs"."execution_id") AS "instants" ON "instants"."parent_id" = "parents"."id" WHERE "parents"."id" IN (SELECT "execution_id" FROM "physical_function_runs" WHERE "id" IN (SELECT id FROM pg_temp.v17_ended_attempts)) GROUP BY "parents"."id") AS "summary" WHERE "target"."id" = "summary"."id" AND (("target"."status" IS DISTINCT FROM "summary"."status") OR ("target"."started_on" IS DISTINCT FROM "summary"."started_on") OR ("target"."ended_on" IS DISTINCT FROM (CASE WHEN ("summary"."finished") THEN COALESCE("summary"."ended_on", transaction_timestamp()) END)))

DROP TABLE IF EXISTS pg_temp.v17_ended_attempts

UPDATE tabsdata_system SET value = '17' WHERE name = 'db_version'
