-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

CREATE TABLE "tags" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "tag_id" text NOT NULL COLLATE "C", "project_id" text NOT NULL COLLATE "C", "entity_type" text NOT NULL, "entity_id" text NOT NULL COLLATE "C", "kind" text NOT NULL, "key" text NOT NULL, "value" text NOT NULL, "valid_from" timestamp with time zone NOT NULL, "defined_by_id" text NOT NULL COLLATE "C", "valid_to" timestamp with time zone NOT NULL )

ALTER TABLE tags ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE tags ENABLE ROW LEVEL SECURITY

ALTER TABLE tags FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON tags USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON tags TO td_across_projects USING (true) WITH CHECK (true)

CREATE FUNCTION td_mint_id(instant timestamptz) RETURNS text
LANGUAGE sql VOLATILE PARALLEL SAFE AS $$
  SELECT string_agg(substr('0123456789abcdefghijklmnopqrstuv', substring(bits FROM i * 5 + 1 FOR 5)::int + 1, 1), '' ORDER BY i)
  FROM (
    SELECT ('x' || encode(set_bit(set_bit(
      overlay(uuid_send(gen_random_uuid())
        PLACING substring(int8send(floor(extract(epoch FROM instant) * 1000)::bigint) FROM 3) FROM 1 FOR 6),
      52, 1), 53, 1), 'hex'))::bit(128) || B'00' AS bits
  ) AS uuid, generate_series(0, 25) AS i
$$

INSERT INTO tags (id, tag_id, project_id, entity_type, entity_id, kind, key, value, valid_from, defined_by_id, valid_to) SELECT td_mint_id(min(i.valid_from)), td_mint_id(min(i.valid_from)), i.project_id, 'F', i.function_id, 'T', '_parent', i.parent_function_id, min(i.valid_from), (array_agg(i.defined_by_id ORDER BY i.valid_from, i.id))[1], max(i.valid_to) FROM (SELECT r.*, sum(r.starts) OVER (PARTITION BY r.function_id ORDER BY r.valid_from, r.id) AS run FROM (SELECT v.id, v.project_id, v.function_id, v.parent_function_id, v.valid_from, v.valid_to, v.defined_by_id, CASE WHEN lag(v.parent_function_id) OVER w = v.parent_function_id AND lag(v.valid_to) OVER w = v.valid_from THEN 0 ELSE 1 END AS starts FROM functions v WHERE v.parent_function_id IS NOT NULL WINDOW w AS (PARTITION BY v.function_id ORDER BY v.valid_from, v.id)) r) i GROUP BY i.function_id, i.run, i.project_id, i.parent_function_id

INSERT INTO tags (id, tag_id, project_id, entity_type, entity_id, kind, key, value, valid_from, defined_by_id, valid_to) SELECT td_mint_id(min(i.valid_from)), td_mint_id(min(i.valid_from)), i.project_id, 'T', i.table_id, 'T', '_parent', i.parent_table_id, min(i.valid_from), (array_agg(i.defined_by_id ORDER BY i.valid_from, i.id))[1], max(i.valid_to) FROM (SELECT r.*, sum(r.starts) OVER (PARTITION BY r.table_id ORDER BY r.valid_from, r.id) AS run FROM (SELECT v.id, v.project_id, v.table_id, v.parent_table_id, v.valid_from, v.valid_to, v.defined_by_id, CASE WHEN lag(v.parent_table_id) OVER w = v.parent_table_id AND lag(v.valid_to) OVER w = v.valid_from THEN 0 ELSE 1 END AS starts FROM tables v WHERE v.parent_table_id IS NOT NULL WINDOW w AS (PARTITION BY v.table_id ORDER BY v.valid_from, v.id)) r) i GROUP BY i.table_id, i.run, i.project_id, i.parent_table_id

DROP FUNCTION td_mint_id(timestamptz)

ALTER TABLE functions DROP COLUMN parent_function_id

ALTER TABLE tables DROP COLUMN parent_table_id

UPDATE tabsdata_system SET value = '19' WHERE name = 'db_version'
