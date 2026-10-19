-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

CREATE TABLE "project_topologies" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "project_id" text NOT NULL COLLATE "C", "valid_from" timestamp with time zone NOT NULL, "valid_to" timestamp with time zone NOT NULL )

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

INSERT INTO project_topologies (id, project_id, valid_from, valid_to)
SELECT td_mint_id(at), project_id, at,
       COALESCE(lead(at) OVER (PARTITION BY project_id ORDER BY at), 'infinity')
FROM (
    SELECT DISTINCT project_id, at FROM (
        SELECT id AS project_id, created_on AS at FROM projects
        UNION ALL SELECT project_id, valid_from FROM functions
        UNION ALL SELECT project_id, valid_to FROM functions WHERE valid_to <> 'infinity'
        UNION ALL SELECT project_id, valid_from FROM tables
        UNION ALL SELECT project_id, valid_to FROM tables WHERE valid_to <> 'infinity'
        UNION ALL SELECT project_id, valid_from FROM dependencies
        UNION ALL SELECT project_id, valid_to FROM dependencies WHERE valid_to <> 'infinity'
        UNION ALL SELECT project_id, valid_from FROM triggers
        UNION ALL SELECT project_id, valid_to FROM triggers WHERE valid_to <> 'infinity'
    ) AS instants
) AS distinct_instants

DROP FUNCTION td_mint_id(timestamptz)

ALTER TABLE project_topologies ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE project_topologies ENABLE ROW LEVEL SECURITY

ALTER TABLE project_topologies FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON project_topologies USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON project_topologies TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE "executions" ADD COLUMN "topology_version_id" text NULL COLLATE "C"

UPDATE executions e SET topology_version_id = COALESCE(
    (SELECT t.id FROM project_topologies t
      WHERE t.project_id = e.project_id AND t.valid_from <= e.triggered_on
      ORDER BY t.valid_from DESC LIMIT 1),
    (SELECT t.id FROM project_topologies t
      WHERE t.project_id = e.project_id ORDER BY t.valid_from LIMIT 1)
)

ALTER TABLE executions ALTER COLUMN topology_version_id SET NOT NULL

ALTER TABLE executions ADD FOREIGN KEY (topology_version_id) REFERENCES project_topologies (id) ON DELETE RESTRICT ON UPDATE RESTRICT

UPDATE tabsdata_system SET value = '16' WHERE name = 'db_version'
