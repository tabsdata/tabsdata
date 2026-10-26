-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

CREATE TABLE "tags" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "tag_id" text NOT NULL COLLATE "C", "project_id" text NOT NULL COLLATE "C", "entity_type" text NOT NULL, "entity_id" text NOT NULL COLLATE "C", "kind" text NOT NULL, "key" text NOT NULL, "value" text NOT NULL, "valid_from" timestamp with time zone NOT NULL, "defined_by_id" text NOT NULL COLLATE "C", "valid_to" timestamp with time zone NOT NULL )

ALTER TABLE tags ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE tags ENABLE ROW LEVEL SECURITY

ALTER TABLE tags FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON tags USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON tags TO td_across_projects USING (true) WITH CHECK (true)

UPDATE tabsdata_system SET value = '19' WHERE name = 'db_version'
