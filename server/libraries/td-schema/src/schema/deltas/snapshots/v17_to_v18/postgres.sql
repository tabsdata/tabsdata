-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

DO $$
DECLARE fk record;
BEGIN
    FOR fk IN SELECT conrelid::regclass AS referrer, conname FROM pg_constraint
              WHERE contype = 'f'
                AND confrelid IN ('collections'::regclass, 'groups'::regclass, 'projects'::regclass)
    LOOP
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', fk.referrer, fk.conname);
    END LOOP;
END $$

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

ALTER TABLE "collections" DROP CONSTRAINT "collections_pkey"

ALTER TABLE "collections" RENAME COLUMN "id" TO "collection_id"

ALTER TABLE "collections" RENAME CONSTRAINT "collections_id_not_null" TO "collections_collection_id_not_null"

ALTER TABLE "collections" ADD COLUMN "id" text COLLATE "C" NULL, ADD COLUMN "valid_from" timestamp with time zone NULL, ADD COLUMN "valid_to" timestamp with time zone NULL, ADD COLUMN "defined_by_id" text COLLATE "C" NULL

INSERT INTO collections (project_id, collection_id, name, description, base_path, created_on, created_by_id, group_id, collection_type, connection_id, modified_on, modified_by_id, id, valid_from, valid_to, defined_by_id) SELECT project_id, collection_id, name, description, base_path, created_on, created_by_id, group_id, collection_type, connection_id, modified_on, modified_by_id, td_mint_id(modified_on), modified_on, 'infinity', modified_by_id FROM collections WHERE name_when_deleted IS NULL AND modified_on > created_on

UPDATE collections c SET id = td_mint_id(c.created_on), valid_from = c.created_on, defined_by_id = c.created_by_id, name = COALESCE(c.name_when_deleted, c.name), valid_to = CASE WHEN c.name_when_deleted IS NOT NULL THEN c.modified_on WHEN c.modified_on > c.created_on THEN c.modified_on ELSE 'infinity'::timestamptz END WHERE c.id IS NULL

ALTER TABLE "collections" ALTER COLUMN "id" SET NOT NULL, ALTER COLUMN "valid_from" SET NOT NULL, ALTER COLUMN "valid_to" SET NOT NULL, ALTER COLUMN "defined_by_id" SET NOT NULL

ALTER TABLE "collections" ADD PRIMARY KEY ("id")

ALTER TABLE "collections" DROP COLUMN "created_on"

ALTER TABLE "collections" DROP COLUMN "created_by_id"

ALTER TABLE "collections" DROP COLUMN "modified_on"

ALTER TABLE "collections" DROP COLUMN "modified_by_id"

ALTER TABLE "collections" DROP COLUMN "name_when_deleted"

ALTER TABLE "groups" DROP CONSTRAINT "groups_pkey"

ALTER TABLE "groups" RENAME COLUMN "id" TO "group_id"

ALTER TABLE "groups" RENAME CONSTRAINT "groups_id_not_null" TO "groups_group_id_not_null"

ALTER TABLE "groups" ADD COLUMN "id" text COLLATE "C" NULL, ADD COLUMN "valid_from" timestamp with time zone NULL, ADD COLUMN "valid_to" timestamp with time zone NULL, ADD COLUMN "defined_by_id" text COLLATE "C" NULL

INSERT INTO groups (project_id, group_id, name, description, created_on, created_by_id, system, kind, modified_on, modified_by_id, id, valid_from, valid_to, defined_by_id) SELECT g.project_id, g.group_id, g.name, g.description, g.created_on, g.created_by_id, g.system, g.kind, g.modified_on, g.modified_by_id, td_mint_id(g.modified_on), g.modified_on, CASE WHEN p.name_when_deleted IS NOT NULL THEN p.modified_on ELSE 'infinity'::timestamptz END, g.modified_by_id FROM groups g JOIN projects p ON p.id = g.project_id WHERE g.modified_on > g.created_on AND g.modified_on < CASE WHEN p.name_when_deleted IS NOT NULL THEN p.modified_on ELSE 'infinity'::timestamptz END

UPDATE groups g SET id = td_mint_id(g.created_on), valid_from = g.created_on, defined_by_id = g.created_by_id, valid_to = CASE WHEN g.modified_on > g.created_on AND g.modified_on < CASE WHEN p.name_when_deleted IS NOT NULL THEN p.modified_on ELSE 'infinity'::timestamptz END THEN g.modified_on ELSE CASE WHEN p.name_when_deleted IS NOT NULL THEN p.modified_on ELSE 'infinity'::timestamptz END END FROM projects p WHERE p.id = g.project_id AND g.id IS NULL

ALTER TABLE "groups" ALTER COLUMN "id" SET NOT NULL, ALTER COLUMN "valid_from" SET NOT NULL, ALTER COLUMN "valid_to" SET NOT NULL, ALTER COLUMN "defined_by_id" SET NOT NULL

ALTER TABLE "groups" ADD PRIMARY KEY ("id")

ALTER TABLE "groups" DROP COLUMN "created_on"

ALTER TABLE "groups" DROP COLUMN "created_by_id"

ALTER TABLE "groups" DROP COLUMN "modified_on"

ALTER TABLE "groups" DROP COLUMN "modified_by_id"

ALTER TABLE "projects" DROP CONSTRAINT "projects_pkey"

ALTER TABLE "projects" RENAME COLUMN "id" TO "project_id"

ALTER TABLE "projects" RENAME CONSTRAINT "projects_id_not_null" TO "projects_project_id_not_null"

ALTER TABLE "projects" ADD COLUMN "id" text COLLATE "C" NULL, ADD COLUMN "valid_from" timestamp with time zone NULL, ADD COLUMN "valid_to" timestamp with time zone NULL, ADD COLUMN "defined_by_id" text COLLATE "C" NULL

INSERT INTO projects (project_id, name, description, base_path, created_on, created_by_id, modified_on, modified_by_id, id, valid_from, valid_to, defined_by_id) SELECT project_id, name, description, base_path, created_on, created_by_id, modified_on, modified_by_id, td_mint_id(modified_on), modified_on, 'infinity', modified_by_id FROM projects WHERE name_when_deleted IS NULL AND modified_on > created_on

UPDATE projects SET id = td_mint_id(created_on), valid_from = created_on, defined_by_id = created_by_id, name = COALESCE(name_when_deleted, name), valid_to = CASE WHEN name_when_deleted IS NOT NULL THEN modified_on WHEN modified_on > created_on THEN modified_on ELSE 'infinity'::timestamptz END WHERE id IS NULL

ALTER TABLE "projects" ALTER COLUMN "id" SET NOT NULL, ALTER COLUMN "valid_from" SET NOT NULL, ALTER COLUMN "valid_to" SET NOT NULL, ALTER COLUMN "defined_by_id" SET NOT NULL

ALTER TABLE "projects" ADD PRIMARY KEY ("id")

ALTER TABLE "projects" DROP COLUMN "created_on"

ALTER TABLE "projects" DROP COLUMN "created_by_id"

ALTER TABLE "projects" DROP COLUMN "modified_on"

ALTER TABLE "projects" DROP COLUMN "modified_by_id"

ALTER TABLE "projects" DROP COLUMN "name_when_deleted"

DROP FUNCTION td_mint_id(timestamptz)

UPDATE tabsdata_system SET value = '18' WHERE name = 'db_version'
