-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

INSERT INTO "tabsdata_system" ("name", "value") VALUES ('instance_id', gen_random_uuid()::text) ON CONFLICT ("name") DO NOTHING

UPDATE tabsdata_system SET value = '20' WHERE name = 'db_version'
