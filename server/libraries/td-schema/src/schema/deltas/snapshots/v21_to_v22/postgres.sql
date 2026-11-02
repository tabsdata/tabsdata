-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

CREATE TABLE "user_ui_data" ( "user_id" text NOT NULL PRIMARY KEY COLLATE "C", "etag" uuid NOT NULL, "modified_on" timestamp with time zone NOT NULL, "data" text NOT NULL, FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

UPDATE tabsdata_system SET value = '22' WHERE name = 'db_version'
