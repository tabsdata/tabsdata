-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

CREATE TABLE "service_tokens" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "user_id" text NOT NULL COLLATE "C", "slot" integer NOT NULL CHECK ("slot" IN (1, 2)), "digest" text NOT NULL UNIQUE, "hint" text NOT NULL, "comment" text NULL, "created_on" timestamp with time zone NOT NULL, "expires_on" timestamp with time zone NOT NULL, "last_used_on" timestamp with time zone NULL, "scope" text NOT NULL, "system" bool NOT NULL, FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

DELETE FROM "sessions" WHERE "access_token_id" = 'k0000000000000000000000004'

UPDATE tabsdata_system SET value = '12' WHERE name = 'db_version'
