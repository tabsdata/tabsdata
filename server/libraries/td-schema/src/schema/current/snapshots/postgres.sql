-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

CREATE TABLE "users" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "name" text NOT NULL UNIQUE, "full_name" text NOT NULL, "email" text NULL UNIQUE, "created_on" timestamp with time zone NOT NULL, "created_by_id" text NOT NULL COLLATE "C", "modified_on" timestamp with time zone NOT NULL, "modified_by_id" text NOT NULL COLLATE "C", "password_hash" text NOT NULL, "password_set_on" timestamp with time zone NOT NULL, "password_must_change" bool NOT NULL, "enabled" bool NOT NULL, "system" bool NOT NULL )

CREATE TABLE "service_tokens" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "user_id" text NOT NULL COLLATE "C", "slot" integer NOT NULL CHECK ("slot" IN (1, 2)), "digest" text NOT NULL UNIQUE, "hint" text NOT NULL, "comment" text NULL, "created_on" timestamp with time zone NOT NULL, "expires_on" timestamp with time zone NOT NULL, "last_used_on" timestamp with time zone NULL, "scope" text NOT NULL, "system" bool NOT NULL, FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "groups" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "project_id" text NOT NULL COLLATE "C", "name" text NOT NULL, "description" text NOT NULL, "created_on" timestamp with time zone NOT NULL, "created_by_id" text NOT NULL COLLATE "C", "modified_on" timestamp with time zone NOT NULL, "modified_by_id" text NOT NULL COLLATE "C", "system" bool NOT NULL, "kind" text NOT NULL )

CREATE TABLE "projects" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "name" text NOT NULL UNIQUE, "description" text NOT NULL, "base_path" text NOT NULL, "created_on" timestamp with time zone NOT NULL, "created_by_id" text NOT NULL COLLATE "C", "modified_on" timestamp with time zone NOT NULL, "modified_by_id" text NOT NULL COLLATE "C", "name_when_deleted" text NULL )

CREATE TABLE "connections" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "project_id" text NOT NULL COLLATE "C", "kind" text NOT NULL, "created_on" timestamp with time zone NOT NULL, "created_by_id" text NOT NULL COLLATE "C", "modified_on" timestamp with time zone NOT NULL, "modified_by_id" text NOT NULL COLLATE "C" )

CREATE TABLE "collections" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "project_id" text NOT NULL COLLATE "C", "name" text NOT NULL, "description" text NOT NULL, "created_on" timestamp with time zone NOT NULL, "created_by_id" text NOT NULL COLLATE "C", "modified_on" timestamp with time zone NOT NULL, "modified_by_id" text NOT NULL COLLATE "C", "name_when_deleted" text NULL, "group_id" text NOT NULL COLLATE "C", "base_path" text NULL, "collection_type" text NOT NULL, "connection_id" text NULL COLLATE "C", FOREIGN KEY ("group_id") REFERENCES "groups" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("connection_id") REFERENCES "connections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "roles" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "name" text NOT NULL UNIQUE, "description" text NOT NULL, "created_on" timestamp with time zone NOT NULL, "created_by_id" text NOT NULL COLLATE "C", "modified_on" timestamp with time zone NOT NULL, "modified_by_id" text NOT NULL COLLATE "C", "kind" text NOT NULL, "project_id" text NULL COLLATE "C", "fixed" bool NOT NULL, FOREIGN KEY ("project_id") REFERENCES "projects" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "users_roles" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "user_id" text NOT NULL COLLATE "C", "role_id" text NOT NULL COLLATE "C", "added_on" timestamp with time zone NOT NULL, "added_by_id" text NOT NULL COLLATE "C", "fixed" bool NOT NULL, FOREIGN KEY ("role_id") REFERENCES "roles" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "sessions" ( "access_token_id" text NOT NULL PRIMARY KEY COLLATE "C", "refresh_token_id" text NOT NULL UNIQUE COLLATE "C", "user_id" text NOT NULL COLLATE "C", "created_on" timestamp with time zone NOT NULL, "expires_on" timestamp with time zone NOT NULL, "status_change_on" timestamp with time zone NOT NULL, "status" text NOT NULL )

CREATE TABLE "permissions" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "role_id" text NOT NULL COLLATE "C", "permission_type" text NOT NULL, "entity_type" text NOT NULL, "entity_id" text NOT NULL COLLATE "C", "granted_by_id" text NOT NULL COLLATE "C", "granted_on" timestamp with time zone NOT NULL, "fixed" bool NOT NULL, FOREIGN KEY ("role_id") REFERENCES "roles" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "bundles" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "project_id" text NOT NULL COLLATE "C", "collection_id" text NOT NULL COLLATE "C", "hash" text NOT NULL, "base_path" text NOT NULL, "status" text NOT NULL, "created_on" timestamp with time zone NOT NULL, "created_by_id" text NOT NULL COLLATE "C", FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "functions" ( "id" bigint NOT NULL PRIMARY KEY, "project_id" text NOT NULL COLLATE "C", "collection_id" text NOT NULL COLLATE "C", "name" text NOT NULL, "description" text NOT NULL, "decorator" text NOT NULL, "runtime_values" text NOT NULL, "function_id" text NOT NULL COLLATE "C", "base_path" text NULL, "stage_id" text NOT NULL COLLATE "C", "stage_base_path" text NOT NULL, "storage_version" text NOT NULL, "bundle_id" text NOT NULL COLLATE "C", "snippet" text NOT NULL, "valid_from" timestamp with time zone NOT NULL, "defined_by_id" text NOT NULL COLLATE "C", "status" text NOT NULL, "connector" text NULL, "connection" text NULL, "enabled" bool DEFAULT TRUE, "valid_to" timestamp with time zone NOT NULL, "function_type" text NOT NULL, "parent_function_id" text NULL COLLATE "C", FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "function_patches" ( "id" bigint NOT NULL PRIMARY KEY, "project_id" text NOT NULL COLLATE "C", "parent_function_version_id" bigint NOT NULL, "function_id" text NOT NULL COLLATE "C", "collection_id" text NOT NULL COLLATE "C", "snippet" text NOT NULL, "bundle_id" text NOT NULL COLLATE "C", "runtime_values" text NOT NULL, "description" text NOT NULL, "valid_from" timestamp with time zone NOT NULL, "defined_by_id" text NOT NULL COLLATE "C", "status" text NOT NULL, "valid_to" timestamp with time zone NOT NULL, FOREIGN KEY ("parent_function_version_id") REFERENCES "functions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "tables" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "project_id" text NOT NULL COLLATE "C", "collection_id" text NOT NULL COLLATE "C", "table_id" text NOT NULL COLLATE "C", "name" text NOT NULL, "function_id" text NULL COLLATE "C", "function_version_id" bigint NOT NULL, "function_param_pos" integer NULL, "private" bool NOT NULL, "partitioned" bool NULL, "valid_from" timestamp with time zone NOT NULL, "defined_by_id" text NOT NULL COLLATE "C", "status" text NOT NULL, "table_type" text NOT NULL DEFAULT 'R', "valid_to" timestamp with time zone NOT NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_id") REFERENCES "functions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "dependencies" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "project_id" text NOT NULL COLLATE "C", "collection_id" text NOT NULL COLLATE "C", "dependency_id" text NOT NULL COLLATE "C", "function_id" text NOT NULL COLLATE "C", "table_collection_id" text NOT NULL COLLATE "C", "table_function_id" text NOT NULL COLLATE "C", "table_id" text NOT NULL COLLATE "C", "table_versions" text NOT NULL, "dep_pos" integer NOT NULL, "valid_from" timestamp with time zone NOT NULL, "defined_by_id" text NOT NULL COLLATE "C", "system" bool NOT NULL, "valid_to" timestamp with time zone NOT NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "triggers" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "project_id" text NOT NULL COLLATE "C", "collection_id" text NOT NULL COLLATE "C", "trigger_id" text NOT NULL COLLATE "C", "function_id" text NOT NULL COLLATE "C", "trigger_by_collection_id" text NOT NULL COLLATE "C", "trigger_by_function_id" text NOT NULL COLLATE "C", "trigger_by_table_id" text NOT NULL COLLATE "C", "status" text NOT NULL, "valid_from" timestamp with time zone NOT NULL, "defined_by_id" text NOT NULL COLLATE "C", "system" bool NOT NULL, "valid_to" timestamp with time zone NOT NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "executions" ( "id" bigint NOT NULL PRIMARY KEY, "project_id" text NOT NULL COLLATE "C", "name" text NULL, "collection_id" text NOT NULL COLLATE "C", "function_version_id" bigint NOT NULL, "triggered_on" timestamp with time zone NOT NULL, "triggered_by_id" text NOT NULL COLLATE "C", "recursion_options" jsonb NULL, "function_id" text NOT NULL DEFAULT '' COLLATE "C", "trigger_type" text NOT NULL DEFAULT 'M', "external_uid" text NULL, "status" text NOT NULL DEFAULT 'S', "started_on" timestamp with time zone NULL, "ended_on" timestamp with time zone NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_id") REFERENCES "functions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "transactions" ( "id" bigint NOT NULL PRIMARY KEY, "project_id" text NOT NULL COLLATE "C", "collection_id" text NOT NULL COLLATE "C", "execution_id" bigint NOT NULL, "transaction_by" text NOT NULL, "transaction_key" text NOT NULL, "triggered_on" timestamp with time zone NOT NULL, "triggered_by_id" text NOT NULL COLLATE "C", "group_id" text NOT NULL COLLATE "C", "status" text NOT NULL DEFAULT 'S', "started_on" timestamp with time zone NULL, "ended_on" timestamp with time zone NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("execution_id") REFERENCES "executions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("group_id") REFERENCES "groups" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "logical_function_runs" ( "id" bigint NOT NULL PRIMARY KEY, "project_id" text NOT NULL COLLATE "C", "collection_id" text NOT NULL COLLATE "C", "execution_id" bigint NOT NULL, "transaction_id" bigint NOT NULL, "function_id" text NOT NULL COLLATE "C", "function_version_id" bigint NOT NULL DEFAULT 197001010000000001, "trigger" text NOT NULL, "triggered_on" timestamp with time zone NOT NULL, "triggered_by_id" text NOT NULL COLLATE "C", "status" text NOT NULL, "group_id" text NOT NULL COLLATE "C", FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("group_id") REFERENCES "groups" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("execution_id") REFERENCES "executions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("transaction_id") REFERENCES "transactions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "logical_table_data_versions" ( "id" bigint NOT NULL PRIMARY KEY, "project_id" text NOT NULL COLLATE "C", "collection_id" text NOT NULL COLLATE "C", "execution_id" bigint NOT NULL, "transaction_id" bigint NOT NULL, "table_id" text NOT NULL COLLATE "C", "producer_logical_function_run_id" bigint NULL, "function_param_pos" integer NULL, "private" bool NOT NULL, "triggered_on" timestamp with time zone NOT NULL, "triggered_by_id" text NOT NULL COLLATE "C", "group_id" text NOT NULL COLLATE "C", FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("group_id") REFERENCES "groups" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("execution_id") REFERENCES "executions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("transaction_id") REFERENCES "transactions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("producer_logical_function_run_id") REFERENCES "logical_function_runs" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "physical_function_runs" ( "id" bigint NOT NULL PRIMARY KEY, "project_id" text NOT NULL COLLATE "C", "collection_id" text NOT NULL COLLATE "C", "function_version_id" bigint NOT NULL, "function_version_patch_id" bigint NULL, "execution_id" bigint NOT NULL, "transaction_id" bigint NOT NULL, "valid_from" timestamp with time zone NOT NULL, "triggered_by_id" text NOT NULL COLLATE "C", "trigger" text NOT NULL, "dispatched_on" timestamp with time zone NULL, "started_on" timestamp with time zone NULL, "ended_on" timestamp with time zone NULL, "function_id" text NOT NULL DEFAULT '' COLLATE "C", "status" text NOT NULL, "base_path" text NOT NULL, "logical_function_run_id" bigint NOT NULL, "valid_to" timestamp with time zone NOT NULL, FOREIGN KEY ("logical_function_run_id") REFERENCES "logical_function_runs" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_id") REFERENCES "functions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_patch_id") REFERENCES "function_patches" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("execution_id") REFERENCES "executions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "physical_table_data_versions" ( "id" bigint NOT NULL PRIMARY KEY, "project_id" text NOT NULL COLLATE "C", "collection_id" text NOT NULL COLLATE "C", "table_id" text NOT NULL COLLATE "C", "name" text NOT NULL, "table_version_id" text NOT NULL COLLATE "C", "function_version_id" bigint NOT NULL, "function_version_patch_id" bigint NULL, "has_data" bool NULL, "schema_changed" bool NULL, "column_count" bigint NULL, "row_count" bigint NULL, "data_bytes" bigint NULL, "schema_hash" text NULL, "columnar_lineage" text NULL, "execution_id" bigint NOT NULL, "transaction_id" bigint NOT NULL, "physical_function_run_id" bigint NOT NULL, "function_param_pos" integer NOT NULL, "with_data_table_data_version_id" bigint NULL, "valid_from" timestamp with time zone NOT NULL, "base_path" text NOT NULL, "logical_table_data_version_id" bigint NOT NULL, "valid_to" timestamp with time zone NOT NULL, FOREIGN KEY ("logical_table_data_version_id") REFERENCES "logical_table_data_versions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_id") REFERENCES "functions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_patch_id") REFERENCES "function_patches" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("execution_id") REFERENCES "executions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("physical_function_run_id") REFERENCES "physical_function_runs" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "table_partitions" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "project_id" text NOT NULL COLLATE "C", "collection_id" text NOT NULL COLLATE "C", "table_id" text NOT NULL COLLATE "C", "table_version_id" text NOT NULL COLLATE "C", "function_version_id" bigint NOT NULL, "table_data_version_id" bigint NOT NULL, "partition_key" text NULL, "partition_deleted" bool NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_id") REFERENCES "functions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("table_data_version_id") REFERENCES "physical_table_data_versions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "workers" ( "id" bigint NOT NULL PRIMARY KEY, "project_id" text NOT NULL COLLATE "C", "collection_id" text NOT NULL COLLATE "C", "execution_id" bigint NULL, "transaction_id" bigint NULL, "function_version_id" bigint NOT NULL, "physical_function_run_id" bigint NULL, "message_status" text NOT NULL, "started_on" timestamp with time zone NULL, "ended_on" timestamp with time zone NULL, "status" text NOT NULL, "worker_type" text NOT NULL, "worker_enabled" bool NOT NULL, "worker_config" text NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("execution_id") REFERENCES "executions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("transaction_id") REFERENCES "transactions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_id") REFERENCES "functions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("physical_function_run_id") REFERENCES "physical_function_runs" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "logical_function_run_inputs" ( "id" text NOT NULL PRIMARY KEY COLLATE "C", "project_id" text NOT NULL COLLATE "C", "collection_id" text NOT NULL COLLATE "C", "execution_id" bigint NOT NULL, "transaction_id" bigint NOT NULL, "logical_function_run_id" bigint NOT NULL, "requirement_logical_table_data_version_id" bigint NULL, "requirement_input_idx" integer NULL, "requirement_dependency_pos" integer NULL, "requirement_version_pos" integer NOT NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("execution_id") REFERENCES "executions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("transaction_id") REFERENCES "transactions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("logical_function_run_id") REFERENCES "logical_function_runs" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("requirement_logical_table_data_version_id") REFERENCES "logical_table_data_versions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "metrics_facts" ( "ts" timestamp with time zone NOT NULL, "ts_hour_ms" bigint NOT NULL, "ts_day_ms" bigint NOT NULL, "ts_week_ms" bigint NOT NULL, "ts_month_ms" bigint NOT NULL, "project_id" text NOT NULL COLLATE "C", "from_group_id" text NULL COLLATE "C", "from_collection_id" text NULL COLLATE "C", "group_id" text NOT NULL COLLATE "C", "collection_id" text NOT NULL COLLATE "C", "execution_id" bigint NOT NULL, "transaction_id" bigint NOT NULL, "function_id" text NOT NULL COLLATE "C", "function_run_id" bigint NOT NULL, "attempt_id" bigint NOT NULL, "attempt_outcome" text NOT NULL, "run_status" text NOT NULL, "replay" bigint NOT NULL, "current" bool NOT NULL, "run_has_inputs" bool NOT NULL, "run_has_outputs" bool NOT NULL, "side" text NOT NULL, "table_id" text NULL COLLATE "C", "table_version_id" bigint NULL, "private" bool NOT NULL, "has_data" bool NOT NULL, "row_count" bigint NOT NULL, "data_bytes" bigint NOT NULL, "first_consumed_on" timestamp with time zone NULL, "edge_kind" text NULL, "edge_first" bool NOT NULL, "edge_first_group" bool NOT NULL )

CREATE OR REPLACE FUNCTION functions__scd2_close_fn() RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('functions:' || NEW.function_id, 0));
  SELECT COALESCE(MIN(valid_from), 'infinity') INTO NEW.valid_to
  FROM functions
  WHERE function_id = NEW.function_id
    AND (valid_from, id) > (NEW.valid_from, NEW.id);
  UPDATE functions SET valid_to = NEW.valid_from
  WHERE function_id = NEW.function_id
    AND (valid_from, id) < (NEW.valid_from, NEW.id)
    AND valid_to > NEW.valid_from;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql

CREATE TRIGGER functions__scd2_close BEFORE INSERT ON functions
FOR EACH ROW EXECUTE FUNCTION functions__scd2_close_fn()

CREATE OR REPLACE FUNCTION function_patches__scd2_close_fn() RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('function_patches:' || NEW.parent_function_version_id, 0));
  SELECT COALESCE(MIN(valid_from), 'infinity') INTO NEW.valid_to
  FROM function_patches
  WHERE parent_function_version_id = NEW.parent_function_version_id
    AND (valid_from, id) > (NEW.valid_from, NEW.id);
  UPDATE function_patches SET valid_to = NEW.valid_from
  WHERE parent_function_version_id = NEW.parent_function_version_id
    AND (valid_from, id) < (NEW.valid_from, NEW.id)
    AND valid_to > NEW.valid_from;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql

CREATE TRIGGER function_patches__scd2_close BEFORE INSERT ON function_patches
FOR EACH ROW EXECUTE FUNCTION function_patches__scd2_close_fn()

CREATE OR REPLACE FUNCTION function_patches__immutable_fn() RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'function_patches is insert-only';
END;
$$ LANGUAGE plpgsql

CREATE TRIGGER function_patches__immutable_no_update BEFORE UPDATE OF id, project_id, parent_function_version_id, function_id, collection_id, snippet, bundle_id, runtime_values, description, valid_from, defined_by_id, status ON function_patches
FOR EACH ROW EXECUTE FUNCTION function_patches__immutable_fn()

CREATE TRIGGER function_patches__immutable_no_delete BEFORE DELETE ON function_patches
FOR EACH ROW EXECUTE FUNCTION function_patches__immutable_fn()

CREATE OR REPLACE FUNCTION tables__scd2_close_fn() RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('tables:' || NEW.table_id, 0));
  SELECT COALESCE(MIN(valid_from), 'infinity') INTO NEW.valid_to
  FROM tables
  WHERE table_id = NEW.table_id
    AND (valid_from, id) > (NEW.valid_from, NEW.id);
  UPDATE tables SET valid_to = NEW.valid_from
  WHERE table_id = NEW.table_id
    AND (valid_from, id) < (NEW.valid_from, NEW.id)
    AND valid_to > NEW.valid_from;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql

CREATE TRIGGER tables__scd2_close BEFORE INSERT ON tables
FOR EACH ROW EXECUTE FUNCTION tables__scd2_close_fn()

CREATE OR REPLACE FUNCTION dependencies__scd2_close_fn() RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('dependencies:' || NEW.dependency_id, 0));
  SELECT COALESCE(MIN(valid_from), 'infinity') INTO NEW.valid_to
  FROM dependencies
  WHERE dependency_id = NEW.dependency_id
    AND (valid_from, id) > (NEW.valid_from, NEW.id);
  UPDATE dependencies SET valid_to = NEW.valid_from
  WHERE dependency_id = NEW.dependency_id
    AND (valid_from, id) < (NEW.valid_from, NEW.id)
    AND valid_to > NEW.valid_from;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql

CREATE TRIGGER dependencies__scd2_close BEFORE INSERT ON dependencies
FOR EACH ROW EXECUTE FUNCTION dependencies__scd2_close_fn()

CREATE OR REPLACE FUNCTION triggers__scd2_close_fn() RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('triggers:' || NEW.trigger_id, 0));
  SELECT COALESCE(MIN(valid_from), 'infinity') INTO NEW.valid_to
  FROM triggers
  WHERE trigger_id = NEW.trigger_id
    AND (valid_from, id) > (NEW.valid_from, NEW.id);
  UPDATE triggers SET valid_to = NEW.valid_from
  WHERE trigger_id = NEW.trigger_id
    AND (valid_from, id) < (NEW.valid_from, NEW.id)
    AND valid_to > NEW.valid_from;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql

CREATE TRIGGER triggers__scd2_close BEFORE INSERT ON triggers
FOR EACH ROW EXECUTE FUNCTION triggers__scd2_close_fn()

CREATE OR REPLACE FUNCTION logical_function_runs__sticky_terminals_fn() RETURNS TRIGGER AS $$
BEGIN
  IF OLD.status IN ('C', 'X', 'Y') AND NEW.status != OLD.status THEN
    RAISE EXCEPTION 'logical_function_runs.status is sticky in terminal state';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql

CREATE TRIGGER logical_function_runs__sticky_terminals BEFORE UPDATE OF status ON logical_function_runs
FOR EACH ROW EXECUTE FUNCTION logical_function_runs__sticky_terminals_fn()

CREATE OR REPLACE FUNCTION physical_function_runs__scd2_close_fn() RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('physical_function_runs:' || NEW.logical_function_run_id, 0));
  SELECT COALESCE(MIN(valid_from), 'infinity') INTO NEW.valid_to
  FROM physical_function_runs
  WHERE logical_function_run_id = NEW.logical_function_run_id
    AND (valid_from, id) > (NEW.valid_from, NEW.id);
  UPDATE physical_function_runs SET valid_to = NEW.valid_from
  WHERE logical_function_run_id = NEW.logical_function_run_id
    AND (valid_from, id) < (NEW.valid_from, NEW.id)
    AND valid_to > NEW.valid_from;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql

CREATE TRIGGER physical_function_runs__scd2_close BEFORE INSERT ON physical_function_runs
FOR EACH ROW EXECUTE FUNCTION physical_function_runs__scd2_close_fn()

CREATE OR REPLACE FUNCTION physical_table_data_versions__scd2_close_fn() RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('physical_table_data_versions:' || NEW.logical_table_data_version_id, 0));
  SELECT COALESCE(MIN(valid_from), 'infinity') INTO NEW.valid_to
  FROM physical_table_data_versions
  WHERE logical_table_data_version_id = NEW.logical_table_data_version_id
    AND (valid_from, id) > (NEW.valid_from, NEW.id);
  UPDATE physical_table_data_versions SET valid_to = NEW.valid_from
  WHERE logical_table_data_version_id = NEW.logical_table_data_version_id
    AND (valid_from, id) < (NEW.valid_from, NEW.id)
    AND valid_to > NEW.valid_from;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql

CREATE TRIGGER physical_table_data_versions__scd2_close BEFORE INSERT ON physical_table_data_versions
FOR EACH ROW EXECUTE FUNCTION physical_table_data_versions__scd2_close_fn()

CREATE VIEW users__with_names WITH (security_invoker = true) AS SELECT "users".*, COALESCE(created_by_user.name, '[' || users.created_by_id || ']') AS "created_by", COALESCE(modified_by_user.name, '[' || users.modified_by_id || ']') AS "modified_by" FROM "users" LEFT JOIN "users" AS "created_by_user" ON "users"."created_by_id" = "created_by_user"."id" LEFT JOIN "users" AS "modified_by_user" ON "users"."modified_by_id" = "modified_by_user"."id"

CREATE VIEW service_tokens__with_names WITH (security_invoker = true) AS SELECT "service_tokens".*, "users"."name" AS "user_name", "users"."enabled" AS "user_enabled" FROM "service_tokens" JOIN "users" ON "service_tokens"."user_id" = "users"."id"

CREATE VIEW groups__with_names WITH (security_invoker = true) AS SELECT "groups"."id", "groups"."project_id", "groups"."name", "groups"."description", "groups"."created_on", "groups"."created_by_id", COALESCE(created_by_user.name, '[' || groups.created_by_id || ']') AS "created_by", "groups"."modified_on", "groups"."modified_by_id", COALESCE(modified_by_user.name, '[' || groups.modified_by_id || ']') AS "modified_by", "groups"."system", "groups"."kind" FROM "groups" LEFT JOIN "users" AS "created_by_user" ON "groups"."created_by_id" = "created_by_user"."id" LEFT JOIN "users" AS "modified_by_user" ON "groups"."modified_by_id" = "modified_by_user"."id"

CREATE VIEW projects_resolved WITH (security_invoker = true) AS SELECT "id", COALESCE("name_when_deleted", "name") AS "name", "description", "base_path", "created_on", "created_by_id", "modified_on", "modified_by_id", "name_when_deleted", "name_when_deleted" IS NOT NULL AS "deleted" FROM "projects"

CREATE VIEW projects_active WITH (security_invoker = true) AS SELECT "projects_resolved"."id", "projects_resolved"."name", "projects_resolved"."description", "projects_resolved"."base_path", "projects_resolved"."created_on", "projects_resolved"."created_by_id", "projects_resolved"."modified_on", "projects_resolved"."modified_by_id" FROM "projects_resolved" WHERE "projects_resolved"."name_when_deleted" IS NULL

CREATE VIEW projects__with_names WITH (security_invoker = true) AS SELECT "projects_active"."id", "projects_active"."name", "projects_active"."description", "projects_active"."base_path", "projects_active"."created_on", "projects_active"."created_by_id", COALESCE(created_by_user.name, '[' || projects_active.created_by_id || ']') AS "created_by", "projects_active"."modified_on", "projects_active"."modified_by_id", COALESCE(modified_by_user.name, '[' || projects_active.modified_by_id || ']') AS "modified_by" FROM "projects_active" LEFT JOIN "users" AS "created_by_user" ON "projects_active"."created_by_id" = "created_by_user"."id" LEFT JOIN "users" AS "modified_by_user" ON "projects_active"."modified_by_id" = "modified_by_user"."id"

CREATE VIEW collections_resolved WITH (security_invoker = true) AS SELECT "collections"."id", "collections"."project_id", COALESCE("collections"."name_when_deleted", "collections"."name") AS "name", "collections"."description", "collections"."created_on", "collections"."created_by_id", "collections"."modified_on", "collections"."modified_by_id", "collections"."name_when_deleted", "collections"."group_id", "collections"."base_path", "collections"."collection_type", "collections"."connection_id", COALESCE("collections"."base_path", "projects"."base_path") AS "effective_base_path", "collections"."name_when_deleted" IS NOT NULL AS "deleted" FROM "collections" LEFT JOIN "projects" ON "collections"."project_id" = "projects"."id"

CREATE VIEW collections_active WITH (security_invoker = true) AS SELECT "collections_resolved"."id", "collections_resolved"."project_id", "collections_resolved"."name", "collections_resolved"."description", "collections_resolved"."created_on", "collections_resolved"."created_by_id", "collections_resolved"."modified_on", "collections_resolved"."modified_by_id", "collections_resolved"."group_id", "collections_resolved"."base_path", "collections_resolved"."collection_type", "collections_resolved"."connection_id", "collections_resolved"."effective_base_path" FROM "collections_resolved" WHERE "collections_resolved"."name_when_deleted" IS NULL

CREATE VIEW collections__with_names WITH (security_invoker = true) AS SELECT "collections_active".*, COALESCE(created_by_user.name, '[' || collections_active.created_by_id || ']') AS "created_by", COALESCE(modified_by_user.name, '[' || collections_active.modified_by_id || ']') AS "modified_by", "groups_for_collection"."name" AS "group", "projects_resolved"."name" AS "project" FROM "collections_active" LEFT JOIN "users" AS "created_by_user" ON "collections_active"."created_by_id" = "created_by_user"."id" LEFT JOIN "users" AS "modified_by_user" ON "collections_active"."modified_by_id" = "modified_by_user"."id" LEFT JOIN "groups" AS "groups_for_collection" ON "collections_active"."group_id" = "groups_for_collection"."id" INNER JOIN "projects_resolved" ON "collections_active"."project_id" = "projects_resolved"."id"

CREATE VIEW roles__with_names WITH (security_invoker = true) AS SELECT "roles"."id", "roles"."name", "roles"."description", "roles"."created_on", "roles"."created_by_id", COALESCE(created_by_user.name, '[' || roles.created_by_id || ']') AS "created_by", "roles"."modified_on", "roles"."modified_by_id", COALESCE(modified_by_user.name, '[' || roles.modified_by_id || ']') AS "modified_by", "roles"."kind", "roles"."project_id", "roles"."fixed" FROM "roles" LEFT JOIN "users" AS "created_by_user" ON "roles"."created_by_id" = "created_by_user"."id" LEFT JOIN "users" AS "modified_by_user" ON "roles"."modified_by_id" = "modified_by_user"."id"

CREATE VIEW users_roles__with_names WITH (security_invoker = true) AS SELECT "users_roles"."id", "users_roles"."user_id", COALESCE(users.name, '[' || users_roles.user_id || ']') AS "user", "users_roles"."role_id", COALESCE(roles.name, '[' || users_roles.role_id || ']') AS "role", "users_roles"."added_on", "users_roles"."added_by_id", COALESCE(added_by_user.name, '[' || users_roles.added_by_id || ']') AS "added_by", "users_roles"."fixed" FROM "users_roles" LEFT JOIN "users" ON "users_roles"."user_id" = "users"."id" LEFT JOIN "roles" ON "users_roles"."role_id" = "roles"."id" LEFT JOIN "users" AS "added_by_user" ON "users_roles"."added_by_id" = "added_by_user"."id"

CREATE VIEW sessions__with_names WITH (security_invoker = true) AS SELECT "sessions".*, "users"."name" AS "user_name" FROM "sessions" JOIN "users" ON "sessions"."user_id" = "users"."id"

CREATE VIEW permissions__with_names WITH (security_invoker = true) AS SELECT "permissions"."id", "permissions"."role_id", "permissions"."permission_type", "permissions"."entity_type", "permissions"."granted_by_id", "permissions"."granted_on", "permissions"."fixed", COALESCE(users.name, '[' || permissions.granted_by_id || ']') AS "granted_by", (CASE WHEN ("permissions"."entity_id" = '00000000000000000000000204') THEN NULL ELSE "permissions"."entity_id" END) AS "entity_id", "roles"."name" AS "role", "projects_resolved"."name" AS "entity" FROM "permissions" LEFT JOIN "users" ON "permissions"."granted_by_id" = "users"."id" LEFT JOIN "roles" ON "permissions"."role_id" = "roles"."id" LEFT JOIN "projects_resolved" ON "permissions"."entity_id" = "projects_resolved"."id"

CREATE VIEW functions__with_names WITH (security_invoker = true) AS SELECT "functions".*, "collections_resolved"."name" AS "collection", "collections_resolved"."deleted" AS "collection_deleted", "collections_resolved"."group_id" AS "group_id", COALESCE(users.name, '[' || functions.defined_by_id || ']') AS "defined_by" FROM "functions" LEFT JOIN "collections_resolved" ON "functions"."collection_id" = "collections_resolved"."id" LEFT JOIN "users" ON "functions"."defined_by_id" = "users"."id"

CREATE VIEW function_patches__with_names WITH (security_invoker = true) AS SELECT "function_patches".*, COALESCE(users.name, '[' || function_patches.defined_by_id || ']') AS "defined_by" FROM "function_patches" LEFT JOIN "users" ON "function_patches"."defined_by_id" = "users"."id"

CREATE VIEW function_versions__with_names WITH (security_invoker = true) AS SELECT "functions".*, "collections_resolved"."name" AS "collection", COALESCE(users.name, '[' || functions.defined_by_id || ']') AS "defined_by", NULL AS "patch_of" FROM "functions" LEFT JOIN "collections_resolved" ON "functions"."collection_id" = "collections_resolved"."id" LEFT JOIN "users" ON "functions"."defined_by_id" = "users"."id" UNION ALL (SELECT "function_patches"."id", "function_patches"."project_id", "function_patches"."collection_id", "parent"."name", "function_patches"."description", "parent"."decorator", "function_patches"."runtime_values", "function_patches"."function_id", "parent"."base_path", "parent"."stage_id", "parent"."stage_base_path", "parent"."storage_version", "function_patches"."bundle_id", "function_patches"."snippet", "function_patches"."valid_from", "function_patches"."defined_by_id", "function_patches"."status", "parent"."connector", "parent"."connection", "parent"."enabled", "function_patches"."valid_to", "parent"."function_type", "parent"."parent_function_id", "collections_resolved"."name", COALESCE(users.name, '[' || function_patches.defined_by_id || ']'), "function_patches"."parent_function_version_id" FROM "function_patches" INNER JOIN "functions" AS "parent" ON "function_patches"."parent_function_version_id" = "parent"."id" LEFT JOIN "collections_resolved" ON "function_patches"."collection_id" = "collections_resolved"."id" LEFT JOIN "users" ON "function_patches"."defined_by_id" = "users"."id")

CREATE VIEW tables__with_names WITH (security_invoker = true) AS SELECT "tables".*, "tables"."function_param_pos" < 0 AS "system", COALESCE(users.name, '[' || tables.defined_by_id || ']') AS "defined_by", "collections_resolved"."name" AS "collection", "collections_resolved"."deleted" AS "collection_deleted", "functions"."name" AS "function" FROM "tables" LEFT JOIN "collections_resolved" ON "tables"."collection_id" = "collections_resolved"."id" LEFT JOIN "functions" ON "tables"."function_version_id" = "functions"."id" LEFT JOIN "users" ON "tables"."defined_by_id" = "users"."id"

CREATE VIEW dependencies__with_names WITH (security_invoker = true) AS SELECT "dependencies".*, "collections_resolved"."name" AS "collection", "collections_resolved"."deleted" AS "collection_deleted", "table_collection"."name" AS "table_collection" FROM "dependencies" LEFT JOIN "collections_resolved" ON "dependencies"."collection_id" = "collections_resolved"."id" LEFT JOIN "users" ON "dependencies"."defined_by_id" = "users"."id" LEFT JOIN "collections_resolved" AS "table_collection" ON "dependencies"."table_collection_id" = "table_collection"."id"

CREATE VIEW triggers__with_names WITH (security_invoker = true) AS SELECT "triggers".*, "collections_resolved"."name" AS "collection", "collections_resolved"."deleted" AS "collection_deleted", "trigger_by_collection"."name" AS "trigger_by_collection" FROM "triggers" LEFT JOIN "collections_resolved" ON "triggers"."collection_id" = "collections_resolved"."id" LEFT JOIN "users" ON "triggers"."defined_by_id" = "users"."id" LEFT JOIN "collections_resolved" AS "trigger_by_collection" ON "triggers"."trigger_by_collection_id" = "trigger_by_collection"."id"

CREATE VIEW triggers__read WITH (security_invoker = true) AS SELECT * FROM "triggers__with_names" WHERE "triggers__with_names"."system" = FALSE

CREATE VIEW executions__with_names WITH (security_invoker = true) AS SELECT "executions".*, "collections_resolved"."name" AS "collection", "collections_resolved"."deleted" AS "collection_deleted", "functions"."name" AS "function", COALESCE(users.name, '[' || executions.triggered_by_id || ']') AS "triggered_by", "projects_resolved"."name" AS "project", (SELECT COALESCE(jsonb_object_agg(inner_fr.status, inner_fr.count), '{}'::jsonb) FROM (SELECT "logical_function_runs"."status", COUNT(*) AS "count" FROM "logical_function_runs" WHERE "logical_function_runs"."execution_id" = "executions"."id" GROUP BY "logical_function_runs"."status") AS "inner_fr") AS "function_run_status_count" FROM "executions" LEFT JOIN "collections_resolved" ON "executions"."collection_id" = "collections_resolved"."id" LEFT JOIN "functions" ON "executions"."function_version_id" = "functions"."id" LEFT JOIN "users" ON "executions"."triggered_by_id" = "users"."id" LEFT JOIN "projects_resolved" ON "executions"."project_id" = "projects_resolved"."id"

CREATE VIEW transactions__with_names WITH (security_invoker = true) AS SELECT "transactions".*, "collections_resolved"."name" AS "collection", "collections_resolved"."deleted" AS "collection_deleted", "executions"."name" AS "execution", COALESCE(users.name, '[' || transactions.triggered_by_id || ']') AS "triggered_by", "groups_for_transactions"."name" AS "group", "projects_resolved"."name" AS "project", (SELECT COALESCE(jsonb_object_agg(inner_fr.status, inner_fr.count), '{}'::jsonb) FROM (SELECT "logical_function_runs"."status", COUNT(*) AS "count" FROM "logical_function_runs" WHERE "logical_function_runs"."transaction_id" = "transactions"."id" GROUP BY "logical_function_runs"."status") AS "inner_fr") AS "function_run_status_count" FROM "transactions" LEFT JOIN "collections_resolved" ON "transactions"."collection_id" = "collections_resolved"."id" LEFT JOIN "executions" ON "transactions"."execution_id" = "executions"."id" LEFT JOIN "users" ON "transactions"."triggered_by_id" = "users"."id" LEFT JOIN "projects_resolved" ON "transactions"."project_id" = "projects_resolved"."id" LEFT JOIN "groups" AS "groups_for_transactions" ON "transactions"."group_id" = "groups_for_transactions"."id"

CREATE VIEW function_runs__with_names WITH (security_invoker = true) AS SELECT "logical_function_runs".*, "functions"."name" AS "name", "collections_resolved"."name" AS "collection", "collections_resolved"."deleted" AS "collection_deleted", "executions"."name" AS "execution", COALESCE(users.name, '[' || logical_function_runs.triggered_by_id || ']') AS "triggered_by", "groups_for_lfr"."name" AS "group", "projects_resolved"."name" AS "project", "physical_function_runs"."id" AS "physical_function_run_id", "physical_function_runs"."started_on" AS "started_on", "physical_function_runs"."ended_on" AS "ended_on", "physical_function_runs"."status" AS "physical_status", "physical_function_runs"."function_version_patch_id" AS "function_version_patch_id" FROM "logical_function_runs" LEFT JOIN "collections_resolved" ON "logical_function_runs"."collection_id" = "collections_resolved"."id" LEFT JOIN "groups" AS "groups_for_lfr" ON "logical_function_runs"."group_id" = "groups_for_lfr"."id" LEFT JOIN "projects_resolved" ON "logical_function_runs"."project_id" = "projects_resolved"."id" LEFT JOIN "functions" ON "logical_function_runs"."function_version_id" = "functions"."id" LEFT JOIN "executions" ON "logical_function_runs"."execution_id" = "executions"."id" LEFT JOIN "users" ON "logical_function_runs"."triggered_by_id" = "users"."id" LEFT JOIN "physical_function_runs" ON "physical_function_runs"."logical_function_run_id" = "logical_function_runs"."id" AND "physical_function_runs"."valid_to" = ('infinity'::timestamptz)

CREATE VIEW table_data_versions__with_names WITH (security_invoker = true) AS SELECT "logical_table_data_versions".*, "collections_resolved"."name" AS "collection", "collections_resolved"."deleted" AS "collection_deleted", "executions"."name" AS "execution", COALESCE(users.name, '[' || logical_table_data_versions.triggered_by_id || ']') AS "triggered_by", "groups_for_ltdv"."name" AS "group", "physical_table_data_versions"."id" AS "physical_table_data_version_id", "physical_table_data_versions"."name" AS "name", "physical_table_data_versions"."table_version_id" AS "table_version_id", "physical_table_data_versions"."function_version_id" AS "function_version_id", "physical_table_data_versions"."function_version_patch_id" AS "function_version_patch_id", "physical_table_data_versions"."has_data" AS "has_data", "physical_table_data_versions"."schema_changed" AS "schema_changed", "physical_table_data_versions"."column_count" AS "column_count", "physical_table_data_versions"."row_count" AS "row_count", "physical_table_data_versions"."data_bytes" AS "data_bytes", "physical_table_data_versions"."schema_hash" AS "schema_hash", "physical_table_data_versions"."with_data_table_data_version_id" AS "with_data_table_data_version_id", "physical_table_data_versions"."physical_function_run_id" AS "physical_function_run_id", "physical_function_runs"."started_on" AS "started_on", "physical_function_runs"."ended_on" AS "ended_on", "logical_function_runs"."status" AS "status", "physical_function_runs"."status" AS "physical_status", "functions"."name" AS "function", "functions"."storage_version" AS "storage_version" FROM "logical_table_data_versions" LEFT JOIN "collections_resolved" ON "logical_table_data_versions"."collection_id" = "collections_resolved"."id" LEFT JOIN "executions" ON "logical_table_data_versions"."execution_id" = "executions"."id" LEFT JOIN "users" ON "logical_table_data_versions"."triggered_by_id" = "users"."id" LEFT JOIN "physical_table_data_versions" ON "physical_table_data_versions"."logical_table_data_version_id" = "logical_table_data_versions"."id" AND "physical_table_data_versions"."valid_to" = ('infinity'::timestamptz) LEFT JOIN "physical_function_runs" ON "physical_function_runs"."id" = "physical_table_data_versions"."physical_function_run_id" LEFT JOIN "logical_function_runs" ON "logical_table_data_versions"."producer_logical_function_run_id" = "logical_function_runs"."id" LEFT JOIN "functions" ON "physical_table_data_versions"."function_version_id" = "functions"."id" LEFT JOIN "groups" AS "groups_for_ltdv" ON "logical_table_data_versions"."group_id" = "groups_for_ltdv"."id"

CREATE VIEW logical_table_data_versions__active WITH (security_invoker = true) AS SELECT "logical_table_data_versions".* FROM "logical_table_data_versions" LEFT JOIN "logical_function_runs" ON "logical_table_data_versions"."producer_logical_function_run_id" = "logical_function_runs"."id" WHERE "logical_function_runs"."status" IS NULL OR "logical_function_runs"."status" NOT IN ('Y', 'X')

CREATE VIEW physical_function_runs__with_names WITH (security_invoker = true) AS SELECT "physical_function_runs".*, "functions"."name" AS "name", "collections_resolved"."name" AS "collection", "collections_resolved"."deleted" AS "collection_deleted", "collections_resolved"."connection_id" AS "connection_id", "connections"."kind" AS "connection", "projects_resolved"."name" AS "project", "executions"."name" AS "execution", "executions"."function_version_id" AS "execution_function_version_id", "functions"."storage_version" AS "storage_version", COALESCE(users.name, '[' || physical_function_runs.triggered_by_id || ']') AS "triggered_by" FROM "physical_function_runs" LEFT JOIN "collections_resolved" ON "physical_function_runs"."collection_id" = "collections_resolved"."id" LEFT JOIN "connections" ON "collections_resolved"."connection_id" = "connections"."id" LEFT JOIN "projects_resolved" ON "physical_function_runs"."project_id" = "projects_resolved"."id" LEFT JOIN "functions" ON "physical_function_runs"."function_version_id" = "functions"."id" LEFT JOIN "executions" ON "physical_function_runs"."execution_id" = "executions"."id" LEFT JOIN "users" ON "physical_function_runs"."triggered_by_id" = "users"."id"

CREATE VIEW physical_table_data_versions__with_function WITH (security_invoker = true) AS SELECT "physical_table_data_versions".*, "physical_function_runs"."triggered_by_id" AS "triggered_by_id", "physical_function_runs"."started_on" AS "started_on", "physical_function_runs"."ended_on" AS "ended_on", "physical_function_runs"."status" AS "status", "functions"."storage_version" AS "storage_version" FROM "physical_table_data_versions" LEFT JOIN "functions" ON "physical_table_data_versions"."function_version_id" = "functions"."id" LEFT JOIN "physical_function_runs" ON "physical_table_data_versions"."physical_function_run_id" = "physical_function_runs"."id"

CREATE VIEW physical_table_data_versions__with_names WITH (security_invoker = true) AS SELECT "physical_table_data_versions__with_function".*, "collections_resolved"."name" AS "collection", "collections_resolved"."deleted" AS "collection_deleted", "functions"."name" AS "function", COALESCE(users.name, '[' || physical_table_data_versions__with_function.triggered_by_id || ']') AS "created_by" FROM "physical_table_data_versions__with_function" LEFT JOIN "collections_resolved" ON "physical_table_data_versions__with_function"."collection_id" = "collections_resolved"."id" LEFT JOIN "functions" ON "physical_table_data_versions__with_function"."function_version_id" = "functions"."id" LEFT JOIN "users" ON "physical_table_data_versions__with_function"."triggered_by_id" = "users"."id"

CREATE VIEW workers__with_names WITH (security_invoker = true) AS SELECT "workers".*, "logical_function_runs"."id" AS "logical_function_run_id", "collections_resolved"."name" AS "collection", "collections_resolved"."deleted" AS "collection_deleted", "collections_resolved"."connection_id" AS "connection_id", "connections"."kind" AS "connection", "executions"."name" AS "execution", "functions"."function_id" AS "function_id", "functions"."name" AS "function", "functions"."enabled" AS "enabled", "projects_resolved"."name" AS "project", "physical_function_runs"."function_version_patch_id" AS "function_version_patch_id" FROM "workers" LEFT JOIN "collections_resolved" ON "workers"."collection_id" = "collections_resolved"."id" LEFT JOIN "connections" ON "collections_resolved"."connection_id" = "connections"."id" LEFT JOIN "executions" ON "workers"."execution_id" = "executions"."id" LEFT JOIN "functions" ON "workers"."function_version_id" = "functions"."id" LEFT JOIN "logical_function_runs" ON "workers"."execution_id" = "logical_function_runs"."execution_id" AND "functions"."function_id" = "logical_function_runs"."function_id" LEFT JOIN "physical_function_runs" ON "workers"."physical_function_run_id" = "physical_function_runs"."id" LEFT JOIN "projects_resolved" ON "workers"."project_id" = "projects_resolved"."id" WHERE "workers"."worker_enabled" = TRUE

CREATE UNIQUE INDEX "service_tokens___user_id__slot___idx" ON "service_tokens" ("user_id", "slot")

CREATE INDEX "groups___name___idx" ON "groups" ("name")

CREATE UNIQUE INDEX "groups___project_id__name___idx" ON "groups" ("project_id", "name")

CREATE INDEX "projects___resolved_name___idx" ON "projects" ((COALESCE("name_when_deleted", "name")))

CREATE INDEX "collections___project_id__resolved_name___idx" ON "collections" ("project_id", (COALESCE("name_when_deleted", "name")))

CREATE INDEX "collections___group_id__project_id___idx" ON "collections" ("group_id", "project_id")

CREATE UNIQUE INDEX "collections___project_id__name___idx" ON "collections" ("project_id", "name")

CREATE INDEX "collections___project_id__id___idx" ON "collections" ("project_id", "id")

CREATE INDEX "roles___project_id___idx" ON "roles" ("project_id")

CREATE INDEX "users_roles___role_id__id___idx" ON "users_roles" ("role_id", "id")

CREATE UNIQUE INDEX "roles___user_id__role_id___idx" ON "users_roles" ("user_id", "role_id")

CREATE INDEX "sessions___status___idx" ON "sessions" ("status")

CREATE INDEX "sessions___expires_on___idx" ON "sessions" ("expires_on")

CREATE INDEX "sessions___user_id___idx" ON "sessions" ("user_id")

CREATE UNIQUE INDEX "permissions___role_id__permission_type__entity___idx" ON "permissions" ("role_id", "permission_type", "entity_type", "entity_id")

CREATE INDEX "functions___collection_id__name___idx" ON "functions" ("collection_id", "name")

CREATE INDEX "functions___collection_id__name___current_idx" ON "functions" ("collection_id", "name") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "functions___project_id__status___current_idx" ON "functions" ("project_id", "status") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "functions___project_id__id___idx" ON "functions" ("project_id", "id")

CREATE INDEX "functions___function_id__valid_from__id___idx" ON "functions" ("function_id", "valid_from", "id")

CREATE INDEX "functions___parent_function_id___current_idx" ON "functions" ("parent_function_id") WHERE "valid_to" = ('infinity'::timestamptz) AND "parent_function_id" IS NOT NULL

CREATE UNIQUE INDEX "functions___function_id___current_uidx" ON "functions" ("function_id") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "function_patches___function_id__valid_from___idx" ON "function_patches" ("function_id", "valid_from")

CREATE INDEX "function_patches___parent_fv_id__valid_from__id___idx" ON "function_patches" ("parent_function_version_id", "valid_from", "id")

CREATE UNIQUE INDEX "function_patches___parent_function_version_id___current_uidx" ON "function_patches" ("parent_function_version_id") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "tables___collection_id__name___current_idx" ON "tables" ("collection_id", "name") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "tables___project_id__status___current_idx" ON "tables" ("project_id", "status") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "tables___project_id__id___idx" ON "tables" ("project_id", "id")

CREATE INDEX "tables___collection_id__name___idx" ON "tables" ("collection_id", "name")

CREATE INDEX "tables___function_version_id___idx" ON "tables" ("function_version_id")

CREATE INDEX "tables___function_id__status___idx" ON "tables" ("function_id", "status")

CREATE INDEX "tables___table_id__valid_from__id___idx" ON "tables" ("table_id", "valid_from", "id")

CREATE UNIQUE INDEX "tables___table_id___current_uidx" ON "tables" ("table_id") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "tables___function_id___current_idx" ON "tables" ("function_id") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "dependencies___project_id___current_idx" ON "dependencies" ("project_id") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "dependencies___project_id__id___idx" ON "dependencies" ("project_id", "id")

CREATE INDEX "dependencies___table_id___current_idx" ON "dependencies" ("table_id") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "dependencies___table_id___idx" ON "dependencies" ("table_id")

CREATE INDEX "dependencies___dependency_id__valid_from__id___idx" ON "dependencies" ("dependency_id", "valid_from", "id")

CREATE INDEX "dependencies___function_id___idx" ON "dependencies" ("function_id")

CREATE UNIQUE INDEX "dependencies___dependency_id___current_uidx" ON "dependencies" ("dependency_id") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "dependencies___function_id___current_idx" ON "dependencies" ("function_id") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "triggers___project_id__status___current_idx" ON "triggers" ("project_id", "status") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "triggers___project_id__id___idx" ON "triggers" ("project_id", "id")

CREATE INDEX "triggers___trigger_by_table_id___current_idx" ON "triggers" ("trigger_by_table_id") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "triggers___trigger_by_table_id___idx" ON "triggers" ("trigger_by_table_id")

CREATE INDEX "triggers___function_id___idx" ON "triggers" ("function_id")

CREATE INDEX "triggers___trigger_id__valid_from__id___idx" ON "triggers" ("trigger_id", "valid_from", "id")

CREATE UNIQUE INDEX "triggers___trigger_id___current_uidx" ON "triggers" ("trigger_id") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "triggers___function_id___current_idx" ON "triggers" ("function_id") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "executions___fv_id__trigger_type__triggered_on___idx" ON "executions" ("function_version_id", "trigger_type", "triggered_on")

CREATE INDEX "executions___project_id__triggered_on__id___idx" ON "executions" ("project_id", "triggered_on", "id")

CREATE INDEX "executions___project_id__id___idx" ON "executions" ("project_id", "id")

CREATE INDEX "executions___triggered_on__id___idx" ON "executions" ("triggered_on", "id")

CREATE INDEX "executions___collection_id__triggered_on__id___idx" ON "executions" ("collection_id", "triggered_on", "id")

CREATE INDEX "executions___project_id__status__triggered_on__id___idx" ON "executions" ("project_id", "status", "triggered_on", "id")

CREATE UNIQUE INDEX "executions___function_id__external_uid___idx" ON "executions" ("function_id", "external_uid")

CREATE INDEX "transactions___group_id__project_id___idx" ON "transactions" ("group_id", "project_id")

CREATE INDEX "transactions___execution_id__triggered_on__id___idx" ON "transactions" ("execution_id", "triggered_on", "id")

CREATE INDEX "transactions___project_id__id___idx" ON "transactions" ("project_id", "id")

CREATE INDEX "transactions___project_id__triggered_on__id___idx" ON "transactions" ("project_id", "triggered_on", "id")

CREATE INDEX "transactions___triggered_on__id___idx" ON "transactions" ("triggered_on", "id")

CREATE INDEX "transactions___project_id__status__triggered_on__id___idx" ON "transactions" ("project_id", "status", "triggered_on", "id")

CREATE INDEX "transactions___project_id__started_on__id___idx" ON "transactions" ("project_id", "started_on", "id")

CREATE INDEX "logical_function_runs___group_id__project_id___idx" ON "logical_function_runs" ("group_id", "project_id")

CREATE INDEX "logical_function_runs___status__project_id__triggered_on___idx" ON "logical_function_runs" ("status", "project_id", "triggered_on")

CREATE INDEX "logical_function_runs___project_id__id___idx" ON "logical_function_runs" ("project_id", "id")

CREATE INDEX "logical_function_runs___function_version_id___idx" ON "logical_function_runs" ("function_version_id")

CREATE UNIQUE INDEX "logical_function_runs___execution_id__function_id___idx" ON "logical_function_runs" ("execution_id", "function_id")

CREATE INDEX "logical_function_runs___function_id__triggered_on__id___idx" ON "logical_function_runs" ("function_id", "triggered_on", "id")

CREATE INDEX "logical_function_runs___execution_id__status___idx" ON "logical_function_runs" ("execution_id", "status")

CREATE INDEX "logical_function_runs___transaction_id__status___idx" ON "logical_function_runs" ("transaction_id", "status")

CREATE INDEX "logical_function_runs___triggered_on__id___idx" ON "logical_function_runs" ("triggered_on", "id")

CREATE INDEX "logical_function_runs___project_id__triggered_on__id___idx" ON "logical_function_runs" ("project_id", "triggered_on", "id")

CREATE INDEX "logical_function_runs___collection_id__triggered_on__id___idx" ON "logical_function_runs" ("collection_id", "triggered_on", "id")

CREATE INDEX "logical_function_runs___execution_id__triggered_on__id___idx" ON "logical_function_runs" ("execution_id", "triggered_on", "id")

CREATE INDEX "logical_function_runs___transaction_id__triggered_on__id___idx" ON "logical_function_runs" ("transaction_id", "triggered_on", "id")

CREATE INDEX "logical_table_data_versions___group_id__project_id___idx" ON "logical_table_data_versions" ("group_id", "project_id")

CREATE INDEX "logical_table_data_versions___table_id__triggered_on__id___idx" ON "logical_table_data_versions" ("table_id", "triggered_on", "id")

CREATE INDEX "ltdv___producer_logical_function_run_id___idx" ON "logical_table_data_versions" ("producer_logical_function_run_id")

CREATE INDEX "logical_table_data_versions___execution_id__table_id___idx" ON "logical_table_data_versions" ("execution_id", "table_id")

CREATE INDEX "logical_table_data_versions___project_id__id___idx" ON "logical_table_data_versions" ("project_id", "id")

CREATE INDEX "ltdv___collection_id__triggered_on__id___idx" ON "logical_table_data_versions" ("collection_id", "triggered_on", "id")

CREATE INDEX "ltdv___execution_id__triggered_on__id___idx" ON "logical_table_data_versions" ("execution_id", "triggered_on", "id")

CREATE INDEX "pfr___status__project_id__valid_from___idx" ON "physical_function_runs" ("status", "project_id", "valid_from")

CREATE INDEX "pfr___status__project_id__dispatched_on___idx" ON "physical_function_runs" ("status", "project_id", "dispatched_on")

CREATE INDEX "pfr___project_id__execution_id__started_on__ended_on___idx" ON "physical_function_runs" ("project_id", "execution_id", "started_on", "ended_on")

CREATE INDEX "pfr___project_id__transaction_id__started_on__ended_on___idx" ON "physical_function_runs" ("project_id", "transaction_id", "started_on", "ended_on")

CREATE INDEX "pfr___function_version_patch_id___idx" ON "physical_function_runs" ("function_version_patch_id")

CREATE INDEX "pfr___function_version_id___idx" ON "physical_function_runs" ("function_version_id")

CREATE INDEX "pfr___project_id__id___idx" ON "physical_function_runs" ("project_id", "id")

CREATE UNIQUE INDEX "pfr___logical_function_run_id___current_uidx" ON "physical_function_runs" ("logical_function_run_id") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "pfr___logical_function_run_id__valid_from__id___idx" ON "physical_function_runs" ("logical_function_run_id", "valid_from", "id")

CREATE UNIQUE INDEX "ptdv___physical_function_run_id__table_id___uidx" ON "physical_table_data_versions" ("physical_function_run_id", "table_id")

CREATE UNIQUE INDEX "ptdv___logical_table_data_version_id___current_uidx" ON "physical_table_data_versions" ("logical_table_data_version_id") WHERE "valid_to" = ('infinity'::timestamptz)

CREATE INDEX "ptdv___project_id__id___idx" ON "physical_table_data_versions" ("project_id", "id")

CREATE INDEX "ptdv___logical_table_data_version_id__valid_from__id___idx" ON "physical_table_data_versions" ("logical_table_data_version_id", "valid_from", "id")

CREATE INDEX "workers___collection_id___idx" ON "workers" ("collection_id")

CREATE INDEX "workers___function_version_id__worker_type___idx" ON "workers" ("function_version_id", "worker_type")

CREATE INDEX "workers___enabled__worker_type__status__project_id__id___idx" ON "workers" ("worker_enabled", "worker_type", "status", "project_id", "id")

CREATE INDEX "workers___started_on__id___idx" ON "workers" ("started_on", "id")

CREATE INDEX "workers___project_id__started_on__id___idx" ON "workers" ("project_id", "started_on", "id")

CREATE INDEX "workers___project_id__status__started_on__id___idx" ON "workers" ("project_id", "status", "started_on", "id")

CREATE INDEX "workers___project_id__id___idx" ON "workers" ("project_id", "id")

CREATE INDEX "workers___physical_function_run_id___idx" ON "workers" ("physical_function_run_id")

CREATE INDEX "logical_function_run_inputs___logical_function_run_id___idx" ON "logical_function_run_inputs" ("logical_function_run_id")

CREATE INDEX "logical_function_run_inputs___execution_id___idx" ON "logical_function_run_inputs" ("execution_id")

CREATE INDEX "lfri___requirement_logical_table_data_version_id___idx" ON "logical_function_run_inputs" ("requirement_logical_table_data_version_id")

CREATE INDEX "metrics_facts___project_id__ts___idx" ON "metrics_facts" ("project_id", "ts") INCLUDE ("from_group_id", "from_collection_id", "group_id", "collection_id", "run_status", "current", "side", "table_id", "private", "has_data", "row_count", "data_bytes", "first_consumed_on", "edge_kind", "edge_first", "edge_first_group")

CREATE INDEX "metrics_facts___ts___idx" ON "metrics_facts" ("ts") INCLUDE ("project_id", "from_group_id", "from_collection_id", "group_id", "collection_id", "run_status", "current", "side", "table_id", "private", "has_data", "row_count", "data_bytes", "first_consumed_on", "edge_kind", "edge_first", "edge_first_group")

CREATE INDEX "metrics_facts___function_run_id___idx" ON "metrics_facts" ("function_run_id")

CREATE INDEX "metrics_facts___attempt_id___idx" ON "metrics_facts" ("attempt_id")

CREATE INDEX "metrics_facts___table_version_id___written___idx" ON "metrics_facts" ("table_version_id") WHERE side = 'O'

CREATE STATISTICS "metrics_facts___grouping___stx" (ndistinct) ON "side", "edge_kind", "from_collection_id", "from_group_id", "collection_id", "group_id", "run_status", "table_id" FROM "metrics_facts"

INSERT INTO "tabsdata_system" ("name", "value") VALUES ('tabsdata_version', '2')

INSERT INTO "users" ("id", "name", "full_name", "email", "created_on", "created_by_id", "modified_on", "modified_by_id", "password_hash", "password_set_on", "password_must_change", "enabled", "system") VALUES ('00000000000000000000000000', 'system', 'System', NULL, CURRENT_TIMESTAMP, '00000000000000000000000000', CURRENT_TIMESTAMP, '00000000000000000000000000', '!', CURRENT_TIMESTAMP, FALSE, FALSE, TRUE)

INSERT INTO "users" ("id", "name", "full_name", "email", "created_on", "created_by_id", "modified_on", "modified_by_id", "password_hash", "password_set_on", "password_must_change", "enabled", "system") VALUES ('00000000000000000000000004', 'admin', 'Administrator', NULL, CURRENT_TIMESTAMP, '00000000000000000000000000', CURRENT_TIMESTAMP, '00000000000000000000000000', '$argon2id$v=19$m=19456,t=2,p=1$ULTV//eju00aMbzZXAORyg$4+7GgsoT3e7lHElgWA2v0OYQHr5SBQzYY3pcUy0/YDM', CURRENT_TIMESTAMP, FALSE, TRUE, FALSE)

INSERT INTO "roles" ("id", "name", "description", "created_on", "created_by_id", "modified_on", "modified_by_id", "kind", "project_id", "fixed") VALUES ('00000000000000000000000008', 'sys_admin', 'System Administrator Role', CURRENT_TIMESTAMP, '00000000000000000000000000', CURRENT_TIMESTAMP, '00000000000000000000000000', 'sys_admin', NULL, TRUE)

INSERT INTO "roles" ("id", "name", "description", "created_on", "created_by_id", "modified_on", "modified_by_id", "kind", "project_id", "fixed") VALUES ('0000000000000000000000000g', 'default', 'Default Role', CURRENT_TIMESTAMP, '00000000000000000000000000', CURRENT_TIMESTAMP, '00000000000000000000000000', 'default', NULL, TRUE)

INSERT INTO "roles" ("id", "name", "description", "created_on", "created_by_id", "modified_on", "modified_by_id", "kind", "project_id", "fixed") VALUES ('0000000000000000000000020o', 'project_creator', 'Project Creator Role', CURRENT_TIMESTAMP, '00000000000000000000000000', CURRENT_TIMESTAMP, '00000000000000000000000000', 'project_creator', NULL, TRUE)

INSERT INTO "users_roles" ("id", "user_id", "role_id", "added_on", "added_by_id", "fixed") SELECT '0000000000000000000000000k', "users"."id", "roles"."id", CURRENT_TIMESTAMP, '00000000000000000000000000', TRUE FROM "users", "roles" WHERE "users"."name" = 'admin' AND "roles"."name" = 'sys_admin'

INSERT INTO "users_roles" ("id", "user_id", "role_id", "added_on", "added_by_id", "fixed") SELECT '0000000000000000000000000o', "users"."id", "roles"."id", CURRENT_TIMESTAMP, '00000000000000000000000000', TRUE FROM "users", "roles" WHERE "users"."name" = 'admin' AND "roles"."name" = 'default'

INSERT INTO "permissions" ("id", "role_id", "permission_type", "entity_type", "entity_id", "granted_by_id", "granted_on", "fixed") SELECT '00000000000000000000000010', "roles"."id", 'manage_access', 'system', '00000000000000000000000204', '00000000000000000000000000', CURRENT_TIMESTAMP, TRUE FROM "roles" WHERE "roles"."name" = 'sys_admin'

INSERT INTO "permissions" ("id", "role_id", "permission_type", "entity_type", "entity_id", "granted_by_id", "granted_on", "fixed") SELECT '00000000000000000000000014', "roles"."id", 'create_projects', 'system', '00000000000000000000000204', '00000000000000000000000000', CURRENT_TIMESTAMP, TRUE FROM "roles" WHERE "roles"."name" = 'sys_admin'

INSERT INTO "permissions" ("id", "role_id", "permission_type", "entity_type", "entity_id", "granted_by_id", "granted_on", "fixed") SELECT '00000000000000000000000100', "roles"."id", 'manage_roles', 'system', '00000000000000000000000204', '00000000000000000000000000', CURRENT_TIMESTAMP, TRUE FROM "roles" WHERE "roles"."name" = 'sys_admin'

INSERT INTO "permissions" ("id", "role_id", "permission_type", "entity_type", "entity_id", "granted_by_id", "granted_on", "fixed") SELECT '00000000000000000000000104', "roles"."id", 'manage_project', 'project', '00000000000000000000000204', '00000000000000000000000000', CURRENT_TIMESTAMP, TRUE FROM "roles" WHERE "roles"."name" = 'sys_admin'

INSERT INTO "permissions" ("id", "role_id", "permission_type", "entity_type", "entity_id", "granted_by_id", "granted_on", "fixed") SELECT '00000000000000000000000108', "roles"."id", 'create_projects', 'system', '00000000000000000000000204', '00000000000000000000000000', CURRENT_TIMESTAMP, FALSE FROM "roles" WHERE "roles"."name" = 'default'

INSERT INTO "permissions" ("id", "role_id", "permission_type", "entity_type", "entity_id", "granted_by_id", "granted_on", "fixed") SELECT '0000000000000000000000020s', "roles"."id", 'create_projects', 'system', '00000000000000000000000204', '00000000000000000000000000', CURRENT_TIMESTAMP, TRUE FROM "roles" WHERE "roles"."name" = 'project_creator'

ALTER TABLE groups ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE groups ENABLE ROW LEVEL SECURITY

ALTER TABLE groups FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON groups USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON groups TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE connections ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE connections ENABLE ROW LEVEL SECURITY

ALTER TABLE connections FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON connections USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON connections TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE collections ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE collections ENABLE ROW LEVEL SECURITY

ALTER TABLE collections FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON collections USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON collections TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE bundles ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE bundles ENABLE ROW LEVEL SECURITY

ALTER TABLE bundles FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON bundles USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON bundles TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE functions ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE functions ENABLE ROW LEVEL SECURITY

ALTER TABLE functions FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON functions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON functions TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE function_patches ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE function_patches ENABLE ROW LEVEL SECURITY

ALTER TABLE function_patches FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON function_patches USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON function_patches TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE tables ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE tables ENABLE ROW LEVEL SECURITY

ALTER TABLE tables FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON tables USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON tables TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE dependencies ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE dependencies ENABLE ROW LEVEL SECURITY

ALTER TABLE dependencies FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON dependencies USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON dependencies TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE triggers ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE triggers ENABLE ROW LEVEL SECURITY

ALTER TABLE triggers FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON triggers USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON triggers TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE executions ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE executions ENABLE ROW LEVEL SECURITY

ALTER TABLE executions FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON executions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON executions TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE transactions ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE transactions ENABLE ROW LEVEL SECURITY

ALTER TABLE transactions FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON transactions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON transactions TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE physical_function_runs ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE physical_function_runs ENABLE ROW LEVEL SECURITY

ALTER TABLE physical_function_runs FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON physical_function_runs USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON physical_function_runs TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE physical_table_data_versions ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE physical_table_data_versions ENABLE ROW LEVEL SECURITY

ALTER TABLE physical_table_data_versions FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON physical_table_data_versions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON physical_table_data_versions TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE table_partitions ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE table_partitions ENABLE ROW LEVEL SECURITY

ALTER TABLE table_partitions FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON table_partitions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON table_partitions TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE workers ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE workers ENABLE ROW LEVEL SECURITY

ALTER TABLE workers FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON workers USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON workers TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE logical_function_runs ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE logical_function_runs ENABLE ROW LEVEL SECURITY

ALTER TABLE logical_function_runs FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON logical_function_runs USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON logical_function_runs TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE logical_table_data_versions ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE logical_table_data_versions ENABLE ROW LEVEL SECURITY

ALTER TABLE logical_table_data_versions FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON logical_table_data_versions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON logical_table_data_versions TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE logical_function_run_inputs ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE logical_function_run_inputs ENABLE ROW LEVEL SECURITY

ALTER TABLE logical_function_run_inputs FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON logical_function_run_inputs USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON logical_function_run_inputs TO td_across_projects USING (true) WITH CHECK (true)

ALTER TABLE metrics_facts ALTER COLUMN project_id SET DEFAULT NULLIF(current_setting('app.project_id', true), '')

ALTER TABLE metrics_facts ENABLE ROW LEVEL SECURITY

ALTER TABLE metrics_facts FORCE ROW LEVEL SECURITY

CREATE POLICY project_isolation ON metrics_facts USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON metrics_facts TO td_across_projects USING (true) WITH CHECK (true)
