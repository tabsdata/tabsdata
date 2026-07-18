-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

CREATE TABLE "users" ( "id" text NOT NULL PRIMARY KEY, "name" text NOT NULL UNIQUE, "full_name" text NOT NULL, "email" text NULL UNIQUE, "created_on" timestamp_with_timezone_text NOT NULL, "created_by_id" text NOT NULL, "modified_on" timestamp_with_timezone_text NOT NULL, "modified_by_id" text NOT NULL, "password_hash" text NOT NULL, "password_set_on" timestamp_with_timezone_text NOT NULL, "password_must_change" boolean NOT NULL, "enabled" boolean NOT NULL, "system" boolean NOT NULL )

CREATE TABLE "groups" ( "id" text NOT NULL, "project_id" text NOT NULL, "name" text NOT NULL, "description" text NOT NULL, "created_on" timestamp_with_timezone_text NOT NULL, "created_by_id" text NOT NULL, "modified_on" timestamp_with_timezone_text NOT NULL, "modified_by_id" text NOT NULL, "system" boolean NOT NULL, PRIMARY KEY ("project_id", "id") )

CREATE TABLE "projects" ( "id" text NOT NULL PRIMARY KEY, "name" text NOT NULL UNIQUE, "description" text NOT NULL, "base_path" text NOT NULL, "created_on" timestamp_with_timezone_text NOT NULL, "created_by_id" text NOT NULL, "modified_on" timestamp_with_timezone_text NOT NULL, "modified_by_id" text NOT NULL, "name_when_deleted" text NULL )

CREATE TABLE "connections" ( "id" text NOT NULL, "project_id" text NOT NULL, "kind" text NOT NULL, "created_on" timestamp_with_timezone_text NOT NULL, "created_by_id" text NOT NULL, "modified_on" timestamp_with_timezone_text NOT NULL, "modified_by_id" text NOT NULL, PRIMARY KEY ("project_id", "id") )

CREATE TABLE "collections" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "name" text NOT NULL, "description" text NOT NULL, "created_on" timestamp_with_timezone_text NOT NULL, "created_by_id" text NOT NULL, "modified_on" timestamp_with_timezone_text NOT NULL, "modified_by_id" text NOT NULL, "name_when_deleted" text NULL, "group_id" text NOT NULL, "base_path" text NULL, "collection_type" text NOT NULL, "connection_id" text NULL, FOREIGN KEY ("project_id", "group_id") REFERENCES "groups" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("project_id", "connection_id") REFERENCES "connections" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "roles" ( "id" text NOT NULL PRIMARY KEY, "name" text NOT NULL UNIQUE, "description" text NOT NULL, "created_on" timestamp_with_timezone_text NOT NULL, "created_by_id" text NOT NULL, "modified_on" timestamp_with_timezone_text NOT NULL, "modified_by_id" text NOT NULL, "kind" text NOT NULL, "project_id" text NULL, "fixed" boolean NOT NULL, FOREIGN KEY ("project_id") REFERENCES "projects" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "users_roles" ( "id" text NOT NULL PRIMARY KEY, "user_id" text NOT NULL, "role_id" text NOT NULL, "added_on" timestamp_with_timezone_text NOT NULL, "added_by_id" text NOT NULL, "fixed" boolean NOT NULL, FOREIGN KEY ("role_id") REFERENCES "roles" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "sessions" ( "access_token_id" text NOT NULL PRIMARY KEY, "refresh_token_id" text NOT NULL UNIQUE, "user_id" text NOT NULL, "created_on" timestamp_with_timezone_text NOT NULL, "expires_on" timestamp_with_timezone_text NOT NULL, "status_change_on" timestamp_with_timezone_text NOT NULL, "status" text NOT NULL )

CREATE TABLE "permissions" ( "id" text NOT NULL PRIMARY KEY, "role_id" text NOT NULL, "permission_type" text NOT NULL, "entity_type" text NOT NULL, "entity_id" text NOT NULL, "granted_by_id" text NOT NULL, "granted_on" timestamp_with_timezone_text NOT NULL, "fixed" boolean NOT NULL, FOREIGN KEY ("role_id") REFERENCES "roles" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "bundles" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "hash" text NOT NULL, "base_path" text NOT NULL, "status" text NOT NULL, "created_on" timestamp_with_timezone_text NOT NULL, "created_by_id" text NOT NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "functions" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "name" text NOT NULL, "description" text NOT NULL, "decorator" text NOT NULL, "runtime_values" text NOT NULL, "function_id" text NOT NULL, "base_path" text NULL, "stage_id" text NOT NULL, "stage_base_path" text NOT NULL, "storage_version" text NOT NULL, "bundle_id" text NOT NULL, "snippet" text NOT NULL, "defined_on" timestamp_with_timezone_text NOT NULL, "defined_by_id" text NOT NULL, "status" text NOT NULL, "connector" text NULL, "connection" text NULL, "enabled" boolean DEFAULT TRUE, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "functions_current" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "name" text NOT NULL, "description" text NOT NULL, "decorator" text NOT NULL, "runtime_values" text NOT NULL, "function_id" text NOT NULL UNIQUE, "base_path" text NULL, "stage_id" text NOT NULL, "stage_base_path" text NOT NULL, "storage_version" text NOT NULL, "bundle_id" text NOT NULL, "snippet" text NOT NULL, "defined_on" timestamp_with_timezone_text NOT NULL, "defined_by_id" text NOT NULL, "status" text NOT NULL, "connector" text NULL, "connection" text NULL, "enabled" boolean DEFAULT TRUE )

CREATE TABLE "function_patches" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "parent_function_version_id" text NOT NULL, "function_id" text NOT NULL, "collection_id" text NOT NULL, "snippet" text NOT NULL, "bundle_id" text NOT NULL, "runtime_values" text NOT NULL, "description" text NOT NULL, "defined_on" timestamp_with_timezone_text NOT NULL, "defined_by_id" text NOT NULL, "status" text NOT NULL, FOREIGN KEY ("parent_function_version_id") REFERENCES "functions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "function_patches_current" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "parent_function_version_id" text NOT NULL UNIQUE, "function_id" text NOT NULL, "collection_id" text NOT NULL, "snippet" text NOT NULL, "bundle_id" text NOT NULL, "runtime_values" text NOT NULL, "description" text NOT NULL, "defined_on" timestamp_with_timezone_text NOT NULL, "defined_by_id" text NOT NULL, "status" text NOT NULL )

CREATE TABLE "tables" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "table_id" text NOT NULL, "name" text NOT NULL, "function_id" text NULL, "function_version_id" text NOT NULL, "function_param_pos" integer NULL, "private" boolean NOT NULL, "partitioned" boolean NULL, "defined_on" timestamp_with_timezone_text NOT NULL, "defined_by_id" text NOT NULL, "status" text NOT NULL, "table_type" text NOT NULL DEFAULT 'R', FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_id") REFERENCES "functions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "tables_current" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "table_id" text NOT NULL UNIQUE, "name" text NOT NULL, "function_id" text NULL, "function_version_id" text NOT NULL, "function_param_pos" integer NULL, "private" boolean NOT NULL, "partitioned" boolean NULL, "defined_on" timestamp_with_timezone_text NOT NULL, "defined_by_id" text NOT NULL, "status" text NOT NULL, "table_type" text NOT NULL DEFAULT 'R' )

CREATE TABLE "dependencies" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "dependency_id" text NOT NULL, "function_id" text NOT NULL, "table_collection_id" text NOT NULL, "table_function_id" text NOT NULL, "table_id" text NOT NULL, "table_versions" text NOT NULL, "dep_pos" integer NOT NULL, "status" text NOT NULL, "defined_on" timestamp_with_timezone_text NOT NULL, "defined_by_id" text NOT NULL, "system" boolean NOT NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "dependencies_current" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "dependency_id" text NOT NULL UNIQUE, "function_id" text NOT NULL, "table_collection_id" text NOT NULL, "table_function_id" text NOT NULL, "table_id" text NOT NULL, "table_versions" text NOT NULL, "dep_pos" integer NOT NULL, "status" text NOT NULL, "defined_on" timestamp_with_timezone_text NOT NULL, "defined_by_id" text NOT NULL, "system" boolean NOT NULL )

CREATE TABLE "triggers" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "trigger_id" text NOT NULL, "function_id" text NOT NULL, "trigger_by_collection_id" text NOT NULL, "trigger_by_function_id" text NOT NULL, "trigger_by_table_id" text NOT NULL, "status" text NOT NULL, "defined_on" timestamp_with_timezone_text NOT NULL, "defined_by_id" text NOT NULL, "system" boolean NOT NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "triggers_current" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "trigger_id" text NOT NULL UNIQUE, "function_id" text NOT NULL, "trigger_by_collection_id" text NOT NULL, "trigger_by_function_id" text NOT NULL, "trigger_by_table_id" text NOT NULL, "status" text NOT NULL, "defined_on" timestamp_with_timezone_text NOT NULL, "defined_by_id" text NOT NULL, "system" boolean NOT NULL )

CREATE TABLE "executions" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "name" text NULL, "collection_id" text NOT NULL, "function_version_id" text NOT NULL, "triggered_on" timestamp_with_timezone_text NOT NULL, "triggered_by_id" text NOT NULL, "recursion_options" jsonb_text NULL, "function_id" text NOT NULL DEFAULT '', "trigger_type" text NOT NULL DEFAULT 'M', "external_uid" text NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_id") REFERENCES "functions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "transactions" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "execution_id" text NOT NULL, "transaction_by" text NOT NULL, "transaction_key" text NOT NULL, "triggered_on" timestamp_with_timezone_text NOT NULL, "triggered_by_id" text NOT NULL, "group_id" text NOT NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("execution_id") REFERENCES "executions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("project_id", "group_id") REFERENCES "groups" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "physical_function_runs" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "function_version_id" text NOT NULL, "function_version_patch_id" text NULL, "execution_id" text NOT NULL, "transaction_id" text NOT NULL, "triggered_on" timestamp_with_timezone_text NOT NULL, "triggered_by_id" text NOT NULL, "trigger" text NOT NULL, "started_on" timestamp_with_timezone_text NULL, "ended_on" timestamp_with_timezone_text NULL, "function_id" text NOT NULL DEFAULT '', "status" text NOT NULL, "ended_on_hour_ms" integer NULL, "ended_on_day_ms" integer NULL, "ended_on_week_ms" integer NULL, "ended_on_month_ms" integer NULL, "metric_table_versions" integer NULL, "metric_rows" integer NULL, "metric_data_bytes" integer NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_id") REFERENCES "functions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_patch_id") REFERENCES "function_patches" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("execution_id") REFERENCES "executions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "physical_table_data_versions" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "table_id" text NOT NULL, "name" text NOT NULL, "table_version_id" text NOT NULL, "function_version_id" text NOT NULL, "function_version_patch_id" text NULL, "has_data" boolean NULL, "schema_changed" boolean NULL, "column_count" integer NULL, "row_count" integer NULL, "data_bytes" integer NULL, "schema_hash" text NULL, "execution_id" text NOT NULL, "transaction_id" text NOT NULL, "physical_function_run_id" text NOT NULL, "function_param_pos" integer NOT NULL, "with_data_table_data_version_id" text NULL, "triggered_on" timestamp_with_timezone_text NOT NULL, "base_path" text NOT NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_id") REFERENCES "functions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_patch_id") REFERENCES "function_patches" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("execution_id") REFERENCES "executions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("physical_function_run_id") REFERENCES "physical_function_runs" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "table_partitions" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "table_id" text NOT NULL, "table_version_id" text NOT NULL, "function_version_id" text NOT NULL, "table_data_version_id" text NOT NULL, "partition_key" text NULL, "partition_deleted" boolean NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_id") REFERENCES "functions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("table_data_version_id") REFERENCES "physical_table_data_versions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "workers" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "execution_id" text NULL, "transaction_id" text NULL, "function_version_id" text NOT NULL, "physical_function_run_id" text NULL, "message_status" text NOT NULL, "started_on" timestamp_with_timezone_text NULL, "ended_on" timestamp_with_timezone_text NULL, "status" text NOT NULL, "worker_type" text NOT NULL, "worker_enabled" boolean NOT NULL, "worker_config" text NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("execution_id") REFERENCES "executions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("transaction_id") REFERENCES "transactions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("function_version_id") REFERENCES "functions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("physical_function_run_id") REFERENCES "physical_function_runs" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "logical_function_runs" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "execution_id" text NOT NULL, "transaction_id" text NOT NULL, "function_id" text NOT NULL, "function_version_id" text NOT NULL DEFAULT '', "trigger" text NOT NULL, "triggered_on" timestamp_with_timezone_text NOT NULL, "triggered_by_id" text NOT NULL, "status" text NOT NULL, "group_id" text NOT NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("project_id", "group_id") REFERENCES "groups" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("execution_id") REFERENCES "executions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("transaction_id") REFERENCES "transactions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "logical_function_run_pointers" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "logical_function_run_id" text NOT NULL, "status" text NOT NULL, "physical_function_run_id" text NULL, "set_on" timestamp_with_timezone_text NOT NULL, "set_by_id" text NOT NULL, "reason" text NOT NULL, FOREIGN KEY ("logical_function_run_id") REFERENCES "logical_function_runs" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "logical_function_run_pointers_current" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "logical_function_run_id" text NOT NULL UNIQUE, "status" text NOT NULL, "physical_function_run_id" text NULL, "set_on" timestamp_with_timezone_text NOT NULL, "set_by_id" text NOT NULL, "reason" text NOT NULL )

CREATE TABLE "logical_table_data_versions" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "execution_id" text NOT NULL, "transaction_id" text NOT NULL, "table_id" text NOT NULL, "producer_logical_function_run_id" text NULL, "function_param_pos" integer NULL, "private" boolean NOT NULL, "triggered_on" timestamp_with_timezone_text NOT NULL, "triggered_by_id" text NOT NULL, "group_id" text NOT NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("project_id", "group_id") REFERENCES "groups" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("execution_id") REFERENCES "executions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("transaction_id") REFERENCES "transactions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("producer_logical_function_run_id") REFERENCES "logical_function_runs" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "logical_table_data_version_pointers" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "logical_table_data_version_id" text NOT NULL, "status" text NOT NULL, "physical_table_data_version_id" text NULL, "set_on" timestamp_with_timezone_text NOT NULL, "set_by_id" text NOT NULL, "reason" text NOT NULL, FOREIGN KEY ("logical_table_data_version_id") REFERENCES "logical_table_data_versions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TABLE "logical_table_data_version_pointers_current" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "logical_table_data_version_id" text NOT NULL UNIQUE, "status" text NOT NULL, "physical_table_data_version_id" text NULL, "set_on" timestamp_with_timezone_text NOT NULL, "set_by_id" text NOT NULL, "reason" text NOT NULL )

CREATE TABLE "logical_function_run_inputs" ( "id" text NOT NULL PRIMARY KEY, "project_id" text NOT NULL, "collection_id" text NOT NULL, "execution_id" text NOT NULL, "transaction_id" text NOT NULL, "logical_function_run_id" text NOT NULL, "requirement_logical_table_data_version_id" text NULL, "requirement_input_idx" integer NULL, "requirement_dependency_pos" integer NULL, "requirement_version_pos" integer NOT NULL, FOREIGN KEY ("collection_id") REFERENCES "collections" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("execution_id") REFERENCES "executions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("transaction_id") REFERENCES "transactions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("logical_function_run_id") REFERENCES "logical_function_runs" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT, FOREIGN KEY ("requirement_logical_table_data_version_id") REFERENCES "logical_table_data_versions" ("id") ON DELETE RESTRICT ON UPDATE RESTRICT )

CREATE TRIGGER functions__maintain_current AFTER INSERT ON functions
FOR EACH ROW BEGIN
  DELETE FROM functions_current WHERE function_id = NEW.function_id;
  INSERT INTO functions_current (id, project_id, collection_id, name, description, decorator, runtime_values, function_id, base_path, stage_id, stage_base_path, storage_version, bundle_id, snippet, defined_on, defined_by_id, status, connector, connection, enabled) VALUES (NEW.id, NEW.project_id, NEW.collection_id, NEW.name, NEW.description, NEW.decorator, NEW.runtime_values, NEW.function_id, NEW.base_path, NEW.stage_id, NEW.stage_base_path, NEW.storage_version, NEW.bundle_id, NEW.snippet, NEW.defined_on, NEW.defined_by_id, NEW.status, NEW.connector, NEW.connection, NEW.enabled);
END

CREATE TRIGGER function_patches__maintain_current AFTER INSERT ON function_patches
FOR EACH ROW WHEN NOT EXISTS (
  SELECT 1 FROM function_patches_current
  WHERE parent_function_version_id = NEW.parent_function_version_id
    AND (defined_on > NEW.defined_on
         OR (defined_on = NEW.defined_on AND id >= NEW.id))
)
BEGIN
  DELETE FROM function_patches_current WHERE parent_function_version_id = NEW.parent_function_version_id;
  INSERT INTO function_patches_current (id, project_id, parent_function_version_id, function_id, collection_id, snippet, bundle_id, runtime_values, description, defined_on, defined_by_id, status) VALUES (NEW.id, NEW.project_id, NEW.parent_function_version_id, NEW.function_id, NEW.collection_id, NEW.snippet, NEW.bundle_id, NEW.runtime_values, NEW.description, NEW.defined_on, NEW.defined_by_id, NEW.status);
END

CREATE TRIGGER function_patches__immutable_no_update BEFORE UPDATE ON function_patches
FOR EACH ROW BEGIN
  SELECT RAISE(ABORT, 'function_patches is insert-only');
END

CREATE TRIGGER function_patches__immutable_no_delete BEFORE DELETE ON function_patches
FOR EACH ROW BEGIN
  SELECT RAISE(ABORT, 'function_patches is insert-only');
END

CREATE TRIGGER tables__maintain_current AFTER INSERT ON tables
FOR EACH ROW BEGIN
  DELETE FROM tables_current WHERE table_id = NEW.table_id;
  INSERT INTO tables_current (id, project_id, collection_id, table_id, name, function_id, function_version_id, function_param_pos, private, partitioned, defined_on, defined_by_id, status, table_type) VALUES (NEW.id, NEW.project_id, NEW.collection_id, NEW.table_id, NEW.name, NEW.function_id, NEW.function_version_id, NEW.function_param_pos, NEW.private, NEW.partitioned, NEW.defined_on, NEW.defined_by_id, NEW.status, NEW.table_type);
END

CREATE TRIGGER dependencies__maintain_current AFTER INSERT ON dependencies
FOR EACH ROW BEGIN
  DELETE FROM dependencies_current WHERE dependency_id = NEW.dependency_id;
  INSERT INTO dependencies_current (id, project_id, collection_id, dependency_id, function_id, table_collection_id, table_function_id, table_id, table_versions, dep_pos, status, defined_on, defined_by_id, system) VALUES (NEW.id, NEW.project_id, NEW.collection_id, NEW.dependency_id, NEW.function_id, NEW.table_collection_id, NEW.table_function_id, NEW.table_id, NEW.table_versions, NEW.dep_pos, NEW.status, NEW.defined_on, NEW.defined_by_id, NEW.system);
END

CREATE TRIGGER triggers__maintain_current AFTER INSERT ON triggers
FOR EACH ROW BEGIN
  DELETE FROM triggers_current WHERE trigger_id = NEW.trigger_id;
  INSERT INTO triggers_current (id, project_id, collection_id, trigger_id, function_id, trigger_by_collection_id, trigger_by_function_id, trigger_by_table_id, status, defined_on, defined_by_id, system) VALUES (NEW.id, NEW.project_id, NEW.collection_id, NEW.trigger_id, NEW.function_id, NEW.trigger_by_collection_id, NEW.trigger_by_function_id, NEW.trigger_by_table_id, NEW.status, NEW.defined_on, NEW.defined_by_id, NEW.system);
END

CREATE TRIGGER logical_function_runs__sticky_terminals BEFORE UPDATE OF status ON logical_function_runs
FOR EACH ROW WHEN OLD.status IN ('C', 'X', 'Y') AND NEW.status != OLD.status
BEGIN
  SELECT RAISE(ABORT, 'logical_function_runs.status is sticky in terminal state');
END

CREATE TRIGGER logical_function_run_pointers__maintain_current AFTER INSERT ON logical_function_run_pointers
FOR EACH ROW WHEN NOT EXISTS (
  SELECT 1 FROM logical_function_run_pointers_current
  WHERE logical_function_run_id = NEW.logical_function_run_id
    AND (set_on > NEW.set_on
         OR (set_on = NEW.set_on AND id >= NEW.id))
)
BEGIN
  DELETE FROM logical_function_run_pointers_current WHERE logical_function_run_id = NEW.logical_function_run_id;
  INSERT INTO logical_function_run_pointers_current (id, project_id, logical_function_run_id, status, physical_function_run_id, set_on, set_by_id, reason) VALUES (NEW.id, NEW.project_id, NEW.logical_function_run_id, NEW.status, NEW.physical_function_run_id, NEW.set_on, NEW.set_by_id, NEW.reason);
END

CREATE TRIGGER logical_function_run_pointers__immutable_no_update BEFORE UPDATE ON logical_function_run_pointers
FOR EACH ROW BEGIN
  SELECT RAISE(ABORT, 'logical_function_run_pointers is insert-only');
END

CREATE TRIGGER logical_function_run_pointers__immutable_no_delete BEFORE DELETE ON logical_function_run_pointers
FOR EACH ROW BEGIN
  SELECT RAISE(ABORT, 'logical_function_run_pointers is insert-only');
END

CREATE TRIGGER logical_table_data_version_pointers__maintain_current AFTER INSERT ON logical_table_data_version_pointers
FOR EACH ROW WHEN NOT EXISTS (
  SELECT 1 FROM logical_table_data_version_pointers_current
  WHERE logical_table_data_version_id = NEW.logical_table_data_version_id
    AND (set_on > NEW.set_on
         OR (set_on = NEW.set_on AND id >= NEW.id))
)
BEGIN
  DELETE FROM logical_table_data_version_pointers_current WHERE logical_table_data_version_id = NEW.logical_table_data_version_id;
  INSERT INTO logical_table_data_version_pointers_current (id, project_id, logical_table_data_version_id, status, physical_table_data_version_id, set_on, set_by_id, reason) VALUES (NEW.id, NEW.project_id, NEW.logical_table_data_version_id, NEW.status, NEW.physical_table_data_version_id, NEW.set_on, NEW.set_by_id, NEW.reason);
END

CREATE TRIGGER logical_table_data_version_pointers__immutable_no_update BEFORE UPDATE ON logical_table_data_version_pointers
FOR EACH ROW BEGIN
  SELECT RAISE(ABORT, 'logical_table_data_version_pointers is insert-only');
END

CREATE TRIGGER logical_table_data_version_pointers__immutable_no_delete BEFORE DELETE ON logical_table_data_version_pointers
FOR EACH ROW BEGIN
  SELECT RAISE(ABORT, 'logical_table_data_version_pointers is insert-only');
END

CREATE VIEW users__with_names AS SELECT "users".*, COALESCE(created_by_user.name, '[' || users.created_by_id || ']') AS "created_by", COALESCE(modified_by_user.name, '[' || users.modified_by_id || ']') AS "modified_by" FROM "users" LEFT JOIN "users" AS "created_by_user" ON "users"."created_by_id" = "created_by_user"."id" LEFT JOIN "users" AS "modified_by_user" ON "users"."modified_by_id" = "modified_by_user"."id"

CREATE VIEW groups__with_names AS SELECT "groups"."id", "groups"."project_id", "groups"."name", "groups"."description", "groups"."created_on", "groups"."created_by_id", COALESCE(created_by_user.name, '[' || groups.created_by_id || ']') AS "created_by", "groups"."modified_on", "groups"."modified_by_id", COALESCE(modified_by_user.name, '[' || groups.modified_by_id || ']') AS "modified_by", "groups"."system" FROM "groups" LEFT JOIN "users" AS "created_by_user" ON "groups"."created_by_id" = "created_by_user"."id" LEFT JOIN "users" AS "modified_by_user" ON "groups"."modified_by_id" = "modified_by_user"."id"

CREATE VIEW projects_active AS SELECT "id", "name", "description", "base_path", "created_on", "created_by_id", "modified_on", "modified_by_id" FROM "projects" WHERE "name_when_deleted" IS NULL

CREATE VIEW projects__with_names AS SELECT "projects_active"."id", "projects_active"."name", "projects_active"."description", "projects_active"."base_path", "projects_active"."created_on", "projects_active"."created_by_id", COALESCE(created_by_user.name, '[' || projects_active.created_by_id || ']') AS "created_by", "projects_active"."modified_on", "projects_active"."modified_by_id", COALESCE(modified_by_user.name, '[' || projects_active.modified_by_id || ']') AS "modified_by" FROM "projects_active" LEFT JOIN "users" AS "created_by_user" ON "projects_active"."created_by_id" = "created_by_user"."id" LEFT JOIN "users" AS "modified_by_user" ON "projects_active"."modified_by_id" = "modified_by_user"."id"

CREATE VIEW collections_active AS SELECT "collections"."id", "collections"."project_id", "collections"."name", "collections"."description", "collections"."created_on", "collections"."created_by_id", "collections"."modified_on", "collections"."modified_by_id", "collections"."group_id", "collections"."base_path", "collections"."collection_type", "collections"."connection_id", COALESCE("collections"."base_path", "projects_active"."base_path") AS "effective_base_path" FROM "collections" LEFT JOIN "projects_active" ON "collections"."project_id" = "projects_active"."id" WHERE "collections"."name_when_deleted" IS NULL

CREATE VIEW collections__with_names AS SELECT "collections_active".*, COALESCE(created_by_user.name, '[' || collections_active.created_by_id || ']') AS "created_by", COALESCE(modified_by_user.name, '[' || collections_active.modified_by_id || ']') AS "modified_by", "groups_for_collection"."name" AS "group" FROM "collections_active" LEFT JOIN "users" AS "created_by_user" ON "collections_active"."created_by_id" = "created_by_user"."id" LEFT JOIN "users" AS "modified_by_user" ON "collections_active"."modified_by_id" = "modified_by_user"."id" LEFT JOIN "groups" AS "groups_for_collection" ON "collections_active"."group_id" = "groups_for_collection"."id" AND "collections_active"."project_id" = "groups_for_collection"."project_id"

CREATE VIEW roles__with_names AS SELECT "roles"."id", "roles"."name", "roles"."description", "roles"."created_on", "roles"."created_by_id", COALESCE(created_by_user.name, '[' || roles.created_by_id || ']') AS "created_by", "roles"."modified_on", "roles"."modified_by_id", COALESCE(modified_by_user.name, '[' || roles.modified_by_id || ']') AS "modified_by", "roles"."kind", "roles"."project_id", "roles"."fixed" FROM "roles" LEFT JOIN "users" AS "created_by_user" ON "roles"."created_by_id" = "created_by_user"."id" LEFT JOIN "users" AS "modified_by_user" ON "roles"."modified_by_id" = "modified_by_user"."id"

CREATE VIEW users_roles__with_names AS SELECT "users_roles"."id", "users_roles"."user_id", COALESCE(users.name, '[' || users_roles.user_id || ']') AS "user", "users_roles"."role_id", COALESCE(roles.name, '[' || users_roles.role_id || ']') AS "role", "users_roles"."added_on", "users_roles"."added_by_id", COALESCE(added_by_user.name, '[' || users_roles.added_by_id || ']') AS "added_by", "users_roles"."fixed" FROM "users_roles" LEFT JOIN "users" ON "users_roles"."user_id" = "users"."id" LEFT JOIN "roles" ON "users_roles"."role_id" = "roles"."id" LEFT JOIN "users" AS "added_by_user" ON "users_roles"."added_by_id" = "added_by_user"."id"

CREATE VIEW sessions__with_names AS SELECT "sessions".*, "users"."name" AS "user_name" FROM "sessions" JOIN "users" ON "sessions"."user_id" = "users"."id"

CREATE VIEW permissions__with_names AS SELECT "permissions"."id", "permissions"."role_id", "permissions"."permission_type", "permissions"."entity_type", "permissions"."granted_by_id", "permissions"."granted_on", "permissions"."fixed", COALESCE(users.name, '[' || permissions.granted_by_id || ']') AS "granted_by", (CASE WHEN ("permissions"."entity_id" = '00000000000000000000000204') THEN NULL ELSE "permissions"."entity_id" END) AS "entity_id", "roles"."name" AS "role", "projects"."name" AS "entity" FROM "permissions" LEFT JOIN "users" ON "permissions"."granted_by_id" = "users"."id" LEFT JOIN "roles" ON "permissions"."role_id" = "roles"."id" LEFT JOIN "projects" ON "permissions"."entity_id" = "projects"."id"

CREATE VIEW functions__with_names AS SELECT "functions".*, "collections"."name" AS "collection", "collections"."group_id" AS "group_id", COALESCE(users.name, '[' || functions.defined_by_id || ']') AS "defined_by" FROM "functions" LEFT JOIN "collections" ON "functions"."collection_id" = "collections"."id" LEFT JOIN "users" ON "functions"."defined_by_id" = "users"."id"

CREATE VIEW functions_current__with_names AS SELECT "functions_current".*, "collections"."name" AS "collection", "collections"."group_id" AS "group_id", COALESCE(users.name, '[' || functions_current.defined_by_id || ']') AS "defined_by" FROM "functions_current" LEFT JOIN "collections" ON "functions_current"."collection_id" = "collections"."id" LEFT JOIN "users" ON "functions_current"."defined_by_id" = "users"."id"

CREATE VIEW tables__with_names AS SELECT "tables".*, "tables"."function_param_pos" < 0 AS "system", COALESCE(users.name, '[' || tables.defined_by_id || ']') AS "defined_by", "collections"."name" AS "collection", "functions"."name" AS "function" FROM "tables" LEFT JOIN "collections" ON "tables"."collection_id" = "collections"."id" LEFT JOIN "functions" ON "tables"."function_version_id" = "functions"."id" LEFT JOIN "users" ON "tables"."defined_by_id" = "users"."id"

CREATE VIEW tables_current__with_names AS SELECT "tables_current".*, "tables_current"."function_param_pos" < 0 AS "system", COALESCE(users.name, '[' || tables_current.defined_by_id || ']') AS "defined_by", "collections"."name" AS "collection", "functions"."name" AS "function" FROM "tables_current" LEFT JOIN "collections" ON "tables_current"."collection_id" = "collections"."id" LEFT JOIN "functions" ON "tables_current"."function_version_id" = "functions"."id" LEFT JOIN "users" ON "tables_current"."defined_by_id" = "users"."id"

CREATE VIEW dependencies__with_names AS SELECT "dependencies".*, "collections"."name" AS "collection", "table_collection"."name" AS "table_collection" FROM "dependencies" LEFT JOIN "collections" ON "dependencies"."collection_id" = "collections"."id" LEFT JOIN "users" ON "dependencies"."defined_by_id" = "users"."id" LEFT JOIN "collections" AS "table_collection" ON "dependencies"."table_collection_id" = "table_collection"."id"

CREATE VIEW dependencies_current__with_names AS SELECT "dependencies_current".*, "collections"."name" AS "collection", "table_collection"."name" AS "table_collection" FROM "dependencies_current" LEFT JOIN "collections" ON "dependencies_current"."collection_id" = "collections"."id" LEFT JOIN "users" ON "dependencies_current"."defined_by_id" = "users"."id" LEFT JOIN "collections" AS "table_collection" ON "dependencies_current"."table_collection_id" = "table_collection"."id"

CREATE VIEW triggers__with_names AS SELECT "triggers".*, "collections"."name" AS "collection", "trigger_by_collection"."name" AS "trigger_by_collection" FROM "triggers" LEFT JOIN "collections" ON "triggers"."collection_id" = "collections"."id" LEFT JOIN "users" ON "triggers"."defined_by_id" = "users"."id" LEFT JOIN "collections" AS "trigger_by_collection" ON "triggers"."trigger_by_collection_id" = "trigger_by_collection"."id"

CREATE VIEW triggers__read AS SELECT * FROM "triggers__with_names" WHERE "triggers__with_names"."system" = FALSE

CREATE VIEW triggers_current__with_names AS SELECT "triggers_current".*, "collections"."name" AS "collection", "trigger_by_collection"."name" AS "trigger_by_collection" FROM "triggers_current" LEFT JOIN "collections" ON "triggers_current"."collection_id" = "collections"."id" LEFT JOIN "users" ON "triggers_current"."defined_by_id" = "users"."id" LEFT JOIN "collections" AS "trigger_by_collection" ON "triggers_current"."trigger_by_collection_id" = "trigger_by_collection"."id"

CREATE VIEW executions__with_names AS SELECT "executions".*, "collections"."name" AS "collection", "functions"."name" AS "function", COALESCE(users.name, '[' || executions.triggered_by_id || ']') AS "triggered_by", "projects"."name" AS "project" FROM "executions" LEFT JOIN "collections" ON "executions"."collection_id" = "collections"."id" LEFT JOIN "functions" ON "executions"."function_version_id" = "functions"."id" LEFT JOIN "users" ON "executions"."triggered_by_id" = "users"."id" LEFT JOIN "projects" ON "executions"."project_id" = "projects"."id"

CREATE VIEW transactions__with_names AS SELECT "transactions".*, "collections"."name" AS "collection", "executions"."name" AS "execution", COALESCE(users.name, '[' || transactions.triggered_by_id || ']') AS "triggered_by", "groups_for_transactions"."name" AS "group", "projects"."name" AS "project" FROM "transactions" LEFT JOIN "collections" ON "transactions"."collection_id" = "collections"."id" LEFT JOIN "executions" ON "transactions"."execution_id" = "executions"."id" LEFT JOIN "users" ON "transactions"."triggered_by_id" = "users"."id" LEFT JOIN "projects" ON "transactions"."project_id" = "projects"."id" LEFT JOIN "groups" AS "groups_for_transactions" ON "transactions"."group_id" = "groups_for_transactions"."id" AND "transactions"."project_id" = "groups_for_transactions"."project_id"

CREATE VIEW physical_function_runs__with_names AS SELECT "physical_function_runs".*, "functions"."name" AS "name", "collections"."name" AS "collection", "executions"."name" AS "execution", "executions"."function_version_id" AS "execution_function_version_id", COALESCE(users.name, '[' || physical_function_runs.triggered_by_id || ']') AS "triggered_by" FROM "physical_function_runs" LEFT JOIN "collections" ON "physical_function_runs"."collection_id" = "collections"."id" LEFT JOIN "functions" ON "physical_function_runs"."function_version_id" = "functions"."id" LEFT JOIN "executions" ON "physical_function_runs"."execution_id" = "executions"."id" LEFT JOIN "users" ON "physical_function_runs"."triggered_by_id" = "users"."id"

CREATE VIEW physical_table_data_versions__with_function AS SELECT "physical_table_data_versions".*, "physical_function_runs"."triggered_by_id" AS "triggered_by_id", "physical_function_runs"."started_on" AS "started_on", "physical_function_runs"."ended_on" AS "ended_on", "physical_function_runs"."status" AS "status", "functions"."storage_version" AS "storage_version" FROM "physical_table_data_versions" LEFT JOIN "functions" ON "physical_table_data_versions"."function_version_id" = "functions"."id" LEFT JOIN "physical_function_runs" ON "physical_table_data_versions"."physical_function_run_id" = "physical_function_runs"."id"

CREATE VIEW physical_table_data_versions__with_names AS SELECT "physical_table_data_versions__with_function".*, "collections"."name" AS "collection", "functions"."name" AS "function", COALESCE(users.name, '[' || physical_table_data_versions__with_function.triggered_by_id || ']') AS "created_by" FROM "physical_table_data_versions__with_function" LEFT JOIN "collections" ON "physical_table_data_versions__with_function"."collection_id" = "collections"."id" LEFT JOIN "functions" ON "physical_table_data_versions__with_function"."function_version_id" = "functions"."id" LEFT JOIN "users" ON "physical_table_data_versions__with_function"."triggered_by_id" = "users"."id"

CREATE VIEW workers__with_names AS SELECT "workers".*, "logical_function_runs"."id" AS "logical_function_run_id", "collections"."name" AS "collection", "collections"."connection_id" AS "connection_id", "executions"."name" AS "execution", "functions"."name" AS "function", "functions"."enabled" AS "enabled", "projects"."name" AS "project", "physical_function_runs"."function_version_patch_id" AS "function_version_patch_id" FROM "workers" LEFT JOIN "collections" ON "workers"."collection_id" = "collections"."id" LEFT JOIN "executions" ON "workers"."execution_id" = "executions"."id" LEFT JOIN "functions" ON "workers"."function_version_id" = "functions"."id" LEFT JOIN "logical_function_runs" ON "workers"."execution_id" = "logical_function_runs"."execution_id" AND "functions"."function_id" = "logical_function_runs"."function_id" LEFT JOIN "physical_function_runs" ON "workers"."physical_function_run_id" = "physical_function_runs"."id" LEFT JOIN "projects" ON "workers"."project_id" = "projects"."id" WHERE "workers"."worker_enabled" = TRUE

CREATE VIEW function_runs__with_names AS SELECT "logical_function_runs".*, "functions"."name" AS "name", "collections"."name" AS "collection", "executions"."name" AS "execution", COALESCE(users.name, '[' || logical_function_runs.triggered_by_id || ']') AS "triggered_by", "groups_for_lfr"."name" AS "group", "projects"."name" AS "project", "physical_function_runs"."id" AS "physical_function_run_id", "physical_function_runs"."started_on" AS "started_on", "physical_function_runs"."ended_on" AS "ended_on", "physical_function_runs"."status" AS "physical_status", "physical_function_runs"."function_version_patch_id" AS "function_version_patch_id", "physical_function_runs"."ended_on_hour_ms" AS "ended_on_hour_ms", "physical_function_runs"."ended_on_day_ms" AS "ended_on_day_ms", "physical_function_runs"."ended_on_week_ms" AS "ended_on_week_ms", "physical_function_runs"."ended_on_month_ms" AS "ended_on_month_ms", "physical_function_runs"."metric_table_versions" AS "metric_table_versions", "physical_function_runs"."metric_rows" AS "metric_rows", "physical_function_runs"."metric_data_bytes" AS "metric_data_bytes" FROM "logical_function_runs" LEFT JOIN "collections" ON "logical_function_runs"."collection_id" = "collections"."id" LEFT JOIN "groups" AS "groups_for_lfr" ON "logical_function_runs"."group_id" = "groups_for_lfr"."id" AND "logical_function_runs"."project_id" = "groups_for_lfr"."project_id" LEFT JOIN "projects" ON "logical_function_runs"."project_id" = "projects"."id" LEFT JOIN "functions" ON "logical_function_runs"."function_version_id" = "functions"."id" LEFT JOIN "executions" ON "logical_function_runs"."execution_id" = "executions"."id" LEFT JOIN "users" ON "logical_function_runs"."triggered_by_id" = "users"."id" LEFT JOIN "logical_function_run_pointers_current" ON "logical_function_run_pointers_current"."logical_function_run_id" = "logical_function_runs"."id" AND "logical_function_run_pointers_current"."status" = 'B' LEFT JOIN "physical_function_runs" ON "physical_function_runs"."id" = "logical_function_run_pointers_current"."physical_function_run_id"

CREATE VIEW global_status_summary AS SELECT (CASE WHEN (COUNT((CASE WHEN ("logical_function_runs"."status" IN ('S')) THEN 1 END)) = COUNT("logical_function_runs"."status")) THEN 'S' WHEN (COUNT((CASE WHEN ("logical_function_runs"."status" IN ('C', 'X', 'Y')) THEN 1 END)) = COUNT("logical_function_runs"."status")) THEN 'F' WHEN (COUNT((CASE WHEN ("logical_function_runs"."status" IN ('C', 'D', 'F', 'H')) THEN 1 END)) = COUNT("logical_function_runs"."status") AND COUNT((CASE WHEN ("logical_function_runs"."status" IN ('F', 'H')) THEN 1 END)) > 0) THEN 'L' WHEN (COUNT((CASE WHEN ("logical_function_runs"."status" IN ('S', 'RR', 'RS', 'R', 'D', 'E')) THEN 1 END)) > 0) THEN 'R' ELSE 'U' END) AS "status", COUNT(*) = COUNT((CASE WHEN ("logical_function_runs"."status" IN ('C', 'X', 'Y')) THEN 1 END)) AS "finished", (SELECT COALESCE(json_group_object(inner_fr.status, inner_fr.count), '{}') FROM (SELECT "logical_function_runs"."status", COUNT(*) AS "count" FROM "logical_function_runs" GROUP BY "logical_function_runs"."status") AS "inner_fr") AS "function_run_status_count" FROM "logical_function_runs"

CREATE VIEW execution_status_summary AS SELECT "executions"."id" AS "execution_id", (CASE WHEN (COUNT((CASE WHEN ("logical_function_runs"."status" IN ('S')) THEN 1 END)) = COUNT("logical_function_runs"."status")) THEN 'S' WHEN (COUNT((CASE WHEN ("logical_function_runs"."status" IN ('C', 'X', 'Y')) THEN 1 END)) = COUNT("logical_function_runs"."status")) THEN 'F' WHEN (COUNT((CASE WHEN ("logical_function_runs"."status" IN ('C', 'D', 'F', 'H')) THEN 1 END)) = COUNT("logical_function_runs"."status") AND COUNT((CASE WHEN ("logical_function_runs"."status" IN ('F', 'H')) THEN 1 END)) > 0) THEN 'L' WHEN (COUNT((CASE WHEN ("logical_function_runs"."status" IN ('S', 'RR', 'RS', 'R', 'D', 'E')) THEN 1 END)) > 0) THEN 'R' ELSE 'U' END) AS "status", COUNT(*) = COUNT((CASE WHEN ("logical_function_runs"."status" IN ('C', 'X', 'Y')) THEN 1 END)) AS "finished", (SELECT COALESCE(json_group_object(inner_fr.status, inner_fr.count), '{}') FROM (SELECT "logical_function_runs"."status", COUNT(*) AS "count" FROM "logical_function_runs" WHERE "logical_function_runs"."execution_id" = "executions"."id" GROUP BY "logical_function_runs"."status") AS "inner_fr") AS "function_run_status_count" FROM "executions" LEFT JOIN "logical_function_runs" ON "logical_function_runs"."execution_id" = "executions"."id" GROUP BY "executions"."id"

CREATE VIEW transaction_status_summary AS SELECT "transactions"."id" AS "transaction_id", (CASE WHEN (COUNT((CASE WHEN ("logical_function_runs"."status" IN ('S')) THEN 1 END)) = COUNT("logical_function_runs"."status")) THEN 'S' WHEN (COUNT((CASE WHEN ("logical_function_runs"."status" IN ('C')) THEN 1 END)) = COUNT("logical_function_runs"."status")) THEN 'C' WHEN (COUNT((CASE WHEN ("logical_function_runs"."status" IN ('X')) THEN 1 END)) = COUNT("logical_function_runs"."status")) THEN 'X' WHEN (COUNT((CASE WHEN ("logical_function_runs"."status" IN ('Y')) THEN 1 END)) = COUNT("logical_function_runs"."status")) THEN 'Y' WHEN (COUNT((CASE WHEN ("logical_function_runs"."status" IN ('D', 'F', 'H')) THEN 1 END)) = COUNT("logical_function_runs"."status") AND COUNT((CASE WHEN ("logical_function_runs"."status" IN ('F', 'H')) THEN 1 END)) > 0) THEN 'L' WHEN (COUNT((CASE WHEN ("logical_function_runs"."status" IN ('S', 'RR', 'RS', 'R', 'D', 'E')) THEN 1 END)) > 0) THEN 'R' ELSE 'U' END) AS "status", COUNT(*) = COUNT((CASE WHEN ("logical_function_runs"."status" IN ('C', 'X', 'Y')) THEN 1 END)) AS "finished", (SELECT COALESCE(json_group_object(inner_fr.status, inner_fr.count), '{}') FROM (SELECT "logical_function_runs"."status", COUNT(*) AS "count" FROM "logical_function_runs" WHERE "logical_function_runs"."transaction_id" = "transactions"."id" GROUP BY "logical_function_runs"."status") AS "inner_fr") AS "function_run_status_count" FROM "transactions" LEFT JOIN "logical_function_runs" ON "logical_function_runs"."transaction_id" = "transactions"."id" GROUP BY "transactions"."id"

CREATE VIEW table_data_versions__with_names AS SELECT "logical_table_data_versions".*, "collections"."name" AS "collection", "executions"."name" AS "execution", COALESCE(users.name, '[' || logical_table_data_versions.triggered_by_id || ']') AS "triggered_by", "groups_for_ltdv"."name" AS "group", "physical_table_data_versions"."id" AS "physical_table_data_version_id", "physical_table_data_versions"."name" AS "name", "physical_table_data_versions"."table_version_id" AS "table_version_id", "physical_table_data_versions"."function_version_id" AS "function_version_id", "physical_table_data_versions"."function_version_patch_id" AS "function_version_patch_id", "physical_table_data_versions"."has_data" AS "has_data", "physical_table_data_versions"."schema_changed" AS "schema_changed", "physical_table_data_versions"."column_count" AS "column_count", "physical_table_data_versions"."row_count" AS "row_count", "physical_table_data_versions"."data_bytes" AS "data_bytes", "physical_table_data_versions"."schema_hash" AS "schema_hash", "physical_table_data_versions"."with_data_table_data_version_id" AS "with_data_table_data_version_id", "physical_table_data_versions"."physical_function_run_id" AS "physical_function_run_id", "physical_function_runs"."started_on" AS "started_on", "physical_function_runs"."ended_on" AS "ended_on", "logical_function_runs"."status" AS "status", "physical_function_runs"."status" AS "physical_status", "functions"."name" AS "function", "functions"."storage_version" AS "storage_version" FROM "logical_table_data_versions" LEFT JOIN "collections" ON "logical_table_data_versions"."collection_id" = "collections"."id" LEFT JOIN "executions" ON "logical_table_data_versions"."execution_id" = "executions"."id" LEFT JOIN "users" ON "logical_table_data_versions"."triggered_by_id" = "users"."id" LEFT JOIN "logical_table_data_version_pointers_current" ON "logical_table_data_version_pointers_current"."logical_table_data_version_id" = "logical_table_data_versions"."id" AND "logical_table_data_version_pointers_current"."status" = 'B' LEFT JOIN "physical_table_data_versions" ON "physical_table_data_versions"."id" = "logical_table_data_version_pointers_current"."physical_table_data_version_id" LEFT JOIN "physical_function_runs" ON "physical_function_runs"."id" = "physical_table_data_versions"."physical_function_run_id" LEFT JOIN "logical_function_runs" ON "logical_table_data_versions"."producer_logical_function_run_id" = "logical_function_runs"."id" LEFT JOIN "functions" ON "physical_table_data_versions"."function_version_id" = "functions"."id" LEFT JOIN "groups" AS "groups_for_ltdv" ON "logical_table_data_versions"."group_id" = "groups_for_ltdv"."id" AND "logical_table_data_versions"."project_id" = "groups_for_ltdv"."project_id"

CREATE VIEW logical_table_data_versions__active AS SELECT "logical_table_data_versions".* FROM "logical_table_data_versions" LEFT JOIN "logical_function_runs" ON "logical_table_data_versions"."producer_logical_function_run_id" = "logical_function_runs"."id" WHERE "logical_function_runs"."status" IS NULL OR "logical_function_runs"."status" NOT IN ('Y', 'X')

CREATE VIEW executions__with_status AS SELECT "executions__with_names".*, "pfr_agg"."started_on" AS "started_on", (CASE WHEN ("execution_status_summary"."finished") THEN "pfr_agg"."ended_on" END) AS "ended_on", "execution_status_summary"."status" AS "status", "execution_status_summary"."function_run_status_count" AS "function_run_status_count" FROM "executions__with_names" LEFT JOIN (SELECT "physical_function_runs"."execution_id" AS "execution_id", MIN("physical_function_runs"."started_on") AS "started_on", MAX("physical_function_runs"."ended_on") AS "ended_on" FROM "physical_function_runs" GROUP BY "physical_function_runs"."execution_id") AS "pfr_agg" ON "pfr_agg"."execution_id" = "executions__with_names"."id" LEFT JOIN "execution_status_summary" AS "execution_status_summary" ON "execution_status_summary"."execution_id" = "executions__with_names"."id"

CREATE VIEW transactions__with_status AS SELECT "transactions__with_names".*, "pfr_agg"."started_on" AS "started_on", (CASE WHEN ("transaction_status_summary"."finished") THEN "pfr_agg"."ended_on" END) AS "ended_on", "transaction_status_summary"."status" AS "status", "transaction_status_summary"."function_run_status_count" AS "function_run_status_count" FROM "transactions__with_names" LEFT JOIN (SELECT "physical_function_runs"."transaction_id" AS "transaction_id", MIN("physical_function_runs"."started_on") AS "started_on", MAX("physical_function_runs"."ended_on") AS "ended_on" FROM "physical_function_runs" GROUP BY "physical_function_runs"."transaction_id") AS "pfr_agg" ON "pfr_agg"."transaction_id" = "transactions__with_names"."id" LEFT JOIN "transaction_status_summary" AS "transaction_status_summary" ON "transaction_status_summary"."transaction_id" = "transactions__with_names"."id"

CREATE INDEX "users___enabled___idx" ON "users" ("enabled")

CREATE INDEX "users___system___idx" ON "users" ("system")

CREATE UNIQUE INDEX "groups___project_id__name___idx" ON "groups" ("project_id", "name")

CREATE UNIQUE INDEX "collections___project_id__name___idx" ON "collections" ("project_id", "name")

CREATE INDEX "collections___group_id___idx" ON "collections" ("group_id")

CREATE INDEX "roles___project_id___idx" ON "roles" ("project_id")

CREATE UNIQUE INDEX "roles___user_id__role_id___idx" ON "users_roles" ("user_id", "role_id")

CREATE INDEX "sessions___expires_on___idx" ON "sessions" ("expires_on")

CREATE INDEX "sessions___user_id___idx" ON "sessions" ("user_id")

CREATE UNIQUE INDEX "permissions___role_id__permission_type__entity___idx" ON "permissions" ("role_id", "permission_type", "entity_type", "entity_id")

CREATE INDEX "permissions___entity_id___idx" ON "permissions" ("entity_id")

CREATE INDEX "bundles___collection_id___idx" ON "bundles" ("collection_id")

CREATE INDEX "functions___collection_id___idx" ON "functions" ("collection_id")

CREATE INDEX "functions___function_id__defined_on__id___idx" ON "functions" ("function_id", "defined_on", "id")

CREATE INDEX "functions_current___collection_id___idx" ON "functions_current" ("collection_id")

CREATE INDEX "functions_current___status___idx" ON "functions_current" ("status")

CREATE INDEX "function_patches___function_id__defined_on___idx" ON "function_patches" ("function_id", "defined_on")

CREATE INDEX "function_patches___parent_function_version_id__defined_on__id___idx" ON "function_patches" ("parent_function_version_id", "defined_on", "id")

CREATE INDEX "tables___function_id__status___idx" ON "tables" ("function_id", "status")

CREATE INDEX "tables___status___idx" ON "tables" ("status")

CREATE INDEX "tables___table_id__defined_on__id___idx" ON "tables" ("table_id", "defined_on", "id")

CREATE INDEX "tables_current___collection_id___idx" ON "tables_current" ("collection_id")

CREATE INDEX "tables_current___function_id___idx" ON "tables_current" ("function_id")

CREATE INDEX "tables_current___status___idx" ON "tables_current" ("status")

CREATE INDEX "dependencies___dependency_id__defined_on__id___idx" ON "dependencies" ("dependency_id", "defined_on", "id")

CREATE INDEX "dependencies___function_id___idx" ON "dependencies" ("function_id")

CREATE INDEX "dependencies___table_function_id___idx" ON "dependencies" ("table_function_id")

CREATE INDEX "dependencies_current___function_id___idx" ON "dependencies_current" ("function_id")

CREATE INDEX "dependencies_current___table_function_id___idx" ON "dependencies_current" ("table_function_id")

CREATE INDEX "triggers___collection_id___idx" ON "triggers" ("collection_id")

CREATE INDEX "triggers___function_id___idx" ON "triggers" ("function_id")

CREATE INDEX "triggers___trigger_id__defined_on__id___idx" ON "triggers" ("trigger_id", "defined_on", "id")

CREATE INDEX "triggers___trigger_by_function_id___idx" ON "triggers" ("trigger_by_function_id")

CREATE INDEX "triggers_current___collection_id___idx" ON "triggers_current" ("collection_id")

CREATE INDEX "triggers_current___function_id___idx" ON "triggers_current" ("function_id")

CREATE INDEX "triggers_current___trigger_by_function_id___idx" ON "triggers_current" ("trigger_by_function_id")

CREATE INDEX "executions___collection_id___idx" ON "executions" ("collection_id")

CREATE INDEX "executions___function_version_id___idx" ON "executions" ("function_version_id")

CREATE INDEX "executions___triggered_by_id___idx" ON "executions" ("triggered_by_id")

CREATE INDEX "executions___triggered_on___idx" ON "executions" ("triggered_on")

CREATE UNIQUE INDEX "executions___function_id__external_uid___idx" ON "executions" ("function_id", "external_uid")

CREATE INDEX "transactions___collection_id___idx" ON "transactions" ("collection_id")

CREATE INDEX "transactions___execution_id___idx" ON "transactions" ("execution_id")

CREATE INDEX "transactions___group_id__collection_id___idx" ON "transactions" ("group_id", "collection_id")

CREATE INDEX "function_runs___collection_id___idx" ON "physical_function_runs" ("collection_id")

CREATE INDEX "function_runs___function_version_id___idx" ON "physical_function_runs" ("function_version_id")

CREATE INDEX "function_runs___execution_id__status___idx" ON "physical_function_runs" ("execution_id", "status")

CREATE INDEX "function_runs___transaction_id__status___idx" ON "physical_function_runs" ("transaction_id", "status")

CREATE INDEX "function_runs___status___idx" ON "physical_function_runs" ("status")

CREATE INDEX "function_runs___ended_on___idx" ON "physical_function_runs" ("ended_on")

CREATE INDEX "physical_table_data_versions___collection_id___idx" ON "physical_table_data_versions" ("collection_id")

CREATE INDEX "physical_table_data_versions___execution_id___idx" ON "physical_table_data_versions" ("execution_id")

CREATE INDEX "physical_table_data_versions___physical_function_run_id___idx" ON "physical_table_data_versions" ("physical_function_run_id")

CREATE INDEX "physical_table_data_versions___table_id___has_data___idx" ON "physical_table_data_versions" ("table_id", "has_data")

CREATE INDEX "physical_table_data_versions___table_id__triggered_on__id___idx" ON "physical_table_data_versions" ("table_id", "triggered_on", "id")

CREATE INDEX "workers___physical_function_run_id___idx" ON "workers" ("physical_function_run_id")

CREATE UNIQUE INDEX "logical_function_runs___execution_id__function_id___idx" ON "logical_function_runs" ("execution_id", "function_id")

CREATE INDEX "logical_function_runs___function_id__triggered_on__id___idx" ON "logical_function_runs" ("function_id", "triggered_on", "id")

CREATE INDEX "logical_function_runs___execution_id__status___idx" ON "logical_function_runs" ("execution_id", "status")

CREATE INDEX "logical_function_runs___transaction_id__status___idx" ON "logical_function_runs" ("transaction_id", "status")

CREATE INDEX "logical_function_runs___status___idx" ON "logical_function_runs" ("status")

CREATE INDEX "logical_function_runs___group_id__collection_id___idx" ON "logical_function_runs" ("group_id", "collection_id")

CREATE INDEX "logical_function_run_pointers___logical_function_run_id__set_on__id___idx" ON "logical_function_run_pointers" ("logical_function_run_id", "set_on", "id")

CREATE INDEX "logical_function_run_pointers___function_run_id___idx" ON "logical_function_run_pointers" ("physical_function_run_id")

CREATE INDEX "lfr_pointers_current___physical_function_run_id___idx" ON "logical_function_run_pointers_current" ("physical_function_run_id")

CREATE INDEX "logical_table_data_versions___table_id__triggered_on__id___idx" ON "logical_table_data_versions" ("table_id", "triggered_on", "id")

CREATE INDEX "logical_table_data_versions___execution_id___idx" ON "logical_table_data_versions" ("execution_id")

CREATE INDEX "logical_table_data_versions___transaction_id___idx" ON "logical_table_data_versions" ("transaction_id")

CREATE INDEX "logical_table_data_versions___producer_logical_function_run_id___idx" ON "logical_table_data_versions" ("producer_logical_function_run_id")

CREATE INDEX "logical_table_data_versions___group_id___idx" ON "logical_table_data_versions" ("group_id")

CREATE INDEX "logical_table_data_version_pointers___logical_table_data_version_id__set_on__id___idx" ON "logical_table_data_version_pointers" ("logical_table_data_version_id", "set_on", "id")

CREATE INDEX "logical_table_data_version_pointers___table_data_version_id___idx" ON "logical_table_data_version_pointers" ("physical_table_data_version_id")

CREATE INDEX "logical_function_run_inputs___logical_function_run_id___idx" ON "logical_function_run_inputs" ("logical_function_run_id")

CREATE INDEX "logical_function_run_inputs___execution_id___idx" ON "logical_function_run_inputs" ("execution_id")

CREATE INDEX "logical_function_run_inputs___requirement_logical_table_data_version_id___idx" ON "logical_function_run_inputs" ("requirement_logical_table_data_version_id")

INSERT INTO "tabsdata_system" ("name", "value") VALUES ('tabsdata_version', '2')

INSERT INTO "users" ("id", "name", "full_name", "email", "created_on", "created_by_id", "modified_on", "modified_by_id", "password_hash", "password_set_on", "password_must_change", "enabled", "system") VALUES ('00000000000000000000000000', 'system', 'System', NULL, CURRENT_TIMESTAMP, '00000000000000000000000000', CURRENT_TIMESTAMP, '00000000000000000000000000', '!', CURRENT_TIMESTAMP, FALSE, FALSE, TRUE)

INSERT INTO "users" ("id", "name", "full_name", "email", "created_on", "created_by_id", "modified_on", "modified_by_id", "password_hash", "password_set_on", "password_must_change", "enabled", "system") VALUES ('00000000000000000000000004', 'admin', 'Administrator', '-', CURRENT_TIMESTAMP, '00000000000000000000000000', CURRENT_TIMESTAMP, '00000000000000000000000000', '$argon2id$v=19$m=19456,t=2,p=1$ULTV//eju00aMbzZXAORyg$4+7GgsoT3e7lHElgWA2v0OYQHr5SBQzYY3pcUy0/YDM', CURRENT_TIMESTAMP, FALSE, TRUE, FALSE)

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

INSERT INTO "functions_current" ("id", "project_id", "collection_id", "name", "description", "decorator", "runtime_values", "function_id", "base_path", "stage_id", "stage_base_path", "storage_version", "bundle_id", "snippet", "defined_on", "defined_by_id", "status", "connector", "connection", "enabled") SELECT "id", "project_id", "collection_id", "name", "description", "decorator", "runtime_values", "function_id", "base_path", "stage_id", "stage_base_path", "storage_version", "bundle_id", "snippet", "defined_on", "defined_by_id", "status", "connector", "connection", "enabled" FROM (SELECT "id", "project_id", "collection_id", "name", "description", "decorator", "runtime_values", "function_id", "base_path", "stage_id", "stage_base_path", "storage_version", "bundle_id", "snippet", "defined_on", "defined_by_id", "status", "connector", "connection", "enabled", row_number() OVER ( PARTITION BY "function_id" ORDER BY "defined_on" DESC, "id" DESC ) AS "rn" FROM "functions") AS "ranked" WHERE "rn" = 1

INSERT INTO "tables_current" ("id", "project_id", "collection_id", "table_id", "name", "function_id", "function_version_id", "function_param_pos", "private", "partitioned", "defined_on", "defined_by_id", "status", "table_type") SELECT "id", "project_id", "collection_id", "table_id", "name", "function_id", "function_version_id", "function_param_pos", "private", "partitioned", "defined_on", "defined_by_id", "status", "table_type" FROM (SELECT "id", "project_id", "collection_id", "table_id", "name", "function_id", "function_version_id", "function_param_pos", "private", "partitioned", "defined_on", "defined_by_id", "status", "table_type", row_number() OVER ( PARTITION BY "table_id" ORDER BY "defined_on" DESC, "id" DESC ) AS "rn" FROM "tables") AS "ranked" WHERE "rn" = 1

INSERT INTO "dependencies_current" ("id", "project_id", "collection_id", "dependency_id", "function_id", "table_collection_id", "table_function_id", "table_id", "table_versions", "dep_pos", "status", "defined_on", "defined_by_id", "system") SELECT "id", "project_id", "collection_id", "dependency_id", "function_id", "table_collection_id", "table_function_id", "table_id", "table_versions", "dep_pos", "status", "defined_on", "defined_by_id", "system" FROM (SELECT "id", "project_id", "collection_id", "dependency_id", "function_id", "table_collection_id", "table_function_id", "table_id", "table_versions", "dep_pos", "status", "defined_on", "defined_by_id", "system", row_number() OVER ( PARTITION BY "dependency_id" ORDER BY "defined_on" DESC, "id" DESC ) AS "rn" FROM "dependencies") AS "ranked" WHERE "rn" = 1

INSERT INTO "triggers_current" ("id", "project_id", "collection_id", "trigger_id", "function_id", "trigger_by_collection_id", "trigger_by_function_id", "trigger_by_table_id", "status", "defined_on", "defined_by_id", "system") SELECT "id", "project_id", "collection_id", "trigger_id", "function_id", "trigger_by_collection_id", "trigger_by_function_id", "trigger_by_table_id", "status", "defined_on", "defined_by_id", "system" FROM (SELECT "id", "project_id", "collection_id", "trigger_id", "function_id", "trigger_by_collection_id", "trigger_by_function_id", "trigger_by_table_id", "status", "defined_on", "defined_by_id", "system", row_number() OVER ( PARTITION BY "trigger_id" ORDER BY "defined_on" DESC, "id" DESC ) AS "rn" FROM "triggers") AS "ranked" WHERE "rn" = 1

INSERT INTO "logical_function_run_pointers_current" ("id", "project_id", "logical_function_run_id", "status", "physical_function_run_id", "set_on", "set_by_id", "reason") SELECT "id", "project_id", "logical_function_run_id", "status", "physical_function_run_id", "set_on", "set_by_id", "reason" FROM (SELECT "id", "project_id", "logical_function_run_id", "status", "physical_function_run_id", "set_on", "set_by_id", "reason", row_number() OVER ( PARTITION BY "logical_function_run_id" ORDER BY "set_on" DESC, "id" DESC ) AS "rn" FROM "logical_function_run_pointers") AS "ranked" WHERE "rn" = 1

INSERT INTO "logical_table_data_version_pointers_current" ("id", "project_id", "logical_table_data_version_id", "status", "physical_table_data_version_id", "set_on", "set_by_id", "reason") SELECT "id", "project_id", "logical_table_data_version_id", "status", "physical_table_data_version_id", "set_on", "set_by_id", "reason" FROM (SELECT "id", "project_id", "logical_table_data_version_id", "status", "physical_table_data_version_id", "set_on", "set_by_id", "reason", row_number() OVER ( PARTITION BY "logical_table_data_version_id" ORDER BY "set_on" DESC, "id" DESC ) AS "rn" FROM "logical_table_data_version_pointers") AS "ranked" WHERE "rn" = 1
