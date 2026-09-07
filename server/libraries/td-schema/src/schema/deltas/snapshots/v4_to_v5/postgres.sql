-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

DROP POLICY project_isolation ON groups

DROP POLICY across_projects ON groups

DROP POLICY project_isolation ON connections

DROP POLICY across_projects ON connections

DROP POLICY project_isolation ON collections

DROP POLICY across_projects ON collections

DROP POLICY project_isolation ON bundles

DROP POLICY across_projects ON bundles

DROP POLICY project_isolation ON functions

DROP POLICY across_projects ON functions

DROP POLICY project_isolation ON function_patches

DROP POLICY across_projects ON function_patches

DROP POLICY project_isolation ON tables

DROP POLICY across_projects ON tables

DROP POLICY project_isolation ON dependencies

DROP POLICY across_projects ON dependencies

DROP POLICY project_isolation ON triggers

DROP POLICY across_projects ON triggers

DROP POLICY project_isolation ON executions

DROP POLICY across_projects ON executions

DROP POLICY project_isolation ON transactions

DROP POLICY across_projects ON transactions

DROP POLICY project_isolation ON physical_function_runs

DROP POLICY across_projects ON physical_function_runs

DROP POLICY project_isolation ON physical_table_data_versions

DROP POLICY across_projects ON physical_table_data_versions

DROP POLICY project_isolation ON table_partitions

DROP POLICY across_projects ON table_partitions

DROP POLICY project_isolation ON workers

DROP POLICY across_projects ON workers

DROP POLICY project_isolation ON logical_function_runs

DROP POLICY across_projects ON logical_function_runs

DROP POLICY project_isolation ON logical_table_data_versions

DROP POLICY across_projects ON logical_table_data_versions

DROP POLICY project_isolation ON logical_function_run_inputs

DROP POLICY across_projects ON logical_function_run_inputs

ALTER TABLE "users" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "created_by_id" TYPE text COLLATE "C", ALTER COLUMN "modified_by_id" TYPE text COLLATE "C"

ALTER TABLE "groups" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "created_by_id" TYPE text COLLATE "C", ALTER COLUMN "modified_by_id" TYPE text COLLATE "C"

ALTER TABLE "projects" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "created_by_id" TYPE text COLLATE "C", ALTER COLUMN "modified_by_id" TYPE text COLLATE "C"

ALTER TABLE "connections" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "created_by_id" TYPE text COLLATE "C", ALTER COLUMN "modified_by_id" TYPE text COLLATE "C"

ALTER TABLE "collections" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "created_by_id" TYPE text COLLATE "C", ALTER COLUMN "modified_by_id" TYPE text COLLATE "C", ALTER COLUMN "group_id" TYPE text COLLATE "C", ALTER COLUMN "connection_id" TYPE text COLLATE "C"

ALTER TABLE "roles" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "created_by_id" TYPE text COLLATE "C", ALTER COLUMN "modified_by_id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C"

ALTER TABLE "users_roles" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "user_id" TYPE text COLLATE "C", ALTER COLUMN "role_id" TYPE text COLLATE "C", ALTER COLUMN "added_by_id" TYPE text COLLATE "C"

ALTER TABLE "sessions" ALTER COLUMN "access_token_id" TYPE text COLLATE "C", ALTER COLUMN "refresh_token_id" TYPE text COLLATE "C", ALTER COLUMN "user_id" TYPE text COLLATE "C"

ALTER TABLE "permissions" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "role_id" TYPE text COLLATE "C", ALTER COLUMN "entity_id" TYPE text COLLATE "C", ALTER COLUMN "granted_by_id" TYPE text COLLATE "C"

ALTER TABLE "bundles" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "collection_id" TYPE text COLLATE "C", ALTER COLUMN "created_by_id" TYPE text COLLATE "C"

ALTER TABLE "functions" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "collection_id" TYPE text COLLATE "C", ALTER COLUMN "function_id" TYPE text COLLATE "C", ALTER COLUMN "stage_id" TYPE text COLLATE "C", ALTER COLUMN "bundle_id" TYPE text COLLATE "C", ALTER COLUMN "defined_by_id" TYPE text COLLATE "C"

ALTER TABLE "function_patches" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "parent_function_version_id" TYPE text COLLATE "C", ALTER COLUMN "function_id" TYPE text COLLATE "C", ALTER COLUMN "collection_id" TYPE text COLLATE "C", ALTER COLUMN "bundle_id" TYPE text COLLATE "C", ALTER COLUMN "defined_by_id" TYPE text COLLATE "C"

ALTER TABLE "tables" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "collection_id" TYPE text COLLATE "C", ALTER COLUMN "table_id" TYPE text COLLATE "C", ALTER COLUMN "function_id" TYPE text COLLATE "C", ALTER COLUMN "function_version_id" TYPE text COLLATE "C", ALTER COLUMN "defined_by_id" TYPE text COLLATE "C"

ALTER TABLE "dependencies" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "collection_id" TYPE text COLLATE "C", ALTER COLUMN "dependency_id" TYPE text COLLATE "C", ALTER COLUMN "function_id" TYPE text COLLATE "C", ALTER COLUMN "table_collection_id" TYPE text COLLATE "C", ALTER COLUMN "table_function_id" TYPE text COLLATE "C", ALTER COLUMN "table_id" TYPE text COLLATE "C", ALTER COLUMN "defined_by_id" TYPE text COLLATE "C"

ALTER TABLE "triggers" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "collection_id" TYPE text COLLATE "C", ALTER COLUMN "trigger_id" TYPE text COLLATE "C", ALTER COLUMN "function_id" TYPE text COLLATE "C", ALTER COLUMN "trigger_by_collection_id" TYPE text COLLATE "C", ALTER COLUMN "trigger_by_function_id" TYPE text COLLATE "C", ALTER COLUMN "trigger_by_table_id" TYPE text COLLATE "C", ALTER COLUMN "defined_by_id" TYPE text COLLATE "C"

ALTER TABLE "executions" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "collection_id" TYPE text COLLATE "C", ALTER COLUMN "function_version_id" TYPE text COLLATE "C", ALTER COLUMN "triggered_by_id" TYPE text COLLATE "C", ALTER COLUMN "function_id" TYPE text COLLATE "C"

ALTER TABLE "transactions" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "collection_id" TYPE text COLLATE "C", ALTER COLUMN "execution_id" TYPE text COLLATE "C", ALTER COLUMN "triggered_by_id" TYPE text COLLATE "C", ALTER COLUMN "group_id" TYPE text COLLATE "C"

ALTER TABLE "logical_function_runs" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "collection_id" TYPE text COLLATE "C", ALTER COLUMN "execution_id" TYPE text COLLATE "C", ALTER COLUMN "transaction_id" TYPE text COLLATE "C", ALTER COLUMN "function_id" TYPE text COLLATE "C", ALTER COLUMN "function_version_id" TYPE text COLLATE "C", ALTER COLUMN "triggered_by_id" TYPE text COLLATE "C", ALTER COLUMN "group_id" TYPE text COLLATE "C"

ALTER TABLE "logical_table_data_versions" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "collection_id" TYPE text COLLATE "C", ALTER COLUMN "execution_id" TYPE text COLLATE "C", ALTER COLUMN "transaction_id" TYPE text COLLATE "C", ALTER COLUMN "table_id" TYPE text COLLATE "C", ALTER COLUMN "producer_logical_function_run_id" TYPE text COLLATE "C", ALTER COLUMN "triggered_by_id" TYPE text COLLATE "C", ALTER COLUMN "group_id" TYPE text COLLATE "C"

ALTER TABLE "physical_function_runs" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "collection_id" TYPE text COLLATE "C", ALTER COLUMN "function_version_id" TYPE text COLLATE "C", ALTER COLUMN "function_version_patch_id" TYPE text COLLATE "C", ALTER COLUMN "execution_id" TYPE text COLLATE "C", ALTER COLUMN "transaction_id" TYPE text COLLATE "C", ALTER COLUMN "triggered_by_id" TYPE text COLLATE "C", ALTER COLUMN "function_id" TYPE text COLLATE "C", ALTER COLUMN "logical_function_run_id" TYPE text COLLATE "C"

ALTER TABLE "physical_table_data_versions" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "collection_id" TYPE text COLLATE "C", ALTER COLUMN "table_id" TYPE text COLLATE "C", ALTER COLUMN "table_version_id" TYPE text COLLATE "C", ALTER COLUMN "function_version_id" TYPE text COLLATE "C", ALTER COLUMN "function_version_patch_id" TYPE text COLLATE "C", ALTER COLUMN "execution_id" TYPE text COLLATE "C", ALTER COLUMN "transaction_id" TYPE text COLLATE "C", ALTER COLUMN "physical_function_run_id" TYPE text COLLATE "C", ALTER COLUMN "with_data_table_data_version_id" TYPE text COLLATE "C", ALTER COLUMN "logical_table_data_version_id" TYPE text COLLATE "C"

ALTER TABLE "table_partitions" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "collection_id" TYPE text COLLATE "C", ALTER COLUMN "table_id" TYPE text COLLATE "C", ALTER COLUMN "table_version_id" TYPE text COLLATE "C", ALTER COLUMN "function_version_id" TYPE text COLLATE "C", ALTER COLUMN "table_data_version_id" TYPE text COLLATE "C"

ALTER TABLE "workers" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "collection_id" TYPE text COLLATE "C", ALTER COLUMN "execution_id" TYPE text COLLATE "C", ALTER COLUMN "transaction_id" TYPE text COLLATE "C", ALTER COLUMN "function_version_id" TYPE text COLLATE "C", ALTER COLUMN "physical_function_run_id" TYPE text COLLATE "C"

ALTER TABLE "logical_function_run_inputs" ALTER COLUMN "id" TYPE text COLLATE "C", ALTER COLUMN "project_id" TYPE text COLLATE "C", ALTER COLUMN "collection_id" TYPE text COLLATE "C", ALTER COLUMN "execution_id" TYPE text COLLATE "C", ALTER COLUMN "transaction_id" TYPE text COLLATE "C", ALTER COLUMN "logical_function_run_id" TYPE text COLLATE "C", ALTER COLUMN "requirement_logical_table_data_version_id" TYPE text COLLATE "C"

CREATE POLICY project_isolation ON groups USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON groups TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON connections USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON connections TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON collections USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON collections TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON bundles USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON bundles TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON functions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON functions TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON function_patches USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON function_patches TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON tables USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON tables TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON dependencies USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON dependencies TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON triggers USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON triggers TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON executions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON executions TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON transactions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON transactions TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON physical_function_runs USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON physical_function_runs TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON physical_table_data_versions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON physical_table_data_versions TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON table_partitions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON table_partitions TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON workers USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON workers TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON logical_function_runs USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON logical_function_runs TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON logical_table_data_versions USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON logical_table_data_versions TO td_across_projects USING (true) WITH CHECK (true)

CREATE POLICY project_isolation ON logical_function_run_inputs USING (project_id = current_setting('app.project_id', true)) WITH CHECK (project_id = current_setting('app.project_id', true))

CREATE POLICY across_projects ON logical_function_run_inputs TO td_across_projects USING (true) WITH CHECK (true)

UPDATE tabsdata_system SET value = '5' WHERE name = 'db_version'
