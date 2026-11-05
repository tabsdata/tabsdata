-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

ALTER TABLE "connections" ADD CONSTRAINT "connections___project_id__id___key" UNIQUE ("project_id", "id")

ALTER TABLE "connection_catalogs" ADD CONSTRAINT "connection_catalogs___project_id__connection_id___key" UNIQUE ("project_id", "connection_id")

ALTER TABLE "functions" ADD CONSTRAINT "functions___project_id__id___key" UNIQUE ("project_id", "id")

ALTER TABLE "function_patches" ADD CONSTRAINT "function_patches___project_id__id___key" UNIQUE ("project_id", "id")

ALTER TABLE "project_topologies" ADD CONSTRAINT "project_topologies___project_id__id___key" UNIQUE ("project_id", "id")

ALTER TABLE "executions" ADD CONSTRAINT "executions___project_id__id___key" UNIQUE ("project_id", "id")

ALTER TABLE "transactions" ADD CONSTRAINT "transactions___project_id__id___key" UNIQUE ("project_id", "id")

ALTER TABLE "logical_function_runs" ADD CONSTRAINT "logical_function_runs___project_id__id___key" UNIQUE ("project_id", "id")

ALTER TABLE "logical_table_data_versions" ADD CONSTRAINT "logical_table_data_versions___project_id__id___key" UNIQUE ("project_id", "id")

ALTER TABLE "physical_function_runs" ADD CONSTRAINT "pfr___project_id__id___key" UNIQUE ("project_id", "id")

ALTER TABLE "physical_table_data_versions" ADD CONSTRAINT "ptdv___project_id__id___key" UNIQUE ("project_id", "id")

ALTER TABLE "collections" DROP CONSTRAINT "collections_connection_id_fkey"

ALTER TABLE "collections" ADD CONSTRAINT "collections___connection_id___fkey" FOREIGN KEY ("project_id", "connection_id") REFERENCES "connections" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "connection_catalogs" DROP CONSTRAINT "connection_catalogs_connection_id_fkey"

ALTER TABLE "connection_catalogs" ADD CONSTRAINT "connection_catalogs___connection_id___fkey" FOREIGN KEY ("project_id", "connection_id") REFERENCES "connections" ("project_id", "id") ON DELETE CASCADE ON UPDATE RESTRICT

ALTER TABLE "connection_entities" DROP CONSTRAINT "connection_entities_connection_id_fkey"

ALTER TABLE "connection_entities" ADD CONSTRAINT "connection_entities___connection_id___fkey" FOREIGN KEY ("project_id", "connection_id") REFERENCES "connection_catalogs" ("project_id", "connection_id") ON DELETE CASCADE ON UPDATE RESTRICT

ALTER TABLE "connection_probes" DROP CONSTRAINT "connection_probes_connection_id_fkey"

ALTER TABLE "connection_probes" ADD CONSTRAINT "connection_probes___connection_id___fkey" FOREIGN KEY ("project_id", "connection_id") REFERENCES "connections" ("project_id", "id") ON DELETE CASCADE ON UPDATE RESTRICT

ALTER TABLE "executions" DROP CONSTRAINT "executions_function_version_id_fkey"

ALTER TABLE "executions" ADD CONSTRAINT "executions___function_version_id___fkey" FOREIGN KEY ("project_id", "function_version_id") REFERENCES "functions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "executions" DROP CONSTRAINT "executions_topology_version_id_fkey"

ALTER TABLE "executions" ADD CONSTRAINT "executions___topology_version_id___fkey" FOREIGN KEY ("project_id", "topology_version_id") REFERENCES "project_topologies" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "function_patches" DROP CONSTRAINT "function_patches_parent_function_version_id_fkey"

ALTER TABLE "function_patches" ADD CONSTRAINT "function_patches___parent_function_version_id___fkey" FOREIGN KEY ("project_id", "parent_function_version_id") REFERENCES "functions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "logical_function_run_inputs" DROP CONSTRAINT "logical_function_run_inputs_execution_id_fkey"

ALTER TABLE "logical_function_run_inputs" ADD CONSTRAINT "lfri___execution_id___fkey" FOREIGN KEY ("project_id", "execution_id") REFERENCES "executions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "logical_function_run_inputs" DROP CONSTRAINT "logical_function_run_inputs_logical_function_run_id_fkey"

ALTER TABLE "logical_function_run_inputs" ADD CONSTRAINT "lfri___logical_function_run_id___fkey" FOREIGN KEY ("project_id", "logical_function_run_id") REFERENCES "logical_function_runs" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "logical_function_run_inputs" DROP CONSTRAINT "logical_function_run_inputs_requirement_logical_table_data_fkey"

ALTER TABLE "logical_function_run_inputs" ADD CONSTRAINT "lfri___requirement_logical_table_data_version_id___fkey" FOREIGN KEY ("project_id", "requirement_logical_table_data_version_id") REFERENCES "logical_table_data_versions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "logical_function_run_inputs" DROP CONSTRAINT "logical_function_run_inputs_transaction_id_fkey"

ALTER TABLE "logical_function_run_inputs" ADD CONSTRAINT "lfri___transaction_id___fkey" FOREIGN KEY ("project_id", "transaction_id") REFERENCES "transactions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "logical_function_runs" DROP CONSTRAINT "logical_function_runs_execution_id_fkey"

ALTER TABLE "logical_function_runs" ADD CONSTRAINT "logical_function_runs___execution_id___fkey" FOREIGN KEY ("project_id", "execution_id") REFERENCES "executions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "logical_function_runs" DROP CONSTRAINT "logical_function_runs_transaction_id_fkey"

ALTER TABLE "logical_function_runs" ADD CONSTRAINT "logical_function_runs___transaction_id___fkey" FOREIGN KEY ("project_id", "transaction_id") REFERENCES "transactions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "logical_table_data_versions" DROP CONSTRAINT "logical_table_data_versions_execution_id_fkey"

ALTER TABLE "logical_table_data_versions" ADD CONSTRAINT "ltdv___execution_id___fkey" FOREIGN KEY ("project_id", "execution_id") REFERENCES "executions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "logical_table_data_versions" DROP CONSTRAINT "logical_table_data_versions_producer_logical_function_run__fkey"

ALTER TABLE "logical_table_data_versions" ADD CONSTRAINT "ltdv___producer_logical_function_run_id___fkey" FOREIGN KEY ("project_id", "producer_logical_function_run_id") REFERENCES "logical_function_runs" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "logical_table_data_versions" DROP CONSTRAINT "logical_table_data_versions_transaction_id_fkey"

ALTER TABLE "logical_table_data_versions" ADD CONSTRAINT "ltdv___transaction_id___fkey" FOREIGN KEY ("project_id", "transaction_id") REFERENCES "transactions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "physical_function_runs" DROP CONSTRAINT "physical_function_runs_execution_id_fkey"

ALTER TABLE "physical_function_runs" ADD CONSTRAINT "pfr___execution_id___fkey" FOREIGN KEY ("project_id", "execution_id") REFERENCES "executions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "physical_function_runs" DROP CONSTRAINT "physical_function_runs_function_version_id_fkey"

ALTER TABLE "physical_function_runs" ADD CONSTRAINT "pfr___function_version_id___fkey" FOREIGN KEY ("project_id", "function_version_id") REFERENCES "functions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "physical_function_runs" DROP CONSTRAINT "physical_function_runs_function_version_patch_id_fkey"

ALTER TABLE "physical_function_runs" ADD CONSTRAINT "pfr___function_version_patch_id___fkey" FOREIGN KEY ("project_id", "function_version_patch_id") REFERENCES "function_patches" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "physical_function_runs" DROP CONSTRAINT "physical_function_runs_logical_function_run_id_fkey"

ALTER TABLE "physical_function_runs" ADD CONSTRAINT "pfr___logical_function_run_id___fkey" FOREIGN KEY ("project_id", "logical_function_run_id") REFERENCES "logical_function_runs" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "physical_table_data_versions" DROP CONSTRAINT "physical_table_data_versions_execution_id_fkey"

ALTER TABLE "physical_table_data_versions" ADD CONSTRAINT "ptdv___execution_id___fkey" FOREIGN KEY ("project_id", "execution_id") REFERENCES "executions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "physical_table_data_versions" DROP CONSTRAINT "physical_table_data_versions_function_version_id_fkey"

ALTER TABLE "physical_table_data_versions" ADD CONSTRAINT "ptdv___function_version_id___fkey" FOREIGN KEY ("project_id", "function_version_id") REFERENCES "functions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "physical_table_data_versions" DROP CONSTRAINT "physical_table_data_versions_function_version_patch_id_fkey"

ALTER TABLE "physical_table_data_versions" ADD CONSTRAINT "ptdv___function_version_patch_id___fkey" FOREIGN KEY ("project_id", "function_version_patch_id") REFERENCES "function_patches" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "physical_table_data_versions" DROP CONSTRAINT "physical_table_data_versions_logical_table_data_version_id_fkey"

ALTER TABLE "physical_table_data_versions" ADD CONSTRAINT "ptdv___logical_table_data_version_id___fkey" FOREIGN KEY ("project_id", "logical_table_data_version_id") REFERENCES "logical_table_data_versions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "physical_table_data_versions" DROP CONSTRAINT "physical_table_data_versions_physical_function_run_id_fkey"

ALTER TABLE "physical_table_data_versions" ADD CONSTRAINT "ptdv___physical_function_run_id___fkey" FOREIGN KEY ("project_id", "physical_function_run_id") REFERENCES "physical_function_runs" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "table_partitions" DROP CONSTRAINT "table_partitions_function_version_id_fkey"

ALTER TABLE "table_partitions" ADD CONSTRAINT "table_partitions___function_version_id___fkey" FOREIGN KEY ("project_id", "function_version_id") REFERENCES "functions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "table_partitions" DROP CONSTRAINT "table_partitions_table_data_version_id_fkey"

ALTER TABLE "table_partitions" ADD CONSTRAINT "table_partitions___table_data_version_id___fkey" FOREIGN KEY ("project_id", "table_data_version_id") REFERENCES "physical_table_data_versions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "tables" DROP CONSTRAINT "tables_function_version_id_fkey"

ALTER TABLE "tables" ADD CONSTRAINT "tables___function_version_id___fkey" FOREIGN KEY ("project_id", "function_version_id") REFERENCES "functions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "transactions" DROP CONSTRAINT "transactions_execution_id_fkey"

ALTER TABLE "transactions" ADD CONSTRAINT "transactions___execution_id___fkey" FOREIGN KEY ("project_id", "execution_id") REFERENCES "executions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "workers" DROP CONSTRAINT "workers_execution_id_fkey"

ALTER TABLE "workers" ADD CONSTRAINT "workers___execution_id___fkey" FOREIGN KEY ("project_id", "execution_id") REFERENCES "executions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "workers" DROP CONSTRAINT "workers_function_version_id_fkey"

ALTER TABLE "workers" ADD CONSTRAINT "workers___function_version_id___fkey" FOREIGN KEY ("project_id", "function_version_id") REFERENCES "functions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "workers" DROP CONSTRAINT "workers_physical_function_run_id_fkey"

ALTER TABLE "workers" ADD CONSTRAINT "workers___physical_function_run_id___fkey" FOREIGN KEY ("project_id", "physical_function_run_id") REFERENCES "physical_function_runs" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "workers" DROP CONSTRAINT "workers_transaction_id_fkey"

ALTER TABLE "workers" ADD CONSTRAINT "workers___transaction_id___fkey" FOREIGN KEY ("project_id", "transaction_id") REFERENCES "transactions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER TABLE "physical_function_runs" ADD CONSTRAINT "pfr___transaction_id___fkey" FOREIGN KEY ("project_id", "transaction_id") REFERENCES "transactions" ("project_id", "id") ON DELETE RESTRICT ON UPDATE RESTRICT

ALTER POLICY project_isolation ON groups USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON project_topologies USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON tags USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON connections USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON collections USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON connection_catalogs USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON connection_entities USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON connection_probes USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON bundles USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON functions USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON function_patches USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON tables USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON dependencies USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON triggers USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON executions USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON transactions USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON physical_function_runs USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON physical_table_data_versions USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON table_partitions USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON workers USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON logical_function_runs USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON logical_table_data_versions USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON logical_function_run_inputs USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

ALTER POLICY project_isolation ON metrics_facts USING (project_id = (SELECT current_setting('app.project_id', true))) WITH CHECK (project_id = (SELECT current_setting('app.project_id', true)))

UPDATE tabsdata_system SET value = '23' WHERE name = 'db_version'
