-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

CREATE TEMP TABLE td_v8_foreign_keys AS SELECT cls.relname AS table_name, con.conname AS constraint_name, pg_get_constraintdef(con.oid) AS definition FROM pg_constraint con JOIN pg_class cls ON cls.oid = con.conrelid JOIN pg_class ref ON ref.oid = con.confrelid JOIN pg_namespace ns ON ns.oid = con.connamespace AND ns.nspname = current_schema() WHERE con.contype = 'f' AND (cls.relname IN ('executions', 'function_patches', 'functions', 'logical_function_run_inputs', 'logical_function_runs', 'logical_table_data_versions', 'physical_function_runs', 'physical_table_data_versions', 'table_partitions', 'tables', 'transactions', 'workers') OR ref.relname IN ('executions', 'function_patches', 'functions', 'logical_function_run_inputs', 'logical_function_runs', 'logical_table_data_versions', 'physical_function_runs', 'physical_table_data_versions', 'table_partitions', 'tables', 'transactions', 'workers'))

DO $$ DECLARE fk record; BEGIN FOR fk IN SELECT * FROM td_v8_foreign_keys LOOP EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', fk.table_name, fk.constraint_name); END LOOP; END $$

ALTER TABLE "logical_function_runs" ALTER COLUMN "function_version_id" DROP DEFAULT

ALTER TABLE "executions" ALTER COLUMN "id" TYPE bigint USING NULLIF(replace(regexp_replace("id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "function_version_id" TYPE bigint USING NULLIF(replace(regexp_replace("function_version_id", '^[^0-9]+', ''), '-', ''), '')::bigint

ALTER TABLE "function_patches" ALTER COLUMN "id" TYPE bigint USING NULLIF(replace(regexp_replace("id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "parent_function_version_id" TYPE bigint USING NULLIF(replace(regexp_replace("parent_function_version_id", '^[^0-9]+', ''), '-', ''), '')::bigint

ALTER TABLE "functions" ALTER COLUMN "id" TYPE bigint USING NULLIF(replace(regexp_replace("id", '^[^0-9]+', ''), '-', ''), '')::bigint

ALTER TABLE "logical_function_run_inputs" ALTER COLUMN "execution_id" TYPE bigint USING NULLIF(replace(regexp_replace("execution_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "transaction_id" TYPE bigint USING NULLIF(replace(regexp_replace("transaction_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "logical_function_run_id" TYPE bigint USING NULLIF(replace(regexp_replace("logical_function_run_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "requirement_logical_table_data_version_id" TYPE bigint USING NULLIF(replace(regexp_replace("requirement_logical_table_data_version_id", '^[^0-9]+', ''), '-', ''), '')::bigint

ALTER TABLE "logical_function_runs" ALTER COLUMN "id" TYPE bigint USING NULLIF(replace(regexp_replace("id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "execution_id" TYPE bigint USING NULLIF(replace(regexp_replace("execution_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "transaction_id" TYPE bigint USING NULLIF(replace(regexp_replace("transaction_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "function_version_id" TYPE bigint USING COALESCE(NULLIF(replace(regexp_replace("function_version_id", '^[^0-9]+', ''), '-', ''), '')::bigint, 197001010000000001)

ALTER TABLE "logical_table_data_versions" ALTER COLUMN "id" TYPE bigint USING NULLIF(replace(regexp_replace("id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "execution_id" TYPE bigint USING NULLIF(replace(regexp_replace("execution_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "transaction_id" TYPE bigint USING NULLIF(replace(regexp_replace("transaction_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "producer_logical_function_run_id" TYPE bigint USING NULLIF(replace(regexp_replace("producer_logical_function_run_id", '^[^0-9]+', ''), '-', ''), '')::bigint

ALTER TABLE "physical_function_runs" ALTER COLUMN "id" TYPE bigint USING NULLIF(replace(regexp_replace("id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "function_version_id" TYPE bigint USING NULLIF(replace(regexp_replace("function_version_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "function_version_patch_id" TYPE bigint USING NULLIF(replace(regexp_replace("function_version_patch_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "execution_id" TYPE bigint USING NULLIF(replace(regexp_replace("execution_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "transaction_id" TYPE bigint USING NULLIF(replace(regexp_replace("transaction_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "logical_function_run_id" TYPE bigint USING NULLIF(replace(regexp_replace("logical_function_run_id", '^[^0-9]+', ''), '-', ''), '')::bigint

ALTER TABLE "physical_table_data_versions" ALTER COLUMN "id" TYPE bigint USING NULLIF(replace(regexp_replace("id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "function_version_id" TYPE bigint USING NULLIF(replace(regexp_replace("function_version_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "function_version_patch_id" TYPE bigint USING NULLIF(replace(regexp_replace("function_version_patch_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "execution_id" TYPE bigint USING NULLIF(replace(regexp_replace("execution_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "transaction_id" TYPE bigint USING NULLIF(replace(regexp_replace("transaction_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "physical_function_run_id" TYPE bigint USING NULLIF(replace(regexp_replace("physical_function_run_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "logical_table_data_version_id" TYPE bigint USING NULLIF(replace(regexp_replace("logical_table_data_version_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "with_data_table_data_version_id" TYPE bigint USING NULLIF(replace(regexp_replace("with_data_table_data_version_id", '^[^0-9]+', ''), '-', ''), '')::bigint

ALTER TABLE "table_partitions" ALTER COLUMN "function_version_id" TYPE bigint USING NULLIF(replace(regexp_replace("function_version_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "table_data_version_id" TYPE bigint USING NULLIF(replace(regexp_replace("table_data_version_id", '^[^0-9]+', ''), '-', ''), '')::bigint

ALTER TABLE "tables" ALTER COLUMN "function_version_id" TYPE bigint USING NULLIF(replace(regexp_replace("function_version_id", '^[^0-9]+', ''), '-', ''), '')::bigint

ALTER TABLE "transactions" ALTER COLUMN "id" TYPE bigint USING NULLIF(replace(regexp_replace("id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "execution_id" TYPE bigint USING NULLIF(replace(regexp_replace("execution_id", '^[^0-9]+', ''), '-', ''), '')::bigint

ALTER TABLE "workers" ALTER COLUMN "id" TYPE bigint USING NULLIF(replace(regexp_replace("id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "execution_id" TYPE bigint USING NULLIF(replace(regexp_replace("execution_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "transaction_id" TYPE bigint USING NULLIF(replace(regexp_replace("transaction_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "function_version_id" TYPE bigint USING NULLIF(replace(regexp_replace("function_version_id", '^[^0-9]+', ''), '-', ''), '')::bigint, ALTER COLUMN "physical_function_run_id" TYPE bigint USING NULLIF(replace(regexp_replace("physical_function_run_id", '^[^0-9]+', ''), '-', ''), '')::bigint

ALTER TABLE "logical_function_runs" ALTER COLUMN "function_version_id" SET DEFAULT 197001010000000001

DO $$ DECLARE fk record; BEGIN FOR fk IN SELECT * FROM td_v8_foreign_keys LOOP EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %I %s', fk.table_name, fk.constraint_name, fk.definition); END LOOP; END $$

DROP TABLE td_v8_foreign_keys

UPDATE tabsdata_system SET value = '8' WHERE name = 'db_version'
