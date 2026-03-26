--
-- Copyright 2026 Tabs Data Inc.
--

-- Users
DROP INDEX IF EXISTS users___name___idx;
DROP INDEX IF EXISTS users___enabled___idx;

-- Collections
DROP INDEX IF EXISTS collections___name___idx;

-- Functions
DROP INDEX IF EXISTS functions___collection_id___idx;

-- Tables
DROP INDEX IF EXISTS tables___function_id__status___idx;
DROP INDEX IF EXISTS tables___status___idx;

-- Executions
DROP INDEX IF EXISTS executions___collection_id___idx;
DROP INDEX IF EXISTS executions___function_version_id___idx;
DROP INDEX IF EXISTS executions___triggered_by_id___idx;
DROP INDEX IF EXISTS executions___triggered_on___idx;

-- Function Runs
DROP INDEX IF EXISTS function_runs___collection_id___idx;
DROP INDEX IF EXISTS function_runs___execution_id___idx;
DROP INDEX IF EXISTS function_runs___transaction_id___idx;
DROP INDEX IF EXISTS function_runs___function_version_id___idx;
DROP INDEX IF EXISTS function_runs___status___idx;

-- Table Data Versions
DROP INDEX IF EXISTS table_data_versions___collection_id___idx;
DROP INDEX IF EXISTS table_data_versions___execution_id___idx;
DROP INDEX IF EXISTS table_data_versions___function_run_id___idx;
DROP INDEX IF EXISTS table_data_versions___table_id___idx;
DROP INDEX IF EXISTS table_data_versions___table_id___has_data___idx;

-- Transactions
DROP INDEX IF EXISTS transactions___collection_id___idx;
DROP INDEX IF EXISTS transactions___execution_id___idx;

-- Triggers
DROP INDEX IF EXISTS triggers___collection_id___idx;
DROP INDEX IF EXISTS triggers___function_id___idx;

-- Bundles
DROP INDEX IF EXISTS bundles___collection_id___idx;
