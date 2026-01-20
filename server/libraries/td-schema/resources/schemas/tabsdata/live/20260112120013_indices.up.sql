--
-- Copyright 2026 Tabs Data Inc.
--

-- Users
CREATE INDEX users___name___idx ON users (name);
CREATE INDEX users___enabled___idx ON users (enabled);

-- Collections
CREATE INDEX collections___name___idx ON collections (name);

-- Functions
CREATE INDEX functions___collection_id___idx ON functions (collection_id);

-- Tables
CREATE INDEX tables___function_id__status___idx ON tables (function_id, status);
CREATE INDEX tables___status___idx ON tables (status);

-- Executions
CREATE INDEX executions___collection_id___idx ON executions (collection_id);
CREATE INDEX executions___function_version_id___idx ON executions (function_version_id);
CREATE INDEX executions___triggered_by_id___idx ON executions (triggered_by_id);
CREATE INDEX executions___triggered_on___idx ON executions (triggered_on);

-- Function Runs
CREATE INDEX function_runs___collection_id___idx ON function_runs (collection_id);
CREATE INDEX function_runs___execution_id___idx ON function_runs (execution_id);
CREATE INDEX function_runs___transaction_id___idx ON function_runs (transaction_id);
CREATE INDEX function_runs___function_version_id___idx ON function_runs (function_version_id);
CREATE INDEX function_runs___status___idx ON function_runs (status);

-- Table Data Versions
CREATE INDEX table_data_versions___collection_id___idx ON table_data_versions (collection_id);
CREATE INDEX table_data_versions___execution_id___idx ON table_data_versions (execution_id);
CREATE INDEX table_data_versions___function_run_id___idx ON table_data_versions (function_run_id);
CREATE INDEX table_data_versions___table_id___idx ON table_data_versions (table_id);
CREATE INDEX table_data_versions___table_id___has_data___idx ON table_data_versions (table_id, has_data);

-- Transactions
CREATE INDEX transactions___collection_id___idx ON transactions (collection_id);
CREATE INDEX transactions___execution_id___idx ON transactions (execution_id);

-- Triggers
CREATE INDEX triggers___collection_id___idx ON triggers (collection_id);
CREATE INDEX triggers___function_id___idx ON triggers (function_id);

-- Bundles
CREATE INDEX bundles___collection_id___idx ON bundles (collection_id);
