--
-- Copyright 2025 Tabs Data Inc.
--

DROP VIEW workers__with_names;
DROP TABLE workers;

DROP VIEW function_requirements__with_names;
DROP VIEW function_requirements__with_status;
DROP TABLE function_requirements;

DROP TABLE table_partitions;

DROP VIEW table_data_versions__with_names;
DROP VIEW table_data_versions__active;
DROP VIEW table_data_versions__with_function;
DROP TABLE table_data_versions;

DROP VIEW transaction_status_summary;
DROP VIEW execution_status_summary;
DROP VIEW global_status_summary;

DROP VIEW function_runs__to_commit;
DROP VIEW function_runs__to_execute;
DROP VIEW function_runs__with_names;
DROP TABLE function_runs;

DROP VIEW transactions__with_status;
DROP VIEW transactions__with_names;
DROP TABLE transactions;

DROP VIEW executions__with_status;
DROP VIEW executions__with_names;
DROP TABLE executions;

DROP VIEW triggers__read;
DROP VIEW triggers__with_names;
DROP TABLE triggers;

DROP VIEW dependencies__read;
DROP VIEW dependencies__with_names;
DROP TABLE dependencies;

DROP TABLE bundles;

DROP VIEW tables__read;
DROP VIEW tables__with_names;
DROP TABLE tables;

DROP VIEW functions__with_names;
DROP TABLE functions;
